package main

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func testLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func TestProfileInterval(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		set     bool
		want    time.Duration
		enabled bool
		warns   bool
	}{
		{"unset is the default", "", false, 5 * time.Minute, true, false},
		{"empty is the default", "", true, 5 * time.Minute, true, false},
		{"a duration", "30s", true, 30 * time.Second, true, false},
		{"surrounding space is ignored", " 10m ", true, 10 * time.Minute, true, false},
		{"0 disables", "0", true, 0, false, false},
		{"0s disables", "0s", true, 0, false, false},
		{"malformed falls back and warns", "soon", true, 5 * time.Minute, true, true},
		{"negative falls back and warns", "-1m", true, 5 * time.Minute, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("NETWORK_PROFILE_INTERVAL", tc.env)
			} else {
				t.Setenv("NETWORK_PROFILE_INTERVAL", "")
			}
			logger, logs := testLogger()

			got, enabled := profileInterval(logger)

			if got != tc.want || enabled != tc.enabled {
				t.Errorf("profileInterval = (%v, %v), want (%v, %v)", got, enabled, tc.want, tc.enabled)
			}
			if warned := strings.Contains(logs.String(), "level=WARN"); warned != tc.warns {
				t.Errorf("warned = %v, want %v; logs:\n%s", warned, tc.warns, logs)
			}
		})
	}
}

// profileGateway is a gateway as the composition root sees one: the port, the
// sites it converges and the memory of what it declared.
type profileGateway struct {
	ports.NetworkGateway
	sites []string
}

func (g *profileGateway) Sites() []string { return g.sites }
func (g *profileGateway) DeclareNodeProfile(context.Context, contract.NodeProfile) (contract.ProfileResult, error) {
	return contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: 1}, nil
}
func (g *profileGateway) LastDeclared(context.Context, shared.SiteId) (contract.NodeProfile, bool, error) {
	return contract.NodeProfile{}, false, nil
}

type noSchedule struct{}

func (noSchedule) NextCutoff(shared.SiteId, time.Time) (contract.NextCutoff, bool) {
	return contract.NextCutoff{}, false
}
func (noSchedule) Schedule(shared.SiteId, time.Time) (contract.CutoffSchedule, bool) {
	return contract.CutoffSchedule{}, false
}

type noCapacity struct{}

func (noCapacity) RemainingCapacity(string, time.Time) (int, bool) { return 0, false }

var liveGatewayWithTwoSites = &profileGateway{sites: []string{"FC-1", "FC-2"}}

func TestWireNodeProfiles_StubModeNeverDeclares(t *testing.T) {
	logger, _ := testLogger()
	if w := wireNodeProfiles(logger, retailnetwork.ModeStub, liveGatewayWithTwoSites, noSchedule{}, noCapacity{}); w != nil {
		t.Fatal("a stub gateway has no network to declare to")
	}
}

func TestWireNodeProfiles_LiveWiresTheUseCaseForTheGatewaysOwnSitesWithItsMemory(t *testing.T) {
	t.Setenv("NETWORK_PROFILE_INTERVAL", "")
	logger, _ := testLogger()

	w := wireNodeProfiles(logger, retailnetwork.ModeLive, liveGatewayWithTwoSites, noSchedule{}, noCapacity{})

	if w == nil {
		t.Fatal("live mode with sites must wire the declaration")
	}
	if w.every != 5*time.Minute {
		t.Errorf("interval = %v, want the 5m default", w.every)
	}
	if want := []shared.SiteId{"FC-1", "FC-2"}; !slices.Equal(w.declare.Sites, want) {
		t.Errorf("sites = %v, want %v", w.declare.Sites, want)
	}
	if w.declare.History == nil || w.declare.Metrics == nil || w.declare.Gateway == nil || w.declare.Clock == nil {
		t.Errorf("use case = %+v, want gateway, history, metrics and clock all wired", w.declare)
	}
}

func TestWireNodeProfiles_ZeroDisablesItLoudly(t *testing.T) {
	t.Setenv("NETWORK_PROFILE_INTERVAL", "0")
	logger, logs := testLogger()

	if w := wireNodeProfiles(logger, retailnetwork.ModeLive, liveGatewayWithTwoSites, noSchedule{}, noCapacity{}); w != nil {
		t.Fatal("NETWORK_PROFILE_INTERVAL=0 must switch the worker off")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "NETWORK_PROFILE_INTERVAL=0") {
		t.Errorf("a disabled declaration must say so at WARN, got:\n%s", logs)
	}
}

func TestWireNodeProfiles_AGatewayWithNoSiteDeclaresNothingAndSaysSo(t *testing.T) {
	t.Setenv("NETWORK_PROFILE_INTERVAL", "")
	logger, logs := testLogger()

	if w := wireNodeProfiles(logger, retailnetwork.ModeLive, &profileGateway{}, noSchedule{}, noCapacity{}); w != nil {
		t.Fatal("no site, nothing to declare")
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("want a WARN, got:\n%s", logs)
	}
}

type countingMetrics struct{ n atomic.Int64 }

func (c *countingMetrics) Declared(context.Context, shared.SiteId, contract.ProfileResult) {
	c.n.Add(1)
}

func TestStartProfileWorker_NilWorkerIsAlreadyDone(t *testing.T) {
	logger, _ := testLogger()
	select {
	case <-startProfileWorker(context.Background(), logger, nil):
	case <-time.After(time.Second):
		t.Fatal("a nil worker must hand back a closed channel so shutdown never waits for it")
	}
}

func TestStartProfileWorker_RunsAtBootThenOnEveryTickAndStopsWithItsContext(t *testing.T) {
	logger, _ := testLogger()
	metrics := &countingMetrics{}
	w := &profileWorker{
		every: 10 * time.Millisecond,
		declare: &usecases.DeclareNodeProfiles{
			Gateway: &profileGateway{}, PathCap: noSchedule{}, Capacity: noCapacity{},
			Clock: fixedNow{}, Logger: logger, Metrics: metrics, Sites: []shared.SiteId{"FC-1"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := startProfileWorker(ctx, logger, w)

	deadline := time.After(5 * time.Second)
	for metrics.n.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d passes in 5s; want one at boot and one per tick", metrics.n.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop with its context")
	}
}

type fixedNow struct{}

func (fixedNow) Now() time.Time { return time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC) }
