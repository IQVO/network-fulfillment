package main

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestInventoryInterval(t *testing.T) {
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
				t.Setenv("NETWORK_INVENTORY_INTERVAL", tc.env)
			} else {
				t.Setenv("NETWORK_INVENTORY_INTERVAL", "")
			}
			logger, logs := testLogger()

			got, enabled := inventoryInterval(logger)

			if got != tc.want || enabled != tc.enabled {
				t.Errorf("inventoryInterval = (%v, %v), want (%v, %v)", got, enabled, tc.want, tc.enabled)
			}
			warned := strings.Contains(logs.String(), "level=WARN")
			if warned != tc.warns {
				t.Errorf("warned = %v, want %v; logs:\n%s", warned, tc.warns, logs)
			}
			if tc.warns && !strings.Contains(logs.String(), "NETWORK_INVENTORY_INTERVAL") {
				t.Errorf("the warning must name the variable, got:\n%s", logs)
			}
		})
	}
}

// feedGateway is a gateway as the composition root sees one when it feeds: the
// port, the sites it converges and a feed counter.
type feedGateway struct {
	ports.NetworkGateway
	sites   []string
	outcome contract.FeedOutcome
	after   func() // called once a feed was taken, when set
	feeds   atomic.Int64
	last    atomic.Pointer[contract.InventoryFeed]
}

func (g *feedGateway) Sites() []string { return g.sites }
func (g *feedGateway) SubmitInventoryFeed(_ context.Context, feed contract.InventoryFeed) (contract.FeedResult, error) {
	g.feeds.Add(1)
	g.last.Store(&feed)
	if g.after != nil {
		g.after()
	}
	return contract.FeedResult{Outcome: g.outcome, Sent: len(feed.Lines)}, nil
}

// listedCatalogue lists the same SKUs at every site.
type listedCatalogue []shared.SKU

func (c listedCatalogue) ListedSKUs(context.Context, shared.SiteId) ([]shared.SKU, error) {
	return slices.Clone(c), nil
}

// fixedOffers is a capability-offer repository holding what a recompute pass
// persisted.
type fixedOffers []capabilityoffer.CapabilityOffer

func (fixedOffers) Save(context.Context, capabilityoffer.CapabilityOffer) error { return nil }
func (o fixedOffers) ListAll(context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	return slices.Clone(o), nil
}

func offersOf(t *testing.T, site shared.SiteId, skus ...shared.SKU) fixedOffers {
	t.Helper()
	var out fixedOffers
	for i, sku := range skus {
		o, err := capabilityoffer.New(sku, site, i+1, i+11, capabilityoffer.BasisPhysical, fixedNow{}.Now())
		if err != nil {
			t.Fatalf("offer %s: %v", sku, err)
		}
		out = append(out, o)
	}
	return out
}

func TestWireInventoryFeed_StubModeNeverFeeds(t *testing.T) {
	logger, _ := testLogger()
	gw := &feedGateway{sites: []string{"FC-1"}}
	if w := wireInventoryFeed(logger, retailnetwork.ModeStub, gw, listedCatalogue{"sku-a"}, fixedOffers{}); w != nil {
		t.Fatal("a stub gateway has no network to feed")
	}
}

func TestWireInventoryFeed_LiveWiresTheUseCaseForTheGatewaysOwnSites(t *testing.T) {
	t.Setenv("NETWORK_INVENTORY_INTERVAL", "")
	logger, _ := testLogger()
	gw := &feedGateway{sites: []string{"FC-1", "FC-2"}}

	w := wireInventoryFeed(logger, retailnetwork.ModeLive, gw, listedCatalogue{"sku-a"}, fixedOffers{})

	if w == nil {
		t.Fatal("live mode with sites must wire the feed")
	}
	if w.every != 5*time.Minute {
		t.Errorf("interval = %v, want the 5m default", w.every)
	}
	if want := []shared.SiteId{"FC-1", "FC-2"}; !slices.Equal(w.feed.Sites, want) {
		t.Errorf("sites = %v, want %v", w.feed.Sites, want)
	}
	if w.feed.Feeder == nil || w.feed.Catalogue == nil || w.feed.Offers == nil || w.feed.Logger == nil {
		t.Errorf("use case = %+v, want the feeder, catalogue, offers and logger all wired", w.feed)
	}
}

func TestWireInventoryFeed_TheIntervalComesFromItsOwnVariable(t *testing.T) {
	t.Setenv("NETWORK_PROFILE_INTERVAL", "1h")
	t.Setenv("NETWORK_INVENTORY_INTERVAL", "90s")
	logger, _ := testLogger()

	w := wireInventoryFeed(logger, retailnetwork.ModeLive, &feedGateway{sites: []string{"FC-1"}}, listedCatalogue{}, fixedOffers{})

	if w == nil || w.every != 90*time.Second {
		t.Fatalf("worker = %+v, want an interval of 90s from NETWORK_INVENTORY_INTERVAL alone", w)
	}
}

func TestWireInventoryFeed_ZeroDisablesItLoudly(t *testing.T) {
	t.Setenv("NETWORK_INVENTORY_INTERVAL", "0")
	logger, logs := testLogger()
	gw := &feedGateway{sites: []string{"FC-1"}}

	if w := wireInventoryFeed(logger, retailnetwork.ModeLive, gw, listedCatalogue{}, fixedOffers{}); w != nil {
		t.Fatal("NETWORK_INVENTORY_INTERVAL=0 must switch the worker off")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "NETWORK_INVENTORY_INTERVAL=0") {
		t.Errorf("a disabled feed must say so at WARN, got:\n%s", logs)
	}
}

func TestWireInventoryFeed_AGatewayWithNoSiteFeedsNothingAndSaysSo(t *testing.T) {
	t.Setenv("NETWORK_INVENTORY_INTERVAL", "")
	logger, logs := testLogger()

	if w := wireInventoryFeed(logger, retailnetwork.ModeLive, &feedGateway{}, listedCatalogue{}, fixedOffers{}); w != nil {
		t.Fatal("no site, nothing to feed")
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("want a WARN, got:\n%s", logs)
	}
}

func TestWireInventoryFeed_AGatewayThatDoesNotSayItsSitesFeedsNothing(t *testing.T) {
	t.Setenv("NETWORK_INVENTORY_INTERVAL", "")
	logger, logs := testLogger()
	var gateway ports.NetworkGateway = retailnetwork.NewStubGateway(logger)

	if w := wireInventoryFeed(logger, retailnetwork.ModeLive, gateway, listedCatalogue{}, fixedOffers{}); w != nil {
		t.Fatal("a gateway that does not expose its sites cannot be fed for")
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("want a WARN, got:\n%s", logs)
	}
}

func TestStartInventoryWorker_NilWorkerIsAlreadyDone(t *testing.T) {
	logger, _ := testLogger()
	select {
	case <-startInventoryWorker(context.Background(), logger, nil):
	case <-time.After(time.Second):
		t.Fatal("a nil worker must hand back a closed channel so shutdown never waits for it")
	}
}

func TestStartInventoryWorker_FeedsAtBootThenOnEveryTickAndStopsWithItsContext(t *testing.T) {
	logger, _ := testLogger()
	gw := &feedGateway{sites: []string{"FC-1"}, outcome: contract.FeedUnchanged}
	w := &inventoryWorker{
		every: 10 * time.Millisecond,
		feed: &usecases.FeedInventory{
			Feeder: gw, Catalogue: listedCatalogue{"sku-a", "sku-b"}, Offers: offersOf(t, "FC-1", "sku-a", "sku-b"),
			Logger: logger, Sites: []shared.SiteId{"FC-1"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := startInventoryWorker(ctx, logger, w)

	deadline := time.After(5 * time.Second)
	for gw.feeds.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d passes in 5s; want one at boot and one per tick", gw.feeds.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop with its context")
	}
	if last := gw.last.Load(); last == nil || last.SiteId != "FC-1" || len(last.Lines) != 2 || !last.Complete {
		t.Errorf("last feed = %+v, want a complete feed of the two listed SKUs for FC-1", last)
	}
}

func TestRunFeedInventory_ReportsOnlyAPassThatSentSomething(t *testing.T) {
	cases := []struct {
		outcome  contract.FeedOutcome
		wantInfo bool
	}{
		{contract.FeedSent, true},
		{contract.FeedConfirmed, true},
		{contract.FeedUnchanged, false},
		{contract.FeedPending, false},
		{contract.FeedFailed, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			logger, logs := testLogger()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// The boot pass feeds once; cancelling as it does ends the loop after it.
			gw := &feedGateway{sites: []string{"FC-1"}, outcome: tc.outcome, after: cancel}
			w := &inventoryWorker{
				every: time.Hour,
				feed: &usecases.FeedInventory{
					Feeder: gw, Catalogue: listedCatalogue{"sku-a"}, Offers: offersOf(t, "FC-1", "sku-a"),
					Sites: []shared.SiteId{"FC-1"},
				},
			}

			runFeedInventory(ctx, w, logger)

			if gw.feeds.Load() != 1 {
				t.Fatalf("%d feeds, want the one boot pass to have made one", gw.feeds.Load())
			}
			if got := strings.Contains(logs.String(), "inventory fed"); got != tc.wantInfo {
				t.Errorf("reported = %v, want %v; logs:\n%s", got, tc.wantInfo, logs)
			}
		})
	}
}
