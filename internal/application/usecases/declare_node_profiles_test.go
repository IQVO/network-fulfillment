package usecases

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

type fakeProfileGateway struct {
	calls   []contract.NodeProfile
	results map[shared.SiteId]contract.ProfileResult
	errs    map[shared.SiteId]error
	onCall  func(contract.NodeProfile)
}

func (f *fakeProfileGateway) DeclareNodeProfile(_ context.Context, p contract.NodeProfile) (contract.ProfileResult, error) {
	f.calls = append(f.calls, p)
	if f.onCall != nil {
		f.onCall(p)
	}
	if err := f.errs[p.SiteId]; err != nil {
		return contract.ProfileResult{}, err
	}
	if r, ok := f.results[p.SiteId]; ok {
		return r, nil
	}
	return contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: 1}, nil
}

type fakeProfileHistory struct {
	last map[shared.SiteId]contract.NodeProfile
	err  error
}

func (f *fakeProfileHistory) LastDeclared(_ context.Context, site shared.SiteId) (contract.NodeProfile, bool, error) {
	p, ok := f.last[site]
	return p, ok, f.err
}

type recordedDeclaration struct {
	site   shared.SiteId
	result contract.ProfileResult
}

type fakeProfileMetrics struct{ got []recordedDeclaration }

func (f *fakeProfileMetrics) Declared(_ context.Context, site shared.SiteId, r contract.ProfileResult) {
	f.got = append(f.got, recordedDeclaration{site, r})
}

// profileNow is a Monday, 13:00 UTC (10:00 in America/Sao_Paulo).
var profileNow = time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC)

var workdays = []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday}

func eligible(id string, cycle time.Duration) contract.EligiblePath {
	return contract.EligiblePath{PathId: id, CycleTimeP95: cycle, CycleTimeKnown: cycle > 0}
}

// profileSchedule has two weekday cutoffs, 10:00 and 18:00, so the longest gap
// between two occurrences is the weekend (Friday 18:00 to Monday 10:00, 64 hours).
func profileSchedule(paths ...contract.EligiblePath) contract.CutoffSchedule {
	return contract.CutoffSchedule{
		Timezone: "America/Sao_Paulo",
		Slots: []contract.ScheduleSlot{
			{LocalTime: "18:00", Days: workdays, NextAt: profileNow.Add(8 * time.Hour), Paths: paths},
			{LocalTime: "10:00", Days: workdays, NextAt: profileNow.Add(24 * time.Hour), Paths: paths},
		},
	}
}

type profileFixture struct {
	uc      *DeclareNodeProfiles
	gateway *fakeProfileGateway
	pathCap *fakePathCap
	cap     *fakeCapacity
	history *fakeProfileHistory
	metrics *fakeProfileMetrics
	logs    *bytes.Buffer
}

func newProfileFixture(sites ...shared.SiteId) *profileFixture {
	f := &profileFixture{
		gateway: &fakeProfileGateway{},
		pathCap: &fakePathCap{
			scheduleFound: true,
			schedule:      profileSchedule(eligible("P1", 90*time.Minute+30*time.Second), eligible("P2", 40*time.Minute)),
		},
		cap:     &fakeCapacity{remaining: map[string]int{"P1": 30, "P2": 20}, known: map[string]bool{"P1": true, "P2": true}},
		history: &fakeProfileHistory{last: map[shared.SiteId]contract.NodeProfile{}},
		metrics: &fakeProfileMetrics{},
		logs:    &bytes.Buffer{},
	}
	if len(sites) == 0 {
		sites = []shared.SiteId{"FC-1"}
	}
	f.uc = &DeclareNodeProfiles{
		Gateway: f.gateway, PathCap: f.pathCap, Capacity: f.cap, Clock: fixedClock{now: profileNow},
		Logger:  slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		History: f.history, Metrics: f.metrics, Sites: sites,
	}
	return f
}

func (f *profileFixture) onlyCall(t *testing.T) contract.NodeProfile {
	t.Helper()
	if len(f.gateway.calls) != 1 {
		t.Fatalf("gateway calls = %d, want 1\nlogs:\n%s", len(f.gateway.calls), f.logs)
	}
	return f.gateway.calls[0]
}

func TestDeclareNodeProfiles_DeclaresWhatTheCachesSupport(t *testing.T) {
	f := newProfileFixture()

	res := f.uc.Execute(context.Background())

	p := f.onlyCall(t)
	if p.SiteId != "FC-1" || p.Timezone != "America/Sao_Paulo" {
		t.Errorf("profile = %+v, want site FC-1 in America/Sao_Paulo", p)
	}
	if p.HandlingTime != 91*time.Minute {
		t.Errorf("HandlingTime = %v, want 91m: the longest p95 (90m30s) rounded UP, never less", p.HandlingTime)
	}
	if len(p.Cutoffs) != 2 || p.Cutoffs[0].LocalTime != "10:00" || p.Cutoffs[1].LocalTime != "18:00" {
		t.Fatalf("cutoffs = %+v, want 10:00 and 18:00 in time order", p.Cutoffs)
	}
	for _, c := range p.Cutoffs {
		if c.MaxOrders != 50 || len(c.Days) != 5 || c.Days[0] != time.Monday || c.Days[4] != time.Friday {
			t.Errorf("cutoff %s = %+v, want ceiling 50 (30+20) on Monday..Friday", c.LocalTime, c)
		}
	}
	if p.ThroughputPerDay != 100 {
		t.Errorf("ThroughputPerDay = %d, want 100: the busiest weekday's two slots of 50", p.ThroughputPerDay)
	}
	if p.Fingerprint == "" {
		t.Error("the profile carries no fingerprint")
	}
	if res.Outcomes[contract.ProfileConfirmed] != 1 {
		t.Errorf("outcomes = %v, want one confirmed", res.Outcomes)
	}
	if len(f.metrics.got) != 1 || f.metrics.got[0].site != "FC-1" || f.metrics.got[0].result.Outcome != contract.ProfileConfirmed {
		t.Errorf("metrics = %+v, want one confirmed FC-1", f.metrics.got)
	}
}

func TestDeclareNodeProfiles_ASlotOfTheSameTimeIsOneCutoffWhateverTheShipMethods(t *testing.T) {
	f := newProfileFixture()
	f.pathCap.schedule.Slots = []contract.ScheduleSlot{
		{LocalTime: "18:00", Days: []time.Weekday{time.Monday, time.Tuesday}, NextAt: profileNow.Add(8 * time.Hour), Paths: []contract.EligiblePath{eligible("P1", time.Hour)}},
		{LocalTime: "18:00", Days: []time.Weekday{time.Tuesday, time.Wednesday}, NextAt: profileNow.Add(6 * time.Hour), Paths: []contract.EligiblePath{eligible("P1", time.Hour), eligible("P2", 30*time.Minute)}},
	}

	f.uc.Execute(context.Background())

	p := f.onlyCall(t)
	if len(p.Cutoffs) != 1 {
		t.Fatalf("cutoffs = %+v, want the two 18:00 slots to be one", p.Cutoffs)
	}
	c := p.Cutoffs[0]
	if len(c.Days) != 3 || c.Days[0] != time.Monday || c.Days[2] != time.Wednesday {
		t.Errorf("days = %v, want Monday..Wednesday united", c.Days)
	}
	if c.MaxOrders != 50 {
		t.Errorf("ceiling = %d, want 50: each path counted once (P1 30 + P2 20), at the earliest next occurrence", c.MaxOrders)
	}
}

func TestDeclareNodeProfiles_APathThatCannotFinishInTheLongestWindowDoesNotSetTheHandlingTime(t *testing.T) {
	f := newProfileFixture()
	f.pathCap.schedule = profileSchedule(eligible("P1", 2*time.Hour), eligible("PX", 100*time.Hour), contract.EligiblePath{PathId: "PN"})

	f.uc.Execute(context.Background())

	if got := f.onlyCall(t).HandlingTime; got != 2*time.Hour {
		t.Errorf("HandlingTime = %v, want 2h: PX (100h) does not fit the 64h window and PN has no cycle time", got)
	}
}

func TestDeclareNodeProfiles_WhatTheCachesDoNotKnowIsNotDeclaredAndTheReasonIsLogged(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*profileFixture)
		outcome contract.ProfileOutcome
		logged  string
	}{
		{"no schedule", func(f *profileFixture) { f.pathCap.scheduleFound = false }, contract.ProfileSkippedNoSchedule, "no cutoff schedule"},
		{"no cycle time at all", func(f *profileFixture) {
			f.pathCap.schedule = profileSchedule(contract.EligiblePath{PathId: "P1"}, contract.EligiblePath{PathId: "P2"})
		}, contract.ProfileSkippedIncomplete, "handling time"},
		{"every cycle time too long", func(f *profileFixture) {
			f.pathCap.schedule = profileSchedule(eligible("P1", 200*time.Hour))
		}, contract.ProfileSkippedIncomplete, "handling time"},
		{"no eligible path", func(f *profileFixture) { f.pathCap.schedule = profileSchedule() }, contract.ProfileSkippedIncomplete, "handling time"},
		{"no capacity figure known", func(f *profileFixture) { f.cap.known = map[string]bool{} }, contract.ProfileSkippedIncomplete, "throughput"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newProfileFixture()
			tc.mutate(f)

			res := f.uc.Execute(context.Background())

			if len(f.gateway.calls) != 0 {
				t.Fatalf("the gateway was called with %+v; an incomplete profile is not declared", f.gateway.calls)
			}
			if res.Outcomes[tc.outcome] != 1 {
				t.Errorf("outcomes = %v, want one %s", res.Outcomes, tc.outcome)
			}
			if len(f.metrics.got) != 1 || f.metrics.got[0].result.Outcome != tc.outcome {
				t.Errorf("metrics = %+v, want one %s", f.metrics.got, tc.outcome)
			}
			if !strings.Contains(f.logs.String(), "level=WARN") || !strings.Contains(f.logs.String(), tc.logged) {
				t.Errorf("want a WARN naming %q, got:\n%s", tc.logged, f.logs)
			}
		})
	}
}

func TestDeclareNodeProfiles_ACeilingIsNeverLoweredByAnObservationButCanRise(t *testing.T) {
	f := newProfileFixture()
	f.history.last["FC-1"] = contract.NodeProfile{SiteId: "FC-1", Cutoffs: []contract.ProfileCutoff{
		{LocalTime: "10:00", Days: workdays, MaxOrders: 80}, // observed now: 50, lower
		{LocalTime: "18:00", Days: workdays, MaxOrders: 10}, // observed now: 50, higher
	}}

	f.uc.Execute(context.Background())

	p := f.onlyCall(t)
	if p.Cutoffs[0].MaxOrders != 80 || p.Cutoffs[1].MaxOrders != 50 {
		t.Errorf("ceilings = %d, %d; want 80 (kept) and 50 (raised)", p.Cutoffs[0].MaxOrders, p.Cutoffs[1].MaxOrders)
	}
	if p.ThroughputPerDay != 130 {
		t.Errorf("ThroughputPerDay = %d, want 130 = 80 + 50", p.ThroughputPerDay)
	}
}

func TestDeclareNodeProfiles_ACeilingOfAnotherSlotShapeIsNotCarriedOver(t *testing.T) {
	f := newProfileFixture()
	f.history.last["FC-1"] = contract.NodeProfile{SiteId: "FC-1", Cutoffs: []contract.ProfileCutoff{
		{LocalTime: "10:00", Days: []time.Weekday{time.Monday}, MaxOrders: 500}, // the slot now serves five days
	}}

	f.uc.Execute(context.Background())

	if got := f.onlyCall(t).Cutoffs[0].MaxOrders; got != 50 {
		t.Errorf("ceiling = %d, want 50: a slot that changed is a different slot", got)
	}
}

func TestDeclareNodeProfiles_WhenWhatWasLastDeclaredCannotBeReadNothingIsDeclared(t *testing.T) {
	f := newProfileFixture()
	f.history.err = errors.New("db down")

	res := f.uc.Execute(context.Background())

	if len(f.gateway.calls) != 0 {
		t.Fatal("declared without the memory that stops a ceiling from being lowered")
	}
	if res.Outcomes[contract.ProfileFailed] != 1 || !strings.Contains(f.logs.String(), "level=ERROR") {
		t.Errorf("outcomes = %v logs:\n%s, want one failed and an ERROR", res.Outcomes, f.logs)
	}
}

func TestDeclareNodeProfiles_TheFingerprintFollowsTheContentNotTheOrderOfTheCaches(t *testing.T) {
	a, b := newProfileFixture(), newProfileFixture()
	b.pathCap.schedule.Slots[0], b.pathCap.schedule.Slots[1] = b.pathCap.schedule.Slots[1], b.pathCap.schedule.Slots[0]
	for i := range b.pathCap.schedule.Slots {
		p := b.pathCap.schedule.Slots[i].Paths
		b.pathCap.schedule.Slots[i].Paths = []contract.EligiblePath{p[1], p[0]}
		b.pathCap.schedule.Slots[i].Days = []time.Weekday{time.Friday, time.Thursday, time.Wednesday, time.Tuesday, time.Monday}
	}
	a.uc.Execute(context.Background())
	b.uc.Execute(context.Background())
	again := newProfileFixture()
	again.uc.Execute(context.Background())

	if a.onlyCall(t).Fingerprint != b.onlyCall(t).Fingerprint {
		t.Error("the same profile listed in another order has another fingerprint: it would burn a revision for nothing")
	}
	if a.onlyCall(t).Fingerprint != again.onlyCall(t).Fingerprint {
		t.Error("two passes over the same caches disagree on the fingerprint")
	}

	changed := newProfileFixture()
	changed.cap.remaining["P1"] = 31
	changed.uc.Execute(context.Background())
	if changed.onlyCall(t).Fingerprint == a.onlyCall(t).Fingerprint {
		t.Error("a changed ceiling kept the same fingerprint: the network would never hear of it")
	}

	slower := newProfileFixture()
	slower.pathCap.schedule = profileSchedule(eligible("P1", 95*time.Minute), eligible("P2", 40*time.Minute))
	slower.uc.Execute(context.Background())
	if slower.onlyCall(t).Fingerprint == a.onlyCall(t).Fingerprint {
		t.Error("a changed handling time kept the same fingerprint")
	}
}

func TestDeclareNodeProfiles_ASubMinuteWobbleOfTheP95IsNotANewProfile(t *testing.T) {
	a, b := newProfileFixture(), newProfileFixture()
	b.pathCap.schedule = profileSchedule(eligible("P1", 90*time.Minute+50*time.Second), eligible("P2", 40*time.Minute))
	a.uc.Execute(context.Background())
	b.uc.Execute(context.Background())

	if a.onlyCall(t).Fingerprint != b.onlyCall(t).Fingerprint {
		t.Error("90m30s and 90m50s (both 91m once rounded up) are different fingerprints")
	}
}

func TestDeclareNodeProfiles_ASiteWithNoNodeYetIsASkipNotAFailure(t *testing.T) {
	f := newProfileFixture()
	f.gateway.errs = map[shared.SiteId]error{"FC-1": contract.ErrSiteNotRegistered}

	res := f.uc.Execute(context.Background())

	if res.Outcomes[contract.ProfileSkippedNoNode] != 1 || res.Outcomes[contract.ProfileFailed] != 0 {
		t.Errorf("outcomes = %v, want one skipped_no_node", res.Outcomes)
	}
	if strings.Contains(f.logs.String(), "level=ERROR") || strings.Contains(f.logs.String(), "level=WARN") {
		t.Errorf("a node that is simply not registered yet is not an alarm, got:\n%s", f.logs)
	}
}

func TestDeclareNodeProfiles_OneSitesFailureNeverStopsTheOthers(t *testing.T) {
	f := newProfileFixture("FC-1", "FC-2", "FC-3")
	f.gateway.errs = map[shared.SiteId]error{"FC-1": errors.New("network down")}
	f.gateway.results = map[shared.SiteId]contract.ProfileResult{"FC-3": {Outcome: contract.ProfileUnchanged, Revision: 4}}

	res := f.uc.Execute(context.Background())

	if len(f.gateway.calls) != 3 {
		t.Fatalf("gateway calls = %d, want all 3 sites tried", len(f.gateway.calls))
	}
	if res.Outcomes[contract.ProfileFailed] != 1 || res.Outcomes[contract.ProfileConfirmed] != 1 || res.Outcomes[contract.ProfileUnchanged] != 1 {
		t.Errorf("outcomes = %v, want one failed, one confirmed, one unchanged", res.Outcomes)
	}
	if !strings.Contains(f.logs.String(), "level=ERROR") || !strings.Contains(f.logs.String(), "network down") {
		t.Errorf("a failed call must be logged at ERROR with its cause, got:\n%s", f.logs)
	}
	if len(f.metrics.got) != 3 || f.metrics.got[2].result.Revision != 4 {
		t.Errorf("metrics = %+v, want one entry per site, the last at revision 4", f.metrics.got)
	}
}

func TestDeclareNodeProfiles_ACancelledContextStopsBeforeTheNextSite(t *testing.T) {
	f := newProfileFixture("FC-1", "FC-2")
	ctx, cancel := context.WithCancel(context.Background())
	f.gateway.onCall = func(contract.NodeProfile) { cancel() }

	f.uc.Execute(ctx)

	if len(f.gateway.calls) != 1 {
		t.Fatalf("gateway calls = %d, want 1: the process is shutting down", len(f.gateway.calls))
	}
}

func TestDeclareNodeProfiles_NoMemoryNoMetricsNoLoggerStillWorks(t *testing.T) {
	f := newProfileFixture()
	f.uc.History, f.uc.Metrics, f.uc.Logger = nil, nil, nil

	res := f.uc.Execute(context.Background())

	if len(f.gateway.calls) != 1 || res.Outcomes[contract.ProfileConfirmed] != 1 {
		t.Fatalf("calls = %d outcomes = %v, want one confirmed declaration", len(f.gateway.calls), res.Outcomes)
	}
	f.pathCap.scheduleFound = false
	f.uc.Execute(context.Background()) // a skip with no logger must not panic
}
