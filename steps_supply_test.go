package main_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of features/inventory_feed_rules.feature and
// features/node_profile_rules.feature (ADR 0027): the two workers that tell the
// network what the site can ship (FeedInventory) and how it operates
// (DeclareNodeProfiles). Both read only local data, so the real use cases run
// over the suite's in-memory offers and caches; the network is a recording
// double (the stub gateway sends nothing and keeps no feed).

// ---- doubles ---------------------------------------------------------------------

// feedRecorder is the network's inventory-feed operation: it records every
// attempt, and answers what the scenario scripted for the site.
type feedRecorder struct {
	feeds []contract.InventoryFeed
	errs  map[shared.SiteId]error
}

func (f *feedRecorder) SubmitInventoryFeed(_ context.Context, feed contract.InventoryFeed) (contract.FeedResult, error) {
	f.feeds = append(f.feeds, feed)
	if err := f.errs[feed.SiteId]; err != nil {
		return contract.FeedResult{}, err
	}
	return contract.FeedResult{Outcome: contract.FeedSent, Sent: len(feed.Lines)}, nil
}

func (f *feedRecorder) forSite(site string) []contract.InventoryFeed {
	var out []contract.InventoryFeed
	for _, feed := range f.feeds {
		if string(feed.SiteId) == site {
			out = append(out, feed)
		}
	}
	return out
}

// fakeCatalogue is the product dictionary read per site: the SKUs the network
// lists there.
type fakeCatalogue struct {
	listed map[shared.SiteId][]shared.SKU
}

func (c *fakeCatalogue) ListedSKUs(_ context.Context, site shared.SiteId) ([]shared.SKU, error) {
	return c.listed[site], nil
}

// unreadableOffers is the offer store failing every read.
type unreadableOffers struct{ ports.CapabilityOfferRepo }

func (unreadableOffers) ListAll(context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	return nil, errors.New("the capability offers cannot be read")
}

// syncBuffer is a log sink the use case may write to while a step reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// scheduleCap is process-path-management's cache seen through Schedule: the
// whole weekly schedule of each site. NextCutoff is the recompute pass's read,
// not this worker's.
type scheduleCap struct {
	schedules map[shared.SiteId]contract.CutoffSchedule
}

func (c *scheduleCap) NextCutoff(shared.SiteId, time.Time) (contract.NextCutoff, bool) {
	return contract.NextCutoff{}, false
}

func (c *scheduleCap) Schedule(site shared.SiteId, _ time.Time) (contract.CutoffSchedule, bool) {
	s, ok := c.schedules[site]
	return s, ok
}

// profileRecorder is the network's node-profile operation.
type profileRecorder struct {
	declared []contract.NodeProfile
	errs     map[shared.SiteId]error
}

func (p *profileRecorder) DeclareNodeProfile(_ context.Context, profile contract.NodeProfile) (contract.ProfileResult, error) {
	p.declared = append(p.declared, profile)
	if err := p.errs[profile.SiteId]; err != nil {
		return contract.ProfileResult{}, err
	}
	return contract.ProfileResult{Outcome: contract.ProfileDeclared, Revision: len(p.declared)}, nil
}

func (p *profileRecorder) lastFor(site string) (contract.NodeProfile, bool) {
	for i := len(p.declared) - 1; i >= 0; i-- {
		if string(p.declared[i].SiteId) == site {
			return p.declared[i], true
		}
	}
	return contract.NodeProfile{}, false
}

// profileMemory is what the gateway last declared, which DeclareNodeProfiles
// reads so a declared ceiling is never lowered by an observation.
type profileMemory struct {
	last       map[shared.SiteId]contract.NodeProfile
	unreadable bool
}

func (m *profileMemory) LastDeclared(_ context.Context, site shared.SiteId) (contract.NodeProfile, bool, error) {
	if m.unreadable {
		return contract.NodeProfile{}, false, errors.New("the last declared profile cannot be read")
	}
	p, ok := m.last[site]
	return p, ok, nil
}

// ---- the per-scenario state -------------------------------------------------------

type supplyWorld struct {
	w *world

	// inventory feed
	feeder     *feedRecorder
	catalogue  *fakeCatalogue
	feedSites  []shared.SiteId
	feedLog    *syncBuffer
	feedUC     *usecases.FeedInventory
	feedReport usecases.FeedReport
	offersDown bool

	// node profile
	cap          *scheduleCap
	profiles     *profileRecorder
	memory       *profileMemory
	profileSites []shared.SiteId
	profileUC    *usecases.DeclareNodeProfiles
	profileRes   usecases.DeclareResult
	curSite      shared.SiteId
}

func (s *supplyWorld) reset() {
	s.feeder = &feedRecorder{errs: map[shared.SiteId]error{}}
	s.catalogue = &fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{}}
	s.feedSites = nil
	s.feedLog = &syncBuffer{}
	s.feedUC = nil
	s.feedReport = usecases.FeedReport{}
	s.offersDown = false

	s.cap = &scheduleCap{schedules: map[shared.SiteId]contract.CutoffSchedule{}}
	s.profiles = &profileRecorder{errs: map[shared.SiteId]error{}}
	s.memory = &profileMemory{last: map[shared.SiteId]contract.NodeProfile{}}
	s.profileSites = nil
	s.profileUC = nil
	s.profileRes = usecases.DeclareResult{}
	s.curSite = ""
}

// ---- inventory feed: Given ----------------------------------------------------------

func (s *supplyWorld) siteLists(site, skuList string) error {
	for _, m := range quoted.FindAllStringSubmatch(skuList, -1) {
		s.catalogue.listed[shared.SiteId(site)] = append(s.catalogue.listed[shared.SiteId(site)], shared.SKU(m[1]))
	}
	return nil
}

func (s *supplyWorld) feedServes(site string) error {
	s.feedSites = append(s.feedSites, shared.SiteId(site))
	return nil
}

func (s *supplyWorld) offerComputed(ctx context.Context, sku, site string, qty int, basis string, minutesAgo int) error {
	physical := qty
	if capabilityoffer.Basis(basis) == capabilityoffer.BasisThroughputConstrained {
		physical = qty + 10
	}
	at := s.w.clock.Now().Add(-time.Duration(minutesAgo) * time.Minute)
	offer, err := capabilityoffer.New(shared.SKU(sku), shared.SiteId(site), qty, physical, capabilityoffer.Basis(basis), at)
	if err != nil {
		return err
	}
	return s.w.offers.Save(ctx, offer)
}

// siteListsFresh lists n SKUs at the site, each with a fresh offer: the size
// scenarios of ADR 0027 §9 (the network takes at most 5000 lines per feed).
func (s *supplyWorld) siteListsFresh(ctx context.Context, site string, n int) error {
	for i := 1; i <= n; i++ {
		sku := fmt.Sprintf("bulk-%05d", i)
		if err := s.offerComputed(ctx, sku, site, 1, string(capabilityoffer.BasisPhysical), 0); err != nil {
			return err
		}
		s.catalogue.listed[shared.SiteId(site)] = append(s.catalogue.listed[shared.SiteId(site)], shared.SKU(sku))
	}
	return nil
}

func (s *supplyWorld) networkRefusesFeed(site, reason string) error {
	switch reason {
	case "has no node":
		s.feeder.errs[shared.SiteId(site)] = contract.ErrSiteNotRegistered
	default:
		s.feeder.errs[shared.SiteId(site)] = errors.New("the network failed the inventory feed")
	}
	return nil
}

func (s *supplyWorld) offersUnreadable() error {
	s.offersDown = true
	return nil
}

// ---- inventory feed: When -----------------------------------------------------------

func (s *supplyWorld) feedRuns(ctx context.Context) error {
	if s.feedUC == nil {
		sites := s.feedSites
		if len(sites) == 0 {
			sites = []shared.SiteId{"site-1"}
		}
		s.feedUC = &usecases.FeedInventory{
			Feeder:    s.feeder,
			Catalogue: s.catalogue,
			Logger:    slog.New(slog.NewTextHandler(s.feedLog, &slog.HandlerOptions{Level: slog.LevelInfo})),
			Sites:     sites,
		}
	}
	s.feedUC.Offers = s.w.offers
	if s.offersDown {
		s.feedUC.Offers = unreadableOffers{s.w.offers}
	}
	s.feedReport = s.feedUC.Execute(ctx)
	return nil
}

// ---- inventory feed: Then -----------------------------------------------------------

func (s *supplyWorld) feedPassEnded(expected int, outcome string) error {
	if got := s.feedReport.Outcomes[contract.FeedOutcome(outcome)]; got != expected {
		return fmt.Errorf("expected %d feed(s) to end %q, got %d (all: %v)", expected, outcome, got, s.feedReport.Outcomes)
	}
	return nil
}

func (s *supplyWorld) feedsSubmitted(expected int, site string) error {
	if got := len(s.feeder.forSite(site)); got != expected {
		return fmt.Errorf("expected %d feed(s) submitted for %q, got %d", expected, site, got)
	}
	return nil
}

func (s *supplyWorld) feedPartIs(part, parts int, site, kind string, lines int) error {
	all := s.feeder.forSite(site)
	// The newest pass wins: walk back from the last submission.
	for i := len(all) - 1; i >= 0; i-- {
		feed := all[i]
		if feed.Part != part || feed.Parts != parts {
			continue
		}
		if complete := kind == "complete"; feed.Complete != complete {
			return fmt.Errorf("expected part %d of %d to be %s, it has Complete=%t", part, parts, kind, feed.Complete)
		}
		if len(feed.Lines) != lines {
			return fmt.Errorf("expected part %d of %d to carry %d lines, it carries %d", part, parts, lines, len(feed.Lines))
		}
		return nil
	}
	return fmt.Errorf("no part %d of %d was submitted for %q", part, parts, site)
}

func (s *supplyWorld) feedCarries(site string, table *godog.Table) error {
	feeds := s.feeder.forSite(site)
	if len(feeds) == 0 {
		return fmt.Errorf("no feed was submitted for %q", site)
	}
	var got []string
	for _, l := range feeds[len(feeds)-1].Lines {
		got = append(got, fmt.Sprintf("%s|%d|%s", l.SKU, l.Quantity, l.Basis))
	}
	var want []string
	for _, row := range table.Rows[1:] {
		want = append(want, fmt.Sprintf("%s|%s|%s", row.Cells[0].Value, row.Cells[1].Value, row.Cells[2].Value))
	}
	if strings.Join(got, ";") != strings.Join(want, ";") {
		return fmt.Errorf("expected the feed lines %v in this order, got %v", want, got)
	}
	return nil
}

func (s *supplyWorld) everyPartAsOf(site, asOf string) error {
	feeds := s.feeder.forSite(site)
	if len(feeds) == 0 {
		return fmt.Errorf("no feed was submitted for %q", site)
	}
	for _, feed := range feeds {
		if got := feed.AsOf.UTC().Format(time.RFC3339); got != asOf {
			return fmt.Errorf("expected part %d to be as of %s, it is as of %s", feed.Part, asOf, got)
		}
	}
	return nil
}

func (s *supplyWorld) unlistedNoteLogged(sku string, expected int) error {
	n := 0
	for _, line := range strings.Split(s.feedLog.String(), "\n") {
		if strings.Contains(line, "has no listing at the site") && strings.Contains(line, "sku="+sku) {
			n++
		}
	}
	if n != expected {
		return fmt.Errorf("expected the no-listing note for %q to be logged %d time(s), logged %d", sku, expected, n)
	}
	return nil
}

func (s *supplyWorld) fingerprints(site, which string) error {
	feeds := s.feeder.forSite(site)
	if len(feeds) < 2 {
		return fmt.Errorf("need two feeds for %q, have %d", site, len(feeds))
	}
	first, last := feeds[0].Fingerprint, feeds[len(feeds)-1].Fingerprint
	if same := first == last; same != (which == "same") {
		return fmt.Errorf("expected the first and the last feed to have the %s fingerprint (%s, %s)", which, first, last)
	}
	return nil
}

// ---- node profile: Given ------------------------------------------------------------

var weekdayNames = map[string]time.Weekday{
	"sunday": time.Sunday, "monday": time.Monday, "tuesday": time.Tuesday, "wednesday": time.Wednesday,
	"thursday": time.Thursday, "friday": time.Friday, "saturday": time.Saturday,
}

func parseDays(spec string) ([]time.Weekday, error) {
	lower := strings.ToLower(spec)
	switch strings.TrimSpace(lower) {
	case "every day":
		return []time.Weekday{0, 1, 2, 3, 4, 5, 6}, nil
	case "weekdays":
		return []time.Weekday{1, 2, 3, 4, 5}, nil
	}
	var days []time.Weekday
	for d := time.Sunday; d <= time.Saturday; d++ {
		if strings.Contains(lower, strings.ToLower(d.String())) {
			days = append(days, d)
		}
	}
	if len(days) == 0 {
		return nil, fmt.Errorf("no weekday in %q", spec)
	}
	return days, nil
}

func (s *supplyWorld) scheduleOf(site, tz string) error {
	s.curSite = shared.SiteId(site)
	s.cap.schedules[s.curSite] = contract.CutoffSchedule{Timezone: tz}
	s.profileSites = append(s.profileSites, s.curSite)
	return nil
}

func (s *supplyWorld) siteServed(site string) error {
	s.profileSites = append(s.profileSites, shared.SiteId(site))
	return nil
}

func (s *supplyWorld) cutoffAt(localTime, daySpec string, hours int) error {
	days, err := parseDays(daySpec)
	if err != nil {
		return err
	}
	sched, ok := s.cap.schedules[s.curSite]
	if !ok {
		return fmt.Errorf("name the site's schedule first")
	}
	sched.Slots = append(sched.Slots, contract.ScheduleSlot{
		LocalTime: localTime, Days: days, NextAt: s.w.clock.Now().Add(time.Duration(hours) * time.Hour),
	})
	s.cap.schedules[s.curSite] = sched
	return nil
}

func (s *supplyWorld) pathServes(pathID string, cycle int, unit string) error {
	d := time.Duration(cycle) * time.Minute
	if unit == "seconds" {
		d = time.Duration(cycle) * time.Second
	}
	return s.attachPath(contract.EligiblePath{PathId: pathID, CycleTimeP95: d, CycleTimeKnown: true})
}

func (s *supplyWorld) pathServesUnknown(pathID string) error {
	return s.attachPath(contract.EligiblePath{PathId: pathID})
}

func (s *supplyWorld) attachPath(p contract.EligiblePath) error {
	sched, ok := s.cap.schedules[s.curSite]
	if !ok || len(sched.Slots) == 0 {
		return fmt.Errorf("add a cutoff to the schedule first")
	}
	last := &sched.Slots[len(sched.Slots)-1]
	last.Paths = append(last.Paths, p)
	s.cap.schedules[s.curSite] = sched
	return nil
}

func (s *supplyWorld) lastDeclared(site string, orders int, localTime string) error {
	days, _ := parseDays("every day")
	s.memory.last[shared.SiteId(site)] = contract.NodeProfile{
		SiteId:  shared.SiteId(site),
		Cutoffs: []contract.ProfileCutoff{{LocalTime: localTime, Days: days, MaxOrders: orders}},
	}
	return nil
}

func (s *supplyWorld) historyUnreadable() error {
	s.memory.unreadable = true
	return nil
}

func (s *supplyWorld) historyReadable() error {
	s.memory.unreadable = false
	return nil
}

func (s *supplyWorld) networkRefusesProfile(site, reason string) error {
	switch reason {
	case "has no node":
		s.profiles.errs[shared.SiteId(site)] = contract.ErrSiteNotRegistered
	default:
		s.profiles.errs[shared.SiteId(site)] = errors.New("the network failed the profile declaration")
	}
	return nil
}

// ---- node profile: When / Then ---------------------------------------------------------

func (s *supplyWorld) profilesDeclared(ctx context.Context) error {
	if s.profileUC == nil {
		s.profileUC = &usecases.DeclareNodeProfiles{
			Gateway:  s.profiles,
			PathCap:  s.cap,
			Capacity: s.w.capacity,
			Clock:    s.w.clock,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			History:  s.memory,
			Sites:    s.profileSites,
		}
	}
	s.profileRes = s.profileUC.Execute(ctx)
	return nil
}

func (s *supplyWorld) profilePassEnded(expected int, outcome string) error {
	if got := s.profileRes.Outcomes[contract.ProfileOutcome(outcome)]; got != expected {
		return fmt.Errorf("expected %d site(s) to end %q, got %d (all: %v)", expected, outcome, got, s.profileRes.Outcomes)
	}
	return nil
}

func (s *supplyWorld) noProfileDeclared(site string) error {
	if p, ok := s.profiles.lastFor(site); ok {
		return fmt.Errorf("expected nothing to be declared for %q, declared %+v", site, p)
	}
	return nil
}

func (s *supplyWorld) declaredProfile(site string) (contract.NodeProfile, error) {
	p, ok := s.profiles.lastFor(site)
	if !ok {
		return contract.NodeProfile{}, fmt.Errorf("no profile was declared for %q", site)
	}
	return p, nil
}

func (s *supplyWorld) profileHandling(site string, minutes int) error {
	p, err := s.declaredProfile(site)
	if err != nil {
		return err
	}
	if got := int(p.HandlingTime / time.Minute); got != minutes || p.HandlingTime%time.Minute != 0 {
		return fmt.Errorf("expected a handling time of %d whole minutes, got %s", minutes, p.HandlingTime)
	}
	return nil
}

func (s *supplyWorld) profileThroughput(site string, perDay int) error {
	p, err := s.declaredProfile(site)
	if err != nil {
		return err
	}
	if p.ThroughputPerDay != perDay {
		return fmt.Errorf("expected a throughput of %d orders per day, got %d", perDay, p.ThroughputPerDay)
	}
	return nil
}

func (s *supplyWorld) profileTimezone(site, tz string) error {
	p, err := s.declaredProfile(site)
	if err != nil {
		return err
	}
	if p.Timezone != tz {
		return fmt.Errorf("expected the profile in timezone %q, got %q", tz, p.Timezone)
	}
	return nil
}

func (s *supplyWorld) profileCutoffCount(site string, n int) error {
	p, err := s.declaredProfile(site)
	if err != nil {
		return err
	}
	if len(p.Cutoffs) != n {
		return fmt.Errorf("expected %d cutoff(s), got %+v", n, p.Cutoffs)
	}
	return nil
}

func (s *supplyWorld) profileCeiling(site string, orders int, localTime string) error {
	p, err := s.declaredProfile(site)
	if err != nil {
		return err
	}
	for _, c := range p.Cutoffs {
		if c.LocalTime == localTime {
			if c.MaxOrders != orders {
				return fmt.Errorf("expected %d orders at %s, got %d", orders, localTime, c.MaxOrders)
			}
			return nil
		}
	}
	return fmt.Errorf("no cutoff at %s in %+v", localTime, p.Cutoffs)
}

func (s *supplyWorld) profileDays(localTime, site, daySpec string) error {
	want, err := parseDays(daySpec)
	if err != nil {
		return err
	}
	p, err := s.declaredProfile(site)
	if err != nil {
		return err
	}
	for _, c := range p.Cutoffs {
		if c.LocalTime == localTime {
			if fmt.Sprint(c.Days) != fmt.Sprint(want) {
				return fmt.Errorf("expected the cutoff at %s on %v, got %v", localTime, want, c.Days)
			}
			return nil
		}
	}
	return fmt.Errorf("no cutoff at %s in %+v", localTime, p.Cutoffs)
}

func (w *world) registerSupplySteps(sc *godog.ScenarioContext) {
	s := &supplyWorld{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.reset()
		return ctx, nil
	})

	sc.Step(`^the site "([^"]*)" lists the SKUs? (.+)$`, s.siteLists)
	sc.Step(`^the inventory feed serves site "([^"]*)"$`, s.feedServes)
	sc.Step(`^an offer for "([^"]*)" at "([^"]*)" advertises (\d+) on basis "([^"]*)" computed (\d+) minutes? ago$`, s.offerComputed)
	sc.Step(`^the site "([^"]*)" lists (\d+) SKUs each with a fresh offer$`, s.siteListsFresh)
	sc.Step(`^the network (has no node|fails) for the inventory feed of site "([^"]*)"$`, func(reason, site string) error {
		return s.networkRefusesFeed(site, reason)
	})
	sc.Step(`^the capability offers cannot be read$`, s.offersUnreadable)
	sc.Step(`^the inventory feed runs$`, s.feedRuns)
	sc.Step(`^the inventory feed pass ended (\d+) times? with "([^"]*)"$`, s.feedPassEnded)
	sc.Step(`^(\d+) inventory feeds? (?:was|were) submitted for site "([^"]*)"$`, s.feedsSubmitted)
	sc.Step(`^part (\d+) of (\d+) of the inventory feed for "([^"]*)" is (complete|partial) with (\d+) lines$`, s.feedPartIs)
	sc.Step(`^the inventory feed for "([^"]*)" carries the lines:$`, s.feedCarries)
	sc.Step(`^every part of the inventory feed for "([^"]*)" is as of "([^"]*)"$`, s.everyPartAsOf)
	sc.Step(`^the no-listing note for "([^"]*)" was logged (\d+) times?$`, s.unlistedNoteLogged)
	sc.Step(`^the first and the last inventory feed for "([^"]*)" have the (same|different) fingerprint$`, s.fingerprints)

	sc.Step(`^the CPT schedule of site "([^"]*)" is in timezone "([^"]*)"$`, s.scheduleOf)
	sc.Step(`^the site "([^"]*)" is served by the node profile worker$`, s.siteServed)
	sc.Step(`^the schedule has a cutoff at "([^"]*)" on (.+), next in (\d+) hours$`, s.cutoffAt)
	sc.Step(`^path "([^"]*)" serves that cutoff with a cycle time of (\d+) (minutes|seconds)$`, s.pathServes)
	sc.Step(`^path "([^"]*)" serves that cutoff with an unknown cycle time$`, s.pathServesUnknown)
	sc.Step(`^the profile last declared for site "([^"]*)" allowed (\d+) orders at "([^"]*)" on every day$`, s.lastDeclared)
	sc.Step(`^the profile history cannot be read$`, s.historyUnreadable)
	sc.Step(`^the profile history can be read again$`, s.historyReadable)
	sc.Step(`^the network (has no node|fails) for the node profile of site "([^"]*)"$`, func(reason, site string) error {
		return s.networkRefusesProfile(site, reason)
	})
	sc.Step(`^the node profiles are declared$`, s.profilesDeclared)
	sc.Step(`^the node profile pass ended (\d+) times? with "([^"]*)"$`, s.profilePassEnded)
	sc.Step(`^no node profile was declared for "([^"]*)"$`, s.noProfileDeclared)
	sc.Step(`^the node profile declared for "([^"]*)" has a handling time of (\d+) minutes$`, s.profileHandling)
	sc.Step(`^the node profile declared for "([^"]*)" has a throughput of (\d+) orders per day$`, s.profileThroughput)
	sc.Step(`^the node profile declared for "([^"]*)" has (\d+) cutoffs?$`, s.profileCutoffCount)
	sc.Step(`^the node profile declared for "([^"]*)" is in timezone "([^"]*)"$`, s.profileTimezone)
	sc.Step(`^the node profile declared for "([^"]*)" allows (\d+) orders at the cutoff at "([^"]*)"$`, s.profileCeiling)
	sc.Step(`^the cutoff at "([^"]*)" of the node profile declared for "([^"]*)" applies on (.+)$`, s.profileDays)
}
