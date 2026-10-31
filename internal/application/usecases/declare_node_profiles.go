package usecases

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// DeclareNodeProfiles declares, for each site this service runs, how the site
// operates: its cutoffs, handling time and throughput (ADR 0027). The data comes
// only from the local caches (the CPT schedule and cycle times of
// process-path-management, the remaining capacity of wes-work-planning): there is
// never a synchronous call to either, so a neighbour's outage cannot stop a
// declaration.
//
// A profile the caches cannot support is not declared, and the reason is logged:
// the handling time declared here becomes part of the deadline the network then
// judges our acknowledgements by, so a guess is worse than no declaration.
//
// The revision and the idempotency of the write belong to the gateway: calling
// this every few minutes with unchanged caches sends nothing.
type DeclareNodeProfiles struct {
	Gateway  ports.ProfileDeclarer
	PathCap  ports.ProcessPathCapability
	Capacity ports.PathCapacity
	Clock    ports.Clock
	Logger   *slog.Logger

	// History lets a declared ceiling survive an observation that fell because
	// work was admitted. Nil means no memory (every ceiling is the observed one).
	History ports.ProfileHistory
	// Metrics is optional.
	Metrics ports.ProfileMetrics

	// Sites are the sites to declare for.
	Sites []shared.SiteId
}

// DeclareResult summarises one pass: how many sites ended in each outcome.
type DeclareResult struct {
	Outcomes map[contract.ProfileOutcome]int
}

// Execute declares every site in turn. One site's failure never stops the
// others; a cancelled context stops the pass before the next site.
func (u *DeclareNodeProfiles) Execute(ctx context.Context) DeclareResult {
	res := DeclareResult{Outcomes: map[contract.ProfileOutcome]int{}}
	for _, site := range u.Sites {
		if ctx.Err() != nil {
			break
		}
		r := u.declareSite(ctx, site)
		res.Outcomes[r.Outcome]++
		if u.Metrics != nil {
			u.Metrics.Declared(ctx, site, r)
		}
	}
	return res
}

func (u *DeclareNodeProfiles) declareSite(ctx context.Context, site shared.SiteId) contract.ProfileResult {
	profile, skipped := u.build(ctx, site)
	if skipped != "" {
		return contract.ProfileResult{Outcome: skipped}
	}
	res, err := u.Gateway.DeclareNodeProfile(ctx, profile)
	switch {
	case errors.Is(err, contract.ErrSiteNotRegistered):
		u.log(ctx, slog.LevelInfo, "node profile: the site has no node on the network yet; declared once it has one", site)
		return contract.ProfileResult{Outcome: contract.ProfileSkippedNoNode}
	case err != nil:
		u.log(ctx, slog.LevelError, "node profile: the declaration could not be made; the next pass retries", site, "err", err)
		return contract.ProfileResult{Outcome: contract.ProfileFailed}
	}
	return res
}

// build derives the site's profile from the caches, or says why it cannot (the
// skip outcome; the reason has been logged).
func (u *DeclareNodeProfiles) build(ctx context.Context, site shared.SiteId) (contract.NodeProfile, contract.ProfileOutcome) {
	now := u.Clock.Now()
	schedule, found := u.PathCap.Schedule(site, now)
	if !found {
		u.log(ctx, slog.LevelWarn, "node profile: no cutoff schedule in the cache for the site yet; nothing is declared", site)
		return contract.NodeProfile{}, contract.ProfileSkippedNoSchedule
	}
	slots := mergeSlots(schedule.Slots)
	handling := handlingTime(slots)
	if handling == 0 {
		u.log(ctx, slog.LevelWarn, "node profile: no eligible path has a cycle time that fits the schedule yet, so there is no handling time to declare; nothing is declared", site)
		return contract.NodeProfile{}, contract.ProfileSkippedIncomplete
	}
	cutoffs, ok := u.cutoffs(ctx, site, slots, now)
	if !ok {
		return contract.NodeProfile{}, contract.ProfileFailed
	}
	perDay := throughputPerDay(cutoffs)
	if perDay == 0 {
		u.log(ctx, slog.LevelWarn, "node profile: no capacity figure is known for any cutoff, so there is no throughput to declare; nothing is declared", site)
		return contract.NodeProfile{}, contract.ProfileSkippedIncomplete
	}
	p := contract.NodeProfile{SiteId: site, Timezone: schedule.Timezone, Cutoffs: cutoffs, HandlingTime: handling, ThroughputPerDay: perDay}
	p.Fingerprint = fingerprint(p)
	return p, ""
}

// cutoffs turns the slots into the profile's cutoffs, each ceiling being the
// larger of what the capacity cache says now and what was last declared for the
// same slot. ok=false means the memory could not be read: declaring without it
// could lower a ceiling, so the pass leaves the site for the next one.
func (u *DeclareNodeProfiles) cutoffs(ctx context.Context, site shared.SiteId, slots []slot, now time.Time) ([]contract.ProfileCutoff, bool) {
	last, ok := u.lastCeilings(ctx, site)
	if !ok {
		return nil, false
	}
	out := make([]contract.ProfileCutoff, len(slots))
	for i, s := range slots {
		observed := slotCeiling(s, now, u.Capacity.RemainingCapacity)
		out[i] = contract.ProfileCutoff{
			LocalTime: s.localTime, Days: s.days,
			MaxOrders: max(observed, last[slotKey(s.localTime, s.days)]),
		}
	}
	return out, true
}

// lastCeilings are the ceilings of the profile last declared, by slot.
func (u *DeclareNodeProfiles) lastCeilings(ctx context.Context, site shared.SiteId) (map[string]int, bool) {
	out := map[string]int{}
	if u.History == nil {
		return out, true
	}
	prev, found, err := u.History.LastDeclared(ctx, site)
	if err != nil {
		u.log(ctx, slog.LevelError, "node profile: what was last declared could not be read; the next pass retries", site, "err", err)
		return nil, false
	}
	if found {
		for _, c := range prev.Cutoffs {
			out[slotKey(c.LocalTime, c.Days)] = c.MaxOrders
		}
	}
	return out, true
}

func (u *DeclareNodeProfiles) log(ctx context.Context, level slog.Level, msg string, site shared.SiteId, attrs ...any) {
	if u.Logger == nil {
		return
	}
	u.Logger.Log(ctx, level, msg, append([]any{"site", string(site)}, attrs...)...)
}
