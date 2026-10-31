// Package usecases implements this context's application services —
// the orchestration layer between inbound adapters and the domain.
package usecases

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// RecomputeCapabilityOffers recomputes and persists, on a schedule
// (ADR 0001 §8), the CapabilityOffer this context would currently
// advertise for every SKU it knows how to sell to the network, at the
// site process-path-management's cache is scoped to.
//
//	advertisedQuantity = min(physicalAvailable, throughputFeasible)
//
// matching ADR 0001's own formula. physicalAvailable comes from
// inventory-storage (InventoryAvailability, a synchronous call -- see
// that port's doc comment for why). throughputFeasible is the remaining
// admission capacity, summed across every path eligible for the site's
// next FEASIBLE CPT cutoff (the first one some path's cycle time can still
// make, ADR 0031), for whichever of those paths this cache has actually
// observed a capacity figure for; a path with no observed figure
// contributes nothing to the sum rather than being assumed unlimited.
//
// This step of the ADR's own staged rollout ("the actual value, still
// entirely in stub") computes and persists the offer for the read
// surface. It calls no gateway: the outward submission is FeedInventory's
// (ADR 0027 part 2), which reads what this use case persisted.
type RecomputeCapabilityOffers struct {
	Translation ports.ProductTranslation
	Inventory   ports.InventoryAvailability
	PathCap     ports.ProcessPathCapability
	Capacity    ports.PathCapacity
	Offers      ports.CapabilityOfferRepo
	Clock       ports.Clock
	Logger      *slog.Logger

	// SiteId is the single site this recompute pass targets.
	// ADR 0001 §Consequences notes order-management's own single-site
	// simplification is inherited unmodified; this field is this
	// context's own mirror of that same simplification, not a new one.
	SiteId shared.SiteId
}

// Result summarizes one recompute pass, for logging at the call site.
type Result struct {
	Examined              int
	ThroughputConstrained int
}

// Execute recomputes and saves one CapabilityOffer per known SKU.
// A single SKU's failure (an inventory-storage call error) is logged
// and skipped, not aborting the whole pass -- the same
// "one bad row never blocks the rest" convention
// SweepAcknowledgementDeadlines and the outbox relay already follow in
// this codebase.
func (u *RecomputeCapabilityOffers) Execute(ctx context.Context) (Result, error) {
	skus, err := u.Translation.KnownSKUs(ctx)
	if err != nil {
		return Result{}, err
	}

	now := u.Clock.Now()
	// The feasible cutoff and its capacity do not depend on the SKU: resolve
	// them once per pass, so every SKU is advertised against the same cutoff.
	feasible, known := u.throughputFeasible(now)

	var res Result
	for _, sku := range skus {
		res.Examined++

		physical, err := u.Inventory.UsableQuantity(ctx, sku)
		if err != nil {
			if u.Logger != nil {
				u.Logger.ErrorContext(ctx, "capability offer: usable inventory lookup failed",
					"sku", sku, "err", err)
			}
			continue
		}

		offer, err := capabilityoffer.Compute(sku, u.SiteId, physical, feasible, known, now)
		if err != nil {
			if u.Logger != nil {
				u.Logger.ErrorContext(ctx, "capability offer: compute failed",
					"sku", sku, "err", err)
			}
			continue
		}
		if offer.Basis() == capabilityoffer.BasisThroughputConstrained {
			res.ThroughputConstrained++
		}
		if err := u.Offers.Save(ctx, offer); err != nil {
			if u.Logger != nil {
				u.Logger.ErrorContext(ctx, "capability offer: save failed",
					"sku", sku, "err", err)
			}
			continue
		}
	}
	return res, nil
}

// feasibleCutoffHorizon is how far ahead of now the walk to the next feasible
// cutoff looks (ADR 0031). A CPT schedule repeats weekly, so if no cutoff in
// one week can be made, none can until a cycle time changes.
const feasibleCutoffHorizon = 7 * 24 * time.Hour

// throughputFeasible sums RemainingCapacity across every path eligible for
// the site's next FEASIBLE cutoff that this cache has actually observed a
// capacity figure for. known=false when there is no schedule at all, or
// not one path's capacity has ever been observed -- meaning capacity is
// not a PROVEN constraint, so the caller falls back to physical rather
// than throttling on a number that was never actually measured.
//
// Eligibility (ADR 0017): a path counts only if its CycleTimeP95 fits
// the time left to the cutoff (CycleTimeP95 <= cutoff - now). A path with
// no cycle-time data (unknown, or zero) stays eligible -- no data is not
// evidence -- and capacity is never scaled by cycle time.
//
// Feasibility (ADR 0031, amending ADR 0017): a cutoff for which every path is
// ruled out by cycle time is not "cannot ship" but "misses this truck"; the
// walk moves on to the following cutoff, and the one after, up to
// feasibleCutoffHorizon from now. Only when no cutoff inside that bound can be
// made is the result a proven zero (known=true).
func (u *RecomputeCapabilityOffers) throughputFeasible(now time.Time) (int, bool) {
	next, hasSchedule := u.PathCap.NextCutoff(u.SiteId, now)
	if !hasSchedule {
		return 0, false
	}
	horizon := now.Add(feasibleCutoffHorizon)
	for !anyPathFits(next, now) {
		// NextCutoff's `after` is inclusive: ask strictly after the cutoff
		// just ruled out, or the cache answers the same one again.
		later, found := u.PathCap.NextCutoff(u.SiteId, next.CutoffAt.Add(time.Nanosecond))
		if !found || !later.CutoffAt.After(next.CutoffAt) || later.CutoffAt.After(horizon) {
			return 0, true
		}
		next = later
	}
	return u.capacityBefore(next, now)
}

// anyPathFits reports whether the cutoff can be made by at least one of its
// paths. A cutoff with no path listed is not ruled out by cycle time: it is
// the "nothing observed" case, left to capacityBefore's fallback.
func anyPathFits(next contract.NextCutoff, now time.Time) bool {
	if len(next.Paths) == 0 {
		return true
	}
	window := next.CutoffAt.Sub(now)
	for _, p := range next.Paths {
		if cycleTimeFits(p, window) {
			return true
		}
	}
	return false
}

// capacityBefore sums the remaining capacity, at this cutoff, of the paths
// whose cycle time fits it and whose figure is known; known=false when no
// eligible path's figure has been observed.
func (u *RecomputeCapabilityOffers) capacityBefore(next contract.NextCutoff, now time.Time) (int, bool) {
	window := next.CutoffAt.Sub(now)
	total := 0
	anyKnown := false
	for _, p := range next.Paths {
		if !cycleTimeFits(p, window) {
			continue
		}
		units, known := u.Capacity.RemainingCapacity(p.PathId, next.CutoffAt)
		if !known {
			continue
		}
		anyKnown = true
		total += units
	}
	return total, anyKnown
}

// cycleTimeFits reports whether p can complete work inside window. Missing
// or zero cycle time fails open (true).
func cycleTimeFits(p contract.EligiblePath, window time.Duration) bool {
	if !p.CycleTimeKnown || p.CycleTimeP95 <= 0 {
		return true
	}
	return p.CycleTimeP95 <= window
}
