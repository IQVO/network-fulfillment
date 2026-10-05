// Package capabilityoffer models what this context tells the network it
// can ship for one SKU at one site — ADR 0001's "one genuinely new
// domain concept".
//
// Most integrations of this shape advertise straight from physical
// stock on hand. This aggregate can do better, because the fleet knows
// something a stock ledger does not: whether the building can actually
// MOVE those units before the next departure. advertisedQuantity is
// therefore the lesser of physical availability and what the eligible
// process path(s) can carry before their next cutoff — never physical
// alone.
package capabilityoffer

import (
	"errors"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Basis names WHICH rule produced AdvertisedQuantity. Recording it is
// what makes a throttled offer auditable ("we are advertising 120 of
// 400 because the path cannot carry more before the cutoff") rather than
// a silent haircut — the same discipline order-management's own
// PromiseBasis already establishes for promises.
type Basis string

const (
	// BasisPhysical: physical stock on hand was the binding constraint
	// (the throughput-feasible figure was at or above it).
	BasisPhysical Basis = "PHYSICAL"

	// BasisThroughputConstrained: the eligible path's remaining
	// admission capacity before its next cutoff was BELOW physical
	// stock, so the lower, honest number was advertised instead.
	BasisThroughputConstrained Basis = "THROUGHPUT_CONSTRAINED"
)

var (
	// ErrEmptySKU rejects an offer naming no product.
	ErrEmptySKU = errors.New("capability offer: sku must not be empty")

	// ErrEmptySiteId rejects an offer naming no site.
	ErrEmptySiteId = errors.New("capability offer: site id must not be empty")

	// ErrNegativeQuantity rejects a negative advertised quantity. Zero
	// is valid and meaningful (nothing sellable right now); negative
	// is never a real inventory position.
	ErrNegativeQuantity = errors.New("capability offer: advertised quantity must not be negative")

	// ErrAdvertisedExceedsPhysical enforces the aggregate's one
	// invariant: we must never tell the network we can ship more than
	// physically exists, regardless of which basis produced the
	// figure.
	ErrAdvertisedExceedsPhysical = errors.New("capability offer: advertised quantity must not exceed physical availability")

	// ErrUnknownBasis rejects any value other than the two declared
	// above — this field is closed, not an open string.
	ErrUnknownBasis = errors.New("capability offer: unknown basis")
)

// CapabilityOffer is the recomputed-on-a-schedule figure this context
// would tell the network it can ship for one (SKU, SiteId) pair.
//
// It is NOT built by a constructor-then-mutate sequence the way
// NetworkOrder is: a CapabilityOffer has no lifecycle of its own
// (accept/reject/confirm) — it is a pure, replaceable snapshot, fully
// superseded by its own next recompute. New is therefore the only way
// to produce one.
type CapabilityOffer struct {
	sku                shared.SKU
	siteId             shared.SiteId
	advertisedQuantity int
	basis              Basis
	computedAt         time.Time
}

// New validates and constructs a CapabilityOffer. physicalAvailable is
// passed alongside advertisedQuantity (rather than derived from it)
// purely so the invariant below can be checked here, at the one point
// in the codebase that is allowed to construct this aggregate.
func New(sku shared.SKU, siteId shared.SiteId, advertisedQuantity int, physicalAvailable int, basis Basis, computedAt time.Time) (CapabilityOffer, error) {
	if sku == "" {
		return CapabilityOffer{}, ErrEmptySKU
	}
	if siteId == "" {
		return CapabilityOffer{}, ErrEmptySiteId
	}
	if advertisedQuantity < 0 {
		return CapabilityOffer{}, ErrNegativeQuantity
	}
	if advertisedQuantity > physicalAvailable {
		return CapabilityOffer{}, ErrAdvertisedExceedsPhysical
	}
	switch basis {
	case BasisPhysical, BasisThroughputConstrained:
	default:
		return CapabilityOffer{}, ErrUnknownBasis
	}
	return CapabilityOffer{
		sku:                sku,
		siteId:             siteId,
		advertisedQuantity: advertisedQuantity,
		basis:              basis,
		computedAt:         computedAt.UTC(),
	}, nil
}

// Compute derives a CapabilityOffer from the two inputs the ADR's
// formula names: physicalAvailable (inventory-storage) and
// throughputFeasible (the lesser of what the eligible path can carry
// before its next cutoff). throughputKnown=false means no capacity
// figure could be determined at all (no eligible path, or the cache has
// not yet replayed) — treated as "capacity is not the binding
// constraint we can currently prove", so the offer falls back to
// physical rather than silently advertising zero.
//
//	advertisedQuantity = min(physicalAvailable, throughputFeasible)
//
// matching ADR 0001's own formula exactly.
func Compute(sku shared.SKU, siteId shared.SiteId, physicalAvailable int, throughputFeasible int, throughputKnown bool, computedAt time.Time) (CapabilityOffer, error) {
	if !throughputKnown || throughputFeasible >= physicalAvailable {
		return New(sku, siteId, physicalAvailable, physicalAvailable, BasisPhysical, computedAt)
	}
	qty := throughputFeasible
	if qty < 0 {
		qty = 0
	}
	return New(sku, siteId, qty, physicalAvailable, BasisThroughputConstrained, computedAt)
}

func (o CapabilityOffer) SKU() shared.SKU         { return o.sku }
func (o CapabilityOffer) SiteId() shared.SiteId   { return o.siteId }
func (o CapabilityOffer) AdvertisedQuantity() int { return o.advertisedQuantity }
func (o CapabilityOffer) Basis() Basis            { return o.basis }
func (o CapabilityOffer) ComputedAt() time.Time   { return o.computedAt }
