// Package ports declares this context's OUT ports. Per the harness
// architecture fitness test, it contains interfaces and nothing else —
// no struct, no function, no constant.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// NetworkOrderRepo persists NetworkOrder aggregates.
//
// FindByRef returns (nil, nil) when no order has this ref: "not found"
// is the application's concern, not the repository's — the same
// convention every sibling service in this fleet follows.
type NetworkOrderRepo interface {
	Save(ctx context.Context, o *networkorder.NetworkOrder) error
	FindByRef(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error)
	ListUnanswered(ctx context.Context) ([]*networkorder.NetworkOrder, error)
	// ListSubmitted returns every order currently SUBMITTED — accepted
	// in full and told to the network, but not yet reconciled against
	// its transaction-status record (ADR 0001 §5). ReconcileSubmittedOrders
	// is the one caller.
	ListSubmitted(ctx context.Context) ([]*networkorder.NetworkOrder, error)
	// ListAll returns every order regardless of state, for the read-only
	// MCP list_network_orders tool (internal/adapters/inbound/mcp) and
	// operator tooling. ListUnanswered above remains the one the sweep
	// and the OLTP HTTP surface use; this is a separate, explicitly wider
	// query rather than a state filter bolted onto it, so a caller that
	// wants "just the working set" cannot be silently widened by a
	// future change here.
	ListAll(ctx context.Context) ([]*networkorder.NetworkOrder, error)
	// ListOpenHolds returns every order whose order-management hold is
	// recorded and has no outcome, in NEW or REJECTED (ADR 0024 §3): the
	// candidates of SweepOrphanedHolds. It is a narrowing, not the orphan
	// rule: the use case still asks the domain (NetworkOrder.HoldOrphaned)
	// whether acknowledgeBy plus the grace has passed.
	ListOpenHolds(ctx context.Context) ([]*networkorder.NetworkOrder, error)
	// ListUnansweredPage is one keyset page of ListUnanswered for the REST
	// surface: ordered by (acknowledgeBy, networkRef), rows strictly after
	// `after` (nil = first page), at most limit rows.
	ListUnansweredPage(ctx context.Context, after *contract.Keyset, limit int) ([]*networkorder.NetworkOrder, error)
}

// NetworkGateway is the ONLY route to the external network. Every call
// to it goes through one adapter with a NETWORK_MODE switch (ADR 0009
// §4), so that stub is reachable without credentials and a live call is
// impossible to make by accident.
//
// It is expressed entirely in OUR vocabulary. The network's own wire
// shapes — purchase orders, product identifiers, acknowledgement codes,
// selling parties — live inside the implementing adapter and are not
// nameable from here.
type NetworkGateway interface {
	// PollDemand fetches demand the network has for us since a given
	// instant. Inbound is a poll, not a push (ADR 0001 §5).
	PollDemand(ctx context.Context, since time.Time) ([]contract.InboundDemand, error)

	// SubmitAcknowledgement tells the network we will fulfil the order
	// in full, or will not fulfil it at all. Accepted=false is a
	// rejection and carries our reason; the protocol has no middle
	// answer. It is idempotent per ref: calling it again after a crash or
	// a failed call never creates a second answer on the network
	// (ADR 0026).
	SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, answer contract.Acknowledgement) error

	// SubmitInventoryFeed tells the network what a site claims it can ship: one
	// feed per site per recompute pass, never one call per SKU (ADR 0027 §9). It
	// is idempotent per content: a feed the network already holds is not sent
	// again, and a refusal of one line never silences the others. A site with no
	// node on the network yet is contract.ErrSiteNotRegistered.
	SubmitInventoryFeed(ctx context.Context, feed contract.InventoryFeed) (contract.FeedResult, error)

	// DeclareNodeProfile tells the network how a site operates: its cutoffs,
	// handling time and throughput (ADR 0027). It is idempotent per content: the
	// same profile again sends nothing, a changed one becomes the next
	// revision, which the adapter owns and persists and which never goes down.
	// A site with no node on the network yet is contract.ErrSiteNotRegistered.
	DeclareNodeProfile(ctx context.Context, profile contract.NodeProfile) (contract.ProfileResult, error)

	// RequestLabel asks the network for the shipping labels of an order that
	// leaves in packageCount packages (at least one), returning only
	// {labelRef, trackingNumber, carrier} per package — never ship-to PII
	// (ADR 0009 §3). The count is the caller's: this context holds no other
	// package data, and none is needed (ADR 0026). Idempotent per ref.
	RequestLabel(ctx context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error)

	// SubmissionStatus reconciles a previously-submitted acknowledgement
	// against the network's own transaction-status record (ADR 0001 §5).
	// A submission is pending until this reports SUCCESS or FAILURE; a
	// 200 on the original submit call is never itself a completed
	// commitment.
	SubmissionStatus(ctx context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error)

	// SubmitShipmentConfirmation tells the network the order shipped at
	// shippedAt, with the tracking numbers of the labels RequestLabel
	// returned for it. It is an error to confirm an order that has no label.
	// Idempotent per ref: the first shippedAt wins (ADR 0026).
	SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef, shippedAt time.Time) error
}

// FulfillmentPlanner is order-management, seen from here through the
// narrowest possible keyhole.
//
// ADR 0001 §7: feasibility is ASKED of order-management and never
// recomputed in this context. The promise math lives in exactly one
// place in this fleet, and it is not here.
type FulfillmentPlanner interface {
	// RaiseHeldOrder creates an order in order-management that is
	// allocated but deliberately NOT released (its ADR 0020
	// releaseOnAllocation=false), returning the local order id and
	// whether the required ship-by deadline is feasible.
	//
	// Held is the whole point: this context must know whether it CAN
	// fulfil before it answers the network, and must not have put work
	// on the floor for demand it may still reject.
	RaiseHeldOrder(ctx context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error)

	// ReleaseHeldOrder commits a previously held order to the floor,
	// called once the network has been acknowledged.
	ReleaseHeldOrder(ctx context.Context, id shared.LocalOrderId) error

	// CancelHeldOrder abandons a held order, freeing its inventory
	// reservations, when we reject the demand instead.
	CancelHeldOrder(ctx context.Context, id shared.LocalOrderId) error
}

// ProductTranslation maps the network's product identity to ours. It is
// the Anti-Corruption Layer's dictionary, and the single place a
// NetworkProductId becomes a SKU.
type ProductTranslation interface {
	// ToSKU returns shared.ErrUnknownProduct when there is no mapping —
	// a business fact (we cannot sell what we cannot identify), whose
	// correct handling is to reject the order inside the acknowledgement
	// window rather than retry or crash.
	ToSKU(ctx context.Context, id shared.NetworkProductId) (shared.SKU, error)

	// KnownSKUs lists every SKU this dictionary maps TO, deduplicated.
	// RecomputeCapabilityOffers (ADR 0001 §8) is the one caller: it is
	// the universe of SKUs this context has any business advertising to
	// the network at all — a SKU this dictionary never translates FROM
	// a NetworkProductId can never be acknowledged on an order either,
	// so advertising capability for it would be a number the network
	// could never actually act on.
	KnownSKUs(ctx context.Context) ([]shared.SKU, error)
}

// ProcessPathCapability answers what process-path-management's own
// fulfillment-capability contract (its ADR 0010) says about one site:
// its CPT schedule of recurring cutoffs, and which paths are eligible
// for each, plus each eligible path's own cycle-time-p95. ADR 0001 §8's
// throughput formula needs both halves — "can this path make the next
// cutoff" depends on cycle time, and "which cutoff is next" depends on
// the schedule — which is why this port's one query answers both at
// once rather than requiring two round trips that could disagree about
// which cutoff they mean.
type ProcessPathCapability interface {
	// NextCutoff returns the next CPT this (siteId) offers on or after
	// `after`, together with the path(s) eligible for it and each
	// eligible path's own CycleTimeP95 (known=false for a path whose
	// cycle time this cache has not yet observed). found=false means
	// the site has no schedule in the cache yet (not: the site has no
	// SKUs).
	NextCutoff(siteId shared.SiteId, after time.Time) (contract.NextCutoff, bool)

	// Schedule returns the site's whole weekly cutoff schedule, each slot with
	// its next occurrence on or after `after` and the paths eligible for it
	// (ADR 0027): NextCutoff answers only the next cutoff, which cannot describe
	// a week. found=false means the site has no schedule in the cache yet.
	Schedule(siteId shared.SiteId, after time.Time) (contract.CutoffSchedule, bool)
}

// ProfileDeclarer is the part of NetworkGateway that declares a node profile
// (ADR 0027). DeclareNodeProfiles depends on this alone; every NetworkGateway
// satisfies it.
type ProfileDeclarer interface {
	DeclareNodeProfile(ctx context.Context, profile contract.NodeProfile) (contract.ProfileResult, error)
}

// InventoryFeeder is the part of NetworkGateway that feeds inventory (ADR 0027
// §9). FeedInventory depends on this alone; every NetworkGateway satisfies it.
type InventoryFeeder interface {
	SubmitInventoryFeed(ctx context.Context, feed contract.InventoryFeed) (contract.FeedResult, error)
}

// FeedCatalogue answers which SKUs the network lists at a site (ADR 0027 §10):
// the SKUs a feed of that site can carry, and so the ones a complete feed must
// account for. A SKU the catalogue does not list at the site is not fed. It is
// the product dictionary read for one site; it names no network identifier.
type FeedCatalogue interface {
	ListedSKUs(ctx context.Context, siteId shared.SiteId) ([]shared.SKU, error)
}

// ProfileHistory answers what the gateway last declared for a site, in our
// vocabulary (ADR 0027 §3). DeclareNodeProfiles reads it so that a declared
// ceiling is never lowered by an observation. The live gateway implements it from
// its own persisted state; found=false means nothing was declared yet.
type ProfileHistory interface {
	LastDeclared(ctx context.Context, siteId shared.SiteId) (profile contract.NodeProfile, found bool, err error)
}

// ProfileMetrics counts what DeclareNodeProfiles did for each site (ADR 0027 §8),
// one call per site and attempt, and keeps the per-site revision gauge. A nil
// implementation is valid: the use case then counts nothing.
type ProfileMetrics interface {
	Declared(ctx context.Context, site shared.SiteId, result contract.ProfileResult)
}

// PathCapacity answers wes-work-planning's own remaining-admission-
// capacity figure for one path at one CPT cutoff (its PathCapacityChanged
// broadcast). known=false covers both "never observed" and a FlowFed
// path reporting no hard ceiling (ADR 0001 §8 treats both identically:
// capacity is not provably the binding constraint, so the offer must
// not be throttled on an unproven number).
type PathCapacity interface {
	RemainingCapacity(pathId string, cutoffAt time.Time) (units int, known bool)
}

// InventoryAvailability answers inventory-storage's own usable-inventory
// read model for one SKU (`GET /inventory/{sku}/usable`): on-hand minus
// active reservations minus held/unlocated stock.
//
// This is a SYNCHRONOUS call, not a Kafka-fed cache, and that is a
// deliberate, documented departure from ADR 0001 §8's literal "three
// Kafka-fed local caches" wording for this one leg specifically (see
// internal/adapters/outbound/inventoryclient's package doc comment for
// why: inventory-storage's stock-ledger events are published to an
// ANALYTICS-ONLY topic, not a cross-context integration topic, and
// rebuilding its available-quantity projection from that ledger in a
// second repository is exactly the kind of promise-math duplication ADR
// 0001 §7 already rejected for order-management's figures — the same
// reasoning applies symmetrically here). RecomputeCapabilityOffers calls
// this from its own background schedule, never from a request's hot
// path, so the synchronous round trip costs latency on a job already
// built to tolerate it, not on anything network-facing.
type InventoryAvailability interface {
	UsableQuantity(ctx context.Context, sku shared.SKU) (int, error)
}

// CapabilityOfferRepo persists the latest recomputed CapabilityOffer per
// (SKU, SiteId). Each recompute pass is a full, replaceable snapshot
// (the aggregate's own doc comment) — Save upserts, it never appends a
// history.
type CapabilityOfferRepo interface {
	Save(ctx context.Context, o capabilityoffer.CapabilityOffer) error
	// ListAll returns every currently-stored offer, for the read-only
	// REST/MCP surface. Iteration order is deliberately not specified.
	ListAll(ctx context.Context) ([]capabilityoffer.CapabilityOffer, error)
}

// CapabilityOfferPager serves the REST collection GET /capability-offers.
type CapabilityOfferPager interface {
	// ListPage is one keyset page: ordered by (sku, siteId), rows strictly
	// after `after` (Sort = sku, ID = siteId; nil = first page), at most
	// limit rows.
	ListPage(ctx context.Context, after *contract.Keyset, limit int) ([]capabilityoffer.CapabilityOffer, error)
}

// EventPublisher publishes this context's integration events.
type EventPublisher interface {
	Publish(ctx context.Context, event any) error
}

// UnitOfWork brackets a use case's state change and the domain event(s)
// it raises so both commit or neither does (ADR 0003, transactional
// outbox — mirrors process-path-management's ADR 0003 port of the same
// name).
//
// Execute runs fn inside one atomic scope. Every Repo.Save and
// EventPublisher.Publish made with the ctx handed to fn is bound to that
// same scope: if fn returns an error the scope is rolled back and
// nothing — neither the aggregate row nor the outbox row(s) — is visible
// afterwards.
//
// Adapters that have no transactional backing (the in-memory repo, the
// log publisher, or the direct Kafka publisher used when DATABASE_URL is
// unset) satisfy this with a pass-through that simply calls fn; the use
// cases stay adapter-agnostic either way.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// OrphanHoldMetrics counts what the orphaned-hold sweep did with each hold
// (ADR 0024 §7), one call per hold. A nil implementation is valid: the sweep
// then counts nothing.
type OrphanHoldMetrics interface {
	Swept(ctx context.Context, outcome contract.HoldSweepOutcome)
}

// Clock is the only source of time in the application layer, so the
// acknowledgement window and its sweep are testable without sleeping.
type Clock interface {
	Now() time.Time
}
