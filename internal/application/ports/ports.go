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
	// in full, or will not fulfil it at all. accepted=false is a
	// rejection; the protocol has no middle answer.
	SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, accepted bool) error

	// SubmitAvailability tells the network what we claim we can ship for
	// one SKU at one site (ADR 0001's submitInventoryUpdate signal).
	SubmitAvailability(ctx context.Context, update contract.AvailabilityUpdate) error

	// DeclareCapability would tell the network our cutoffs and cycle
	// times directly. See contract.CapabilityDeclaration's doc comment:
	// the network's real API has no such operation, so every adapter in
	// this codebase today treats this as a documented no-op kept for
	// port-shape completeness and for a future program that might accept
	// one.
	DeclareCapability(ctx context.Context, offer contract.CapabilityDeclaration) error

	// RequestLabel asks the network for a shipping label, returning only
	// {labelRef, trackingNumber, carrier} — never ship-to PII (ADR 0009
	// §3).
	RequestLabel(ctx context.Context, ref shared.NetworkRef) (contract.LabelResult, error)

	// SubmissionStatus reconciles a previously-submitted acknowledgement
	// against the network's own transaction-status record (ADR 0001 §5).
	// A submission is pending until this reports SUCCESS or FAILURE; a
	// 200 on the original submit call is never itself a completed
	// commitment.
	SubmissionStatus(ctx context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error)

	// SubmitShipmentConfirmation tells the network the order has
	// shipped.
	SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef) error
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

// Clock is the only source of time in the application layer, so the
// acknowledgement window and its sweep are testable without sleeping.
type Clock interface {
	Now() time.Time
}
