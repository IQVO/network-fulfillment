// Package contract holds the data shapes that cross this context's OUT
// ports — the arguments and results of ports.NetworkGateway and
// ports.FulfillmentPlanner.
//
// They live here rather than in ports because the harness architecture
// fitness test requires internal/application/ports to contain
// INTERFACES AND NOTHING ELSE, and because keeping the shapes separate
// from the interfaces makes it obvious that they are part of the
// contract rather than of any one adapter.
//
// Everything here is expressed in OUR vocabulary. No network
// identifier format, no Amazon wire field, no HTTP or SQL type appears
// in this package — that translation is the job of the adapter on the
// far side of the port (ADR 0001 §2).
package contract

import (
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// InboundDemand is one unit of demand as it arrives from the network,
// BEFORE translation into the domain. Product ids are still the
// network's, because translating them is precisely what the receiving
// use case does — and a translation failure is a business decision
// (reject the order), not a parse error to be buried in an adapter.
type InboundDemand struct {
	NetworkRef     shared.NetworkRef
	SiteId         shared.SiteId
	RequiredShipBy time.Time
	Lines          []InboundLine
}

// InboundLine is one line of untranslated inbound demand.
type InboundLine struct {
	NetworkLineRef   shared.NetworkLineRef
	NetworkProductId shared.NetworkProductId
	Quantity         int
}

// AvailabilityUpdate is what we claim we can ship for one SKU at one
// site — submitInventoryUpdate in the network's own vocabulary (ADR 0001,
// "The obvious next question"). The quantity itself is computed by
// CapabilityOffer's capacity-aware advertised availability; this shape
// only carries the result outward across NetworkGateway.SubmitAvailability.
type AvailabilityUpdate struct {
	SiteId   shared.SiteId
	SKU      shared.SKU
	Quantity int
}

// CapabilityDeclaration is DeclareCapability's argument. ADR 0001's own
// research found NO operation in the network's real API surface that
// accepts a capability declaration directly — cutoffs and cycle times are
// Vendor Central configuration, not an API surface (see
// references/network-fulfillment-sp-api-boundary.md). This port method
// exists so the shape ADR 0001 §"Decision" originally sketched
// (PollDemand, SubmitAnswer, SubmitAvailability, DeclareCapability,
// RequestLabel, SubmitShipmentConfirmation, SubmissionStatus) is complete
// and a future program that DOES accept a declaration (ADR 0009 §"A real
// SP-API adapter is deferred, not dropped") is an adapter swap, not a
// port redesign. Every adapter in this codebase today treats it as a
// documented no-op.
type CapabilityDeclaration struct {
	SiteId             shared.SiteId
	SKU                shared.SKU
	AdvertisedQuantity int
}

// LabelResult is what RequestLabel returns: exactly the three fields ADR
// 0009 §3 allows to cross into this context — never a ship-to name,
// address or phone number, in either direction.
type LabelResult struct {
	LabelRef       string
	TrackingNumber string
	Carrier        string
}

// SubmissionStatusValue is transaction-status reconciliation's answer for
// one previously-submitted acknowledgement or shipment confirmation (ADR
// 0001 §5: asynchronous submission modeled as pending-until-reconciled,
// never done-on-HTTP-200).
type SubmissionStatusValue string

const (
	// SubmissionPending: the network has not yet settled the submission
	// one way or the other. Not an error — a caller should leave the
	// order as-is and retry reconciliation on the next pass.
	SubmissionPending SubmissionStatusValue = "PENDING"
	// SubmissionSuccess: the network confirms it holds the submission we
	// sent. ReconcileSubmittedOrders settles the order to ACKNOWLEDGED.
	SubmissionSuccess SubmissionStatusValue = "SUCCESS"
	// SubmissionFailure: the network rejected the submission itself —
	// distinct from OUR domain rejecting the demand. ReconcileSubmittedOrders
	// rejects the order with RejectionReasonSubmissionFailed.
	SubmissionFailure SubmissionStatusValue = "FAILURE"
)

// HeldOrderRequest asks order-management for an allocated-but-held
// order, and for a verdict on whether the network's deadline can be met.
//
// It carries SKUs, quantities, a site and a deadline — and nothing else.
// order-management never learns that a network exists, which is the
// boundary ADR 0001 §2 draws and the reason the Amazon vocabulary cannot
// leak inward.
type HeldOrderRequest struct {
	SiteId         shared.SiteId
	RequiredShipBy time.Time
	Lines          map[shared.SKU]int
}

// HeldOrderResult is order-management's answer.
//
// Feasible is the verdict from its PromisePolicy.FeasibleBy — computed
// there, never recomputed here (ADR 0001 §7). PromisedCutoff is the
// departure it committed to, meaningful only when Feasible is true.
type HeldOrderResult struct {
	LocalOrderId   shared.LocalOrderId
	Feasible       bool
	PromisedCutoff time.Time
}
