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
	"errors"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Errors a FulfillmentPlanner returns for the two answers of
// order-management's cancel (DELETE /orders/{id}) that are ANSWERS and not
// failures (ADR 0024 §4). Anything else it returns is a failure to retry.
var (
	// ErrHeldOrderNotFound: order-management does not know the order (404).
	// For a cancel that is done: the hold, and its reservations, are gone.
	ErrHeldOrderNotFound = errors.New("order-management: held order not found")

	// ErrHeldOrderAlreadyReleased: order-management refused the cancel
	// because the order was already released (409, or 422). It is not an
	// orphan: someone committed it to the floor.
	ErrHeldOrderAlreadyReleased = errors.New("order-management: held order already released")
)

// HoldSweepOutcome is the label of the orphaned-hold sweep's counter.
type HoldSweepOutcome string

const (
	// HoldSweepCancelled: the orphaned hold was cancelled (or found gone).
	HoldSweepCancelled HoldSweepOutcome = "cancelled"
	// HoldSweepAlreadyReleased: not an orphan, order-management had released it.
	HoldSweepAlreadyReleased HoldSweepOutcome = "already_released"
	// HoldSweepFailed: the cancel or its record failed; retried next pass.
	HoldSweepFailed HoldSweepOutcome = "failed"
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
	// AcknowledgeBy is the deadline the network itself set for answering this
	// demand (ADR 0026). Zero means the network gave none, and the local
	// acknowledgement window from receipt applies; non-zero replaces that
	// window, so the sweeps judge the order by the network's real clock.
	AcknowledgeBy time.Time
	Lines         []InboundLine
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

// NodeProfile is DeclareNodeProfile's argument (ADR 0027): what we declare about
// how a site operates, in OUR terms. The network keeps it as the node's
// operational profile, and its handling time becomes part of the deadline we are
// then judged by, so a profile is only ever built from what the local caches
// actually know, never from a guess.
//
// It replaces the per-SKU CapabilityDeclaration that ADR 0001 sketched and that no
// adapter ever acted on: a capability belongs to a node, the quantity of a SKU to
// the inventory feed.
type NodeProfile struct {
	SiteId shared.SiteId
	// Timezone is the IANA zone the cutoffs' local times are read in.
	Timezone string
	// Cutoffs are the recurring handover times, one per distinct local time.
	Cutoffs []ProfileCutoff
	// HandlingTime is how long a release takes before it can be handed over: the
	// longest known cycle-time-p95 among the eligible paths (ADR 0027 §3).
	HandlingTime time.Duration
	// ThroughputPerDay is the most orders the site declares it can take on its
	// busiest weekday.
	ThroughputPerDay int
	// Fingerprint identifies the content above. The gateway treats it as opaque:
	// equal fingerprints are the same declaration, a different one is a new
	// revision.
	Fingerprint string
}

// ProfileCutoff is one recurring handover time of a NodeProfile.
type ProfileCutoff struct {
	// LocalTime is HH:mm in the profile's Timezone.
	LocalTime string
	// Days are the weekdays it applies to, Sunday first, no duplicates.
	Days []time.Weekday
	// MaxOrders is the declared ceiling of orders for this cutoff; 0 means not
	// known (the declaration then says nothing about it).
	MaxOrders int
}

// ProfileOutcome is what one DeclareNodeProfiles attempt came to for a site, and
// the label of its counter (ADR 0027 §8).
type ProfileOutcome string

const (
	// ProfileDeclared: a revision was written to the network in this call and has
	// not been settled yet.
	ProfileDeclared ProfileOutcome = "declared"
	// ProfilePending: a revision written earlier is still being processed.
	ProfilePending ProfileOutcome = "pending"
	// ProfileConfirmed: the network settled the revision as accepted in this call.
	ProfileConfirmed ProfileOutcome = "confirmed"
	// ProfileUnchanged: the content is the one the network already holds; nothing
	// was sent.
	ProfileUnchanged ProfileOutcome = "unchanged"
	// ProfileFailed: the network refused the revision, or the call failed.
	ProfileFailed ProfileOutcome = "failed"
	// ProfileSkippedNoNode: the site has no node on the network yet.
	ProfileSkippedNoNode ProfileOutcome = "skipped_no_node"
	// ProfileSkippedNoSchedule: the cache has no cutoff schedule for the site yet.
	ProfileSkippedNoSchedule ProfileOutcome = "skipped_no_schedule"
	// ProfileSkippedIncomplete: the caches know the schedule but not enough to
	// declare (no cycle time, or no capacity figure) and a guess is never declared.
	ProfileSkippedIncomplete ProfileOutcome = "skipped_incomplete"
)

// ProfileResult is DeclareNodeProfile's answer.
type ProfileResult struct {
	Outcome ProfileOutcome
	// Revision is the profile revision this call concerned (0 when none).
	Revision int
	// Errors carries the network's error list for a ProfileFailed outcome.
	Errors string
}

// ErrSiteNotRegistered: the gateway has no node for the site, so nothing can be
// declared for it yet. It is a state, not a failure: the node is registered by
// the demand loop (ADR 0026 Decision 7) and the next attempt finds it.
var ErrSiteNotRegistered = errors.New("network gateway: the site has no node on the network yet")

// CutoffSchedule is ProcessPathCapability.Schedule's answer: a site's whole
// weekly cutoff schedule, where NextCutoff names only the next cutoff.
type CutoffSchedule struct {
	// Timezone is the IANA zone the slots' local times are read in.
	Timezone string
	Slots    []ScheduleSlot
}

// ScheduleSlot is one recurring cutoff with the paths eligible for it.
type ScheduleSlot struct {
	// LocalTime is HH:mm.
	LocalTime string
	// Days are the weekdays the cutoff applies to.
	Days []time.Weekday
	// NextAt is the instant of its next occurrence on or after the time the
	// schedule was asked for, resolved the way NextCutoff resolves it (so it
	// matches the keys of PathCapacity); zero when none falls in the lookahead.
	NextAt time.Time
	Paths  []EligiblePath
}

// Acknowledgement is our one answer to a piece of demand (ADR 0026): accept it
// in full, or reject it with the reason this context gives. The reason is OUR
// vocabulary (shared.RejectionReason); mapping it to whatever the network calls
// it is the adapter's job. Reason is empty exactly when Accepted is true.
type Acknowledgement struct {
	Accepted bool
	Reason   shared.RejectionReason
}

// LabelResult is what RequestLabel returns, once per package: exactly the three fields ADR
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
	// SubmissionNotSent: the gateway holds no record that the answer was ever
	// sent (a crash between recording the order as SUBMITTED and the call that
	// tells the network). The network cannot settle what it never received, so
	// ReconcileSubmittedOrders sends the answer again; the gateway keeps that
	// call idempotent (ADR 0026).
	SubmissionNotSent SubmissionStatusValue = "NOT_SENT"
)

// HeldOrderRequest asks order-management for an allocated-but-held
// order, and for a verdict on whether the network's deadline can be met.
//
// It carries SKUs, quantities, a site and a deadline — and nothing else.
// order-management never learns that a network exists, which is the
// boundary ADR 0001 §2 draws and the reason the Amazon vocabulary cannot
// leak inward.
type HeldOrderRequest struct {
	// NetworkRef identifies the network order this hold is raised for. It is
	// the source of the Idempotency-Key order-management requires on
	// POST /orders (its idempotency middleware), so a retried raise replays
	// the same local order instead of creating a duplicate. Required.
	NetworkRef     shared.NetworkRef
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

// NextCutoff is ports.ProcessPathCapability.NextCutoff's result: one
// upcoming CPT and the paths eligible to make it.
type NextCutoff struct {
	CutoffAt time.Time
	Paths    []EligiblePath
}

// EligiblePath is one path eligible for a NextCutoff, with its own
// cycle-time-p95 (process-path-management's ADR 0010 figure).
type EligiblePath struct {
	PathId         string
	CycleTimeP95   time.Duration
	CycleTimeKnown bool
}
