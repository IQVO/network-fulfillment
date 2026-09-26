package shared

import "time"

// DomainEvent is the minimal shape every event raised by this domain
// implements — mirrors the same lightweight interface used across the
// fleet's other services (see e.g. process-path-management's or
// labor-performance's domain/shared package), which the outbound Kafka
// adapter wraps in a CloudEvents-style envelope, not the domain layer.
type DomainEvent interface {
	EventName() string
	OccurredAt() time.Time
}

// NetworkOrderReceived is raised the moment demand from the network is
// turned into a NetworkOrder aggregate — whether or not every line could
// be translated. It is the fact "the network sent us this, and the
// acknowledgement clock is now running", independent of how we go on to
// answer it.
type NetworkOrderReceived struct {
	NetworkRef     NetworkRef
	SiteId         SiteId
	RequiredShipBy time.Time
	AcknowledgeBy  time.Time
	// LineCount is the number of lines that were successfully translated
	// into our vocabulary. Zero for demand rejected as untranslatable
	// (ReceiveUntranslatable) — that order is lineless by construction.
	LineCount int
	At        time.Time
}

func (e NetworkOrderReceived) EventName() string     { return "NetworkOrderReceived" }
func (e NetworkOrderReceived) OccurredAt() time.Time { return e.At }

// NetworkOrderAcknowledged is raised once we have committed to fulfilling
// an order in full: the network has been told yes, and a local order has
// been raised (and is about to be released) in order-management.
type NetworkOrderAcknowledged struct {
	NetworkRef   NetworkRef
	SiteId       SiteId
	LocalOrderId LocalOrderId
	// ReceivedAt is carried alongside At so a consumer can compute
	// acknowledgement latency (At - ReceivedAt) without a second lookup —
	// exactly the "Acknowledgement & Translation" report's own metric.
	ReceivedAt time.Time
	At         time.Time
}

func (e NetworkOrderAcknowledged) EventName() string     { return "NetworkOrderAcknowledged" }
func (e NetworkOrderAcknowledged) OccurredAt() time.Time { return e.At }

// RejectionReason distinguishes WHY an order was refused, so a consumer
// (in particular the analytics data product) can split rejections by
// cause rather than treat every "no" the same way — an untranslatable
// product is a catalogue gap, an infeasible deadline is a capacity
// signal, and a missed acknowledgement window is an operational failure.
// These three are exhaustive over every path that calls
// NetworkOrder.Reject() in this codebase today.
type RejectionReason string

const (
	// RejectionReasonUntranslatableSKU: the Anti-Corruption Layer had no
	// SKU mapping for one or more lines (shared.ErrUnknownProduct).
	RejectionReasonUntranslatableSKU RejectionReason = "UNTRANSLATABLE_SKU"

	// RejectionReasonInfeasibleDeadline: order-management's promise
	// policy could not meet the network's requiredShipBy.
	RejectionReasonInfeasibleDeadline RejectionReason = "INFEASIBLE_DEADLINE"

	// RejectionReasonAcknowledgementDeadlineMissed: the 24h
	// acknowledgement window closed with no answer ever sent
	// (SweepAcknowledgementDeadlines).
	RejectionReasonAcknowledgementDeadlineMissed RejectionReason = "ACKNOWLEDGEMENT_DEADLINE_MISSED"
)

// NetworkOrderRejected is raised whenever we tell the network no — inside
// the window (untranslatable product, infeasible deadline) or because the
// window itself closed unanswered (the sweep).
type NetworkOrderRejected struct {
	NetworkRef NetworkRef
	SiteId     SiteId
	Reason     RejectionReason
	At         time.Time
}

func (e NetworkOrderRejected) EventName() string     { return "NetworkOrderRejected" }
func (e NetworkOrderRejected) OccurredAt() time.Time { return e.At }

// NetworkOrderShipmentConfirmed is raised once a shipment has been
// confirmed back to the network, closing the order.
//
// NOTE: NetworkOrder.ConfirmShipment() exists on the aggregate but no use
// case in this codebase calls it yet — the inbound leg that observes a
// real shipment (e.g. a fulfillment-execution PackageManifested consumer)
// is not yet built. This event is modelled now, alongside the other three,
// so the domain's event vocabulary and the outbound Kafka/analytics wiring
// are already correct the day that leg is added; it is exercised here only
// at the domain level (EventName/OccurredAt) until then.
type NetworkOrderShipmentConfirmed struct {
	NetworkRef   NetworkRef
	SiteId       SiteId
	LocalOrderId LocalOrderId
	At           time.Time
}

func (e NetworkOrderShipmentConfirmed) EventName() string     { return "NetworkOrderShipmentConfirmed" }
func (e NetworkOrderShipmentConfirmed) OccurredAt() time.Time { return e.At }
