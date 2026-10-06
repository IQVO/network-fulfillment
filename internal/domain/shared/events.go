package shared

import "time"

// DomainEvent is the minimal shape every event raised by this domain
// implements — mirrors the same lightweight interface used across the
// fleet's other services (see e.g. process-path-management's or
// labor-performance's domain/shared package), which the outbound Kafka
// adapter wraps in a CloudEvents-style envelope, not the domain layer.
//
// Events carry NO serialisation tags: their JSON wire shape lives in the
// adapter (internal/adapters/outbound/eventwire), guarded by the
// no-struct-tags sensor in internal/architecture.
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
// an order in full: the network has been told yes (the order is
// SUBMITTED), and a local order has been raised in order-management.
//
// It is published at SUBMITTED, i.e. BEFORE ReconcileSubmittedOrders
// settles the submission against the network's transaction status
// (ADR 0001 §5); the later SUBMITTED -> ACKNOWLEDGED settlement raises no
// event, and a failed reconciliation raises NetworkOrderRejected with
// reason SUBMISSION_FAILED. Consumers must not read this event as a
// settled commitment.
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
// These four are exhaustive over every path that calls
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

	// RejectionReasonSubmissionFailed: a submitted acknowledgement's
	// transaction-status reconciliation (ADR 0001 §5) reported FAILURE —
	// the network itself refused what we told it we would do
	// (ReconcileSubmittedOrders).
	RejectionReasonSubmissionFailed RejectionReason = "SUBMISSION_FAILED"
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
// It is raised by ConfirmNetworkOrderShipment, which is driven by the
// explicit POST /network-orders/{networkRef}/shipment-confirmation
// endpoint (ADR 0014) rather than by a PackageManifested consumer: no
// persisted WorkUnitId -> NetworkRef mapping exists yet to correlate one.
type NetworkOrderShipmentConfirmed struct {
	NetworkRef   NetworkRef
	SiteId       SiteId
	LocalOrderId LocalOrderId
	At           time.Time
}

func (e NetworkOrderShipmentConfirmed) EventName() string     { return "NetworkOrderShipmentConfirmed" }
func (e NetworkOrderShipmentConfirmed) OccurredAt() time.Time { return e.At }

// AcknowledgementDeadlineAtRisk is raised by SweepAcknowledgementDeadlines
// for every order found still NEW past its acknowledgeBy instant (ADR
// 0001 §6). It is a REPORTED FACT, not a state transition: the sweep
// itself never mutates the aggregate, and this event is expected to
// re-fire on every pass for as long as the condition holds true — a
// consumer must not treat it as edge-triggered. RejectOverdueOrders is
// the separate path that performs the actual rejection, with its own
// audit trail (NetworkOrderRejected, reason=ACKNOWLEDGEMENT_DEADLINE_MISSED).
type AcknowledgementDeadlineAtRisk struct {
	NetworkRef    NetworkRef
	SiteId        SiteId
	AcknowledgeBy time.Time
	At            time.Time
}

func (e AcknowledgementDeadlineAtRisk) EventName() string     { return "AcknowledgementDeadlineAtRisk" }
func (e AcknowledgementDeadlineAtRisk) OccurredAt() time.Time { return e.At }
