package networkorder

import (
	"errors"
	"time"
)

// A shipment can leave before the network has settled our acknowledgement:
// the acknowledgement window is 24 h, an order is SUBMITTED until
// ReconcileSubmittedOrders hears the network's verdict, and a load may depart
// in between. ADR 0014's addendum D7 records that early shipment on the order
// instead of dead-lettering it, and the confirm-pending pass settles it once
// the order is ACKNOWLEDGED. This file is that fact, kept like the hold in
// hold.go: optional, persisted on the order's row, restored by a repository.

// ErrEmptyShipmentRef rejects noting a shipment that names no shipment: the
// reference is what the confirmation is keyed on, so a blank one could never
// be told apart from another shipment later.
var ErrEmptyShipmentRef = errors.New("a pending shipment needs a shipment reference")

// PendingShipment is a shipment confirmation that arrived while the order was
// still SUBMITTED and has not been confirmed to the network yet.
type PendingShipment struct {
	// ShipmentRef is the shipment's reference in the source that knows it left
	// (outbound-shipping's load id): the ledger key of the eventual
	// confirmation.
	ShipmentRef string
	// PackageCount is the order's package count, 0 when the source did not
	// carry one (the confirm path maps <= 0 to its single-package placeholder).
	PackageCount int
	// NotedAt is when the order recorded the shipment.
	NotedAt time.Time
}

// NoteShipmentBeforeAcknowledge records that the shipment left while the order
// is still SUBMITTED. Any other state is ErrConfirmBeforeAcknowledge, exactly
// as ConfirmShipment says it: a shipped order the network never took (NEW,
// REJECTED) is an anomaly for an operator, and an ACKNOWLEDGED or CONFIRMED
// one has no reason to wait.
//
// The first noted shipment wins. The same ref again is a no-op success (the
// event is redelivered), and so is a different ref while one is pending: the
// order keeps the first, and the caller, who sees both refs, logs the second.
// A count <= 0 is stored as 0.
func (o *NetworkOrder) NoteShipmentBeforeAcknowledge(shipmentRef string, packageCount int, now time.Time) error {
	if o.state != StateSubmitted {
		return ErrConfirmBeforeAcknowledge
	}
	if shipmentRef == "" {
		return ErrEmptyShipmentRef
	}
	if o.pendingShipment != nil {
		return nil
	}
	if packageCount < 0 {
		packageCount = 0
	}
	o.pendingShipment = &PendingShipment{ShipmentRef: shipmentRef, PackageCount: packageCount, NotedAt: now}
	return nil
}

// PendingShipment returns the early shipment waiting for this order's
// acknowledgement, and false when there is none. It is a copy.
func (o *NetworkOrder) PendingShipment() (PendingShipment, bool) {
	if o.pendingShipment == nil {
		return PendingShipment{}, false
	}
	return *o.pendingShipment, true
}

// DropPendingShipment clears the pending shipment without confirming it. The
// sweep uses it for an order that no longer needs it: already CONFIRMED (by
// the manual endpoint) or REJECTED. Nothing pending is a no-op.
func (o *NetworkOrder) DropPendingShipment() { o.pendingShipment = nil }

// RestorePendingShipment rebuilds a persisted pending shipment. Only a
// repository should call it, like RestoreHold: the stored row already
// satisfied the invariants.
func (o *NetworkOrder) RestorePendingShipment(p PendingShipment) { o.pendingShipment = &p }
