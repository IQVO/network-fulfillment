package usecases

import (
	"context"
	"fmt"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// singlePackage is how many packages a confirmed order leaves in when no
// fleet fact carries the real number (ADR 0026). The event path keeps this
// placeholder until outbound-shipping's ShipmentConfirmed carries a
// per-order package count — an additive producer change that has not landed
// in the merged contract yet (the ADR 0014 addendum records the gap); the
// consumer's decode already tolerates the field, so the real count flows
// the day it ships.
const singlePackage = 1

// ConfirmNetworkOrderShipment answers the network that one of its orders
// has shipped (ADR 0001 §5's `submitShipmentConfirmations`), closing the
// order and raising NetworkOrderShipmentConfirmed.
//
// ADR 0001's own Rollout step 6 names the LITERAL trigger as
// `fulfillment-execution`'s existing PackageManifested event. This use
// case deliberately does NOT do that (see
// docs/adr/0014-explicit-shipment-confirmation-endpoint.md and its 2026-11
// addendum for the full reasoning): no persisted mapping from a
// PackageManifested's WorkUnitId back to this context's NetworkRef exists,
// and ADR 0001's own Hard rule 4 ("Correlation is a persisted mapping,
// never a string convention") forbids inventing one as a fragile string
// join. The correlation that DID land is outbound-shipping's
// ShipmentConfirmed joining orders[].order_id on
// NetworkOrder.localOrderId — the mapping ADR 0014 itself named as the
// natural future trigger.
//
// TWO inbound paths route here, and exactly one confirmation may ever
// reach the network per order (ADR 0014 addendum, D6):
//
//   - the EVENT path: the ShipmentConfirmed consumer resolves each
//     orders[].order_id to its NetworkOrder via FindByLocalOrderId and
//     calls ExecuteForShipment with the load id as the shipment reference;
//   - the MANUAL path: ADR 0014's operator endpoint calls Execute with the
//     NetworkRef in hand, kept for one release as the operator fallback.
//
// Idempotency is two-legged, so neither path can double-submit:
//
//  1. the (shipment reference, order id) pair in the
//     ShipmentConfirmationLog: a duplicate submission — the same event
//     redelivered, or the manual endpoint re-fired for a shipment the
//     event already confirmed — is a no-op returning the existing
//     confirmation;
//  2. the aggregate itself: an order already CONFIRMED is returned as-is,
//     whichever path confirmed it first (the state is terminal).
type ConfirmNetworkOrderShipment struct {
	Orders  ports.NetworkOrderRepo
	Gateway ports.NetworkGateway
	Events  ports.EventPublisher
	Clock   ports.Clock
	// UnitOfWork brackets the Save+Publish pair atomically, exactly like
	// every other use case in this package (ADR 0003). Optional.
	UnitOfWork ports.UnitOfWork
	// Confirmations is the (shipment reference, order id) idempotency
	// ledger. The composition root MUST set it; a nil value only skips the
	// ledger leg (the aggregate-state leg still holds) so hand-built test
	// fixtures without one keep compiling.
	Confirmations ports.ShipmentConfirmationLog
}

// Execute is the MANUAL path (ADR 0014's endpoint): the shipment reference
// is the network's own ref — the one identifier an operator holding this
// order's paperwork reliably has — and the label count stays ADR 0026's
// single-package placeholder.
func (uc *ConfirmNetworkOrderShipment) Execute(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	return uc.ExecuteForShipment(ctx, ref, string(ref), singlePackage)
}

// ExecuteForShipment is the ONE confirmation path both callers share.
// shipmentRef is the shipment's reference in the source that knows it left
// (outbound-shipping's load id on the event path); packageCount is the
// order's package count when the caller knows it, and <= 0 falls back to
// singlePackage.
//
// Idempotent: an order already CONFIRMED, or a (shipmentRef, order id)
// pair already recorded, is returned as-is rather than re-confirmed, so a
// retried call (at-least-once delivery, or an operator re-submitting)
// never double-submits to the network or double-publishes the event.
func (uc *ConfirmNetworkOrderShipment) ExecuteForShipment(ctx context.Context, ref shared.NetworkRef, shipmentRef string, packageCount int) (*networkorder.NetworkOrder, error) {
	o, err := uc.Orders.FindByRef(ctx, ref)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, ErrOrderNotFound
	}

	// The idempotency key: shipment reference + the identifying order id
	// (the order-management OrderId when one is linked — the event's join
	// key — else the NetworkRef the manual path named).
	orderId := string(ref)
	if id := o.LocalOrderId(); id != nil {
		orderId = string(*id)
	}
	if shipmentRef == "" {
		shipmentRef = string(ref)
	}
	if uc.Confirmations != nil {
		confirmed, err := uc.Confirmations.Exists(ctx, shipmentRef, orderId)
		if err != nil {
			return nil, err
		}
		if confirmed {
			return o, nil // duplicate submission: the existing confirmation
		}
	}
	if o.State() == networkorder.StateConfirmed {
		return o, nil // confirmed already, by whichever path got there first
	}
	if packageCount <= 0 {
		packageCount = singlePackage
	}

	// The labels first: the network confirms a shipment only with a label for
	// every package, and asking is idempotent. A failure here leaves the order
	// ACKNOWLEDGED and the call retryable, instead of CONFIRMED with nothing
	// to confirm.
	if err := o.CheckConfirmable(); err != nil {
		return nil, err
	}
	if _, err := uc.Gateway.RequestLabel(ctx, o.NetworkRef(), packageCount); err != nil {
		return nil, fmt.Errorf("request label: %w", err)
	}
	if err := o.ConfirmShipment(); err != nil {
		return nil, err
	}

	now := uc.Clock.Now()
	var localOrderId shared.LocalOrderId
	if id := o.LocalOrderId(); id != nil {
		localOrderId = *id
	}
	confirmations := uc.Confirmations
	shipmentRefForRecord, orderIdForRecord := shipmentRef, orderId

	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		if confirmations != nil {
			// Inside the same atomic scope as the CONFIRMED save and the
			// outbox write: the ledger only ever claims a confirmation
			// that also happened.
			if err := confirmations.Record(ctx, shipmentRefForRecord, orderIdForRecord, o.NetworkRef(), now); err != nil {
				return err
			}
		}
		return uc.Events.Publish(ctx, shared.NetworkOrderShipmentConfirmed{
			NetworkRef:   o.NetworkRef(),
			SiteId:       o.SiteId(),
			LocalOrderId: localOrderId,
			At:           now,
		})
	}); err != nil {
		return nil, err
	}

	if err := uc.Gateway.SubmitShipmentConfirmation(ctx, o.NetworkRef(), now); err != nil {
		return nil, fmt.Errorf("submit shipment confirmation: %w", err)
	}
	return o, nil
}
