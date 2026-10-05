package usecases

import (
	"context"
	"fmt"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ConfirmNetworkOrderShipment answers the network that one of its orders
// has shipped (ADR 0001 §5's `submitShipmentConfirmations`), closing the
// order and raising NetworkOrderShipmentConfirmed.
//
// ADR 0001's own Rollout step 6 names the LITERAL trigger as
// `fulfillment-execution`'s existing PackageManifested event. This use
// case deliberately does NOT do that (see this package's companion doc
// note / docs/adr/0009-explicit-shipment-confirmation-endpoint.md for the
// full reasoning): there is no persisted mapping from a
// PackageManifested's WorkUnitId back to this context's NetworkRef
// anywhere in this codebase today, and ADR 0001's own Hard rule 4
// ("Correlation is a persisted mapping, never a string convention")
// already forbids inventing one as a fragile string/convention join at
// the exact moment a cross-context event arrives. An explicit, named
// trigger — a direct call naming the NetworkRef — is correct NOW; a
// PackageManifested-driven cache can replace this call's caller later
// without changing anything below this line, once that persisted
// correlation exists.
type ConfirmNetworkOrderShipment struct {
	Orders  ports.NetworkOrderRepo
	Gateway ports.NetworkGateway
	Events  ports.EventPublisher
	Clock   ports.Clock
	// UnitOfWork brackets the Save+Publish pair atomically, exactly like
	// every other use case in this package (ADR 0003). Optional.
	UnitOfWork ports.UnitOfWork
}

// Execute confirms shipment for the NetworkOrder identified by ref.
//
// Idempotent: an order already CONFIRMED is returned as-is rather than
// re-confirmed, so a retried call (the caller's own at-least-once
// delivery, or an operator re-submitting) never double-submits to the
// network or double-publishes the event.
func (uc *ConfirmNetworkOrderShipment) Execute(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	o, err := uc.Orders.FindByRef(ctx, ref)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, ErrOrderNotFound
	}
	if o.State() == networkorder.StateConfirmed {
		return o, nil
	}

	if err := o.ConfirmShipment(); err != nil {
		return nil, err
	}

	now := uc.Clock.Now()
	var localOrderId shared.LocalOrderId
	if id := o.LocalOrderId(); id != nil {
		localOrderId = *id
	}

	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
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

	if err := uc.Gateway.SubmitShipmentConfirmation(ctx, o.NetworkRef()); err != nil {
		return nil, fmt.Errorf("submit shipment confirmation: %w", err)
	}
	return o, nil
}
