package usecases

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// DeferShipmentConfirmation records a shipment that left before the network
// settled our acknowledgement (ADR 0014 addendum D7).
//
// An order is SUBMITTED until ReconcileSubmittedOrders hears the network's
// verdict, the acknowledgement window is 24 h, and a load can legitimately
// depart in between. ConfirmNetworkOrderShipment refuses such an order
// (networkorder.ErrConfirmBeforeAcknowledge), and dead-lettering the event
// would lose the confirmation until an operator replays it by hand. The
// ShipmentConfirmed consumer calls this instead, so the fact is kept on the
// order and ConfirmPendingShipments settles it once the order is ACKNOWLEDGED.
//
// Only a SUBMITTED order is deferred. Any other state returns
// networkorder.ErrConfirmBeforeAcknowledge unchanged, so a shipped order the
// network never took (NEW, REJECTED) keeps today's retry-then-dead-letter path:
// that one is a genuine anomaly for an operator. ACKNOWLEDGED returns it too,
// on purpose: the order was settled between the consumer's attempt and this
// load, and the consumer's retry will confirm it directly.
type DeferShipmentConfirmation struct {
	Orders ports.NetworkOrderRepo
	Clock  ports.Clock
	// UnitOfWork brackets the Save like every other use case here. Optional.
	UnitOfWork ports.UnitOfWork
	// Logger reports a second shipment that lost to the first one noted.
	// Optional: nil means slog.Default().
	Logger *slog.Logger
}

// Execute notes shipmentRef (outbound-shipping's load id) and packageCount
// (0 when the source carries none) on the order ref, on a fresh load, retrying
// on a version conflict (migration 0013): the reconcile pass may settle the
// order while this runs, and the reload then sees the new state.
//
// Idempotent: the first shipment noted wins. The same shipment again, or a
// different one while one is pending, returns nil without writing.
func (uc *DeferShipmentConfirmation) Execute(ctx context.Context, ref shared.NetworkRef, shipmentRef string, packageCount int) error {
	return withVersionRetry(func() error {
		o, err := uc.Orders.FindByRef(ctx, ref)
		if err != nil {
			return err
		}
		if o == nil {
			return ErrOrderNotFound
		}
		return uc.note(ctx, o, shipmentRef, packageCount)
	})
}

func (uc *DeferShipmentConfirmation) note(ctx context.Context, o *networkorder.NetworkOrder, shipmentRef string, packageCount int) error {
	first, alreadyPending := o.PendingShipment()
	if err := o.NoteShipmentBeforeAcknowledge(shipmentRef, packageCount, uc.Clock.Now()); err != nil {
		return err
	}
	if alreadyPending {
		if first.ShipmentRef != shipmentRef {
			uc.logger().Warn("a second shipment arrived before the order was acknowledged; keeping the first",
				"order", o.NetworkRef(), "pending_shipment", first.ShipmentRef, "ignored_shipment", shipmentRef)
		}
		return nil
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		return uc.Orders.Save(ctx, o)
	}); err != nil {
		return fmt.Errorf("defer shipment %q for %s: %w", shipmentRef, o.NetworkRef(), err)
	}
	return nil
}

func (uc *DeferShipmentConfirmation) logger() *slog.Logger {
	if uc.Logger != nil {
		return uc.Logger
	}
	return slog.Default()
}
