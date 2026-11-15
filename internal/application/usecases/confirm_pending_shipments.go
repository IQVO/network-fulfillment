package usecases

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// pendingShipmentBatch bounds how many orders one pass examines. A healthy
// fleet has almost none (a pending shipment lives from an early departure to
// the next reconcile), so this only caps a backlog after an outage;
// ListPendingShipments lists the orders that can be settled first, so a
// backlog of SUBMITTED ones never starves them.
const pendingShipmentBatch = 100

// ConfirmPendingShipments settles the shipments DeferShipmentConfirmation
// recorded (ADR 0014 addendum D7). Per order carrying one:
//
//   - ACKNOWLEDGED: confirm it through ConfirmNetworkOrderShipment's one
//     confirmation path (labels, ledger, NetworkOrderShipmentConfirmed), with
//     the noted shipment reference and package count. The ledger and the
//     CONFIRMED state keep it exactly-once, like the event path.
//   - SUBMITTED: skip; the network has not settled the acknowledgement yet.
//   - CONFIRMED or REJECTED: the fact is no use any more (the manual endpoint
//     confirmed it first, or the network refused the order), so drop it.
//
// Like ReconcileSubmittedOrders it never lets one order abort the pass: a
// failure is logged and counted, and the order is tried again on the next one.
type ConfirmPendingShipments struct {
	Orders  ports.NetworkOrderRepo
	Confirm *ConfirmNetworkOrderShipment
	// UnitOfWork brackets the Save of a drop. Optional.
	UnitOfWork ports.UnitOfWork
	// Logger reports each settled, dropped or failed order. Optional: nil
	// means slog.Default().
	Logger *slog.Logger
}

// ConfirmPendingShipmentsResult reports what one pass did.
type ConfirmPendingShipmentsResult struct {
	Examined  int
	Confirmed int
	Dropped   int
	Failed    int
}

func (uc *ConfirmPendingShipments) Execute(ctx context.Context) (ConfirmPendingShipmentsResult, error) {
	pending, err := uc.Orders.ListPendingShipments(ctx, pendingShipmentBatch)
	if err != nil {
		return ConfirmPendingShipmentsResult{}, err
	}
	res := ConfirmPendingShipmentsResult{Examined: len(pending)}
	for _, o := range pending {
		fact, ok := o.PendingShipment()
		if !ok {
			continue
		}
		outcome, err := uc.settle(ctx, o, fact)
		if err != nil {
			res.Failed++
			uc.logger().Error("could not settle the shipment noted before acknowledgement; retrying next pass",
				"order", o.NetworkRef(), "shipment", fact.ShipmentRef, "err", err)
			continue
		}
		switch outcome {
		case settledConfirmed:
			res.Confirmed++
			uc.logger().Info("shipment noted before acknowledgement confirmed", "order", o.NetworkRef(), "shipment", fact.ShipmentRef)
		case settledDropped:
			res.Dropped++
			uc.logger().Info("dropped a shipment noted before acknowledgement: the order needs no confirmation",
				"order", o.NetworkRef(), "shipment", fact.ShipmentRef, "state", o.State())
		}
	}
	return res, nil
}

type pendingOutcome int

const (
	settledSkipped pendingOutcome = iota
	settledConfirmed
	settledDropped
)

func (uc *ConfirmPendingShipments) settle(ctx context.Context, o *networkorder.NetworkOrder, fact networkorder.PendingShipment) (pendingOutcome, error) {
	switch o.State() {
	case networkorder.StateAcknowledged:
		if _, err := uc.Confirm.ExecuteForShipment(ctx, o.NetworkRef(), fact.ShipmentRef, fact.PackageCount); err != nil {
			return settledSkipped, fmt.Errorf("confirm: %w", err)
		}
		return settledConfirmed, nil
	case networkorder.StateConfirmed, networkorder.StateRejected:
		if err := uc.drop(ctx, o.NetworkRef()); err != nil {
			return settledSkipped, fmt.Errorf("drop: %w", err)
		}
		return settledDropped, nil
	default:
		return settledSkipped, nil
	}
}

// drop clears the fact on a fresh load, retrying on a version conflict. An
// order that vanished, or lost its fact to another writer, has nothing left to
// drop.
func (uc *ConfirmPendingShipments) drop(ctx context.Context, ref shared.NetworkRef) error {
	return withVersionRetry(func() error {
		o, err := uc.Orders.FindByRef(ctx, ref)
		if err != nil || o == nil {
			return err
		}
		o.DropPendingShipment()
		return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			return uc.Orders.Save(ctx, o)
		})
	})
}

func (uc *ConfirmPendingShipments) logger() *slog.Logger {
	if uc.Logger != nil {
		return uc.Logger
	}
	return slog.Default()
}
