package main_test

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of the deferred-shipment scenarios in features/shipment_event_rules.feature
// (ADR 0014 addendum D7): a ShipmentConfirmed whose order is still SUBMITTED is
// recorded on the order, and the real ConfirmPendingShipments use case confirms
// it once the order is ACKNOWLEDGED. The deliveries themselves reuse the steps
// above (the real consumer's Handle).

func (s *shipmentWorld) pendingShipmentConfirmationsRun(ctx context.Context) error {
	_, err := s.confirmPending.Execute(ctx)
	return err
}

func (s *shipmentWorld) findOrder(ctx context.Context, ref string) (*networkorder.NetworkOrder, error) {
	o, err := s.w.orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil {
		return nil, fmt.Errorf("find %q: %w", ref, err)
	}
	if o == nil {
		return nil, fmt.Errorf("no network order %q", ref)
	}
	return o, nil
}

func (s *shipmentWorld) waitingForTheShipment(ctx context.Context, ref, load string) error {
	o, err := s.findOrder(ctx, ref)
	if err != nil {
		return err
	}
	if p, ok := o.PendingShipment(); !ok || p.ShipmentRef != load {
		return fmt.Errorf("expected %q to be waiting for the shipment of load %q, it holds %+v (recorded: %v)", ref, load, p, ok)
	}
	return nil
}

func (s *shipmentWorld) waitingForNoShipment(ctx context.Context, ref string) error {
	o, err := s.findOrder(ctx, ref)
	if err != nil {
		return err
	}
	if p, ok := o.PendingShipment(); ok {
		return fmt.Errorf("expected %q to hold no pending shipment, it holds %+v", ref, p)
	}
	return nil
}

func (s *shipmentWorld) registerDeferredSteps(sc *godog.ScenarioContext) {
	sc.Step(`^pending shipment confirmations run$`, s.pendingShipmentConfirmationsRun)
	sc.Step(`^network order \"([^\"]*)\" is waiting for the shipment of load \"([^\"]*)\"$`, s.waitingForTheShipment)
	sc.Step(`^network order \"([^\"]*)\" is waiting for no shipment$`, s.waitingForNoShipment)
}
