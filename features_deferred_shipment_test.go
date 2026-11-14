package main_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of the deferred shipment scenarios in features/shipment_confirmation.feature
// (ADR 0014 addendum D7): outbound-shipping's ShipmentConfirmed is fed to the REAL
// ShipmentConfirmedConsumer.Handle as a CloudEvent, and the confirm-pending pass is
// the real ConfirmPendingShipments use case, both over the in-memory repository.

// shippingWorld is the event path of the world: one shared confirm use case (so
// its (shipment, order) ledger keeps a confirmation to one), the consumer
// and the pass that settles what the consumer deferred.
type shippingWorld struct {
	consumer       *inboundkafka.ShipmentConfirmedConsumer
	confirmPending *usecases.ConfirmPendingShipments
	// lastErr is what the consumer answered to the last report: nil means the
	// message is committed, an error means it is retried and then dead-lettered.
	lastErr error
}

func (w *world) resetShipping(logger *slog.Logger) {
	confirm := &usecases.ConfirmNetworkOrderShipment{
		Orders: w.orders, Gateway: w.gateway, Events: w.bus, Clock: w.clock,
		Confirmations: memory.NewShipmentConfirmationLog(),
	}
	w.shipping = shippingWorld{
		consumer: &inboundkafka.ShipmentConfirmedConsumer{
			Orders: w.orders, Confirm: confirm, Logger: logger, Now: w.clock.Now,
			Defer: &usecases.DeferShipmentConfirmation{Orders: w.orders, Clock: w.clock, Logger: logger},
		},
		confirmPending: &usecases.ConfirmPendingShipments{Orders: w.orders, Confirm: confirm, Logger: logger},
	}
}

func (w *world) outboundShippingReportsLoadShipped(ctx context.Context, loadId, ref string) error {
	o, err := w.orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil {
		return err
	}
	if o == nil || o.LocalOrderId() == nil {
		return fmt.Errorf("network order %q has no order-management order to ship", ref)
	}
	e := event.New("1.0")
	e.SetID("evt-" + loadId)
	e.SetSource("/warehouse/outbound-shipping")
	e.SetType(inboundkafka.TypeShipmentConfirmed)
	e.SetSubject(loadId)
	e.SetTime(w.clock.Now())
	if err := e.SetData("application/json", map[string]any{
		"load_id": loadId, "site_id": "SITE-1", "cpt_id": "cpt-1600", "carrier": "STDOUT",
		"trailer_id": "trl-91", "seal_number": "SL-4471", "departed_at": w.clock.Now().Format(time.RFC3339),
		"orders":  []map[string]any{{"order_id": string(*o.LocalOrderId())}},
		"version": 1,
	}); err != nil {
		return err
	}
	raw, err := e.MarshalJSON()
	if err != nil {
		return err
	}
	w.shipping.lastErr = w.shipping.consumer.Handle(ctx, loadId, raw)
	return nil
}

func (w *world) pendingShipmentConfirmationsRun(ctx context.Context) error {
	_, err := w.shipping.confirmPending.Execute(ctx)
	return err
}

func (w *world) theShipmentReportIsCommitted() error {
	if err := w.shipping.lastErr; err != nil {
		return fmt.Errorf("expected the report to be committed without a dead letter, the consumer answered: %w", err)
	}
	return nil
}

func (w *world) theShipmentReportFailsWith(text string) error {
	err := w.shipping.lastErr
	if err == nil {
		return fmt.Errorf("expected the report to fail with %q (retried, then dead-lettered), the consumer committed it", text)
	}
	if !strings.Contains(err.Error(), text) {
		return fmt.Errorf("expected the report to fail with %q, the consumer answered: %w", text, err)
	}
	return nil
}

func (w *world) findOrder(ctx context.Context, ref string) (*networkorder.NetworkOrder, error) {
	o, err := w.orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil {
		return nil, fmt.Errorf("find %q: %w", ref, err)
	}
	if o == nil {
		return nil, fmt.Errorf("no network order %q", ref)
	}
	return o, nil
}

func (w *world) networkOrderIsWaitingForTheShipment(ctx context.Context, ref, loadId string) error {
	o, err := w.findOrder(ctx, ref)
	if err != nil {
		return err
	}
	if p, ok := o.PendingShipment(); !ok || p.ShipmentRef != loadId {
		return fmt.Errorf("expected %q to be waiting for the shipment of load %q, it holds %+v (recorded: %v)", ref, loadId, p, ok)
	}
	return nil
}

func (w *world) networkOrderIsWaitingForNoShipment(ctx context.Context, ref string) error {
	o, err := w.findOrder(ctx, ref)
	if err != nil {
		return err
	}
	if p, ok := o.PendingShipment(); ok {
		return fmt.Errorf("expected %q to hold no pending shipment, it holds %+v", ref, p)
	}
	return nil
}

func (w *world) registerDeferredShipmentSteps(sc *godog.ScenarioContext) {
	sc.Step(`^outbound-shipping reports load \"([^\"]*)\" shipped carrying network order \"([^\"]*)\"$`, w.outboundShippingReportsLoadShipped)
	sc.Step(`^the shipment report is committed without a dead letter$`, w.theShipmentReportIsCommitted)
	sc.Step(`^the shipment report fails with \"([^\"]*)\"$`, w.theShipmentReportFailsWith)
	sc.Step(`^pending shipment confirmations run$`, w.pendingShipmentConfirmationsRun)
	sc.Step(`^network order \"([^\"]*)\" is waiting for the shipment of load \"([^\"]*)\"$`, w.networkOrderIsWaitingForTheShipment)
	sc.Step(`^network order \"([^\"]*)\" is waiting for no shipment$`, w.networkOrderIsWaitingForNoShipment)
}
