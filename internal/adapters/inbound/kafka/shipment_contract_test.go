package kafka

// Event consumer contract tests (E1-E4) for the outbound-shipping edge:
// this repo's ShipmentConfirmedConsumer subscribes to outbound-shipping's
// integration topic warehouse.outbound-shipping.events
// (OutboundShippingTopic, wired into kafka.ReaderConfig by
// NewShipmentConfirmedConsumer) and acts on ShipmentConfirmed only. The
// contract it validates against is the PINNED copy of outbound-shipping's
// own apis/asyncapi.yaml (testdata/outbound-shipping.asyncapi.yaml,
// registered in contracts/pinned.json). Golden fixtures are the producer
// spec's OWN examples.

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// E1: the topic this repo's reader actually subscribes to is documented
// by the producer's pinned spec, and the one CloudEvents type the
// decoder's switch routes on is documented on that channel.
func TestPinnedAsyncAPICoversSubscriptions_OutboundShippingEvents(t *testing.T) {
	documented := loadPinnedAsyncAPI(t, pinnedOutboundShippingSpec).documentedTypes(t, OutboundShippingTopic)
	if _, ok := documented[TypeShipmentConfirmed]; !ok {
		t.Errorf("pinned outbound-shipping spec does not document %s on %s (documented: %v)", TypeShipmentConfirmed, OutboundShippingTopic, documented)
	}
}

// E2: the producer's pinned ShipmentConfirmed example decodes through the
// REAL cloudevents.Decode and routes through the REAL
// ShipmentConfirmedConsumer.Handle → ConfirmNetworkOrderShipment for
// exactly the orders of the wire's orders[] this repo knows: the two
// example order_ids are seeded as ACKNOWLEDGED network orders (the state
// the confirmation path requires), so both must be confirmed and logged
// against the wire's load_id.
func TestGoldenEventsDecode_OutboundShippingShipmentConfirmed(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShippingSpec)
	for _, ex := range doc.goldenExamples(t, "ShipmentConfirmed") {
		t.Run(ex.Name, func(t *testing.T) { runShipmentConfirmedExample(t, ex) })
	}
}

// runShipmentConfirmedExample seeds every order the wire names as ACKNOWLEDGED
// (the state the confirmation path requires), runs the real consumer over the
// golden payload, and asserts each order was confirmed against the wire's
// load_id.
func runShipmentConfirmedExample(t *testing.T, ex pinnedExample) {
	t.Helper()
	f := newShipmentConsumerFixture(t)
	data := ex.Payload["data"].(map[string]any)
	loadId, _ := data["load_id"].(string)
	orders := data["orders"].([]any)
	for _, o := range orders {
		orderId, _ := o.(map[string]any)["order_id"].(string)
		acknowledgedWithLocalOrder(t, f.orders, shared.NetworkRef("po-"+orderId), shared.LocalOrderId(orderId))
	}
	if err := f.consumer.Handle(context.Background(), goldenKey(ex.Payload), goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("handle golden example: %v", err)
	}
	if len(f.events.events) != len(orders) {
		t.Fatalf("golden event confirmed %d network orders, the wire names %d", len(f.events.events), len(orders))
	}
	assertOrdersConfirmed(t, f, loadId, orders)
}

func assertOrdersConfirmed(t *testing.T, f *shipmentConsumerFixture, loadId string, orders []any) {
	t.Helper()
	for _, o := range orders {
		orderId, _ := o.(map[string]any)["order_id"].(string)
		ok, err := f.confirmations.Exists(context.Background(), loadId, orderId)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("order %s was not confirmed against load %s", orderId, loadId)
		}
	}
}

// E3: every golden fixture on the subscribed channel carries the
// fleet-mandatory CloudEvents 1.0 envelope.
func TestCloudEventsEnvelopeInvariants_OutboundShipping(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShippingSpec)
	for _, name := range doc.producerMessageRefs(t, OutboundShippingTopic) {
		for _, ex := range doc.goldenExamples(t, name) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example %s/%s: %v", name, ex.Name, err)
			}
			assertFleetEnvelope(t, ex, evt, "/warehouse/outbound-shipping", "urn:warehouse:outbound-shipping:events:"+name+":v1")
		}
	}
}

// E4: unknown ADDITIVE attributes in data must not fail the decode or
// the handling, an unknown event TYPE on the shared topic is silently
// skipped (no confirmation), and a message that is not a CloudEvent at
// all is a deterministic decode error wrapping ErrNotCloudEvent — the
// signal handleWithDeadLetter dead-letters once.
func TestUnknownAttributesTolerated_OutboundShipping(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShippingSpec)
	golden := doc.goldenExamples(t, "ShipmentConfirmed")[0].Payload

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	f := newShipmentConsumerFixture(t)
	if err := f.consumer.Handle(context.Background(), goldenKey(tolerant), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(f.gateway.submits) != 0 || len(f.events.events) != 0 {
		t.Errorf("unknown data attributes had effects: submits=%d events=%d", len(f.gateway.submits), len(f.events.events))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.outbound-shipping.outboundload.LoadClosed"
	if err := f.consumer.Handle(context.Background(), goldenKey(otherType), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(f.gateway.submits) != 0 || len(f.events.events) != 0 {
		t.Errorf("unknown event type had effects: submits=%d events=%d", len(f.gateway.submits), len(f.events.events))
	}

	// Not a CloudEvent: deterministic decode error wrapping
	// ErrNotCloudEvent — the DLQ trigger.
	if err := f.consumer.Handle(context.Background(), "load-1", []byte(`{"flat":"envelope"}`)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
