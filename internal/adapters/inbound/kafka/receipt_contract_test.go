package kafka

// Event consumer contract tests (E1-E4) for the inbound-receiving edge:
// this repo's ReceiptConsumer subscribes to inbound-receiving's
// integration topic warehouse.inbound-receiving.events (ReceiptTopic,
// wired into kafka.ReaderConfig by NewReceiptConsumer) and acts on the
// three receipt types of ADR 0028. The contract it validates against is
// the PINNED copy of inbound-receiving's own apis/asyncapi.yaml
// (testdata/inbound-receiving.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's OWN
// examples — real wire documents inbound-receiving committed to.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// E1: the topic this repo's reader actually subscribes to (the
// ReceiptTopic constant NewReceiptConsumer wires into kafka.ReaderConfig)
// is documented by the producer's pinned spec, and every CloudEvents type
// the decoder's switch routes on is documented on that channel.
func TestPinnedAsyncAPICoversSubscriptions_InboundReceivingEvents(t *testing.T) {
	documented := loadPinnedAsyncAPI(t, pinnedInboundReceivingSpec).documentedTypes(t, ReceiptTopic)
	for _, typ := range []string{TypeReceiptCartonReceived, TypeCartonDiscrepancyRaised, TypeReceiptClosed} {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned inbound-receiving spec does not document %s on %s (documented: %v)", typ, ReceiptTopic, documented)
		}
	}
}

// newReceiptContractFixture builds the real consumer over the projection
// spy from receipt_consumer_test.go, on a fixed clock.
func newReceiptContractFixture() (*ReceiptConsumer, *projectionSpy) {
	spy := &projectionSpy{}
	return &ReceiptConsumer{
		Projection: spy,
		Logger:     nil,
		Now:        func() time.Time { return time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC) },
	}, spy
}

// E2: every receipt example the producer's pinned spec documents on the
// subscribed channel decodes through the REAL cloudevents.Decode and
// routes through the REAL ReceiptConsumer.Handle → ReceiptProjection:
// exactly one record of the right kind, carrying the wire's own facts and
// keyed by the CloudEvents id (the dedupe key the projection claims).
func TestGoldenEventsDecode_InboundReceivingReceipts(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInboundReceivingSpec)
	for _, name := range []string{"ReceiptCartonReceived", "CartonDiscrepancyRaised", "ReceiptClosed"} {
		for _, ex := range doc.goldenExamples(t, name) {
			t.Run(name+"/"+ex.Name, func(t *testing.T) {
				consumer, spy := newReceiptContractFixture()
				raw := goldenJSON(t, ex.Payload)
				if err := consumer.Handle(context.Background(), goldenKey(ex.Payload), raw); err != nil {
					t.Fatalf("handle golden example: %v", err)
				}
				assertReceiptRouted(t, ex, spy)
			})
		}
	}
}

// assertReceiptRouted asserts the golden event produced exactly one
// projection record of the kind its type selects, with the wire's facts.
func assertReceiptRouted(t *testing.T, ex pinnedExample, spy *projectionSpy) {
	t.Helper()
	if len(spy.ids) != 1 {
		t.Fatalf("golden event recorded %d projection writes, want 1", len(spy.ids))
	}
	if spy.ids[0] != ex.Payload["id"] {
		t.Errorf("projection claimed id %q, wire CloudEvents id is %q (the dedupe key)", spy.ids[0], ex.Payload["id"])
	}
	data, _ := ex.Payload["data"].(map[string]any)
	switch ex.Payload["type"] {
	case TypeReceiptCartonReceived:
		assertOneCarton(t, spy, data)
	case TypeCartonDiscrepancyRaised:
		assertOneFinding(t, spy, data, goldenKey(ex.Payload))
	case TypeReceiptClosed:
		assertOneClosed(t, spy, data)
	}
}

func assertOneCarton(t *testing.T, spy *projectionSpy, data map[string]any) {
	t.Helper()
	if len(spy.cartons) != 1 {
		t.Fatalf("ReceiptCartonReceived routed to %d carton facts, want 1", len(spy.cartons))
	}
	f := spy.cartons[0]
	if f.ReceiptID != data["receipt_id"] || f.AsnNumber != data["asn_number"] || f.SSCC != data["sscc"] {
		t.Errorf("carton fact %+v does not carry the wire's receipt/asn/sscc", f)
	}
}

func assertOneFinding(t *testing.T, spy *projectionSpy, data map[string]any, key string) {
	t.Helper()
	if len(spy.findings) != 1 {
		t.Fatalf("CartonDiscrepancyRaised routed to %d findings, want 1", len(spy.findings))
	}
	f := spy.findings[0]
	if f.ReceiptID != data["receipt_id"] || f.Kind != data["kind"] {
		t.Errorf("finding %+v does not carry the wire's receipt/kind", f)
	}
	// The finding's ASN is the Kafka key (the payload names none).
	if f.AsnNumber != key {
		t.Errorf("finding asn %q, Kafka key (subject) is %q", f.AsnNumber, key)
	}
}

func assertOneClosed(t *testing.T, spy *projectionSpy, data map[string]any) {
	t.Helper()
	if len(spy.closed) != 1 {
		t.Fatalf("ReceiptClosed routed to %d closed facts, want 1", len(spy.closed))
	}
	f := spy.closed[0]
	if f.ReceiptID != data["receipt_id"] || f.AsnNumber != data["asn_number"] {
		t.Errorf("closed fact %+v does not carry the wire's receipt/asn", f)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/inbound-receiving,
// type, subject, time, datacontenttype application/json, dataschema
// urn:warehouse:inbound-receiving:events:<Name>:v1 — and the structured
// content-type on the examples that carry Kafka headers.
func TestCloudEventsEnvelopeInvariants_InboundReceiving(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInboundReceivingSpec)
	for _, name := range doc.producerMessageRefs(t, ReceiptTopic) {
		for _, ex := range doc.goldenExamples(t, name) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example %s/%s: %v", name, ex.Name, err)
			}
			assertFleetEnvelope(t, ex, evt, "/warehouse/inbound-receiving", "urn:warehouse:inbound-receiving:events:"+name+":v1")
		}
	}
}

// E4: this consumer is a Conformist on a shared fan-out topic — unknown
// ADDITIVE attributes in data must not fail the decode or the handling
// (the pinned ReceiptClosed example's own discrepancies[] block is the
// live case: this consumer's DTO ignores it), an unknown event TYPE is
// silently skipped (not an error, no projection write), and a message
// that is not a CloudEvent at all is a deterministic decode error
// wrapping cloudevents.ErrNotCloudEvent — the signal
// handleWithDeadLetter dead-letters once and never retries.
func TestUnknownAttributesTolerated_InboundReceiving(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInboundReceivingSpec)
	golden := doc.goldenExamples(t, "ReceiptCartonReceived")[0].Payload

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, spy := newReceiptContractFixture()
	if err := consumer.Handle(context.Background(), goldenKey(tolerant), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(spy.cartons) != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d carton facts", len(spy.cartons))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.inbound-receiving.asn.AsnLateForArrival"
	if err := consumer.Handle(context.Background(), goldenKey(otherType), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(spy.ids) != 1 {
		t.Errorf("unknown event type was acted on (%d projection writes)", len(spy.ids))
	}

	// Not a CloudEvent: deterministic decode error wrapping
	// ErrNotCloudEvent — the DLQ trigger.
	if _, err := cloudevents.Decode([]byte(`{"asn_number":"legacy"}`)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
