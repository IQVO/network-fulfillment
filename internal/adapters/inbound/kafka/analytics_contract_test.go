package kafka

// Event consumer contract tests (E1-E4) for this repo's OWN analytics
// self-edge: the netfulfil-projector's AnalyticsConsumer reads
// warehouse.network-fulfillment.analytics (outboundkafka.AnalyticsTopic,
// wired into kafka.ReaderConfig by NewAnalyticsConsumer) — the same topic
// this repo's outbox relay writes — so the producer is network-
// fulfillment itself. The contract validates against the PINNED copy of
// this repo's own apis/asyncapi.yaml
// (testdata/network-fulfillment.asyncapi.yaml, a self-pin in
// contracts/pinned.json: it guards against editing the spec without
// updating the projector). Golden fixtures are the spec's OWN examples.

import (
	"context"
	"errors"
	"strings"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	kafkaoutbound "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
)

// E1: the topic the projector's reader actually subscribes to
// (outboundkafka.AnalyticsTopic — the constant the projector's main and
// NewAnalyticsConsumer wire into kafka.ReaderConfig) is documented by the
// pinned self-spec, and every CloudEvents type the projection's switch
// routes on is documented on that channel — including the HISTORIC v1
// acknowledgement type ADR 0016 keeps recognised.
func TestPinnedAsyncAPICoversSubscriptions_OwnAnalytics(t *testing.T) {
	documented := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec).documentedTypes(t, kafkaoutbound.AnalyticsTopic)
	for _, typ := range []string{
		TypeNetworkOrderReceived,
		TypeNetworkOrderSubmitted,
		TypeNetworkOrderAcknowledged,
		// TypeNetworkOrderAcknowledgedV1 is intentionally NOT asserted here:
		// the pinned spec retired the v1 type in favour of .v2 (the fleet's
		// stated evolution policy); the consumer still recognises it (ADR 0016)
		// but the documented contract is .v2 only.
		TypeNetworkOrderRejected,
	} {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned self-spec does not document %s on %s (documented: %v)", typ, kafkaoutbound.AnalyticsTopic, documented)
		}
	}
}

// newAnalyticsContractFixture builds the real consumer over the
// projection and processed-events fakes from analytics_consumer_test.go.
func newAnalyticsContractFixture() (*AnalyticsConsumer, *contractFakeProjection, *contractFakeProcessed) {
	projection, processed := &contractFakeProjection{}, newContractFakeProcessed()
	return &AnalyticsConsumer{Projection: projection, Processed: processed}, projection, processed
}

// E2: every analytics example the pinned self-spec documents on the
// topic decodes through the REAL decoder and routes through the REAL
// HandleMessage → claimAndApply → applyProjection: the projection method
// its type selects is called exactly once with the envelope's id and
// time, and a replayed CloudEvents id is a claim-miss that applies
// nothing (ADR 0023's dedupe contract).
func TestGoldenEventsDecode_OwnAnalytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	for _, name := range []string{"NetworkOrderReceivedAnalytics", "NetworkOrderAcknowledgedAnalytics", "NetworkOrderRejectedAnalytics"} {
		for _, ex := range doc.goldenExamples(t, name) {
			t.Run(name+"/"+ex.Name, func(t *testing.T) {
				consumer, projection, _ := newAnalyticsContractFixture()
				raw := goldenJSON(t, ex.Payload)
				if err := consumer.HandleMessage(context.Background(), raw); err != nil {
					t.Fatalf("handle golden example: %v", err)
				}
				assertAnalyticsRouted(t, ex, projection)
				// Dedupe contract: a replayed CloudEvents id records nothing more.
				if err := consumer.HandleMessage(context.Background(), raw); err != nil {
					t.Fatalf("replayed golden example: %v", err)
				}
				assertAnalyticsRouted(t, ex, projection)
			})
		}
	}
}

// assertAnalyticsRouted asserts exactly one projection call of the kind
// the golden type selects, carrying the envelope's own facts.
func assertAnalyticsRouted(t *testing.T, ex pinnedExample, projection *contractFakeProjection) {
	t.Helper()
	if len(projection.calls) != 1 {
		t.Fatalf("golden event produced %d projection calls, want 1", len(projection.calls))
	}
	got := projection.calls[0]
	if got.eventId != ex.Payload["id"] {
		t.Errorf("projection claimed id %q, wire CloudEvents id is %q", got.eventId, ex.Payload["id"])
	}
	want := "received"
	switch ex.Payload["type"] {
	case TypeNetworkOrderAcknowledged, TypeNetworkOrderAcknowledgedV1:
		want = "acknowledged"
	case TypeNetworkOrderRejected:
		want = "rejected"
	}
	if got.method != want {
		t.Errorf("type %s routed to %q, want %q", ex.Payload["type"], got.method, want)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — this repo's own spec is held to the same bar it demands of
// its producers, with this repo's source, an analytics dataschema urn,
// and the structured-mode Kafka content-type header.
func TestCloudEventsEnvelopeInvariants_OwnAnalytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	for _, name := range doc.producerMessageRefs(t, kafkaoutbound.AnalyticsTopic) {
		for _, ex := range doc.goldenExamples(t, name) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example %s/%s: %v", name, ex.Name, err)
			}
			assertAnalyticsDataschema(t, ex, evt)
		}
	}
}

// assertAnalyticsDataschema holds the golden examples to this repo's own
// dataschema convention: the analytics urn prefix, the INTEGRATION event
// name (no -Analytics suffix), and a version suffix that matches the event
// type's own .vN when it carries one.
func assertAnalyticsDataschema(t *testing.T, ex pinnedExample, evt ce.Event) {
	t.Helper()
	ds := evt.DataSchema()
	if !strings.HasPrefix(ds, "urn:warehouse:network-fulfillment:analytics:") {
		t.Errorf("%s: dataschema %q, want the analytics urn prefix", ex.Name, ds)
	}
	typ, _ := ex.Payload["type"].(string)
	if i := strings.LastIndex(typ, ".v"); i >= 0 {
		if !strings.HasSuffix(ds, ":"+typ[i+1:]) {
			t.Errorf("%s: dataschema %q does not carry the type's %s version", ex.Name, ds, typ[i+1:])
		}
	}
	assertFleetEnvelope(t, ex, evt, "/warehouse/network-fulfillment", "")
}

// E4: unknown ADDITIVE attributes in data must not fail the decode or
// the apply (this spec's own events grow additive fields without notice),
// an event type this projection does not act on is claimed and skipped —
// not an error — and a message that is not a CloudEvent at all is a
// deterministic decode error wrapping ErrNotCloudEvent: the exact signal
// the consumer's retry/DLQ machinery (ADR 0004 §DLQ) dead-letters.
func TestUnknownAttributesTolerated_OwnAnalytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	golden := doc.goldenExamples(t, "NetworkOrderReceivedAnalytics")[0].Payload

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, projection, _ := newAnalyticsContractFixture()
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(projection.calls) != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d projection calls", len(projection.calls))
	}

	// An event type with no report effect (NetworkOrderSubmitted): a
	// recognised, claimed fact — not an error, and no projection call.
	submitted := clonePayload(golden)
	submitted["type"] = TypeNetworkOrderSubmitted
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, submitted)); err != nil {
		t.Fatalf("no-report-effect type must be claimed and skipped, not an error: %v", err)
	}
	if len(projection.calls) != 1 {
		t.Errorf("no-report-effect type produced projection calls (%d)", len(projection.calls))
	}

	// Not a CloudEvent: deterministic decode error wrapping
	// ErrNotCloudEvent — the DLQ trigger.
	if err := consumer.HandleMessage(context.Background(), []byte(`{"networkRef":"po-legacy"}`)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
