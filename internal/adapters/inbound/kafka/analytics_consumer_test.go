package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// call captures one projection-store method invocation.
type call struct {
	method  string
	eventId string
	at      time.Time
	latency float64
	reason  string
}

// fakeProjection records the calls the consumer makes so a test can
// assert the envelope was routed to the right method.
type fakeProjection struct {
	calls []call
}

func (f *fakeProjection) ApplyNetworkOrderReceived(_ context.Context, eventId string, at time.Time) error {
	f.calls = append(f.calls, call{method: "received", eventId: eventId, at: at})
	return nil
}

func (f *fakeProjection) ApplyNetworkOrderAcknowledged(_ context.Context, eventId string, at time.Time, latencySeconds float64) error {
	f.calls = append(f.calls, call{method: "acknowledged", eventId: eventId, at: at, latency: latencySeconds})
	return nil
}

func (f *fakeProjection) ApplyNetworkOrderRejected(_ context.Context, eventId string, at time.Time, reason string) error {
	f.calls = append(f.calls, call{method: "rejected", eventId: eventId, at: at, reason: reason})
	return nil
}

// fakeProcessed is an in-memory ProcessedEvents.
type fakeProcessed struct {
	seen map[string]bool
}

func newFakeProcessed() *fakeProcessed { return &fakeProcessed{seen: map[string]bool{}} }

func (p *fakeProcessed) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if p.seen[eventId] {
		return false, nil
	}
	p.seen[eventId] = true
	return true, nil
}

// ceType returns the full CloudEvents type for a NetworkOrder event.
func ceType(eventName string) string {
	return "com.warehouse.wes.network-fulfillment.networkorder." + eventName
}

// envelope builds a structured-mode CloudEvents 1.0 analytics message, the
// only wire format this consumer accepts.
func envelope(t *testing.T, id, eventType string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/network-fulfillment")
	e.SetType(eventType)
	e.SetSubject("po-1")
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:network-fulfillment:analytics:X:v1")
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal cloudevent: %v", err)
	}
	return b
}

// analyticsRoutingCase is one row of TestAnalyticsConsumer_RoutesEachEventType:
// an envelope of a given event type must route to exactly one projection
// method carrying the envelope's identity and timestamp plus the payload
// fields that method parses out of the data block.
type analyticsRoutingCase struct {
	name       string
	eventType  string
	data       map[string]any
	wantMethod string
	wantReason string
	wantLat    float64
}

// runAnalyticsRoutingCase drives one envelope through the consumer and
// asserts the single projection call it was routed to.
func runAnalyticsRoutingCase(t *testing.T, at time.Time, tt analyticsRoutingCase) {
	t.Helper()

	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	raw := envelope(t, "evt-1", tt.eventType, at, tt.data)
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	if len(proj.calls) != 1 {
		t.Fatalf("calls = %d, want 1: %+v", len(proj.calls), proj.calls)
	}
	got := proj.calls[0]
	if got.method != tt.wantMethod {
		t.Errorf("method = %q, want %q", got.method, tt.wantMethod)
	}
	if got.eventId != "evt-1" {
		t.Errorf("eventId = %q, want evt-1", got.eventId)
	}
	if !got.at.Equal(at) {
		t.Errorf("at = %v, want %v", got.at, at)
	}
	if tt.wantReason != "" && got.reason != tt.wantReason {
		t.Errorf("reason = %q, want %q", got.reason, tt.wantReason)
	}
	if tt.wantLat != 0 && got.latency != tt.wantLat {
		t.Errorf("latency = %v, want %v", got.latency, tt.wantLat)
	}
}

func TestAnalyticsConsumer_RoutesEachEventType(t *testing.T) {
	at := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)

	tests := []analyticsRoutingCase{
		{
			name:       "NetworkOrderReceived",
			eventType:  ceType("NetworkOrderReceived"),
			data:       map[string]any{"networkRef": "po-1", "siteId": "site-1", "lineCount": 2},
			wantMethod: "received",
		},
		{
			name:       "NetworkOrderAcknowledged",
			eventType:  ceType("NetworkOrderAcknowledged"),
			data:       map[string]any{"networkRef": "po-1", "receivedAt": at.Add(-90 * time.Second).Format(time.RFC3339Nano)},
			wantMethod: "acknowledged",
			wantLat:    90,
		},
		{
			name:       "NetworkOrderRejected",
			eventType:  ceType("NetworkOrderRejected"),
			data:       map[string]any{"networkRef": "po-1", "reason": "UNTRANSLATABLE_SKU"},
			wantMethod: "rejected",
			wantReason: "UNTRANSLATABLE_SKU",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runAnalyticsRoutingCase(t, at, tt)
		})
	}
}

func TestAnalyticsConsumer_DedupesOnEventId(t *testing.T) {
	at := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	raw := envelope(t, "evt-dup", ceType("NetworkOrderReceived"), at, map[string]any{"networkRef": "po-1"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("first HandleMessage: %v", err)
	}
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("second HandleMessage: %v", err)
	}

	if len(proj.calls) != 1 {
		t.Fatalf("calls = %d, want 1 (deduped)", len(proj.calls))
	}
}

func TestAnalyticsConsumer_IgnoresUnknownEventType(t *testing.T) {
	at := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	raw := envelope(t, "evt-1", "SomethingElseEntirely", at, map[string]any{})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 {
		t.Fatalf("calls = %d, want 0 for an unknown event type", len(proj.calls))
	}
	if len(processed.seen) != 0 {
		t.Fatalf("an unknown event type must not be marked processed")
	}
}

func TestAnalyticsConsumer_ClampsNegativeLatencyToZero(t *testing.T) {
	at := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	// receivedAt AFTER the event time is a clock-skew corner case; latency
	// must clamp to zero rather than go negative.
	raw := envelope(t, "evt-1", ceType("NetworkOrderAcknowledged"), at, map[string]any{
		"receivedAt": at.Add(1 * time.Hour).Format(time.RFC3339Nano),
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 1 || proj.calls[0].latency != 0 {
		t.Fatalf("calls = %+v, want a single call with latency 0", proj.calls)
	}
}

func TestAnalyticsConsumer_RejectsMalformedMessage(t *testing.T) {
	c := &inboundkafka.AnalyticsConsumer{Projection: &fakeProjection{}, Processed: newFakeProcessed()}
	if err := c.HandleMessage(context.Background(), []byte("not json")); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

// TestAnalyticsConsumer_RejectsLegacyFlatEnvelope proves the retired
// flat / analytics-v1 envelope is rejected as not-a-CloudEvent and never
// parsed: nothing is marked processed and no projection is applied.
func TestAnalyticsConsumer_RejectsLegacyFlatEnvelope(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	legacy := []byte(`{"event_id":"evt-legacy","event_type":"NetworkOrderReceived","occurred_at":"2026-09-11T08:00:00Z",` +
		`"source":"network-fulfillment","schema_version":1,"data":{"networkRef":"po-1"}}`)
	if err := c.HandleMessage(context.Background(), legacy); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
	if len(proj.calls) != 0 || len(processed.seen) != 0 {
		t.Fatalf("legacy message was parsed: calls=%+v seen=%v", proj.calls, processed.seen)
	}
}

// TestAnalyticsConsumer_IgnoresShortNameType proves dispatch is on the
// FULL type string: a CloudEvent whose type is only the short event name
// is an unknown type, silently skipped.
func TestAnalyticsConsumer_IgnoresShortNameType(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	raw := envelope(t, "evt-1", "NetworkOrderReceived", time.Now(), map[string]any{})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 || len(processed.seen) != 0 {
		t.Fatalf("short-name type must not be dispatched: calls=%+v", proj.calls)
	}
}

func TestAnalyticsConsumer_TypeConstantsAreFullTypes(t *testing.T) {
	for got, want := range map[string]string{
		inboundkafka.TypeNetworkOrderReceived:     ceType("NetworkOrderReceived"),
		inboundkafka.TypeNetworkOrderAcknowledged: ceType("NetworkOrderAcknowledged"),
		inboundkafka.TypeNetworkOrderRejected:     ceType("NetworkOrderRejected"),
	} {
		if got != want {
			t.Errorf("type = %q, want %q", got, want)
		}
	}
}

func TestNewUniqueConsumerGroup_DiffersAcrossCalls(t *testing.T) {
	a := inboundkafka.NewUniqueConsumerGroup(inboundkafka.AnalyticsConsumerGroupPrefix)
	b := inboundkafka.NewUniqueConsumerGroup(inboundkafka.AnalyticsConsumerGroupPrefix)
	if a == b {
		t.Fatalf("expected two unique consumer group ids, got the same value twice: %q", a)
	}
}
