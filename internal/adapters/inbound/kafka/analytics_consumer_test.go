package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
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

func envelope(t *testing.T, eventId, eventType string, at time.Time, data map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	env := map[string]any{
		"event_id":       eventId,
		"event_type":     eventType,
		"occurred_at":    at.Format(time.RFC3339Nano),
		"source":         "network-fulfillment",
		"schema_version": 1,
		"data":           json.RawMessage(raw),
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

func TestAnalyticsConsumer_RoutesEachEventType(t *testing.T) {
	at := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		eventType  string
		data       map[string]any
		wantMethod string
		wantReason string
		wantLat    float64
	}{
		{
			name:       "NetworkOrderReceived",
			eventType:  "NetworkOrderReceived",
			data:       map[string]any{"networkRef": "po-1", "siteId": "site-1", "lineCount": 2},
			wantMethod: "received",
		},
		{
			name:       "NetworkOrderAcknowledged",
			eventType:  "NetworkOrderAcknowledged",
			data:       map[string]any{"networkRef": "po-1", "receivedAt": at.Add(-90 * time.Second).Format(time.RFC3339Nano)},
			wantMethod: "acknowledged",
			wantLat:    90,
		},
		{
			name:       "NetworkOrderRejected",
			eventType:  "NetworkOrderRejected",
			data:       map[string]any{"networkRef": "po-1", "reason": "UNTRANSLATABLE_SKU"},
			wantMethod: "rejected",
			wantReason: "UNTRANSLATABLE_SKU",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
		})
	}
}

func TestAnalyticsConsumer_DedupesOnEventId(t *testing.T) {
	at := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed}

	raw := envelope(t, "evt-dup", "NetworkOrderReceived", at, map[string]any{"networkRef": "po-1"})
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

	// receivedAt AFTER occurred_at is a clock-skew corner case; latency
	// must clamp to zero rather than go negative.
	raw := envelope(t, "evt-1", "NetworkOrderAcknowledged", at, map[string]any{
		"receivedAt": at.Add(1 * time.Hour).Format(time.RFC3339Nano),
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 1 || proj.calls[0].latency != 0 {
		t.Fatalf("calls = %+v, want a single call with latency 0", proj.calls)
	}
}

func TestAnalyticsConsumer_RejectsMalformedEnvelope(t *testing.T) {
	c := &inboundkafka.AnalyticsConsumer{Projection: &fakeProjection{}, Processed: newFakeProcessed()}
	if err := c.HandleMessage(context.Background(), []byte("not json")); err == nil {
		t.Fatal("expected an error for a malformed envelope")
	}
}

func TestNewUniqueConsumerGroup_DiffersAcrossCalls(t *testing.T) {
	a := inboundkafka.NewUniqueConsumerGroup(inboundkafka.AnalyticsConsumerGroupPrefix)
	b := inboundkafka.NewUniqueConsumerGroup(inboundkafka.AnalyticsConsumerGroupPrefix)
	if a == b {
		t.Fatalf("expected two unique consumer group ids, got the same value twice: %q", a)
	}
}
