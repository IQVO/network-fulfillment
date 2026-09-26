package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// fakeWriter captures the messages handed to WriteMessages so a test can
// assert on the published envelope without a live broker.
type fakeWriter struct {
	msgs []kafkago.Message
	err  error
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if w.err != nil {
		return w.err
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func TestPublisher_PublishesEachEventType(t *testing.T) {
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		event     shared.DomainEvent
		wantType  string
		wantKey   string
		wantField string
		wantValue any
	}{
		{
			name:      "NetworkOrderReceived",
			event:     shared.NetworkOrderReceived{NetworkRef: "po-1", SiteId: "site-1", LineCount: 2, At: at},
			wantType:  "NetworkOrderReceived",
			wantKey:   "po-1",
			wantField: "lineCount",
			wantValue: float64(2),
		},
		{
			name:      "NetworkOrderAcknowledged",
			event:     shared.NetworkOrderAcknowledged{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", At: at},
			wantType:  "NetworkOrderAcknowledged",
			wantKey:   "po-1",
			wantField: "localOrderId",
			wantValue: "ord-1",
		},
		{
			name:      "NetworkOrderRejected",
			event:     shared.NetworkOrderRejected{NetworkRef: "po-1", SiteId: "site-1", Reason: shared.RejectionReasonUntranslatableSKU, At: at},
			wantType:  "NetworkOrderRejected",
			wantKey:   "po-1",
			wantField: "reason",
			wantValue: "UNTRANSLATABLE_SKU",
		},
		{
			name:      "NetworkOrderShipmentConfirmed",
			event:     shared.NetworkOrderShipmentConfirmed{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", At: at},
			wantType:  "NetworkOrderShipmentConfirmed",
			wantKey:   "po-1",
			wantField: "localOrderId",
			wantValue: "ord-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &fakeWriter{}
			p := outboundkafka.NewPublisher(nil, func() string { return "evt-fixed" })
			p.Writer = w

			if err := p.Publish(context.Background(), tt.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.msgs) != 1 {
				t.Fatalf("expected 1 message, got %d", len(w.msgs))
			}
			msg := w.msgs[0]
			if string(msg.Key) != tt.wantKey {
				t.Errorf("key = %q, want %q", string(msg.Key), tt.wantKey)
			}

			var env outboundkafka.Envelope
			if err := json.Unmarshal(msg.Value, &env); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if env.EventType != tt.wantType {
				t.Errorf("event_type = %q, want %q", env.EventType, tt.wantType)
			}
			if env.EventId != "evt-fixed" {
				t.Errorf("event_id = %q, want evt-fixed", env.EventId)
			}
			if env.Source != "network-fulfillment" {
				t.Errorf("source = %q, want network-fulfillment", env.Source)
			}
			if !env.OccurredAt.Equal(at) {
				t.Errorf("occurred_at = %v, want %v", env.OccurredAt, at)
			}

			var data map[string]any
			if err := json.Unmarshal(env.Data, &data); err != nil {
				t.Fatalf("unmarshal data: %v", err)
			}
			if got := data[tt.wantField]; got != tt.wantValue {
				t.Errorf("data[%q] = %v (%T), want %v (%T)", tt.wantField, got, got, tt.wantValue, tt.wantValue)
			}
		})
	}
}

func TestPublisher_PropagatesWriteError(t *testing.T) {
	boom := errors.New("broker down")
	p := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	p.Writer = &fakeWriter{err: boom}

	err := p.Publish(context.Background(), shared.NetworkOrderReceived{NetworkRef: "po-1", At: time.Now()})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapped broker down", err)
	}
}

// TestPublisher_RejectsNonDomainEvent asserts a value that does not
// implement shared.DomainEvent is refused rather than published without a
// usable event_type/occurred_at.
func TestPublisher_RejectsNonDomainEvent(t *testing.T) {
	p := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	w := &fakeWriter{}
	p.Writer = w

	err := p.Publish(context.Background(), "not a domain event")
	if err == nil {
		t.Fatal("expected an error for a non-DomainEvent value")
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no message written, got %d", len(w.msgs))
	}
}

// TestPublisher_Close verifies Close delegates to a *kafkago.Writer when
// one is in use, and is a no-op over a fake.
func TestPublisher_Close(t *testing.T) {
	p := outboundkafka.NewPublisher([]string{"localhost:9092"}, func() string { return "evt" })
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	fake.Writer = &fakeWriter{}
	if err := fake.Close(); err != nil {
		t.Fatalf("Close over fake writer: %v", err)
	}
}
