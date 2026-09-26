// Package kafka provides the outbound adapter that publishes
// network-fulfillment domain events onto Kafka, satisfying
// ports.EventPublisher.
//
// This publisher emits EVERY domain event onto the integration topic —
// the whole Published Language a downstream Conformist would consume —
// mirroring facility-layout's ADR-0009 pattern and process-path-management's
// ADR 0002.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Topic is the integration topic network-fulfillment publishes its
// Published Language onto, following the estate convention
// warehouse.<context>.events.
const Topic = "warehouse.network-fulfillment.events"

// Envelope is the CloudEvents-like wrapper shared across the
// warehouse-systems services' integration topics. data carries the domain
// event's own JSON.
type Envelope struct {
	EventId    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
}

// Writer is the subset of *kafkago.Writer the Publisher needs, so tests can
// substitute a fake without a live broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Publisher publishes network-fulfillment domain events onto Kafka. It
// satisfies ports.EventPublisher.
type Publisher struct {
	Writer Writer
	NewId  func() string
}

// NewPublisher constructs a Publisher writing to Topic on brokers. newId
// mints the envelope event_id (e.g. a UUID).
func NewPublisher(brokers []string, newId func() string) *Publisher {
	return &Publisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  Topic,
			Balancer:               &kafkago.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
		NewId: newId,
	}
}

// Publish emits event onto Topic wrapped in an Envelope. event must be one
// of this context's shared.DomainEvent types (a *usecases.recordingPublisher
// in tests aside, that is the only thing the application layer ever hands
// an EventPublisher); a value that is not is rejected rather than
// published headerless.
func (p *Publisher) Publish(ctx context.Context, event any) error {
	de, ok := event.(shared.DomainEvent)
	if !ok {
		return fmt.Errorf("kafka: event %T does not implement shared.DomainEvent", event)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafka: marshal event data: %w", err)
	}
	env := Envelope{
		EventId:    p.NewId(),
		EventType:  de.EventName(),
		OccurredAt: de.OccurredAt(),
		Source:     "network-fulfillment",
		Data:       data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("kafka: marshal envelope: %w", err)
	}

	msg := kafkago.Message{Key: []byte(aggregateKey(de)), Value: payload}
	if err := p.Writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka: publish %s: %w", de.EventName(), err)
	}
	return nil
}

// aggregateKey returns the partition/ordering key for an event: the
// NetworkRef of the aggregate that raised it, so every event for one
// network order lands on the same partition and preserves per-aggregate
// order.
func aggregateKey(event shared.DomainEvent) string {
	switch e := event.(type) {
	case shared.NetworkOrderReceived:
		return string(e.NetworkRef)
	case shared.NetworkOrderAcknowledged:
		return string(e.NetworkRef)
	case shared.NetworkOrderRejected:
		return string(e.NetworkRef)
	case shared.NetworkOrderShipmentConfirmed:
		return string(e.NetworkRef)
	default:
		return event.EventName()
	}
}

// Close releases the underlying Kafka writer.
func (p *Publisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}
