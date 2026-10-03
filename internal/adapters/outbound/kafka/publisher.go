// Package kafka provides the outbound adapter that publishes
// network-fulfillment domain events onto Kafka, satisfying
// ports.EventPublisher.
//
// This publisher emits EVERY domain event onto the integration topic —
// the whole Published Language a downstream Conformist would consume —
// mirroring facility-layout's ADR-0009 pattern and process-path-management's
// ADR 0002.
//
// Every message is a CloudEvents 1.0 event in structured content mode
// (ADR 0008): built by internal/adapters/kafka/cloudevents and carrying
// the content-type header. There is no other envelope.
package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Topic is the integration topic network-fulfillment publishes its
// Published Language onto, following the estate convention
// warehouse.<context>.events.
const Topic = "warehouse.network-fulfillment.events"

// Entity is the `type` entity segment for every event this context
// raises: they are all raised by the NetworkOrder aggregate.
const Entity = "networkorder"

// dataSchemaVersion is the dataschema version of every payload this
// service publishes today (on both streams).
const dataSchemaVersion = 1

// Writer is the subset of *kafkago.Writer the Publisher needs, so tests can
// substitute a fake without a live broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Encoded is one wire-ready message (Value is the CloudEvents
// structured-mode JSON; Headers always include the content-type header), produced by Encode without touching
// the broker. Topic is set explicitly (rather than pinned on the
// Writer): the outbox relay's Sink has no fixed topic of its own, and a
// single relay pass may forward rows destined for either the
// integration topic or the analytics topic, so the topic must travel
// per-message.
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// Encoder turns one or more domain events into their Kafka wire form for
// one topic, without sending them. Both the integration publisher (this
// file) and the analytics publisher (analytics_publisher.go) implement
// it, so postgres.OutboxPublisher can fan a single event out to several
// topics inside one transaction.
type Encoder interface {
	Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error)
}

// Publisher publishes network-fulfillment domain events onto Kafka. It
// satisfies ports.EventPublisher, and its Send method also serves as the
// outbox relay's Sink (the underlying Writer carries no fixed topic — see
// NewPublisher — so one Publisher instance can relay rows for both the
// integration and the analytics topic).
type Publisher struct {
	Writer Writer
	NewId  func() string
}

// NewPublisher constructs a Publisher writing to brokers. newId mints the
// CloudEvents `id` (a UUID) exactly once per encoded event; the outbox
// persists the encoded bytes, so a redelivery carries the same id. The underlying Writer carries NO fixed
// topic: Encode always stamps Topic explicitly onto the Encoded message,
// so the same Writer can carry both integration and analytics traffic
// when this Publisher is reused as the relay's Sink.
//
// Balancer is kafkago.Hash (FNV-1a over Message.Key), not LeastBytes:
// kafka-go's Writer does not hash Message.Key into a partition decision
// automatically just because Encode/aggregateKey sets a non-nil,
// per-NetworkRef key — the Balancer field is a fully separate knob, and
// LeastBytes routes purely by cumulative byte volume, ignoring Key
// entirely. Hash is what actually turns aggregateKey's per-NetworkRef key
// into a same-aggregate-same-partition guarantee. This matters now that
// warehouse-infra PR #42 scaled every business topic (including this
// one) from 1 to 8 partitions: at 1 partition the missing key-aware
// balancer was invisible (total order was preserved by accident), but at
// 8 partitions a consumer could observe this NetworkRef's events out of
// order. See ADR 0005.
func NewPublisher(brokers []string, newId func() string) *Publisher {
	return &Publisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewId: newId,
	}
}

// Encode translates events into their integration-topic wire form (one
// CloudEvent each, dataschema stream "events"), without sending them.
func (p *Publisher) Encode(_ context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	return encodeAll(Topic, cloudevents.StreamEvents, p.NewId, events)
}

// encodeAll builds one CloudEvents-encoded message per event for topic,
// with dataschema stream stream. Shared by Publisher and
// AnalyticsPublisher so both streams carry identical attributes apart
// from dataschema and id.
func encodeAll(topic, stream string, newId func() string, events []shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, event := range events {
		key := aggregateKey(event)
		value, err := cloudevents.New(cloudevents.Spec{
			ID:        newId(),
			Entity:    Entity,
			EventName: event.EventName(),
			Subject:   key,
			Time:      event.OccurredAt(),
			Stream:    stream,
			Version:   dataSchemaVersion,
			Data:      event,
		})
		if err != nil {
			return nil, fmt.Errorf("kafka: encode %s for %s: %w", event.EventName(), topic, err)
		}
		out = append(out, Encoded{
			Topic:     topic,
			EventType: cloudevents.Type(Entity, event.EventName()),
			Key:       []byte(key),
			Value:     value,
			Headers:   []kafkago.Header{cloudevents.ContentTypeHeader()},
		})
	}
	return out, nil
}

// Publish emits event onto Topic as a CloudEvent. event must
// implement shared.DomainEvent; a value that does not is rejected rather
// than published headerless.
func (p *Publisher) Publish(ctx context.Context, event any) error {
	de, ok := event.(shared.DomainEvent)
	if !ok {
		return fmt.Errorf("kafka: event %T does not implement shared.DomainEvent", event)
	}
	encoded, err := p.Encode(ctx, de)
	if err != nil {
		return err
	}
	if err := p.Send(ctx, encoded...); err != nil {
		return fmt.Errorf("kafka: publish %s: %w", de.EventName(), err)
	}
	return nil
}

// Send writes already-encoded messages to their respective enc.Topic in
// one WriteMessages call. This is what the outbox relay calls (via the
// Sink interface) to forward drained rows to the broker; it is also
// reused by Publish for the direct (no-outbox) path.
func (p *Publisher) Send(ctx context.Context, encoded ...Encoded) error {
	if len(encoded) == 0 {
		return nil
	}
	msgs := make([]kafkago.Message, len(encoded))
	for i, enc := range encoded {
		msgs[i] = kafkago.Message{Topic: enc.Topic, Key: enc.Key, Value: enc.Value, Headers: enc.Headers}
	}
	return p.Writer.WriteMessages(ctx, msgs...)
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
