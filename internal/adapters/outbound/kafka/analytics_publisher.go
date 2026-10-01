package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the analytics data product
// consumes. It is separate from the integration topic (Topic) so the
// OLTP integration contract and the analytical read-model stream evolve
// independently.
const AnalyticsTopic = "warehouse.network-fulfillment.analytics"

// AnalyticsPublisher publishes network-fulfillment domain events onto
// AnalyticsTopic as CloudEvents (same `type` as the integration stream,
// dataschema urn:warehouse:network-fulfillment:analytics:<Event>:v1). It satisfies ports.EventPublisher
// and is a SEPARATE adapter from Publisher: the integration publisher
// (publisher.go) publishes the same events to
// warehouse.network-fulfillment.events and is left untouched. The
// composition root fans out to BOTH so the integration and analytics
// streams stay independent — directly when there is no outbox to bind
// them to, or via the transactional outbox (both encoders feeding the
// same OutboxPublisher) when DATABASE_URL is configured.
//
// Consistent with the ADR-0009-equivalent integration publisher, this
// adapter is trace-free: no OTel package is used by the analytics
// processes, so no producer span is opened and no trace headers are
// injected.
type AnalyticsPublisher struct {
	Writer Writer
	NewId  func() string
}

// NewAnalyticsPublisher constructs an AnalyticsPublisher writing to
// brokers. The underlying Writer carries NO fixed topic (mirrors
// NewPublisher): Encode always stamps AnalyticsTopic explicitly onto the
// Encoded message.
//
// Balancer is kafkago.Hash, matching NewPublisher's choice (see its doc
// comment): this publisher already keys every message by NetworkRef
// (aggregateKey), but LeastBytes would silently discard that key for
// partition routing — Hash is what actually turns the key into a
// same-aggregate-same-partition guarantee on AnalyticsTopic's 8
// partitions (warehouse-infra PR #42). See ADR 0005.
func NewAnalyticsPublisher(brokers []string, newId func() string) *AnalyticsPublisher {
	return &AnalyticsPublisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewId: newId,
	}
}

// Encode translates events into their analytics-topic wire form, without
// sending them.
func (p *AnalyticsPublisher) Encode(_ context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	return encodeAll(AnalyticsTopic, cloudevents.StreamAnalytics, p.NewId, events)
}

// Publish emits event onto AnalyticsTopic as a CloudEvent.
func (p *AnalyticsPublisher) Publish(ctx context.Context, event any) error {
	de, ok := event.(shared.DomainEvent)
	if !ok {
		return fmt.Errorf("kafka: event %T does not implement shared.DomainEvent", event)
	}
	encoded, err := p.Encode(ctx, de)
	if err != nil {
		return err
	}
	if err := p.Send(ctx, encoded...); err != nil {
		return fmt.Errorf("kafka: publish %s analytics event: %w", de.EventName(), err)
	}
	return nil
}

// Send writes already-encoded messages to their respective enc.Topic in
// one WriteMessages call — the same Sink shape Publisher.Send exposes, so
// either publisher can serve as the outbox relay's sink.
func (p *AnalyticsPublisher) Send(ctx context.Context, encoded ...Encoded) error {
	if len(encoded) == 0 {
		return nil
	}
	msgs := make([]kafkago.Message, len(encoded))
	for i, enc := range encoded {
		msgs[i] = kafkago.Message{Topic: enc.Topic, Key: enc.Key, Value: enc.Value, Headers: enc.Headers}
	}
	return p.Writer.WriteMessages(ctx, msgs...)
}

// Close releases the underlying Kafka writer.
func (p *AnalyticsPublisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}
