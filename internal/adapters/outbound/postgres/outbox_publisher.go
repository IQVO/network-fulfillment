package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// OutboxPublisher implements ports.EventPublisher by writing each
// encoder's Kafka wire form of event into outbox_events instead of the
// broker. When called inside UnitOfWork.Execute the insert joins the use
// case's transaction, so the aggregate change and every one of its
// outbox rows commit together or not at all. OutboxRelay later drains
// the table onto Kafka.
//
// One outbox row is written per (event x encoder), so this service's two
// existing publish targets — the integration topic and the analytics
// topic (cmd/netfulfil's fanOutPublisher) — can never diverge: both are
// enqueued in the exact same transaction as the aggregate write.
//
// Publish's signature matches ports.EventPublisher exactly
// (Publish(ctx, event any) error, not variadic and not
// shared.DomainEvent) — this context's port shape, kept as-is per the
// rollout brief.
type OutboxPublisher struct {
	pool     *pgxpool.Pool
	encoders []outboundkafka.Encoder
}

// NewOutboxPublisher constructs an OutboxPublisher over pool that fans
// each event through every encoder given, in order. Passing no encoders
// is a programmer error the caller must avoid; the composition root
// always supplies at least the integration encoder.
func NewOutboxPublisher(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *OutboxPublisher {
	return &OutboxPublisher{pool: pool, encoders: encoders}
}

// Publish stores event's encoded message for every configured encoder in
// the outbox. It never touches Kafka. event must implement
// shared.DomainEvent — the same contract the direct Kafka publishers
// enforce — so a value that does not is rejected rather than silently
// enqueued with no usable envelope.
func (p *OutboxPublisher) Publish(ctx context.Context, event any) error {
	de, ok := event.(shared.DomainEvent)
	if !ok {
		return fmt.Errorf("postgres: event %T does not implement shared.DomainEvent", event)
	}

	q := querierFrom(ctx, p.pool)
	for _, enc := range p.encoders {
		encodedBatch, err := enc.Encode(ctx, de)
		if err != nil {
			return err
		}
		for _, encoded := range encodedBatch {
			headers, err := marshalHeaders(encoded.Headers)
			if err != nil {
				return fmt.Errorf("postgres: marshal outbox headers for %s/%s: %w", encoded.Topic, encoded.EventType, err)
			}
			if _, err := q.Exec(ctx, `
				INSERT INTO outbox_events (topic, event_type, key, value, headers)
				VALUES ($1, $2, $3, $4, $5)
			`, encoded.Topic, encoded.EventType, encoded.Key, encoded.Value, headers); err != nil {
				return fmt.Errorf("postgres: enqueue outbox event %s for %s: %w", encoded.EventType, encoded.Topic, err)
			}
		}
	}
	return nil
}

// outboxHeader is the JSON wire shape of one kafka-go header stored in
// outbox_events.headers.
type outboxHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// marshalHeaders serializes kafka-go headers into the JSONB shape stored
// on outbox_events.headers, so the relay can reconstruct them exactly
// (e.g. W3C trace headers injected by the encoder).
func marshalHeaders(headers []kafkago.Header) ([]byte, error) {
	out := make([]outboxHeader, 0, len(headers))
	for _, h := range headers {
		out = append(out, outboxHeader{Key: h.Key, Value: h.Value})
	}
	return json.Marshal(out)
}
