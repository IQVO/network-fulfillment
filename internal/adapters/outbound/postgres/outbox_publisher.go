package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

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
//
// The W3C trace context of ctx (the span of the request or poll pass that
// ran the use case) is injected into each row's headers with the global
// propagator (gap fix D27, ADR 0022): `traceparent` and, when present,
// `tracestate`/`baggage` are appended after the CloudEvents content-type
// header. The relay copies the stored headers onto the Kafka message, so a
// consumer can continue the trace. Without an active span nothing is added.
func (p *OutboxPublisher) Publish(ctx context.Context, event any) error {
	de, ok := event.(shared.DomainEvent)
	if !ok {
		return fmt.Errorf("postgres: event %T does not implement shared.DomainEvent", event)
	}

	trace := traceHeaders(ctx)
	q := querierFrom(ctx, p.pool)
	for _, enc := range p.encoders {
		encodedBatch, err := enc.Encode(ctx, de)
		if err != nil {
			return err
		}
		for _, encoded := range encodedBatch {
			headers, err := marshalHeaders(withTraceHeaders(encoded.Headers, trace))
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

// traceHeaders returns the W3C trace context of ctx as Kafka headers, in a
// stable key order, using the global propagator telemetry.Setup installs
// (trace-context + baggage). Empty when ctx carries no span.
func traceHeaders(ctx context.Context) []kafkago.Header {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	keys := carrier.Keys()
	sort.Strings(keys)
	out := make([]kafkago.Header, 0, len(keys))
	for _, k := range keys {
		out = append(out, kafkago.Header{Key: k, Value: []byte(carrier.Get(k))})
	}
	return out
}

// withTraceHeaders appends trace to hs, skipping a key hs already has (an
// encoder-set header always wins). hs itself is never modified.
func withTraceHeaders(hs, trace []kafkago.Header) []kafkago.Header {
	if len(trace) == 0 {
		return hs
	}
	out := append(make([]kafkago.Header, 0, len(hs)+len(trace)), hs...)
	for _, t := range trace {
		if !hasHeader(hs, t.Key) {
			out = append(out, t)
		}
	}
	return out
}

func hasHeader(hs []kafkago.Header, key string) bool {
	for _, h := range hs {
		if h.Key == key {
			return true
		}
	}
	return false
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
