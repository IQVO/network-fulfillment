// Package kafka contains network-fulfillment's inbound Kafka adapters.
// Today that is the analytics consumer only: it consumes THIS SERVICE'S
// OWN analytics topic, replaying its own past-tense events into the
// analytical read model, mirroring facility-layout's ADR-0010 and
// process-path-management's ADR 0007 pattern.
//
// Consistent with the rest of the analytics pipeline, this consumer is
// trace-free: it opens no spans and reads no trace headers.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// AnalyticsConsumerGroupPrefix names the Kafka consumer group the
// analytics projector reads under. It is a PREFIX, not the group itself:
// NewUniqueConsumerGroup appends hostname+PID+timestamp so each process
// instance gets its own group. Sharing one fixed group id across process
// instances would let a brand-new projector inherit an EARLIER instance's
// already-advanced committed offset and be marked healthy having replayed
// nothing itself — a failure mode this fleet has been bitten by twice.
const AnalyticsConsumerGroupPrefix = "network-fulfillment-analytics"

// NewUniqueConsumerGroup mints a consumer group id unique to this process
// instance: prefix, hostname, PID, and a nanosecond timestamp. Call it
// once per process start, never reuse the result across restarts.
func NewUniqueConsumerGroup(prefix string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%s-%d-%d", prefix, host, os.Getpid(), time.Now().UnixNano())
}

// ProcessedEvents is the consumer's idempotency gate: MarkProcessed
// records an event id if it has not been seen and reports whether this
// call was the first to record it. It is declared here (rather than in
// application/ports) because it is an analytics-only concern the OLTP
// layers never touch; the analyticsstore ConsumedEventsRepo implements
// it.
type ProcessedEvents interface {
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// analyticsEnvelope is the inbound decode shape of the Envelope v1 wrapper
// on the analytics topic. Declared here (rather than imported from the
// outbound publisher) so this inbound adapter does not depend on an
// outbound adapter (arch-go enforced).
type analyticsEnvelope struct {
	EventId       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Source        string          `json:"source"`
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}

// analyticsEventData is the subset of every event's own JSON this consumer
// needs to compute the acknowledgement report: the reason for a rejection,
// and the receivedAt timestamp an acknowledgement carries so latency can
// be derived without a second lookup.
type analyticsEventData struct {
	Reason     string    `json:"reason"`
	ReceivedAt time.Time `json:"receivedAt"`
}

// AnalyticsConsumer reads analytics events off the analytics topic and
// applies each to the acknowledgement-report ProjectionStore, exactly once
// per event_id despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  ProcessedEvents
	Logger     *slog.Logger
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under groupID (see NewUniqueConsumerGroup).
func NewAnalyticsConsumer(brokers []string, topic, groupID string, projection report.ProjectionStore, processed ProcessedEvents, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new consumer group starts at the EARLIEST offset: the
		// analytics projection must see the full history of the topic
		// (it is a replayable read model), not just what arrives after
		// this process boots.
		StartOffset: kafkago.FirstOffset,
	})
	return &AnalyticsConsumer{Reader: reader, Projection: projection, Processed: processed, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. A handling error is logged and the loop
// continues so one bad message cannot wedge the projector.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.HandleMessage(ctx, msg.Value); err != nil {
			c.Logger.ErrorContext(ctx, "analytics message handling failed", "error", err)
		}
	}
}

// Close releases the underlying Kafka reader.
func (c *AnalyticsConsumer) Close() error {
	return c.Reader.Close()
}

// HandleMessage decodes raw as an analyticsEnvelope and applies the
// matching projection method for its event_type. Event types outside the
// projection contract are ignored (and not marked processed). For a
// projecting event it dedupes on event_id via ProcessedEvents before
// applying, so a redelivery is a no-op. It is exported separately from Run
// so tests can feed raw envelopes without a live broker.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	var env analyticsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("analytics: decode envelope: %w", err)
	}

	switch env.EventType {
	case "NetworkOrderReceived", "NetworkOrderAcknowledged", "NetworkOrderRejected":
	default:
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, env.EventId)
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	var data analyticsEventData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return fmt.Errorf("analytics: decode event data: %w", err)
	}

	switch env.EventType {
	case "NetworkOrderReceived":
		return c.Projection.ApplyNetworkOrderReceived(ctx, env.EventId, env.OccurredAt)
	case "NetworkOrderAcknowledged":
		latency := env.OccurredAt.Sub(data.ReceivedAt).Seconds()
		if latency < 0 {
			latency = 0
		}
		return c.Projection.ApplyNetworkOrderAcknowledged(ctx, env.EventId, env.OccurredAt, latency)
	case "NetworkOrderRejected":
		return c.Projection.ApplyNetworkOrderRejected(ctx, env.EventId, env.OccurredAt, data.Reason)
	default:
		return nil
	}
}
