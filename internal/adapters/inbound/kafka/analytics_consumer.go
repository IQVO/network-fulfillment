// Package kafka contains network-fulfillment's inbound Kafka adapters.
// Today that is the analytics consumer only: it consumes THIS SERVICE'S
// OWN analytics topic, replaying its own past-tense events into the
// analytical read model, mirroring facility-layout's ADR-0010 and
// process-path-management's ADR 0007 pattern.
//
// Consistent with the rest of the analytics pipeline, this consumer is
// trace-free: it opens no spans and reads no trace headers.
//
// Every message must be a CloudEvents 1.0 event (ADR 0008): it is decoded
// with internal/adapters/kafka/cloudevents.Decode, dispatched on the FULL
// `type` string, deduped on the CloudEvents `id`, and reads its occurred-at
// from the `time` attribute. Anything that fails CloudEvents validation
// (including the retired flat envelope) is a deterministic poison message:
// it is dead-lettered immediately, never retried and never parsed as a
// legacy shape.
//
// ADR 0004 (ported from order-management's ADR 0025) adds in-process
// retry and a dead-letter topic here — see handleMessage's doc comment
// for the two-phase retry split this consumer needs that
// order-management's RepromiseConsumer does not, because this
// consumer's dedupe gate (ProcessedEvents.MarkProcessed) is a separate,
// non-transactional call rather than one committed atomically with the
// projection write.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
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

// dlqTopicSuffix names the dead-letter topic this consumer publishes a
// poison message to, relative to its OWN source topic (never a fixed
// constant) — mirrors order-management's RepromiseConsumer convention
// exactly, so an isolated test topic gets its own isolated DLQ topic for
// free.
const dlqTopicSuffix = ".dlq"

// maxHandlerAttempts bounds each retried phase (markProcessed, apply) in
// handleMessage: 1 initial attempt plus up to 2 retries, matching the
// reference's "up to 3" bound.
const maxHandlerAttempts = 3

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 2 * time.Second
)

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

// The full CloudEvents `type` strings this consumer's projection handles.
// Built from the shared cloudevents helper (not imported from the outbound
// publisher, so this inbound adapter never depends on an outbound adapter
// — arch-go enforced).
var (
	TypeNetworkOrderReceived     = cloudevents.Type("networkorder", "NetworkOrderReceived")
	TypeNetworkOrderAcknowledged = cloudevents.Type("networkorder", "NetworkOrderAcknowledged")
	TypeNetworkOrderRejected     = cloudevents.Type("networkorder", "NetworkOrderRejected")
)

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
// per CloudEvents id despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  ProcessedEvents
	Logger     *slog.Logger
	// dlqWriter publishes a poison message (ADR 0004 §DLQ) to
	// topic+dlqTopicSuffix after maxHandlerAttempts in-process retries
	// of a phase (markProcessed or apply) all fail with a genuine
	// infrastructure error. nil in a zero-value AnalyticsConsumer some
	// existing unit tests construct directly (they call HandleMessage
	// directly and never reach the DLQ path) — dlqPublish itself guards
	// against a nil writer so those tests keep compiling unchanged.
	dlqWriter *kafkago.Writer
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under groupID (see NewUniqueConsumerGroup). The dead-letter
// topic is always derived as topic+dlqTopicSuffix, so an isolated test
// topic/group gets its own isolated DLQ topic for free.
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
	return &AnalyticsConsumer{
		Reader:     reader,
		Projection: projection,
		Processed:  processed,
		Logger:     logger,
		dlqWriter: &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: topic + dlqTopicSuffix,
			// Hash (not the zero-value default, RoundRobin — see
			// kafka-go's Writer.Config, which defaults Balancer to
			// RoundRobin when nil): dlqPublish forwards the SAME
			// aggregate key the source message carried (msg.Key,
			// below), specifically so a manual replay tool can process
			// one partition's dead letters without interleaving
			// unrelated aggregates and, if it replays in offset order,
			// preserve each aggregate's original relative event order.
			// RoundRobin would scatter one aggregate's dead-lettered
			// events across the DLQ topic's partitions despite the key
			// being set, defeating that. See ADR 0005.
			Balancer: &kafkago.Hash{},
		},
	}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. Each fetched message is retried in-process
// (ADR 0004 §DLQ) before being dead-lettered on exhaustion; either way
// the offset is committed once handleMessage returns, so one poison
// message can never block every other event behind it on this
// partition.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.handleMessage(ctx, msg); err != nil {
			return err
		}
	}
}

// Close releases the underlying Kafka reader and, if configured, the DLQ
// writer.
func (c *AnalyticsConsumer) Close() error {
	readerErr := c.Reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// handleMessage decodes/filters msg (a message that is not a valid
// CloudEvent is a permanent failure: dead-lettered immediately with no
// retry; an unrecognized `type` is silently committed past), then retries the mark-processed and
// projection-apply phases INDEPENDENTLY, each up to maxHandlerAttempts.
//
// The two phases are retried SEPARATELY, unlike order-management's
// RepromiseConsumer (which retries its whole handler as one unit,
// because its dedupe gate commits atomically with the state change it
// guards). This consumer's ProcessedEvents.MarkProcessed is a plain,
// separate call: if it succeeds and is then followed by an Apply
// failure, retrying the WHOLE handler from the top would call
// MarkProcessed again, see the event already marked, and skip Apply
// entirely — silently losing the projection update. Retrying only the
// phase that actually failed avoids that trap.
func (c *AnalyticsConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	topic := c.Reader.Config().Topic

	env, recognized, decodeErr := decodeAnalyticsEvent(msg.Value)
	if decodeErr != nil {
		// Not a valid CloudEvent (e.g. a retired flat-envelope message):
		// deterministic poison, dead-lettered at once without retry.
		c.log(ctx, "analytics: message is not a valid CloudEvent, sending to dead-letter topic",
			"topic", topic, "partition", msg.Partition, "offset", msg.Offset, "error", decodeErr)
		if dlqErr := c.dlqPublish(ctx, msg, decodeErr); dlqErr != nil {
			return fmt.Errorf("analytics: publish to dead-letter topic: %w", dlqErr)
		}
		return c.commit(ctx, msg)
	}
	if !recognized {
		return c.commit(ctx, msg)
	}

	isNew, err := c.markProcessedWithRetry(ctx, env.ID())
	if err != nil {
		return c.deadLetter(ctx, msg, topic, env, "mark_processed", err)
	}
	if !isNew {
		return c.commit(ctx, msg)
	}

	if err := c.applyWithRetry(ctx, env); err != nil {
		return c.deadLetter(ctx, msg, topic, env, "apply", err)
	}
	return c.commit(ctx, msg)
}

// deadLetter logs and publishes msg to the dead-letter topic after a
// phase has exhausted its retries, then commits the offset regardless —
// one poison message must never block every other event on this
// partition.
func (c *AnalyticsConsumer) deadLetter(ctx context.Context, msg kafkago.Message, topic string, env ce.Event, phase string, cause error) error {
	c.log(ctx, "analytics: exhausted retries, sending to dead-letter topic",
		"topic", topic, "dlq_topic", topic+dlqTopicSuffix, "phase", phase,
		"ce_id", env.ID(), "ce_type", env.Type(), "attempts", maxHandlerAttempts, "error", cause)
	if dlqErr := c.dlqPublish(ctx, msg, cause); dlqErr != nil {
		return fmt.Errorf("analytics: publish to dead-letter topic: %w", dlqErr)
	}
	return c.commit(ctx, msg)
}

// markProcessedWithRetry retries Processed.MarkProcessed with jittered
// exponential backoff, bounded to maxHandlerAttempts total attempts and
// to ctx's own deadline/cancellation. Safe to retry as a whole: an
// erroring call never marks the event (only a SUCCESSFUL call can), so
// re-attempting after a failure cannot double-mark or skip anything.
func (c *AnalyticsConsumer) markProcessedWithRetry(ctx context.Context, eventId string) (bool, error) {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.RetryNotifyWithData(func() (bool, error) {
		return c.Processed.MarkProcessed(ctx, eventId)
	}, bounded, nil)
}

// applyWithRetry retries the projection-apply step (decode of the
// event-specific payload plus the matching Projection.Apply* call) with
// jittered exponential backoff, bounded exactly like
// markProcessedWithRetry.
func (c *AnalyticsConsumer) applyWithRetry(ctx context.Context, env ce.Event) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		return c.applyProjection(ctx, env)
	}, bounded)
}

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool) to the dead-letter topic. A nil dlqWriter (the
// zero-value AnalyticsConsumer some unit tests construct directly,
// which never exercises this path) is a documented no-op rather than a
// nil-pointer panic.
func (c *AnalyticsConsumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.Reader.Config().Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// commit acknowledges msg so it is never redelivered. Only a commit
// failure itself aborts the consume loop.
func (c *AnalyticsConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.Reader.CommitMessages(ctx, msg)
}

func (c *AnalyticsConsumer) log(ctx context.Context, msg string, args ...any) {
	if c.Logger != nil {
		c.Logger.WarnContext(ctx, msg, args...)
	}
}

// decodeAnalyticsEvent decodes raw as a CloudEvents 1.0 event and reports
// whether its full `type` is one this consumer's projection contract
// recognizes. A decode error (wrapping cloudevents.ErrNotCloudEvent) is
// permanent (never retried); an unrecognized type is a normal, expected
// skip (forward compatibility), not an error.
func decodeAnalyticsEvent(raw []byte) (ce.Event, bool, error) {
	env, err := cloudevents.Decode(raw)
	if err != nil {
		return ce.Event{}, false, fmt.Errorf("analytics: decode event: %w", err)
	}
	switch env.Type() {
	case TypeNetworkOrderReceived, TypeNetworkOrderAcknowledged, TypeNetworkOrderRejected:
		return env, true, nil
	default:
		return env, false, nil
	}
}

// applyProjection decodes env's payload (DataAs) and applies the matching
// projection method, using the CloudEvents `id` and `time` attributes.
// Retried independently of MarkProcessed — see handleMessage's doc
// comment for why.
func (c *AnalyticsConsumer) applyProjection(ctx context.Context, env ce.Event) error {
	var data analyticsEventData
	if err := env.DataAs(&data); err != nil {
		return fmt.Errorf("analytics: decode event data: %w", err)
	}
	at := env.Time()

	switch env.Type() {
	case TypeNetworkOrderReceived:
		return c.Projection.ApplyNetworkOrderReceived(ctx, env.ID(), at)
	case TypeNetworkOrderAcknowledged:
		latency := at.Sub(data.ReceivedAt).Seconds()
		if latency < 0 {
			latency = 0
		}
		return c.Projection.ApplyNetworkOrderAcknowledged(ctx, env.ID(), at, latency)
	case TypeNetworkOrderRejected:
		return c.Projection.ApplyNetworkOrderRejected(ctx, env.ID(), at, data.Reason)
	default:
		return nil
	}
}

// HandleMessage decodes raw as a CloudEvent and applies the matching
// projection method for its `type` — a thin, non-retrying wrapper around
// decodeAnalyticsEvent/applyProjection/MarkProcessed, used by unit tests
// that do not need Run's retry/DLQ machinery (which needs a real
// *kafkago.Reader/Message). A message that is not a valid CloudEvent
// returns an error wrapping cloudevents.ErrNotCloudEvent.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	env, recognized, err := decodeAnalyticsEvent(raw)
	if err != nil {
		return err
	}
	if !recognized {
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, env.ID())
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	return c.applyProjection(ctx, env)
}
