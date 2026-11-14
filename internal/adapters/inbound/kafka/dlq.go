package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// Dead-letter header keys carried on every DLQ'd message: where it came
// from, why it died, and when. A DLQ'd message keeps its original headers
// and key so an operator can replay it verbatim.
const (
	dlqHeaderSourceTopic = "x-dlq-source-topic"
	dlqHeaderError       = "x-dlq-error"
	dlqHeaderFailedAt    = "x-dlq-failed-at"
)

// deadLetterMessage copies msg with the three dead-letter headers appended
// (original headers and key preserved). Every consumer's DLQ path builds
// its message here so the header set stays identical across the fleet.
func deadLetterMessage(msg kafkago.Message, sourceTopic string, cause error, failedAt time.Time) kafkago.Message {
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: dlqHeaderSourceTopic, Value: []byte(sourceTopic)},
		kafkago.Header{Key: dlqHeaderError, Value: []byte(cause.Error())},
		kafkago.Header{Key: dlqHeaderFailedAt, Value: []byte(failedAt.UTC().Format(time.RFC3339))},
	)
	return kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers}
}

// handleWithDeadLetter is the message loop every event consumer shares:
// handle runs under the fleet's bounded retry policy, where a
// deterministic decode failure — not a CloudEvent, or a payload the
// producer's contract does not recognize (badPayload) — fails permanently
// instead of retrying. A handler still failing after the retries is
// dead-lettered and committed past, so a poison message never blocks the
// partition behind it; a cancelled context returns without dead-lettering.
func handleWithDeadLetter(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	handle func(context.Context, string, []byte) error,
	badPayload error,
	reader *kafkago.Reader,
	dlq func(context.Context, kafkago.Message, error) error,
	msg kafkago.Message,
) error {
	policy := backoff.NewExponentialBackOff(backoff.WithInitialInterval(retryInitialInterval), backoff.WithMaxInterval(retryMaxInterval))
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)
	err := backoff.Retry(func() error {
		err := handle(ctx, string(msg.Key), msg.Value)
		if errors.Is(err, cloudevents.ErrNotCloudEvent) || errors.Is(err, badPayload) {
			return backoff.Permanent(err)
		}
		return err
	}, bounded)
	if err == nil {
		return reader.CommitMessages(ctx, msg)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	logger.Warn(name+": sending the message to the dead-letter topic",
		"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
	if dlqErr := dlq(ctx, msg, err); dlqErr != nil {
		return fmt.Errorf("%s: publish to dead-letter topic: %w", name, dlqErr)
	}
	return reader.CommitMessages(ctx, msg)
}
