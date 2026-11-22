package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
)

// crlfKafkaReader is a standalone reader (no consumer group, whose rebalance
// loop would block instead) over an unreachable broker: CommitMessages fails
// at once (connection refused), which the log-hygiene tests ignore — they
// assert on the records logged BEFORE the commit attempt.
func crlfKafkaReader() *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{"127.0.0.1:1"}, Topic: "logsafe.topic",
		MaxWait: time.Millisecond,
	})
}

// TestShipmentConfirmedConsumerLogsNoEventDerivedLineBreaks drives the two
// Info paths whose attributes are event-derived (ce_id, load_id, order_id)
// with values carrying CR/LF: no log attribute may carry the line break.
func TestShipmentConfirmedConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &ShipmentConfirmedConsumer{
		Reader: crlfKafkaReader(),
		Orders: memory.NewNetworkOrderRepo(),
		Logger: slog.New(rec),
		Now:    time.Now,
	}
	ctx := context.Background()

	// An order_id no NetworkOrder carries: skipped and logged.
	skip := ce.New("1.0")
	skip.SetID("id" + crlfForged)
	skip.SetSource("urn:logsafe")
	skip.SetType(TypeShipmentConfirmed)
	if err := skip.SetData("application/json", map[string]any{
		"load_id": "load" + crlfForged,
		"orders":  []map[string]any{{"order_id": "order" + crlfForged, "package_count": 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.confirm(ctx, "load"+crlfForged, skip); err != nil {
		t.Fatal(err)
	}

	// A schema-legal event with an empty orders[] array: logged.
	empty := ce.New("1.0")
	empty.SetID("id" + crlfForged)
	empty.SetSource("urn:logsafe")
	empty.SetType(TypeShipmentConfirmed)
	if err := empty.SetData("application/json", map[string]any{
		"load_id": "load" + crlfForged,
		"orders":  []map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.confirm(ctx, "load"+crlfForged, empty); err != nil {
		t.Fatal(err)
	}

	rec.assertNoLineBreaks(t, 2)
}

// TestAnalyticsDeadLetterLogsNoEventDerivedLineBreaks drives the analytics
// consumer's exhausted-retries dead-letter log with a CloudEvent id/type and
// a cause error carrying CR/LF: no log attribute may carry the line break.
func TestAnalyticsDeadLetterLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &AnalyticsConsumer{Reader: crlfKafkaReader(), Logger: slog.New(rec)}
	env := ce.New("1.0")
	env.SetID("id" + crlfForged)
	env.SetSource("urn:logsafe")
	env.SetType("type" + crlfForged)
	// dlqPublish is a documented no-op with a nil writer; the commit failure
	// against the unreachable broker is returned and ignored here.
	_ = c.deadLetter(context.Background(), kafkago.Message{Topic: "logsafe.topic"}, "logsafe.topic", env, "claim_and_apply", errors.New("boom"+crlfForged))
	rec.assertNoLineBreaks(t, 1)
}

// TestHandleWithDeadLetterLogsNoEventDerivedLineBreaks drives the shared
// retry-then-dead-letter helper with a permanent error whose text carries
// CR/LF (a crafted load_id inside a contract error): the dead-letter Warn
// must not log the line break.
func TestHandleWithDeadLetterLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	handle := func(context.Context, string, []byte) error {
		return fmt.Errorf("%w: ShipmentConfirmed %s has an orders[] entry without order_id", errBadShipmentEvent, "load"+crlfForged)
	}
	dlq := func(context.Context, kafkago.Message, error) error { return nil }
	err := handleWithDeadLetter(context.Background(), slog.New(rec), "shipment-confirmed consumer",
		handle, errBadShipmentEvent, crlfKafkaReader(), dlq,
		kafkago.Message{Topic: "logsafe.topic", Key: []byte("load" + crlfForged)})
	if err != nil {
		t.Logf("handleWithDeadLetter returned %v (commit against the unreachable broker)", err)
	}
	rec.assertNoLineBreaks(t, 1)
}
