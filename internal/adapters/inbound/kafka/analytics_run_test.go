package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// This file unit-tests the consumer's Run/Close/DLQ wiring that the
// HandleMessage-level tests (analytics_consumer_test.go, kafka_test) cannot
// reach: the constructor's reader/writer configuration, the retry bounds of
// claimAndApplyWithRetry, and the nil-writer/nil-logger guards. The paths
// that need a live broker (a DLQ publish that succeeds, a commit that
// succeeds) stay in the integration-tagged files.

// ---- fakes (package-local: the kafka_test fakes live in another package) ----

type stubProjection struct {
	received     int
	acknowledged int
	rejected     int
}

func (p *stubProjection) ApplyNetworkOrderReceived(_ context.Context, _ string, _ time.Time) error {
	p.received++
	return nil
}

func (p *stubProjection) ApplyNetworkOrderAcknowledged(_ context.Context, _ string, _ time.Time, _ float64) error {
	p.acknowledged++
	return nil
}

func (p *stubProjection) ApplyNetworkOrderRejected(_ context.Context, _ string, _ time.Time, _ string) error {
	p.rejected++
	return nil
}

var _ report.ProjectionStore = (*stubProjection)(nil)

// flakyProcessed fails the first failAt MarkProcessed calls, then behaves
// like the in-memory dedupe gate.
type flakyProcessed struct {
	failAt int
	calls  int
	seen   map[string]bool
}

func (p *flakyProcessed) MarkProcessed(_ context.Context, id string) (bool, error) {
	p.calls++
	if p.calls <= p.failAt {
		return false, fmt.Errorf("mark processed attempt %d: connection reset", p.calls)
	}
	if p.seen[id] {
		return false, nil
	}
	if p.seen == nil {
		p.seen = map[string]bool{}
	}
	p.seen[id] = true
	return true, nil
}

// ---- NewAnalyticsConsumer ---------------------------------------------------

func TestNewAnalyticsConsumer_WiresReaderAndDLQWriter(t *testing.T) {
	c := NewAnalyticsConsumer([]string{"127.0.0.1:9092"}, "analytics-topic", "group-1",
		&stubProjection{}, &flakyProcessed{}, nil, nil)
	t.Cleanup(func() { _ = c.Close() })

	cfg := c.Reader.Config()
	if cfg.Topic != "analytics-topic" {
		t.Errorf("reader topic = %q, want analytics-topic", cfg.Topic)
	}
	if cfg.GroupID != "group-1" {
		t.Errorf("reader group = %q, want group-1", cfg.GroupID)
	}
	if cfg.StartOffset != kafkago.FirstOffset {
		t.Errorf("reader StartOffset = %v, want FirstOffset (the projection must replay the full topic history)", cfg.StartOffset)
	}
	if c.dlqWriter == nil {
		t.Fatal("dlqWriter not wired")
	}
	if c.dlqWriter.Topic != "analytics-topic.dlq" {
		t.Errorf("DLQ topic = %q, want analytics-topic.dlq (always derived from the source topic)", c.dlqWriter.Topic)
	}
	if _, ok := c.dlqWriter.Balancer.(*kafkago.Hash); !ok {
		t.Errorf("DLQ balancer = %T, want *kafkago.Hash (ADR 0005)", c.dlqWriter.Balancer)
	}
	if c.dlqWriter.RequiredAcks != kafkago.RequireAll {
		t.Errorf("DLQ RequiredAcks = %v, want RequireAll", c.dlqWriter.RequiredAcks)
	}
	if c.Atomic != nil {
		t.Errorf("Atomic = %v, want nil passed through untouched", c.Atomic)
	}
}

func TestNewAnalyticsConsumer_NilLoggerFallsBackToDefault(t *testing.T) {
	c := NewAnalyticsConsumer([]string{"127.0.0.1:9092"}, "t", "g", &stubProjection{}, &flakyProcessed{}, nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	if c.Logger == nil {
		t.Fatal("Logger = nil, want slog.Default()")
	}
}

// ---- Close ------------------------------------------------------------------

func TestClose_NilDLQWriterReturnsReaderErr(t *testing.T) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", GroupID: "g"})
	c := &AnalyticsConsumer{Reader: reader}
	if err := c.Close(); err != nil {
		t.Fatalf("Close on a never-connected reader = %v, want nil", err)
	}
}

func TestClose_ClosesReaderAndDLQWriter(t *testing.T) {
	c := NewAnalyticsConsumer([]string{"127.0.0.1:9092"}, "t", "g", &stubProjection{}, &flakyProcessed{}, nil, nil)
	if err := c.Close(); err != nil {
		t.Fatalf("Close = %v, want nil on never-connected reader+writer", err)
	}
}

// ---- Run --------------------------------------------------------------------

func TestRun_ReturnsNilOnCancelledContext(t *testing.T) {
	c := NewAnalyticsConsumer([]string{"127.0.0.1:1"}, "t", "g", &stubProjection{}, &flakyProcessed{}, nil, nil)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run on a cancelled context = %v, want nil (clean stop)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// ---- claimAndApplyWithRetry ---------------------------------------------------

func TestClaimAndApplyWithRetry_SucceedsFirstAttempt(t *testing.T) {
	processed := &flakyProcessed{failAt: 0}
	proj := &stubProjection{}
	c := &AnalyticsConsumer{Projection: proj, Processed: processed}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.claimAndApplyWithRetry(ctx, validEvent(t, "evt-1")); err != nil {
		t.Fatalf("claimAndApplyWithRetry: %v", err)
	}
	if processed.calls != 1 {
		t.Errorf("attempts = %d, want 1", processed.calls)
	}
	if proj.received != 1 {
		t.Errorf("projection calls = %d, want 1", proj.received)
	}
}

func TestClaimAndApplyWithRetry_RecoversTransientFailure(t *testing.T) {
	processed := &flakyProcessed{failAt: 1}
	proj := &stubProjection{}
	c := &AnalyticsConsumer{Projection: proj, Processed: processed}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.claimAndApplyWithRetry(ctx, validEvent(t, "evt-1")); err != nil {
		t.Fatalf("claimAndApplyWithRetry: %v", err)
	}
	if processed.calls != 2 {
		t.Errorf("attempts = %d, want 2 (one failure, one success)", processed.calls)
	}
	if proj.received != 1 {
		t.Errorf("projection calls = %d, want 1 (the failed attempt rolled back)", proj.received)
	}
}

func TestClaimAndApplyWithRetry_ExhaustsAfterMaxAttempts(t *testing.T) {
	processed := &flakyProcessed{failAt: maxHandlerAttempts}
	proj := &stubProjection{}
	c := &AnalyticsConsumer{Projection: proj, Processed: processed}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	err := c.claimAndApplyWithRetry(ctx, validEvent(t, "evt-1"))
	if err == nil {
		t.Fatal("expected an error after every attempt failed")
	}
	if !errors.Is(err, context.DeadlineExceeded) && processed.calls != maxHandlerAttempts {
		t.Errorf("attempts = %d, want %d", processed.calls, maxHandlerAttempts)
	}
	if proj.received != 0 {
		t.Errorf("projection calls = %d, want 0 (the claim never succeeded)", proj.received)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Errorf("retry policy took %v, want it bounded well below the ctx deadline", elapsed)
	}
}

// ---- handleMessage / deadLetter / dlqPublish / commit / log -------------------
//
// commit() needs a live broker to succeed; these tests exercise the rest of
// the pipeline against a real (never-connected) reader and a short-lived
// context, asserting the pipeline ran by its effects and that the commit
// failure is what surfaces.

func TestHandleMessage_AppliesThenFailsOnlyAtCommit(t *testing.T) {
	proj := &stubProjection{}
	processed := &flakyProcessed{}
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "t", GroupID: "g"})
	t.Cleanup(func() { _ = reader.Close() })
	c := &AnalyticsConsumer{Reader: reader, Projection: proj, Processed: processed}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	raw := cloudEventJSON(t, "evt-1", TypeNetworkOrderReceived, time.Now())

	err := c.handleMessage(ctx, kafkago.Message{Value: raw})
	if err == nil {
		t.Fatal("expected the unreachable-broker commit to fail the message")
	}
	if proj.received != 1 {
		t.Errorf("projection calls = %d, want 1 (claim+apply ran before the commit)", proj.received)
	}
}

func TestHandleMessage_PoisonMessageSkipsProjection(t *testing.T) {
	proj := &stubProjection{}
	processed := &flakyProcessed{}
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "t", GroupID: "g"})
	t.Cleanup(func() { _ = reader.Close() })
	// nil dlqWriter: the publish is a documented no-op (dlqPublish guard).
	c := &AnalyticsConsumer{Reader: reader, Projection: proj, Processed: processed}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := c.handleMessage(ctx, kafkago.Message{Value: []byte(`{"not":"a cloudevent"}`)})
	if err == nil {
		t.Fatal("expected the unreachable-broker commit to fail the message")
	}
	if proj.received != 0 {
		t.Errorf("projection calls = %d, want 0 (a poison message is never projected)", proj.received)
	}
	if processed.calls != 0 {
		t.Errorf("mark processed calls = %d, want 0 (a poison message is never claimed)", processed.calls)
	}
}

func TestDeadLetter_CommitsPastThePoisonClaim(t *testing.T) {
	proj := &stubProjection{}
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "t", GroupID: "g"})
	t.Cleanup(func() { _ = reader.Close() })
	c := &AnalyticsConsumer{Reader: reader, Projection: proj, Processed: &flakyProcessed{failAt: 99}}

	env, _, err := decodeAnalyticsEvent(cloudEventJSON(t, "evt-1", TypeNetworkOrderReceived, time.Now()))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	// nil dlqWriter -> dlqPublish no-op, so the only error is the commit.
	if err := c.deadLetter(ctx, kafkago.Message{Value: []byte("x")}, "t", env, "claim_and_apply", errors.New("exhausted")); err == nil {
		t.Fatal("expected the unreachable-broker commit to fail")
	}
}

func TestDlqPublish_NilWriterIsADocumentedNoop(t *testing.T) {
	c := &AnalyticsConsumer{} // zero value: no reader, no writer
	if err := c.dlqPublish(context.Background(), kafkago.Message{Value: []byte("x")}, errors.New("boom")); err != nil {
		t.Fatalf("dlqPublish with nil writer = %v, want nil no-op", err)
	}
}

func TestLog_NilLoggerIsSafe(t *testing.T) {
	c := &AnalyticsConsumer{}
	c.log(context.Background(), "nothing should panic") // nil Logger guard
	c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	c.log(context.Background(), "nor with a logger")
}

// validEvent decodes a fresh, recognized CloudEvent for the retry tests.
func validEvent(t *testing.T, id string) ce.Event {
	t.Helper()
	env, _, err := decodeAnalyticsEvent(cloudEventJSON(t, id, TypeNetworkOrderReceived, time.Now()))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return env
}

// cloudEventJSON builds a structured-mode CloudEvents 1.0 message.
func cloudEventJSON(t *testing.T, id, eventType string, at time.Time) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/network-fulfillment")
	e.SetType(eventType)
	e.SetSubject("po-1")
	e.SetTime(at)
	if err := e.SetData(ce.ApplicationJSON, map[string]any{}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("encode cloudevent: %v", err)
	}
	return b
}
