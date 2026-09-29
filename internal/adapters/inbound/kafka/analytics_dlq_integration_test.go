//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
)

// syncFakeProcessed is fakeProcessed plus a mutex: this integration test
// reads its "seen" state from the main test goroutine (a polling wait
// loop) while AnalyticsConsumer.Run's own goroutine writes to it
// concurrently, unlike every existing unit test in this package (which
// only ever calls a fake synchronously, in one goroutine at a time).
type syncFakeProcessed struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newSyncFakeProcessed() *syncFakeProcessed { return &syncFakeProcessed{seen: map[string]bool{}} }

func (p *syncFakeProcessed) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen[eventId] {
		return false, nil
	}
	p.seen[eventId] = true
	return true, nil
}

func (p *syncFakeProcessed) has(eventId string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen[eventId]
}

// syncFakeProjection is fakeProjection plus a mutex, for the same
// cross-goroutine reason as syncFakeProcessed above.
type syncFakeProjection struct {
	mu    sync.Mutex
	calls []call
}

func (f *syncFakeProjection) ApplyNetworkOrderReceived(_ context.Context, eventId string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{method: "received", eventId: eventId, at: at})
	return nil
}

func (f *syncFakeProjection) ApplyNetworkOrderAcknowledged(_ context.Context, eventId string, at time.Time, latencySeconds float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{method: "acknowledged", eventId: eventId, at: at, latency: latencySeconds})
	return nil
}

func (f *syncFakeProjection) ApplyNetworkOrderRejected(_ context.Context, eventId string, at time.Time, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{method: "rejected", eventId: eventId, at: at, reason: reason})
	return nil
}

func (f *syncFakeProjection) eventIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, len(f.calls))
	for i, c := range f.calls {
		ids[i] = c.eventId
	}
	return ids
}

// alwaysFailingProcessedEventsFor wraps a real ProcessedEvents so
// MarkProcessed fails with a genuine infrastructure error for exactly
// poisonEventID, on EVERY call, while every other event_id is delegated
// unchanged -- letting one poison message coexist in the SAME test with
// a normal, successfully-processed message on the SAME partition.
type alwaysFailingProcessedEventsFor struct {
	inboundkafka.ProcessedEvents
	poisonEventID string
}

func (p *alwaysFailingProcessedEventsFor) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	if eventID == p.poisonEventID {
		return false, fmt.Errorf("simulated poison-message infrastructure failure for event %s", eventID)
	}
	return p.ProcessedEvents.MarkProcessed(ctx, eventID)
}

// TestAnalyticsConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition
// is the ADR 0004 §DLQ acceptance test, ported verbatim (structurally)
// from order-management's RepromiseConsumer equivalent: a message whose
// mark-processed phase ALWAYS fails must, after exactly
// maxHandlerAttempts (3) in-process retries, land on "<topic>.dlq" with
// the raw original payload plus error context headers, and the consumer
// must commit past it and keep processing -- a well-formed message
// published right after the poison one must be handled without delay,
// proving the partition was never blocked on the one bad message.
func TestAnalyticsConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("nf-analytics-dlq-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.network-fulfillment.analytics.dlq-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createTopic(t, ctx, brokers, topic)
	createTopic(t, ctx, brokers, dlqTopic)

	poisonEventID := fmt.Sprintf("evt-dlq-poison-%d", time.Now().UnixNano())
	goodEventID := fmt.Sprintf("evt-dlq-good-%d", time.Now().UnixNano())

	processed := newSyncFakeProcessed()
	failingProcessed := &alwaysFailingProcessedEventsFor{ProcessedEvents: processed, poisonEventID: poisonEventID}
	projection := &syncFakeProjection{}

	consumer := inboundkafka.NewAnalyticsConsumer(
		brokers,
		topic,
		fmt.Sprintf("network-fulfillment-analytics-dlq-itest-%d", time.Now().UnixNano()),
		projection,
		failingProcessed,
		nil,
	)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	// Start reading the DLQ topic BEFORE publishing, so the poison
	// message's eventual dead-letter write is never missed to a race.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	at := time.Now().UTC().Truncate(time.Second)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(poisonEventID),
		Value: envelopeBytes(t, poisonEventID, "NetworkOrderReceived", at, map[string]any{}),
	}); err != nil {
		t.Fatalf("publish poison NetworkOrderReceived: %v", err)
	}

	// Assert the poison message lands on the DLQ topic with the raw
	// payload and error context, after the retry budget is exhausted.
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Key) != poisonEventID {
		t.Errorf("DLQ message key = %q, want %q (raw key preserved)", string(dlqMsg.Key), poisonEventID)
	}
	var dlqPayload map[string]any
	if err := json.Unmarshal(dlqMsg.Value, &dlqPayload); err != nil {
		t.Fatalf("DLQ message value is not the raw original JSON payload: %v", err)
	}
	if dlqPayload["event_id"] != poisonEventID {
		t.Errorf("DLQ payload event_id = %v, want %q -- payload must be byte-identical to the original for manual replay", dlqPayload["event_id"], poisonEventID)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)
	if h := headerValue(dlqMsg.Headers, "x-dlq-error"); h == "" {
		t.Error("DLQ message missing x-dlq-error header with failure context")
	}
	if h := headerValue(dlqMsg.Headers, "x-dlq-failed-at"); h == "" {
		t.Error("DLQ message missing x-dlq-failed-at header")
	}

	// Now publish a well-formed message right after the poison one,
	// and confirm it is processed without delay -- proving the
	// partition was not blocked behind the poison message.
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(goodEventID),
		Value: envelopeBytes(t, goodEventID, "NetworkOrderReceived", at, map[string]any{}),
	}); err != nil {
		t.Fatalf("publish well-formed NetworkOrderReceived: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		if processed.has(goodEventID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("well-formed message was not processed -- partition appears blocked behind the poison message")
		}
		time.Sleep(100 * time.Millisecond)
	}

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	// The healthy event's projection call must be the ONLY one applied
	// -- the poison message never got past MarkProcessed, no matter how
	// many times it was retried.
	appliedEventIDs := projection.eventIDs()
	if len(appliedEventIDs) != 1 || appliedEventIDs[0] != goodEventID {
		t.Fatalf("applied projection calls = %v, want exactly [%q]", appliedEventIDs, goodEventID)
	}
}

func envelopeBytes(t *testing.T, eventId, eventType string, at time.Time, data map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	env := map[string]any{
		"event_id":       eventId,
		"event_type":     eventType,
		"occurred_at":    at.Format(time.RFC3339Nano),
		"source":         "network-fulfillment",
		"schema_version": 1,
		"data":           json.RawMessage(raw),
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

// createTopic creates topic on brokers via a raw kafka-go admin
// connection, and fails the test if it cannot -- these DLQ tests need
// their OWN topic (never a fixed/shared name), so this is called once
// per topic per test, mirroring order-management's own
// createRepromiseTopic helper.
func createTopic(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker %s: %v", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("resolve controller: %v", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer func() { _ = controllerConn.Close() }()

	if err := controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

func assertHeader(t *testing.T, headers []kafkago.Header, key, want string) {
	t.Helper()
	got := headerValue(headers, key)
	if got != want {
		t.Errorf("header %q = %q, want %q", key, got, want)
	}
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
