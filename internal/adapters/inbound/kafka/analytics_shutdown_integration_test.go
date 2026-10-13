//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
)

// blockingProjection parks every apply call until the handler's context is
// cancelled, then reports that cancellation -- exactly what a real
// projection store does when the pod is told to stop mid-write.
type blockingProjection struct {
	entered chan struct{}
}

func (p *blockingProjection) block(ctx context.Context) error {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func (p *blockingProjection) ApplyNetworkOrderReceived(ctx context.Context, _ string, _ time.Time) error {
	return p.block(ctx)
}

func (p *blockingProjection) ApplyNetworkOrderAcknowledged(ctx context.Context, _ string, _ time.Time, _ float64) error {
	return p.block(ctx)
}

func (p *blockingProjection) ApplyNetworkOrderRejected(ctx context.Context, _ string, _ time.Time, _ string) error {
	return p.block(ctx)
}

// TestAnalyticsConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing pins
// the shutdown contract: cancelling Run's context while a healthy message is
// being handled is a clean stop (Run returns nil), and that message is NOT
// dead-lettered -- its offset stays uncommitted, so it is redelivered on the
// next start and the processed-events dedupe absorbs the replay. Before the
// fix the cancellation surfaced from Run as "context canceled" (the flaky
// failure of the DLQ test's shutdown assertion) and the retry wrapper's
// cancellation error was routed into deadLetter as if the message were poison.
func TestAnalyticsConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("nf-analytics-shutdown-itest-%d", time.Now().UnixNano())))
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

	topic := fmt.Sprintf("warehouse.network-fulfillment.analytics.shutdown-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createTopic(t, ctx, brokers, topic)
	createTopic(t, ctx, brokers, dlqTopic)

	projection := &blockingProjection{entered: make(chan struct{}, 1)}
	consumer := inboundkafka.NewAnalyticsConsumer(
		brokers,
		topic,
		fmt.Sprintf("network-fulfillment-analytics-shutdown-itest-%d", time.Now().UnixNano()),
		projection,
		newSyncFakeProcessed(),
		nil,
		nil,
	)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	eventID := fmt.Sprintf("evt-shutdown-%d", time.Now().UnixNano())
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(eventID),
		Value: envelopeBytes(t, eventID, "NetworkOrderReceived", time.Now().UTC().Truncate(time.Second), map[string]any{}),
	}); err != nil {
		t.Fatalf("publish NetworkOrderReceived: %v", err)
	}

	select {
	case <-projection.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("the consumer never started applying the message")
	}
	consumeCancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	if msg, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(msg.Key), msg.Headers)
	}
}
