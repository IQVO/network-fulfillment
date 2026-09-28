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

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// createKafkaTopicWithPartitions creates topic with the given partition
// count against a live broker and waits for it to become visible via
// ReadPartitions before returning. Used to exercise the exact "1->8
// partitions" scaleup scenario (warehouse-infra PR #42) that exposed the
// LeastBytes-ignores-Key bug fleet-wide.
func createKafkaTopicWithPartitions(t *testing.T, ctx context.Context, brokers []string, topic string, numPartitions int) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) >= numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q never became ready with %d partitions", topic, numPartitions)
}

// TestPublisherKeysMessagesForSameNetworkRefOntoTheSamePartition is the
// real-Kafka-level guarantee behind the Hash-balancer fix: on an
// 8-partition topic (mirroring the Phase 3 partition scaleup,
// warehouse-infra PR #42), every integration event published for the
// SAME NetworkRef must land on the SAME partition, while a DIFFERENT
// NetworkRef's event is free to land elsewhere. This is exactly what
// Kafka's default partitioner provides once a non-nil Key is set AND the
// Writer's Balancer actually hashes it — Publisher.Encode/aggregateKey
// already set the key correctly before this fix, but the writer's
// LeastBytes balancer ignored it entirely for routing purposes, so this
// test would have failed (same-key messages scattered across partitions)
// against the pre-fix code even though the key was populated. It passes
// only once Balancer is &kafkago.Hash{}.
func TestPublisherKeysMessagesForSameNetworkRefOntoTheSamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("network-fulfillment-kafka-itest-partitioning"),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	const numPartitions = 8
	topic := fmt.Sprintf("warehouse.network-fulfillment.events.itest-part-%d", time.Now().UnixNano())
	createKafkaTopicWithPartitions(t, ctx, brokers, topic, numPartitions)

	publisher := outboundkafka.NewPublisher(brokers, func() string { return "evt-fixed" })
	// Point the publisher's writer at the isolated test topic instead of
	// the package-pinned Topic constant, exactly like Encode/Send do for
	// the outbox relay path (topic travels per-message there); here we
	// swap in a dedicated *kafkago.Writer with the test topic fixed so
	// Publish's plain Send(ctx, enc...) still writes to the real topic
	// name kafkago.Message carries (enc.Topic), not the constant.
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}
	t.Cleanup(func() { _ = writer.Close() })
	publisher.Writer = writer

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const sameRef = shared.NetworkRef("net-itest-same-partition")
	const otherRef = shared.NetworkRef("net-itest-other-partition")

	// Publish 3 events for sameRef (received, acknowledged, shipment
	// confirmed — the real per-aggregate ordering concern) plus 1 for a
	// different NetworkRef, to prove the key -- not accident -- drives
	// partition placement.
	events := []struct {
		ref   shared.NetworkRef
		event shared.DomainEvent
	}{
		{sameRef, shared.NetworkOrderReceived{NetworkRef: sameRef, SiteId: "site-1", RequiredShipBy: occurredAt.Add(48 * time.Hour), AcknowledgeBy: occurredAt.Add(24 * time.Hour), LineCount: 2, At: occurredAt}},
		{sameRef, shared.NetworkOrderAcknowledged{NetworkRef: sameRef, SiteId: "site-1", LocalOrderId: "lo-1", ReceivedAt: occurredAt, At: occurredAt.Add(time.Hour)}},
		{sameRef, shared.NetworkOrderShipmentConfirmed{NetworkRef: sameRef, SiteId: "site-1", LocalOrderId: "lo-1", At: occurredAt.Add(2 * time.Hour)}},
		{otherRef, shared.NetworkOrderReceived{NetworkRef: otherRef, SiteId: "site-2", RequiredShipBy: occurredAt.Add(48 * time.Hour), AcknowledgeBy: occurredAt.Add(24 * time.Hour), LineCount: 1, At: occurredAt}},
	}
	for i, e := range events {
		encoded, err := publisher.Encode(ctx, e.event)
		if err != nil {
			t.Fatalf("encode event %d for %s: %v", i, e.ref, err)
		}
		// Stamp the isolated test topic (Encode always stamps the
		// package-pinned Topic constant) before sending, mirroring how
		// the outbox relay routes per-message.
		for j := range encoded {
			encoded[j].Topic = topic
		}
		if err := publisher.Send(ctx, encoded...); err != nil {
			t.Fatalf("send event %d for %s: %v", i, e.ref, err)
		}
	}

	// Read every message back with its partition, one reader per
	// partition (a single Reader without a fixed Partition would only
	// see whatever the group-coordinator assigns it, not every
	// partition's full contents).
	partitionOf := map[string]int{}
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p,
			MaxWait:   2 * time.Second,
		})
		func() {
			defer func() { _ = reader.Close() }()
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			defer readCancel()
			for {
				msg, err := reader.ReadMessage(readCtx)
				if err != nil {
					return // timeout: no more messages on this partition
				}
				partitionOf[string(msg.Key)] = p
			}
		}()
	}

	if len(partitionOf) != 2 {
		t.Fatalf("observed keys->partition = %v, want exactly 2 distinct keys (sameRef, otherRef)", partitionOf)
	}
	samePartition, ok := partitionOf[string(sameRef)]
	if !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", sameRef, partitionOf)
	}
	if _, ok := partitionOf[string(otherRef)]; !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", otherRef, partitionOf)
	}

	// Re-verify by reading samePartition alone from the start and
	// counting how many of sameRef's 3 messages landed there -- all 3
	// must be present, proving the guarantee isn't a one-message
	// coincidence.
	countOnSamePartition := 0
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   samePartition,
		StartOffset: kafkago.FirstOffset,
		MaxWait:     2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	for {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			break
		}
		if string(msg.Key) == string(sameRef) {
			countOnSamePartition++
		}
	}
	if countOnSamePartition != 3 {
		t.Errorf("found %d of sameRef's 3 messages on partition %d, want 3 (all events for one aggregate must share a partition)", countOnSamePartition, samePartition)
	}
}
