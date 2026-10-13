package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// TestNewDLQWriter_WaitsForAllInSyncReplicas pins acks=all on the DLQ
// writer: kafka-go defaults RequiredAcks to RequireNone (acks=0), so a
// broker blip would silently drop the poison message while the consumer
// commits its offset as if it had been dead-lettered.
func TestNewDLQWriter_WaitsForAllInSyncReplicas(t *testing.T) {
	w := newDLQWriter([]string{"localhost:9092"}, "warehouse.network-fulfillment.analytics.dlq")
	t.Cleanup(func() { _ = w.Close() })
	if w.Topic != "warehouse.network-fulfillment.analytics.dlq" {
		t.Fatalf("DLQ topic = %q, want warehouse.network-fulfillment.analytics.dlq", w.Topic)
	}
	if _, ok := w.Balancer.(*kafkago.Hash); !ok {
		t.Fatalf("DLQ Balancer = %T, want *kafkago.Hash (ADR 0005: keep one aggregate's dead letters on one partition)", w.Balancer)
	}
	if w.RequiredAcks != kafkago.RequireAll {
		t.Fatalf("DLQ RequiredAcks = %v, want RequireAll (kafka-go's default is RequireNone, acks=0)", w.RequiredAcks)
	}
	if w.Async {
		t.Fatal("DLQ writer must be synchronous so a failed dead-letter write reaches the consumer")
	}
}
