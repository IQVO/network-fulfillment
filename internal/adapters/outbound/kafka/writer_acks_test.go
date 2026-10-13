package kafka_test

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
)

// TestPublishers_WaitForAllInSyncReplicas pins acks=all and synchronous
// writes on both event publishers. kafka-go defaults RequiredAcks to
// RequireNone (acks=0): WriteMessages returns nil without the broker ever
// confirming, so the outbox relay (or a direct caller) would mark an event
// published that the broker never stored. Synchronous (no Async) is what
// lets a failed write return its error to the outbox/caller.
func TestPublishers_WaitForAllInSyncReplicas(t *testing.T) {
	brokers := []string{"localhost:9092"}
	publisher := outboundkafka.NewPublisher(brokers, func() string { return "evt" })
	analytics := outboundkafka.NewAnalyticsPublisher(brokers, func() string { return "evt" })
	t.Cleanup(func() {
		_ = publisher.Close()
		_ = analytics.Close()
	})
	for name, writer := range map[string]outboundkafka.Writer{
		"integration publisher": publisher.Writer,
		"analytics publisher":   analytics.Writer,
	} {
		t.Run(name, func(t *testing.T) {
			w, ok := writer.(*kafkago.Writer)
			if !ok {
				t.Fatalf("writer = %T, want *kafkago.Writer", writer)
			}
			if w.RequiredAcks != kafkago.RequireAll {
				t.Fatalf("RequiredAcks = %v, want RequireAll (kafka-go's default is RequireNone, acks=0)", w.RequiredAcks)
			}
			if w.Async {
				t.Fatal("publisher writer must be synchronous so a failed write reaches the outbox/caller")
			}
		})
	}
}
