package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// TestAnalyticsPublisher_GoldenCloudEventPerEventType asserts every event
// type on the analytics stream: same `type` as the integration stream,
// dataschema ...:analytics:<Event>:v1, and no schema_version field.
func TestAnalyticsPublisher_GoldenCloudEventPerEventType(t *testing.T) {
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	for _, tt := range goldenCases(at) {
		t.Run(tt.name, func(t *testing.T) {
			w := &fakeWriter{}
			p := outboundkafka.NewAnalyticsPublisher(nil, func() string { return fixedID })
			p.Writer = w
			if err := p.Publish(context.Background(), tt.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.msgs) != 1 {
				t.Fatalf("expected 1 message, got %d", len(w.msgs))
			}
			assertGoldenMessage(t, w.msgs[0], outboundkafka.AnalyticsTopic, wantCloudEvent(tt.name, tt.typeSuffix, "analytics", tt.version, tt.data))
		})
	}
}

func TestAnalyticsPublisher_PropagatesWriteError(t *testing.T) {
	boom := errors.New("broker down")
	p := outboundkafka.NewAnalyticsPublisher(nil, func() string { return "evt" })
	p.Writer = &fakeWriter{err: boom}

	err := p.Publish(context.Background(), shared.NetworkOrderReceived{NetworkRef: "po-1", At: time.Now()})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapped broker down", err)
	}
}

func TestAnalyticsPublisher_RejectsNonDomainEvent(t *testing.T) {
	p := outboundkafka.NewAnalyticsPublisher(nil, func() string { return "evt" })
	w := &fakeWriter{}
	p.Writer = w

	if err := p.Publish(context.Background(), 42); err == nil {
		t.Fatal("expected an error for a non-DomainEvent value")
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no message written, got %d", len(w.msgs))
	}
}

func TestAnalyticsPublisher_Close(t *testing.T) {
	p := outboundkafka.NewAnalyticsPublisher([]string{"localhost:9092"}, func() string { return "evt" })
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake := outboundkafka.NewAnalyticsPublisher(nil, func() string { return "evt" })
	fake.Writer = &fakeWriter{}
	if err := fake.Close(); err != nil {
		t.Fatalf("Close over fake writer: %v", err)
	}
}
