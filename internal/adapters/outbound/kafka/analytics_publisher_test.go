package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestAnalyticsPublisher_PublishesToAnalyticsTopicWithSchemaVersion(t *testing.T) {
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	w := &fakeWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, func() string { return "evt-fixed" })
	p.Writer = w

	event := shared.NetworkOrderAcknowledged{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", At: at}
	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}

	var env outboundkafka.AnalyticsEnvelope
	if err := json.Unmarshal(w.msgs[0].Value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.EventType != "NetworkOrderAcknowledged" {
		t.Errorf("event_type = %q, want NetworkOrderAcknowledged", env.EventType)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want 1", env.SchemaVersion)
	}
	if env.Source != "network-fulfillment" {
		t.Errorf("source = %q, want network-fulfillment", env.Source)
	}
	if string(w.msgs[0].Key) != "po-1" {
		t.Errorf("key = %q, want po-1", string(w.msgs[0].Key))
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
