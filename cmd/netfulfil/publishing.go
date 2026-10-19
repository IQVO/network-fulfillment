package main

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// wireEventPublisher builds the outbound EventPublisher: the
// transactional outbox (ADR 0003) and the relay that drains it. There is
// no other path — no log sink, no direct-to-Kafka publisher (the
// real-wiring ADR): both topics (warehouse.network-fulfillment.events and
// warehouse.network-fulfillment.analytics) are enqueued into outbox_events
// in the SAME Postgres transaction as the aggregate write, and the
// returned relay is ALWAYS non-nil for the caller to run alongside the
// HTTP server. The store and the two topics can then never diverge.
func wireEventPublisher(pool *pgxpool.Pool, brokers []string, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	integration := outboundkafka.NewPublisher(brokers, uuidLike)
	analytics := outboundkafka.NewAnalyticsPublisher(brokers, uuidLike)
	closeFn := func() {
		_ = integration.Close()
		_ = analytics.Close()
	}

	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"integration_topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)
	outboxPublisher := postgres.NewOutboxPublisher(pool, integration, analytics)
	// Any of the two Kafka publishers can serve as the relay's Sink: both
	// share the same underlying Writer shape (no fixed topic; Send stamps
	// enc.Topic per message), so one relay drains rows bound for either
	// topic without needing its own third adapter.
	// OUTBOX_MAX_ATTEMPTS (ADR 0021): a row that fails this many sends is
	// excluded from the relay's claims and logged at ERROR, so one poison
	// row cannot block every later row, for both topics, forever.
	relay := postgres.NewOutboxRelay(pool, integration, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)),
		postgres.WithMaxAttempts(outboxMaxAttempts(logger)))
	return outboxPublisher, relay, closeFn
}

// uuidLike mints the CloudEvents id stamped on each published event.
func uuidLike() string { return uuid.NewString() }
