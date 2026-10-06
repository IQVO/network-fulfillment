package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundevents "github.com/claudioed/network-fulfillment/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// fanOutPublisher forwards every domain event to each wrapped
// EventPublisher in order, so a single EVENT_PUBLISHER=kafka run publishes
// to BOTH the integration topic and the analytics topic. A publish
// failure on any target aborts and is returned, rather than silently
// dropping a stream.
type fanOutPublisher []ports.EventPublisher

func (f fanOutPublisher) Publish(ctx context.Context, event any) error {
	for _, p := range f {
		if err := p.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// wireEventPublisher chooses the outbound EventPublisher.
//
// EVENT_PUBLISHER unset (the default) keeps the existing log publisher,
// matching the fleet-wide convention (see facility-layout's
// cmd/facility/main.go). EVENT_PUBLISHER=kafka fans out to both the
// integration topic (warehouse.network-fulfillment.events) and the
// analytics topic (warehouse.network-fulfillment.analytics):
//
//   - with pool == nil (DATABASE_URL unset) both topics are written
//     DIRECTLY — there is no transaction to bind them to, matching this
//     service's in-memory dev mode.
//   - with pool != nil (DATABASE_URL set) both are instead enqueued into
//     the transactional outbox (ADR 0003) in the SAME Postgres
//     transaction as the aggregate write, and a non-nil *OutboxRelay is
//     returned for the caller to run alongside the HTTP server. The
//     store and the two topics can then never diverge.
func wireEventPublisher(pool *pgxpool.Pool, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	if os.Getenv("EVENT_PUBLISHER") != "kafka" {
		return outboundevents.NewLogPublisher(logger), nil, func() {}
	}

	brokers := strings.Split(kafkaBrokers(), ",")
	integration := outboundkafka.NewPublisher(brokers, uuidLike)
	analytics := outboundkafka.NewAnalyticsPublisher(brokers, uuidLike)
	closeFn := func() {
		_ = integration.Close()
		_ = analytics.Close()
	}

	if pool == nil {
		logPublisherConfigured(logger, "direct", brokers)
		return fanOutPublisher{integration, analytics}, nil, closeFn
	}

	logPublisherConfigured(logger, "outbox", brokers)
	outboxPublisher := postgres.NewOutboxPublisher(pool, integration, analytics)
	// Any of the two Kafka publishers can serve as the relay's Sink: both
	// share the same underlying Writer shape (no fixed topic; Send stamps
	// enc.Topic per message), so one relay drains rows bound for either
	// topic without needing its own third adapter.
	relay := postgres.NewOutboxRelay(pool, integration, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
	return outboxPublisher, relay, closeFn
}

func logPublisherConfigured(logger *slog.Logger, mode string, brokers []string) {
	logger.Info("event publisher configured", "publisher", "kafka", "mode", mode,
		"integration_topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)
}

// uuidLike mints the CloudEvents id stamped on each published event.
func uuidLike() string { return uuid.NewString() }
