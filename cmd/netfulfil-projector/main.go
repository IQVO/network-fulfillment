// Command netfulfil-projector is the WRITER composition root of the
// network-fulfillment "Network Order Acknowledgement & Translation" data
// product. It consumes the analytics Kafka topic, projects each event
// into the analytical Postgres database via the idempotent
// PostgresProjection, and serves only a health endpoint on an admin port.
// It is the single writer of the analytical database and serves no
// reports; the reader (cmd/netfulfil-reports) is a separate deployable.
//
// The consumer group is minted fresh per process instance
// (hostname+PID+timestamp, see internal/adapters/inbound/kafka's
// NewUniqueConsumerGroup) rather than a single fixed group id: a shared
// group would let a brand-new projector instance inherit an EARLIER
// instance's already-advanced committed offset and report healthy having
// itself replayed nothing — a failure mode this fleet has been bitten by
// twice.
//
// Consistent with the rest of the analytics pipeline, this process is
// trace-free: network-fulfillment has no observability/OTel package for
// it.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
)

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is
// unset: the projector is the writer of the analytical database and
// cannot start without it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

func main() {
	if err := run(); err != nil {
		slog.Error("netfulfil-projector exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()

	adminAddr := getenv("ADMIN_ADDR", ":8091")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	// The projector owns the analytical schema: run its migrations on
	// start.
	if err := postgres.RunMigrations(analyticsURL, migrationsPath); err != nil {
		return err
	}

	pool, err := analyticsstore.NewPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	projection := analyticsstore.NewPostgresProjection(pool)
	consumed := analyticsstore.NewConsumedEventsRepo(pool)
	group := inboundkafka.NewUniqueConsumerGroup(inboundkafka.AnalyticsConsumerGroupPrefix)
	consumer := inboundkafka.NewAnalyticsConsumer(kafkaBrokers, outboundkafka.AnalyticsTopic, group, projection, consumed, logger)
	defer func() { _ = consumer.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: adminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("projector admin server listening", "addr", adminAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	go func() {
		logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", group, "brokers", kafkaBrokers)
		if err := consumer.Run(consumerCtx); err != nil {
			logger.Error("analytics consumer stopped", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancelConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// newLogger builds a JSON slog logger at the given level. The analytics
// processes log structured JSON but do not wire OTel, so this is a plain
// handler.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
