// Command netfulfil is network-fulfillment's OLTP composition root: the
// one place every layer is wired together.
//
// With no configuration at all it runs fully functional on in-memory
// adapters and a stub network gateway — no Postgres, no broker, and no
// network credentials in existence (ADR 0001 §4).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	outboundevents "github.com/claudioed/network-fulfillment/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

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
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "direct",
			"integration_topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)
		return fanOutPublisher{integration, analytics}, nil, closeFn
	}

	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"integration_topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)
	outboxPublisher := postgres.NewOutboxPublisher(pool, integration, analytics)
	// Any of the two Kafka publishers can serve as the relay's Sink: both
	// share the same underlying Writer shape (no fixed topic; Send stamps
	// enc.Topic per message), so one relay drains rows bound for either
	// topic without needing its own third adapter.
	relay := postgres.NewOutboxRelay(pool, integration, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
	return outboxPublisher, relay, closeFn
}

// uuidLike mints the event_id stamped on each published event.
func uuidLike() string { return uuid.NewString() }

func kafkaBrokers() string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return v
	}
	return "localhost:9092"
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed value (logged only implicitly: the relay interval is a
// tuning knob, not a contract worth failing boot over).
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mode := network.ParseMode(os.Getenv("NETWORK_MODE"))
	gateway, err := network.NewGateway(mode, logger)
	if err != nil {
		// Refusing to boot is deliberate. A deployment that asked for a
		// real network and silently got a stub would look healthy while
		// answering nobody.
		logger.Error("cannot wire network gateway", "mode", mode, "err", err)
		os.Exit(1)
	}
	logger.Info("network gateway wired", "mode", mode)

	// Declarative stub seeding, the same shape as the fleet's
	// PATH_CATALOGUE_FILE. This is how a stub deployment is given
	// something to receive: the network offers no push (ADR 0001 section 5),
	// so the alternative would be a write endpoint that exists only for
	// testing and has no production counterpart.
	if err := seedStubDemand(gateway, logger); err != nil {
		logger.Error("cannot load stub demand", "err", err)
		os.Exit(1)
	}

	translation := memory.NewProductTranslation()
	// The Anti-Corruption Layer's dictionary. Without it the map is empty
	// and EVERY order rejects as untranslatable — the inbound leg looks
	// alive while answering the network in the negative every time, and
	// the cause is invisible because refusing unknown products is also
	// correct behaviour.
	if path := os.Getenv("PRODUCT_TRANSLATION_FILE"); path != "" {
		n, err := memory.LoadProductTranslationFile(translation, path)
		if err != nil {
			logger.Error("cannot load product translation", "err", err)
			os.Exit(1)
		}
		logger.Info("product translation loaded", "file", path, "products", n)
	} else {
		// WARN, not INFO: a deployment with no dictionary is running, and
		// will reject everything the network sends.
		logger.Warn("no PRODUCT_TRANSLATION_FILE set; every network order will be rejected as untranslatable")
	}

	// Loaded before the database is opened, deliberately: every failure
	// path above this line may os.Exit freely, whereas one below it would
	// skip `defer closeOrders()` and leak the pool. The dictionary needs
	// no database, so there is no reason for it to sit after one.
	orders, pool, closeOrders, err := wireOrders(context.Background(), logger)
	if err != nil {
		// Same reasoning as the gateway above: a deployment that asked
		// for a database and silently got an in-memory map would look
		// healthy while forgetting every acknowledgement deadline it owes
		// the moment it restarts.
		logger.Error("cannot wire order repository", "err", err)
		os.Exit(1)
	}
	defer closeOrders()

	omBase := os.Getenv("ORDER_MANAGEMENT_URL")
	if omBase == "" {
		omBase = "http://localhost:8080"
	}
	planner := ordermanagement.NewPlanner(omBase, nil)

	eventPublisher, relay, closeEventPublisher := wireEventPublisher(pool, logger)
	defer closeEventPublisher()

	// The UnitOfWork is nil exactly when pool is (in-memory mode): the
	// use cases treat that as "no transactional backing" and run
	// Save+Publish back to back, unchanged from before this rollout.
	var uow ports.UnitOfWork
	if pool != nil {
		uow = postgres.NewUnitOfWork(pool)
	}

	receive := &usecases.ReceiveNetworkDemand{
		Orders:      orders,
		Gateway:     gateway,
		Planner:     planner,
		Translation: translation,
		Events:      eventPublisher,
		Clock:       systemClock{},
		UnitOfWork:  uow,
	}
	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders:     orders,
		Planner:    planner,
		Events:     eventPublisher,
		Clock:      systemClock{},
		UnitOfWork: uow,
	}
	// The inbound leg. Until now ReceiveNetworkDemand was constructed and
	// DISCARDED (`_ = receive`), so nothing in a deployed environment
	// could create a NetworkOrder at all.
	inbound := poller.New(gateway, receive, systemClock{}, poller.Config{
		Interval: pollInterval(),
	}, logger)

	api := &inboundhttp.Server{
		Orders:      orders,
		Poller:      inbound,
		Clock:       systemClock{},
		NetworkMode: string(mode),
	}

	srv := &http.Server{
		Addr:              addr(),
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runSweep(ctx, sweep, logger)
	go inbound.Run(ctx)

	// The outbox relay (ADR 0003) runs alongside the HTTP server in the
	// same process, draining outbox_events onto Kafka. It is only wired
	// (non-nil) when both DATABASE_URL and EVENT_PUBLISHER=kafka are set.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	if relay != nil {
		go func() {
			defer close(relayDone)
			logger.Info("outbox relay running", "topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic)
			if err := relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("outbox relay stopped", "err", err)
			}
		}()
	} else {
		close(relayDone)
	}

	go func() {
		logger.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	// Let the relay finish its in-flight pass so an event committed by a
	// request that completed just before shutdown is not stranded until
	// the next pod boots.
	stopRelay()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
	}
	logger.Info("stopped")
}

// runSweep drives the acknowledgement-deadline sweep on a ticker. An
// unanswered order holds real inventory reservations, so the sweep is
// what keeps a missed SLA from quietly becoming unsellable stock.
func runSweep(ctx context.Context, sweep *usecases.SweepAcknowledgementDeadlines, logger *slog.Logger) {
	ticker := time.NewTicker(sweepInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := sweep.Execute(ctx)
			if err != nil {
				logger.Error("acknowledgement sweep failed", "err", err)
				continue
			}
			if res.Missed > 0 {
				logger.Warn("acknowledgement deadlines missed", "examined", res.Examined, "missed", res.Missed)
			}
		}
	}
}

// wireOrders chooses the NetworkOrderRepo implementation.
//
// With no DATABASE_URL the service runs on the in-memory repo, which is
// what keeps a local run and the unit suite free of infrastructure (ADR
// 0001 §4). With one set, it MUST reach Postgres: returning an error
// rather than falling back is the whole point, because the fallback is
// silent and its cost is the acknowledgement deadlines this context owes
// the network.
//
// Migrations run here, at startup, matching every sibling service in this
// fleet — the alternative is a separate job that can be forgotten, and a
// schema that lags the binary is how a context starts answering wrongly
// rather than not at all.
//
// The returned pool is nil exactly when the repo is the in-memory one;
// main uses that alone to decide whether a UnitOfWork/outbox is wired,
// so the two can never disagree about which mode is active.
func wireOrders(ctx context.Context, logger *slog.Logger) (ports.NetworkOrderRepo, *pgxpool.Pool, func(), error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Info("order repository wired", "backend", "memory")
		return memory.NewNetworkOrderRepo(), nil, func() {}, nil
	}

	// Retried, because in this fleet EVERY injected pod's first outbound
	// TCP dial is reset ~10s after the app starts (Istio native sidecars;
	// `holdApplicationUntilProxyStarts` is a no-op for them). A single
	// attempt turns that known, transient condition into CrashLoopBackOff:
	// observed live — migrations failed with "read: connection reset by
	// peer", the process exited, and the pod never got far enough to serve
	// its own health probe.
	//
	// The retry is NOT a weakening of the fail-closed rule. After the
	// budget is exhausted this still refuses to boot; it just stops
	// treating a sidecar warm-up as a permanent failure.
	if err := retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(databaseURL, migrationsPath())
	}); err != nil {
		return nil, nil, nil, err
	}

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open pool: %w", err)
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first real failure would surface inside a
	// request rather than at boot — turning a misconfigured deployment
	// into an intermittent 500 instead of a refusal to start.
	if err := retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, nil, nil, err
	}

	logger.Info("order repository wired", "backend", "postgres")
	return postgres.NewNetworkOrderRepo(pool), pool, pool.Close, nil
}

// bootRetries and bootRetryDelay bound the startup retry budget.
//
// ~31s total (1+2+4+8+16), comfortably past the ~10s first-dial reset and
// still far inside the liveness probe's own tolerance, so a genuinely
// unreachable database still fails the pod rather than hanging it.
const (
	bootRetries    = 5
	bootRetryDelay = time.Second
)

// retry runs op with exponential backoff, returning the LAST error so a
// permanent failure still reports its real cause rather than "timed out".
func retry(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return retryWithDelay(ctx, logger, what, bootRetryDelay, op)
}

// retryWithDelay is retry with the base delay injected, so tests can
// exercise the give-up path without sleeping out the real ~31s budget.
func retryWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	delay := base
	var err error
	for attempt := 1; attempt <= bootRetries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == bootRetries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, bootRetries, err)
}

// migrationsPath is where the migrations live in the container image (see
// Dockerfile), overridable for a local run from the repo root.
func migrationsPath() string {
	if p := os.Getenv("MIGRATIONS_PATH"); p != "" {
		return p
	}
	return "/app/migrations"
}

// seedStubDemand loads NETWORK_SEED_FILE into the stub gateway.
//
// Only meaningful in stub mode, and it type-asserts rather than taking a
// *StubGateway so the composition root keeps depending on the port. A
// seed file set against a real gateway is a configuration MISTAKE worth
// failing on, not something to ignore: it means someone expected demand
// to appear and it silently never would.
func seedStubDemand(gateway ports.NetworkGateway, logger *slog.Logger) error {
	path := os.Getenv("NETWORK_SEED_FILE")
	if path == "" {
		return nil
	}
	stub, ok := gateway.(*network.StubGateway)
	if !ok {
		return fmt.Errorf("NETWORK_SEED_FILE is set but the gateway is not the stub: seeded demand would never be delivered")
	}
	n, err := network.LoadSeedFile(stub, path, time.Now().UTC())
	if err != nil {
		return err
	}
	logger.Info("stub demand seeded", "file", path, "demands", n)
	return nil
}

func pollInterval() time.Duration {
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	// Well under the 24h acknowledgement window, so a restart or a brief
	// outage cannot eat a meaningful fraction of it.
	return time.Minute
}

func sweepInterval() time.Duration {
	if v := os.Getenv("SWEEP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return time.Minute
}

func addr() string {
	if p := os.Getenv("PORT"); p != "" {
		return ":" + p
	}
	return ":8080"
}
