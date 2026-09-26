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

// wireEventPublisher chooses the outbound EventPublisher: EVENT_PUBLISHER=kafka
// fans out to both the integration topic (warehouse.network-fulfillment.events)
// and the analytics topic (warehouse.network-fulfillment.analytics); unset (the
// default) keeps the existing log publisher, matching the fleet-wide
// convention (see facility-layout's cmd/facility/main.go). Kafka is
// selected independently of the order-repository backend, so the
// Published Language reaches the broker whether the store is Postgres or
// in-memory.
func wireEventPublisher(logger *slog.Logger) (ports.EventPublisher, func()) {
	if os.Getenv("EVENT_PUBLISHER") != "kafka" {
		return outboundevents.NewLogPublisher(logger), func() {}
	}

	brokers := strings.Split(kafkaBrokers(), ",")
	integration := outboundkafka.NewPublisher(brokers, uuidLike)
	analytics := outboundkafka.NewAnalyticsPublisher(brokers, uuidLike)
	logger.Info("event publisher configured", "publisher", "kafka",
		"integration_topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)

	pub := fanOutPublisher{integration, analytics}
	closeFn := func() {
		_ = integration.Close()
		_ = analytics.Close()
	}
	return pub, closeFn
}

// uuidLike mints the event_id stamped on each published event.
func uuidLike() string { return uuid.NewString() }

func kafkaBrokers() string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return v
	}
	return "localhost:9092"
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
	orders, closeOrders, err := wireOrders(context.Background(), logger)
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

	eventPublisher, closeEventPublisher := wireEventPublisher(logger)
	defer closeEventPublisher()

	receive := &usecases.ReceiveNetworkDemand{
		Orders:      orders,
		Gateway:     gateway,
		Planner:     planner,
		Translation: translation,
		Events:      eventPublisher,
		Clock:       systemClock{},
	}
	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders:  orders,
		Planner: planner,
		Events:  eventPublisher,
		Clock:   systemClock{},
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
func wireOrders(ctx context.Context, logger *slog.Logger) (ports.NetworkOrderRepo, func(), error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Info("order repository wired", "backend", "memory")
		return memory.NewNetworkOrderRepo(), func() {}, nil
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
		return nil, nil, err
	}

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open pool: %w", err)
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first real failure would surface inside a
	// request rather than at boot — turning a misconfigured deployment
	// into an intermittent 500 instead of a refusal to start.
	if err := retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, nil, err
	}

	logger.Info("order repository wired", "backend", "postgres")
	return postgres.NewNetworkOrderRepo(pool), pool.Close, nil
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
