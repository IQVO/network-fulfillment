// Command netfulfil is network-fulfillment's OLTP composition root: the
// one place every layer is wired together.
//
// It runs only on a real Postgres (DATABASE_URL) and a real Kafka
// (KAFKA_BROKERS); both are mandatory and a missing one is a boot error
// that names the variable (the real-wiring ADR). The network gateway may
// still be the stub (NETWORK_MODE), an external-system seam, not a
// database or broker stand-in.
//
// The root is split within package main by concern: config.go (env
// knobs), wiring.go (use cases, gateway, planner, API), database.go
// (order repository + migrations), publishing.go (outbox + relay),
// capability.go (CapabilityOffer pipeline), workers.go (tickers),
// shutdown.go (serve + graceful shutdown) and retry.go (boot retry).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func main() {
	os.Exit(run(context.Background()))
}

// infra is what bootInfra builds before any use case exists: the
// network gateway, the product dictionary, the order repository and its
// pool, and the CapabilityOffer pipeline.
type infra struct {
	gateway     ports.NetworkGateway
	mode        retailnetwork.Mode
	translation ports.ProductTranslation
	orders      ports.NetworkOrderRepo
	pool        *pgxpool.Pool
	offers      ports.CapabilityOfferRepo
	recompute   *usecases.RecomputeCapabilityOffers
	capDone     []chan struct{}
	sites       []shared.SiteId
}

// run wires every adapter and use case in startup order, serves until a
// signal arrives or ctx is cancelled (the integration test cancels it; main
// passes a background context), and returns the process exit code. Deferred
// closers run when run returns, after serve's graceful shutdown has stopped
// every goroutine touching the pool.
func run(ctx context.Context) int {
	// One signal-aware context for the whole process, so the Kafka cache
	// consumers started during boot stop with the server on SIGTERM too.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := newLogger(getenv("LOG_LEVEL", "info"))

	// Mandatory configuration FIRST: before telemetry, the listener or any
	// dial, so a missing variable fails the boot immediately.
	cfg, err := loadBootConfig(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return 1
	}

	// OTLP trace + metric export and Go runtime metrics (ADR 0019). Never
	// blocks on the Collector. MUST run before wireAPI/Routes(): otelchi
	// binds the global MeterProvider when the middleware is built.
	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
	otelClose, err := telemetry.Setup(otelCtx, serviceName,
		getenv("SERVICE_VERSION", "dev"), getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	otelCancel()
	if err != nil {
		logger.Error("telemetry setup failed", "error", err)
		return 1
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelClose(closeCtx); err != nil {
			logger.Error("telemetry flush failed", "error", err)
		}
	}()

	// The one gauge registry: order-management's breaker and the retail
	// network's (ADR 0026 Decision 8) both report into it, served at /metrics.
	circuitBreakerMetrics := telemetry.NewCircuitBreakerMetrics()

	in, closeInfra, ok := bootInfra(ctx, logger, cfg, circuitBreakerMetrics)
	if !ok {
		return 1
	}
	defer closeInfra()

	planner := wirePlanner(circuitBreakerMetrics)

	// readiness gates GET /readyz (ADR 0004 §graceful shutdown). The
	// zero value is ready; SetNotReady is called as the FIRST step of
	// the shutdown sequence (shutdownGracefully), before the HTTP server
	// itself stops accepting connections, so a Kubernetes readinessProbe
	// has a chance to observe the flip and stop routing new traffic
	// during the drain window that follows.
	//
	// The gate also requires Postgres and Kafka to answer: /readyz is 503
	// dependency-unavailable while either is down, /healthz stays 200.
	readiness := inboundhttp.NewReadiness(inboundhttp.DefaultReadinessTTL, inboundhttp.DefaultReadinessTimeout,
		inboundhttp.Dependency{Name: "postgres", Check: in.pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(cfg.kafkaBrokers).Check},
	)

	eventPublisher, relay, closeEventPublisher := wireEventPublisher(in.pool, cfg.kafkaBrokers, logger)
	defer closeEventPublisher()

	receive, sweep, rejectOverdue := wireUseCases(in.orders, in.gateway, planner, in.translation, eventPublisher, in.pool)
	reconcile := wireReconcile(in.orders, in.gateway, planner, eventPublisher, in.pool)
	orphanHolds := wireOrphanHoldSweep(in.orders, planner, eventPublisher, in.pool, logger)

	// The inbound leg. Until now ReceiveNetworkDemand was constructed and
	// DISCARDED (`_ = receive`), so nothing in a deployed environment
	// could create a NetworkOrder at all.
	inbound := poller.New(in.gateway, receive, system.Clock{}, poller.Config{
		Interval: pollInterval(),
	}, logger)

	api := wireAPI(apiDeps{
		orders:      in.orders,
		gateway:     in.gateway,
		mode:        in.mode,
		poller:      inbound,
		readiness:   readiness,
		metrics:     circuitBreakerMetrics,
		offers:      in.offers,
		events:      eventPublisher,
		pool:        in.pool,
		logger:      logger,
		serviceName: serviceName,
	})

	// The inbound loop (ADR 0028): live mode only, stopped before the pool closes.
	loop, stopLoop, err := startInboundLoop(ctx, stop, cfg, in, circuitBreakerMetrics, logger)
	if err != nil {
		logger.Error("cannot wire the inbound loop", "err", err)
		return 1
	}
	defer loop.stop(stopLoop, inboundLoopStopBudget)

	code := serve(ctx, logger, newHTTPServer(api), sweep, rejectOverdue, orphanHolds, reconcile, inbound, relay, readiness, in.recompute, in.capDone)
	if loop.failed() && code == 0 {
		code = 1 // a dead receipt consumer is a failure, whatever the signal said
	}
	return code
}

// bootInfra builds the pieces that must exist before any use case, in
// startup order. On failure it logs the cause itself and reports ok=false
// WITHOUT having registered any closer — the process is exiting, so an
// open pool/reader at that point is irrelevant. On success the returned
// closer releases the capability-offer caches first, then the order
// repository (the reverse of their construction).
func bootInfra(ctx context.Context, logger *slog.Logger, cfg bootConfig, breaker *telemetry.CircuitBreakerMetrics) (in infra, closeAll func(), ok bool) {
	in.mode = cfg.network.mode

	// Node definitions and the product dictionary are plain files: read them
	// before the database is dialled so a malformed one fails the boot at once.
	nodes, err := loadNetworkNodes(logger, cfg.network)
	if err != nil {
		logger.Error("cannot load network node definitions", "err", err)
		return infra{}, nil, false
	}

	in.translation, err = loadProductTranslation(logger)
	if err != nil {
		logger.Error("cannot load product translation", "err", err)
		return infra{}, nil, false
	}

	orders, pool, closeOrders, err := wireOrders(context.Background(), logger, cfg.databaseURL)
	if err != nil {
		// A database that cannot be reached fails the boot: there is no
		// in-memory fallback that would look healthy while forgetting
		// every acknowledgement deadline it owes on the next restart.
		logger.Error("cannot wire order repository", "err", err)
		return infra{}, nil, false
	}
	in.orders, in.pool = orders, pool

	// The gateway comes after the pool because the live one keeps its
	// identifier maps and submission ledger in this service's Postgres
	// (ADR 0026 Decision 4; ADR 0025: live mode never runs without it).
	gateway, err := wireGateway(logger, cfg.network, nodes, pool, breaker)
	if err != nil {
		logger.Error("cannot wire network gateway", "mode", cfg.network.mode, "err", err)
		closeOrders()
		return infra{}, nil, false
	}
	in.gateway = gateway
	in.sites = networkSites(nodes)

	// Declarative stub seeding, the same shape as the fleet's
	// PATH_CATALOGUE_FILE. This is how a stub deployment is given
	// something to receive: the network offers no push (ADR 0001 section 5),
	// so the alternative would be a write endpoint that exists only for
	// testing and has no production counterpart. It refuses a live gateway.
	if err := seedStubDemand(gateway, logger); err != nil {
		logger.Error("cannot load stub demand", "err", err)
		closeOrders()
		return infra{}, nil, false
	}

	// CapabilityOffer (ADR 0001 §8): always on, because Kafka is a
	// mandatory dependency (the real-wiring ADR); the two Kafka cache
	// consumers it needs run on cfg.kafkaBrokers. See wireCapabilityOffer's
	// own doc comment for what it wires. A failure here exits without any
	// closer registered (the process is terminating regardless of any open
	// pool/reader).
	offers, recompute, capDone, closeCapabilityOffer, err := wireCapabilityOffer(ctx, pool, cfg.kafkaBrokers, in.translation, logger)
	if err != nil {
		logger.Error("cannot wire capability offer pipeline", "err", err)
		return infra{}, nil, false
	}
	in.offers, in.recompute, in.capDone = offers, recompute, capDone

	return in, func() {
		closeCapabilityOffer()
		closeOrders()
	}, true
}
