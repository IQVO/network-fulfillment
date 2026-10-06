// Command netfulfil is network-fulfillment's OLTP composition root: the
// one place every layer is wired together.
//
// With no configuration at all it runs fully functional on in-memory
// adapters and a stub network gateway — no Postgres, no broker, and no
// network credentials in existence (ADR 0001 §4).
//
// The root is split within package main by concern: config.go (env
// knobs), wiring.go (use cases, gateway, planner, API), database.go
// (order repository + migrations), publishing.go (event publisher /
// outbox), capability.go (CapabilityOffer pipeline), workers.go (tickers),
// shutdown.go (serve + graceful shutdown) and retry.go (boot retry).
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

func main() {
	os.Exit(run())
}

// infra is what bootInfra builds before any use case exists: the
// network gateway, the product dictionary, the order repository (and its
// pool, nil in in-memory mode) and the optional CapabilityOffer pipeline.
type infra struct {
	gateway     ports.NetworkGateway
	mode        network.Mode
	translation ports.ProductTranslation
	orders      ports.NetworkOrderRepo
	pool        *pgxpool.Pool
	offers      ports.CapabilityOfferRepo
	recompute   *usecases.RecomputeCapabilityOffers
	capDone     []chan struct{}
}

// run wires every adapter and use case in startup order, serves until a
// signal arrives, and returns the process exit code. Deferred closers run
// when run returns, after serve's graceful shutdown has stopped every
// goroutine touching the pool.
func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	in, closeInfra, ok := bootInfra(logger)
	if !ok {
		return 1
	}
	defer closeInfra()

	planner, circuitBreakerMetrics := wirePlanner()

	// readiness gates GET /readyz (ADR 0004 §graceful shutdown). The
	// zero value is ready; SetNotReady is called as the FIRST step of
	// the shutdown sequence (shutdownGracefully), before the HTTP server
	// itself stops accepting connections, so a Kubernetes readinessProbe
	// has a chance to observe the flip and stop routing new traffic
	// during the drain window that follows.
	readiness := &inboundhttp.Readiness{}

	eventPublisher, relay, closeEventPublisher := wireEventPublisher(in.pool, logger)
	defer closeEventPublisher()

	receive, sweep, rejectOverdue := wireUseCases(in.orders, in.gateway, planner, in.translation, eventPublisher, in.pool)
	reconcile := wireReconcile(in.orders, in.gateway, planner, eventPublisher, in.pool)

	// The inbound leg. Until now ReceiveNetworkDemand was constructed and
	// DISCARDED (`_ = receive`), so nothing in a deployed environment
	// could create a NetworkOrder at all.
	inbound := poller.New(in.gateway, receive, systemClock{}, poller.Config{
		Interval: pollInterval(),
	}, logger)

	api := wireAPI(apiDeps{
		orders:    in.orders,
		gateway:   in.gateway,
		mode:      in.mode,
		poller:    inbound,
		readiness: readiness,
		metrics:   circuitBreakerMetrics,
		offers:    in.offers,
		events:    eventPublisher,
		pool:      in.pool,
	})

	serve(context.Background(), logger, newHTTPServer(api), sweep, rejectOverdue, reconcile, inbound, relay, readiness, in.recompute, in.capDone)
	return 0
}

// bootInfra builds the pieces that must exist before any use case, in
// startup order. On failure it logs the cause itself and reports ok=false
// WITHOUT having registered any closer — the process is exiting, so an
// open pool/reader at that point is irrelevant. On success the returned
// closer releases the capability-offer caches first, then the order
// repository (the reverse of their construction).
func bootInfra(logger *slog.Logger) (in infra, closeAll func(), ok bool) {
	gateway, mode, err := wireGateway(logger)
	if err != nil {
		logger.Error("cannot wire network gateway", "mode", mode, "err", err)
		return infra{}, nil, false
	}
	in.gateway, in.mode = gateway, mode

	// Declarative stub seeding, the same shape as the fleet's
	// PATH_CATALOGUE_FILE. This is how a stub deployment is given
	// something to receive: the network offers no push (ADR 0001 section 5),
	// so the alternative would be a write endpoint that exists only for
	// testing and has no production counterpart.
	if err := seedStubDemand(gateway, logger); err != nil {
		logger.Error("cannot load stub demand", "err", err)
		return infra{}, nil, false
	}

	in.translation, err = loadProductTranslation(logger)
	if err != nil {
		logger.Error("cannot load product translation", "err", err)
		return infra{}, nil, false
	}

	orders, pool, closeOrders, err := wireOrders(context.Background(), logger)
	if err != nil {
		// Same reasoning as the gateway above: a deployment that asked
		// for a database and silently got an in-memory map would look
		// healthy while forgetting every acknowledgement deadline it owes
		// the moment it restarts.
		logger.Error("cannot wire order repository", "err", err)
		return infra{}, nil, false
	}
	in.orders, in.pool = orders, pool

	// CapabilityOffer (ADR 0001 §8), gated behind CAPABILITY_OFFER_ENABLED
	// (default false) so this service's zero-config, no-broker-required
	// boot (ADR 0001 §4) is unaffected when the feature is not wanted —
	// mirroring the EVENT_PUBLISHER=kafka convention used elsewhere for
	// another optional Kafka dependency. See wireCapabilityOffer's own
	// doc comment for what "enabled" wires. A failure here exits without
	// any closer registered (os.Exit-equivalent: the process is
	// terminating regardless of any open pool/reader).
	offers, recompute, capDone, closeCapabilityOffer, err := wireCapabilityOffer(context.Background(), pool, in.translation, logger)
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
