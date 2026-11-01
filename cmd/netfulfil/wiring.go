package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/producttranslation"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/resilience"
)

// loadNetworkNodes reads NETWORK_NODES_FILE (ADR 0026 Decision 7): what the
// network needs to register each site that is not yet one of its nodes.
//
// It runs before the database is dialled, like the product dictionary, so a
// malformed file stops the boot early and by name. The file is optional only
// when every site is already a node on the network (adoption): a site with
// neither a node nor a definition is logged by the gateway at each poll, not
// fatal. Stub mode never reads it.
func loadNetworkNodes(logger *slog.Logger, cfg networkConfig) ([]retailnetwork.NodeDefinition, error) {
	if cfg.mode != retailnetwork.ModeLive {
		return nil, nil
	}
	if cfg.nodesFile == "" {
		logger.Info("no NETWORK_NODES_FILE set; live mode can only adopt sites that already are nodes on the network")
		return nil, nil
	}
	nodes, err := retailnetwork.LoadNodesFile(cfg.nodesFile)
	if err != nil {
		return nil, err
	}
	logger.Info("network node definitions loaded", "file", cfg.nodesFile, "nodes", len(nodes))
	return nodes, nil
}

// wireGateway wires the outbound network gateway for the NETWORK_MODE the
// boot configuration carries; the composition root logs the mode at startup
// and exposes it on the status surface, because the running value must be
// verifiable, not assumed.
//
// Stub is the default and talks to nobody. Live (ADR 0026) calls the Vendor
// API at NETWORK_BASE_URL as NETWORK_VENDOR_ID and keeps its identifier maps
// and submission ledger in the service's own Postgres, which is why it takes
// the pool. The sites it converges onto network nodes are SITE_ID plus every
// site of the node definitions. The client's timeout, retry and breaker
// behaviour is the one ADR 0026 Decision 8 fixes: it has no environment knob.
// The product dictionary is how it finds the listing a SKU is fed under at a
// site (ADR 0027 §10); without it the gateway could not feed inventory.
//
// Refusing to boot on a bad live configuration is deliberate. A deployment that
// asked for a real network and silently got a stub would look healthy while
// answering nobody.
func wireGateway(
	logger *slog.Logger,
	cfg networkConfig,
	nodes []retailnetwork.NodeDefinition,
	listings retailnetwork.ListingResolver,
	pool *pgxpool.Pool,
	breaker resilience.StateRecorder,
) (ports.NetworkGateway, error) {
	if cfg.mode != retailnetwork.ModeLive {
		logger.Info("network gateway wired", "mode", cfg.mode)
		return retailnetwork.NewStubGateway(logger), nil
	}
	gateway, err := retailnetwork.NewLiveGateway(retailnetwork.LiveConfig{
		BaseURL:  cfg.baseURL,
		VendorID: cfg.vendorID,
		Sites:    []string{string(siteId())},
		Nodes:    nodes,
		Store:    retailnetwork.NewPGStore(pool),
		Listings: listings,
		Logger:   logger,
		Recorder: breaker,
	})
	if err != nil {
		return nil, err
	}
	logger.Info("network gateway wired", "mode", cfg.mode, "baseURL", cfg.baseURL, "vendorId", cfg.vendorID, "site", siteId(), "nodeDefinitions", len(nodes))
	return gateway, nil
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
	stub, ok := gateway.(*retailnetwork.StubGateway)
	if !ok {
		return fmt.Errorf("NETWORK_SEED_FILE is set but the gateway is not the stub: seeded demand would never be delivered")
	}
	n, err := retailnetwork.LoadSeedFile(stub, path, time.Now().UTC())
	if err != nil {
		return err
	}
	logger.Info("stub demand seeded", "file", path, "demands", n)
	return nil
}

// loadProductTranslation builds the Anti-Corruption Layer's dictionary
// from PRODUCT_TRANSLATION_FILE.
//
// Without it the map is empty and EVERY order rejects as untranslatable
// — the inbound leg looks alive while answering the network in the
// negative every time, and the cause is invisible because refusing
// unknown products is also correct behaviour.
//
// The one dictionary serves three readers, each through its own narrow port:
// the translation of a network product to a SKU (inbound), the SKUs a site
// lists (the inventory feed's catalogue) and the listing a SKU is fed under
// there (the live gateway). It is read-only once loaded.
func loadProductTranslation(logger *slog.Logger) (*producttranslation.Dictionary, error) {
	translation := producttranslation.New()
	path := os.Getenv("PRODUCT_TRANSLATION_FILE")
	if path == "" {
		// WARN, not INFO: a deployment with no dictionary is running, and
		// will reject everything the network sends.
		logger.Warn("no PRODUCT_TRANSLATION_FILE set; every network order will be rejected as untranslatable")
		return translation, nil
	}
	n, err := producttranslation.LoadFile(translation, path)
	if err != nil {
		return nil, err
	}
	logger.Info("product translation loaded", "file", path, "products", n)
	return translation, nil
}

// wirePlanner wires the order-management planner client wrapped in the
// shared circuit breaker (ADR 0004, ported from order-management's ADR
// 0025): RaiseHeldOrder/ReleaseHeldOrder/CancelHeldOrder all share ONE
// breaker instance guarding this one downstream dependency, with no
// permissive/fail-open fallback (see ordermanagement/breaker.go's
// package doc comment for why the OPEN-state behaviour is simply to
// propagate a wrapped error). Deadline feasibility is always ASKED of
// order-management, never recomputed here (ADR 0001 §7). The returned
// metrics wire the breaker's OnStateChange into the
// circuit_breaker_state gauge (ADR 0004), served at GET /metrics; they
// never fail (see NewCircuitBreakerMetrics' own doc comment), so there
// is no degraded-but-non-fatal branch to log, unlike order-management's
// OTel-instrument-name variant. The metrics are built by the caller because the
// retail-network breaker (ADR 0026 Decision 8) reports into the same gauge.
func wirePlanner(metrics *telemetry.CircuitBreakerMetrics) ports.FulfillmentPlanner {
	omBase := os.Getenv("ORDER_MANAGEMENT_URL")
	if omBase == "" {
		omBase = "http://localhost:8080"
	}
	rawPlanner := ordermanagement.NewPlanner(omBase, nil)
	return ordermanagement.NewBreakerClient(rawPlanner, metrics)
}

// wireUseCases builds the use cases this composition root serves,
// sharing one repository, planner, publisher and clock between them.
// The UnitOfWork is always the Postgres one: Save and Publish (the
// outbox enqueue) commit or roll back together.
//
// SweepAcknowledgementDeadlines and RejectOverdueOrders are deliberately
// two separate use cases (ADR 0001 §6): the sweep only ever reports
// AcknowledgementDeadlineAtRisk and never mutates the aggregate;
// RejectOverdueOrders is the separate path that performs the actual
// rejection, with its own audit trail.
func wireUseCases(
	orders ports.NetworkOrderRepo,
	gateway ports.NetworkGateway,
	planner ports.FulfillmentPlanner,
	translation ports.ProductTranslation,
	events ports.EventPublisher,
	pool *pgxpool.Pool,
) (*usecases.ReceiveNetworkDemand, *usecases.SweepAcknowledgementDeadlines, *usecases.RejectOverdueOrders) {
	uow := postgres.NewUnitOfWork(pool)
	receive := &usecases.ReceiveNetworkDemand{
		Orders:      orders,
		Gateway:     gateway,
		Planner:     planner,
		Translation: translation,
		Events:      events,
		Clock:       system.Clock{},
		UnitOfWork:  uow,
	}
	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: orders,
		Events: events,
		Clock:  system.Clock{},
	}
	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders:     orders,
		Planner:    planner,
		Events:     events,
		Clock:      system.Clock{},
		UnitOfWork: uow,
	}
	return receive, sweep, rejectOverdue
}

// wireOrphanHoldSweep builds the orphaned-hold sweep (ADR 0024), or returns
// nil when ORPHAN_HOLD_SWEEP_ENABLED turns it off (a safety net inside this
// context, not a database or broker stand-in).
func wireOrphanHoldSweep(
	orders ports.NetworkOrderRepo,
	planner ports.FulfillmentPlanner,
	events ports.EventPublisher,
	pool *pgxpool.Pool,
	logger *slog.Logger,
) *usecases.SweepOrphanedHolds {
	if !orphanHoldSweepEnabled(logger) {
		logger.Warn("orphaned-hold sweep disabled by ORPHAN_HOLD_SWEEP_ENABLED: a hold nobody decides stays reserved in order-management")
		return nil
	}
	return &usecases.SweepOrphanedHolds{
		Orders:     orders,
		Planner:    planner,
		Events:     events,
		Clock:      system.Clock{},
		UnitOfWork: postgres.NewUnitOfWork(pool),
		Metrics:    telemetry.NewOrphanHoldMetrics(nil),
		Grace:      orphanHoldGrace(logger),
	}
}

// wireReconcile builds the submitted-order reconciliation use case
// (ADR 0001 §5), on the Postgres UnitOfWork.
func wireReconcile(
	orders ports.NetworkOrderRepo,
	gateway ports.NetworkGateway,
	planner ports.FulfillmentPlanner,
	events ports.EventPublisher,
	pool *pgxpool.Pool,
) *usecases.ReconcileSubmittedOrders {
	return &usecases.ReconcileSubmittedOrders{
		Orders:     orders,
		Gateway:    gateway,
		Planner:    planner,
		Events:     events,
		Clock:      system.Clock{},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
}

// apiDeps are the collaborators wireAPI assembles the REST server from.
type apiDeps struct {
	orders      ports.NetworkOrderRepo
	gateway     ports.NetworkGateway
	mode        retailnetwork.Mode
	poller      *poller.Poller
	readiness   *inboundhttp.Readiness
	metrics     *telemetry.CircuitBreakerMetrics
	offers      ports.CapabilityOfferRepo
	events      ports.EventPublisher
	pool        *pgxpool.Pool
	logger      *slog.Logger
	serviceName string
}

// wireAPI builds the REST server and its ConfirmShipment use case.
func wireAPI(d apiDeps) *inboundhttp.Server {
	api := &inboundhttp.Server{
		Orders:          d.orders,
		Poller:          d.poller,
		Clock:           system.Clock{},
		NetworkMode:     string(d.mode),
		Readiness:       d.readiness,
		MetricsRegistry: d.metrics.Registry,
		Offers:          d.offers,
		Logger:          d.logger,
		ServiceName:     d.serviceName,
	}

	api.ConfirmShipment = &usecases.ConfirmNetworkOrderShipment{
		Orders:     d.orders,
		Gateway:    d.gateway,
		Events:     d.events,
		Clock:      system.Clock{},
		UnitOfWork: postgres.NewUnitOfWork(d.pool),
	}
	api.Idempotency = postgres.NewIdempotencyStore(d.pool)
	return api
}
