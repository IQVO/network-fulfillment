package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// wireGateway wires the outbound network gateway for the NETWORK_MODE env
// var, returning the parsed mode alongside it so the composition root can
// both log it at startup and expose it on the status surface — the
// running value must be verifiable, not assumed. NETWORK_BASE_URL is the
// live-mode target (ADR 0009 §4); read here so it reaches the gateway
// even though no live adapter exists yet to dial it with.
func wireGateway(logger *slog.Logger) (ports.NetworkGateway, network.Mode, error) {
	mode := network.ParseMode(os.Getenv("NETWORK_MODE"))
	baseURL := os.Getenv("NETWORK_BASE_URL")
	if mode == network.ModeLive && baseURL == "" {
		return nil, mode, fmt.Errorf("NETWORK_MODE=live requires NETWORK_BASE_URL to be set")
	}
	gateway, err := network.NewGateway(mode, baseURL, logger)
	if err != nil {
		// Refusing to boot is deliberate. A deployment that asked for a
		// real network and silently got a stub would look healthy while
		// answering nobody.
		return nil, mode, err
	}
	logger.Info("network gateway wired", "mode", mode, "baseURL", baseURL)
	return gateway, mode, nil
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

// loadProductTranslation builds the Anti-Corruption Layer's dictionary
// from PRODUCT_TRANSLATION_FILE.
//
// Without it the map is empty and EVERY order rejects as untranslatable
// — the inbound leg looks alive while answering the network in the
// negative every time, and the cause is invisible because refusing
// unknown products is also correct behaviour.
func loadProductTranslation(logger *slog.Logger) (ports.ProductTranslation, error) {
	translation := memory.NewProductTranslation()
	path := os.Getenv("PRODUCT_TRANSLATION_FILE")
	if path == "" {
		// WARN, not INFO: a deployment with no dictionary is running, and
		// will reject everything the network sends.
		logger.Warn("no PRODUCT_TRANSLATION_FILE set; every network order will be rejected as untranslatable")
		return translation, nil
	}
	n, err := memory.LoadProductTranslationFile(translation, path)
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
// OTel-instrument-name variant.
func wirePlanner() (ports.FulfillmentPlanner, *telemetry.CircuitBreakerMetrics) {
	omBase := os.Getenv("ORDER_MANAGEMENT_URL")
	if omBase == "" {
		omBase = "http://localhost:8080"
	}
	rawPlanner := ordermanagement.NewPlanner(omBase, nil)
	metrics := telemetry.NewCircuitBreakerMetrics()
	return ordermanagement.NewBreakerClient(rawPlanner, metrics), metrics
}

// wireUseCases builds the use cases this composition root serves,
// sharing one repository, planner, publisher and clock between them.
// The UnitOfWork is nil exactly when pool is (in-memory mode): the use
// cases treat that as "no transactional backing" and run Save+Publish
// back to back, unchanged from before this rollout.
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
	var uow ports.UnitOfWork
	if pool != nil {
		uow = postgres.NewUnitOfWork(pool)
	}
	receive := &usecases.ReceiveNetworkDemand{
		Orders:      orders,
		Gateway:     gateway,
		Planner:     planner,
		Translation: translation,
		Events:      events,
		Clock:       systemClock{},
		UnitOfWork:  uow,
	}
	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: orders,
		Events: events,
		Clock:  systemClock{},
	}
	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders:     orders,
		Planner:    planner,
		Events:     events,
		Clock:      systemClock{},
		UnitOfWork: uow,
	}
	return receive, sweep, rejectOverdue
}

// wireReconcile builds the submitted-order reconciliation use case
// (ADR 0001 §5). Its UnitOfWork is set only when pool is non-nil, the
// same "nil means no transactional backing" convention as wireUseCases.
func wireReconcile(
	orders ports.NetworkOrderRepo,
	gateway ports.NetworkGateway,
	planner ports.FulfillmentPlanner,
	events ports.EventPublisher,
	pool *pgxpool.Pool,
) *usecases.ReconcileSubmittedOrders {
	reconcile := &usecases.ReconcileSubmittedOrders{
		Orders:  orders,
		Gateway: gateway,
		Planner: planner,
		Events:  events,
		Clock:   systemClock{},
	}
	if pool != nil {
		reconcile.UnitOfWork = postgres.NewUnitOfWork(pool)
	}
	return reconcile
}

// wireUnitOfWork returns a UnitOfWork bound to pool, or nil when pool is
// nil (in-memory mode) — the same "nil means no transactional backing"
// convention wireUseCases already establishes for ReceiveNetworkDemand
// and SweepAcknowledgementDeadlines.
func wireUnitOfWork(pool *pgxpool.Pool) ports.UnitOfWork {
	if pool == nil {
		return nil
	}
	return postgres.NewUnitOfWork(pool)
}

// apiDeps are the collaborators wireAPI assembles the REST server from.
type apiDeps struct {
	orders    ports.NetworkOrderRepo
	gateway   ports.NetworkGateway
	mode      network.Mode
	poller    *poller.Poller
	readiness *inboundhttp.Readiness
	metrics   *telemetry.CircuitBreakerMetrics
	offers    ports.CapabilityOfferRepo
	events    ports.EventPublisher
	pool      *pgxpool.Pool
}

// wireAPI builds the REST server and its ConfirmShipment use case.
func wireAPI(d apiDeps) *inboundhttp.Server {
	api := &inboundhttp.Server{
		Orders:          d.orders,
		Poller:          d.poller,
		Clock:           systemClock{},
		NetworkMode:     string(d.mode),
		Readiness:       d.readiness,
		MetricsRegistry: d.metrics.Registry,
		Offers:          d.offers,
	}

	api.ConfirmShipment = &usecases.ConfirmNetworkOrderShipment{
		Orders:     d.orders,
		Gateway:    d.gateway,
		Events:     d.events,
		Clock:      systemClock{},
		UnitOfWork: wireUnitOfWork(d.pool),
	}
	return api
}
