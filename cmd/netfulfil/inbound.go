package main

// The inbound loop's composition (ADR 0028): live mode only. Sellers ship
// SSCC-labelled cartons to our sites; the network tells us through the Vendor
// API; this loop registers an ASN in inbound-receiving for each shipment.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/inboundreceiving"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/productmaster"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	"github.com/claudioed/network-fulfillment/internal/resilience"
)

// defaultInboundIntakeInterval is how often the intake loop polls and retries.
// Shipments are not a 24-hour clock like purchase orders, but a dock should hear
// about a truck on its way within a minute.
const defaultInboundIntakeInterval = 30 * time.Second

// inboundLoopStopBudget bounds the wait for the loop at shutdown.
const inboundLoopStopBudget = 5 * time.Second

// inboundConfig is the inbound loop's boot configuration. It exists only in live
// mode: a stub deployment has no network to poll.
type inboundConfig struct {
	enabled             bool
	inboundReceivingURL string // INBOUND_RECEIVING_URL, required in live mode
	productMasterURL    string // PRODUCT_MASTER_URL, required in live mode
	// receiptConsumerGroup is INBOUND_RECEIPT_CONSUMER_GROUP, required in live
	// mode: the consumer group of this service's own on inbound-receiving's
	// events topic. It is durable on purpose (a receipt closed while the service
	// was down is read on the next start), unlike the replay caches' unique groups.
	receiptConsumerGroup string
}

// loadInboundConfig reads the two sibling URLs the loop cannot run without. A
// missing one stops the boot with one error naming every absent variable, like
// the Vendor API's (ADR 0026 Decision 10).
func loadInboundConfig(get func(string) string, mode retailnetwork.Mode) (inboundConfig, error) {
	if mode != retailnetwork.ModeLive {
		return inboundConfig{}, nil
	}
	cfg := inboundConfig{
		enabled:             true,
		inboundReceivingURL: strings.TrimSpace(get("INBOUND_RECEIVING_URL")),
		productMasterURL:    strings.TrimSpace(get("PRODUCT_MASTER_URL")),

		receiptConsumerGroup: strings.TrimSpace(get("INBOUND_RECEIPT_CONSUMER_GROUP")),
	}
	var missing []string
	if cfg.inboundReceivingURL == "" {
		missing = append(missing, "INBOUND_RECEIVING_URL")
	}
	if cfg.productMasterURL == "" {
		missing = append(missing, "PRODUCT_MASTER_URL")
	}
	if cfg.receiptConsumerGroup == "" {
		missing = append(missing, "INBOUND_RECEIPT_CONSUMER_GROUP")
	}
	if len(missing) > 0 {
		return inboundConfig{}, fmt.Errorf("NETWORK_MODE=live requires %s to be set (the inbound loop, ADR 0028)", strings.Join(missing, " and "))
	}
	return cfg, nil
}

// inboundIntakeInterval bounds how often the intake loop runs.
func inboundIntakeInterval() time.Duration {
	return durationEnv("INBOUND_INTAKE_INTERVAL", defaultInboundIntakeInterval)
}

// networkSites are the sites this service owns on the network: SITE_ID plus every
// site of the node definitions (the same set the gateway converges onto nodes).
func networkSites(nodes []retailnetwork.NodeDefinition) []shared.SiteId {
	seen := map[shared.SiteId]bool{siteId(): true}
	sites := []shared.SiteId{siteId()}
	for _, n := range nodes {
		if s := shared.SiteId(n.SiteCode); !seen[s] {
			seen[s] = true
			sites = append(sites, s)
		}
	}
	return sites
}

// inboundLoop is the running loop: nil in stub mode. It is three things: the
// intake ticker (shipments in), the receipt consumer (inbound-receiving's events
// into the local projection) and the return ticker (closed receipts out).
type inboundLoop struct {
	intake   *usecases.IntakeInboundShipments
	returns  *usecases.ReturnInboundReceipts
	consumer *inboundkafka.ReceiptConsumer
	interval time.Duration
	logger   *slog.Logger
	wg       sync.WaitGroup
	// onDeath is called when the consumer stops on its own: the process must
	// not keep running half deaf, so it asks for the whole shutdown.
	onDeath func()
	died    atomic.Bool
}

// wireInboundLoop builds the loop, or returns nil when the mode has none.
func wireInboundLoop(
	cfg inboundConfig,
	gateway ports.NetworkGateway,
	sites []shared.SiteId,
	pool *pgxpool.Pool,
	brokers []string,
	breaker resilience.StateRecorder,
	logger *slog.Logger,
) (*inboundLoop, error) {
	if !cfg.enabled {
		return nil, nil
	}
	inbound, ok := gateway.(ports.InboundGateway)
	if !ok {
		return nil, errors.New("the network gateway cannot poll inbound shipments")
	}
	receivingREST, err := fleetrest.New(fleetrest.Config{Dependency: inboundreceiving.DependencyName, BaseURL: cfg.inboundReceivingURL, Recorder: breaker})
	if err != nil {
		return nil, err
	}
	masterREST, err := fleetrest.New(fleetrest.Config{Dependency: productmaster.DependencyName, BaseURL: cfg.productMasterURL, Recorder: breaker})
	if err != nil {
		return nil, err
	}
	repo := postgres.NewInboundShipmentRepo(pool)
	logger.Info("inbound loop wired", "sites", len(sites), "inboundReceiving", cfg.inboundReceivingURL, "productMaster", cfg.productMasterURL, "interval", inboundIntakeInterval())
	projection := postgres.NewReceiptProjectionRepo(pool)
	return &inboundLoop{
		returns:  &usecases.ReturnInboundReceipts{Receipts: projection, Gateway: inbound, Clock: system.Clock{}, Logger: logger},
		consumer: inboundkafka.NewReceiptConsumer(brokers, inboundkafka.ReceiptTopic, cfg.receiptConsumerGroup, projection, logger),
		intake: &usecases.IntakeInboundShipments{
			Sites: sites, Gateway: inbound, Shipments: repo,
			Resolver:  &productmaster.CachingResolver{Inner: productmaster.New(masterREST), Cache: repo},
			Registrar: inboundreceiving.New(receivingREST),
			Clock:     system.Clock{}, Logger: logger,
		},
		interval: inboundIntakeInterval(),
		logger:   logger,
	}, nil
}

// startInboundLoop wires and starts the loop under a context of its own (the
// caller cancels it through the returned func, after serve has returned, and
// before the pool closes). requestShutdown is what the loop calls when its
// receipt consumer dies. In stub mode the loop is nil and nothing starts.
func startInboundLoop(
	ctx context.Context,
	requestShutdown func(),
	cfg bootConfig,
	in infra,
	breaker resilience.StateRecorder,
	logger *slog.Logger,
) (*inboundLoop, context.CancelFunc, error) {
	loop, err := wireInboundLoop(cfg.inbound, in.gateway, in.sites, in.pool, cfg.kafkaBrokers, breaker, logger)
	loopCtx, cancel := context.WithCancel(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if loop != nil {
		loop.onDeath = requestShutdown
	}
	loop.start(loopCtx)
	return loop, cancel, nil
}

// start runs the intake ticker until ctx ends.
func (l *inboundLoop) start(ctx context.Context) {
	if l == nil {
		return
	}
	l.wg.Add(2)
	go func() {
		defer l.wg.Done()
		l.run(ctx)
	}()
	go func() {
		defer l.wg.Done()
		if err := l.consumer.Run(ctx); err != nil && ctx.Err() == nil {
			l.logger.Error("the receipt consumer stopped on its own; shutting down", "err", err)
			l.died.Store(true)
			if l.onDeath != nil {
				l.onDeath()
			}
		}
	}()
}

func (l *inboundLoop) run(ctx context.Context) {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		l.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *inboundLoop) tick(ctx context.Context) {
	l.tickReturn(ctx)
	res, err := l.intake.Execute(ctx)
	if err != nil && ctx.Err() == nil {
		l.logger.Error("inbound intake pass failed", "err", err)
	}
	if res.New > 0 || res.Registered > 0 || res.Retrying > 0 || res.Rejected > 0 {
		l.logger.Info("inbound shipments processed", "polled", res.Polled, "new", res.New,
			"registered", res.Registered, "retrying", res.Retrying, "rejected", res.Rejected)
	}
}

func (l *inboundLoop) tickReturn(ctx context.Context) {
	res, err := l.returns.Execute(ctx)
	if err != nil && ctx.Err() == nil {
		l.logger.Error("inbound receipt return pass failed", "err", err)
	}
	if res.Sent > 0 || res.Retrying > 0 || res.Rejected > 0 {
		l.logger.Info("inbound receipts processed", "sent", res.Sent, "retrying", res.Retrying, "rejected", res.Rejected)
	}
}

// stop cancels the loop and waits for it, bounded: a call in flight returns as
// soon as its context ends.
func (l *inboundLoop) stop(cancel context.CancelFunc, wait time.Duration) {
	cancel()
	if l == nil {
		return
	}
	done := make(chan struct{})
	go func() { l.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(wait):
		l.logger.Warn("the inbound loop did not stop before the shutdown deadline")
	}
	if err := l.consumer.Close(); err != nil {
		l.logger.Warn("closing the receipt consumer failed", "err", err)
	}
}

// failed reports whether the loop asked for the shutdown because a component died.
func (l *inboundLoop) failed() bool { return l != nil && l.died.Load() }
