package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/inventoryclient"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/pathcapacitycache"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/processpathcache"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// wireCapabilityOffer wires ADR 0001 §8's CapabilityOffer pipeline: the
// two Kafka-fed caches (process-path-management's CPT schedule / cycle
// times, wes-work-planning's PathCapacityChanged), the inventory-storage
// REST client (internal/adapters/outbound/inventoryclient — a
// deliberate, documented departure from a third Kafka cache; see that
// package's own doc comment), the CapabilityOfferRepo, and the
// RecomputeCapabilityOffers use case a ticker in serve() drives.
//
// Gated behind CAPABILITY_OFFER_ENABLED=true (default false): this
// service's defining property is that it runs fully functional with NO
// configuration at all (ADR 0001 §4), and two more mandatory Kafka
// topics plus a mandatory inventory-storage dependency would break that
// for every existing deployment that has not opted in — the same reason
// EVENT_PUBLISHER=kafka is opt-in above. Disabled (the default), every
// return value is its zero value and the caller leaves api.Offers nil,
// which simply does not register the read endpoints/tools (the same
// "nil means not registered" convention MetricsRegistry/Reports already
// follow).
//
// When enabled, boot BLOCKS (bounded by each cache's own
// WaitReadyTimeout) until BOTH caches have replayed their topic's full
// history: an empty, not-yet-replayed cache would otherwise silently
// advertise a zero or under-counted throughput figure, which is a worse
// failure mode than a slightly slower boot (ADR 0001 §8: a throttled
// offer must be provably correct, never a guess from an incomplete
// read model).
func wireCapabilityOffer(
	ctx context.Context,
	pool *pgxpool.Pool,
	translation ports.ProductTranslation,
	logger *slog.Logger,
) (ports.CapabilityOfferRepo, *usecases.RecomputeCapabilityOffers, []chan struct{}, func(), error) {
	noop := func() {}
	if !capabilityOfferEnabled() {
		return nil, nil, nil, noop, nil
	}

	pathCap, capacity, runDone, err := startCapabilityOfferCaches(ctx, logger)
	if err != nil {
		return nil, nil, nil, noop, err
	}

	offers := wireOffersRepo(pool, logger)
	recompute := &usecases.RecomputeCapabilityOffers{
		Translation: translation,
		Inventory:   wireInventoryClient(),
		PathCap:     pathCap,
		Capacity:    capacity,
		Offers:      offers,
		Clock:       systemClock{},
		Logger:      logger,
		SiteId:      siteId(),
	}

	closeFn := func() { closeCaches(pathCap, capacity) }
	return offers, recompute, runDone, closeFn, nil
}

// startCapabilityOfferCaches dials both Kafka-fed caches, starts their
// Run goroutines, and blocks until both have replayed their topic's full
// history. See wireCapabilityOffer's own doc comment for why boot blocks
// here rather than serving traffic against an incomplete read model.
func startCapabilityOfferCaches(ctx context.Context, logger *slog.Logger) (*processpathcache.Consumer, *pathcapacitycache.Consumer, []chan struct{}, error) {
	pathCap, capacity, err := dialCapabilityOfferCaches(ctx, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	pathCapDone := runCacheConsumer(ctx, logger, "process-path capability cache consumer stopped", pathCap.Run)
	capacityDone := runCacheConsumer(ctx, logger, "path capacity cache consumer stopped", capacity.Run)

	if err := waitCachesReady(ctx, logger, pathCap, capacity); err != nil {
		return nil, nil, nil, err
	}

	return pathCap, capacity, []chan struct{}{pathCapDone, capacityDone}, nil
}

// dialCapabilityOfferCaches constructs both consumers.
//
// Retried for the same reason the Postgres dials are: each NewConsumer
// call dials the broker synchronously to capture the readiness watermark
// before any consuming begins, and that dial is exactly this fleet's
// known ~10s post-start first-outbound-dial reset (Istio native
// sidecars; docs/adr/0011-boot-retry-for-istio-first-dial-reset.md).
func dialCapabilityOfferCaches(ctx context.Context, logger *slog.Logger) (*processpathcache.Consumer, *pathcapacitycache.Consumer, error) {
	brokers := strings.Split(kafkaBrokers(), ",")

	var pathCap *processpathcache.Consumer
	if err := retry(ctx, logger, "dial process-path-management kafka topic", func() error {
		c, err := processpathcache.NewConsumer(ctx, brokers, logger)
		if err != nil {
			return err
		}
		pathCap = c
		return nil
	}); err != nil {
		return nil, nil, fmt.Errorf("start process-path capability cache: %w", err)
	}

	var capacity *pathcapacitycache.Consumer
	if err := retry(ctx, logger, "dial work-planning kafka topic", func() error {
		c, err := pathcapacitycache.NewConsumer(ctx, brokers, logger)
		if err != nil {
			return err
		}
		capacity = c
		return nil
	}); err != nil {
		_ = pathCap.Close()
		return nil, nil, fmt.Errorf("start path capacity cache: %w", err)
	}
	return pathCap, capacity, nil
}

// runCacheConsumer runs one cache consumer in a goroutine and returns a
// channel closed once it has returned.
func runCacheConsumer(ctx context.Context, logger *slog.Logger, stoppedMsg string, run func(context.Context) error) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error(stoppedMsg, "err", err)
		}
	}()
	return done
}

// waitCachesReady blocks until both caches report ready (bounded by their
// WaitReadyTimeouts), closing them on failure.
func waitCachesReady(ctx context.Context, logger *slog.Logger, pathCap *processpathcache.Consumer, capacity *pathcapacitycache.Consumer) error {
	logger.Info("waiting for capability-offer caches to replay their initial history before accepting traffic")
	waitCtx, cancel := context.WithTimeout(ctx, processpathcache.WaitReadyTimeout+pathcapacitycache.WaitReadyTimeout)
	defer cancel()
	if err := pathCap.WaitReady(waitCtx); err != nil {
		closeCaches(pathCap, capacity)
		return fmt.Errorf("process-path capability cache did not become ready within %s: %w", processpathcache.WaitReadyTimeout, err)
	}
	if err := capacity.WaitReady(waitCtx); err != nil {
		closeCaches(pathCap, capacity)
		return fmt.Errorf("path capacity cache did not become ready within %s: %w", pathcapacitycache.WaitReadyTimeout, err)
	}
	logger.Info("capability-offer caches are ready")
	return nil
}

// wireInventoryClient wires the REST client for inventory-storage's
// usable-inventory read model (INVENTORY_STORAGE_URL, defaulting to
// localhost for local dev, matching ORDER_MANAGEMENT_URL's own default
// in wirePlanner).
func wireInventoryClient() ports.InventoryAvailability {
	inventoryURL := os.Getenv("INVENTORY_STORAGE_URL")
	if inventoryURL == "" {
		inventoryURL = "http://localhost:8080"
	}
	return inventoryclient.NewClient(inventoryURL, nil)
}

// wireOffersRepo chooses the CapabilityOfferRepo backend, mirroring
// wireOrders's own in-memory/Postgres choice.
func wireOffersRepo(pool *pgxpool.Pool, logger *slog.Logger) ports.CapabilityOfferRepo {
	if pool != nil {
		logger.Info("capability offer repository wired", "backend", "postgres")
		return postgres.NewCapabilityOfferRepo(pool)
	}
	logger.Info("capability offer repository wired", "backend", "memory")
	return memory.NewCapabilityOfferRepo()
}

func closeCaches(pathCap *processpathcache.Consumer, capacity *pathcapacitycache.Consumer) {
	if pathCap != nil {
		_ = pathCap.Close()
	}
	if capacity != nil {
		_ = capacity.Close()
	}
}

// runRecompute drives RecomputeCapabilityOffers on a ticker, exactly
// mirroring runSweep's shape. recompute is nil when CAPABILITY_OFFER_ENABLED
// is not set, in which case this goroutine does nothing and returns
// immediately once ctx is done.
func runRecompute(ctx context.Context, recompute *usecases.RecomputeCapabilityOffers, logger *slog.Logger) {
	if recompute == nil {
		return
	}
	ticker := time.NewTicker(recomputeInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := recompute.Execute(ctx)
			if err != nil {
				logger.Error("capability offer recompute failed", "err", err)
				continue
			}
			logger.Info("capability offer recompute completed",
				"examined", res.Examined, "throughput_constrained", res.ThroughputConstrained)
		}
	}
}
