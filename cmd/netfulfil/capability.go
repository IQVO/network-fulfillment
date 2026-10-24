package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/inventoryclient"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/pathcapacitycache"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/processpathcache"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
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
// Always on: there is no flag that turns the pipeline off (the real-wiring
// ADR; loadBootConfig refuses CAPABILITY_OFFER_ENABLED=false).
//
// Boot BLOCKS (bounded by each cache's own
// WaitReadyTimeout) until BOTH caches have replayed their topic's full
// history: an empty, not-yet-replayed cache would otherwise silently
// advertise a zero or under-counted throughput figure, which is a worse
// failure mode than a slightly slower boot (ADR 0001 §8: a throttled
// offer must be provably correct, never a guess from an incomplete
// read model).
func wireCapabilityOffer(
	ctx context.Context,
	pool *pgxpool.Pool,
	brokers []string,
	translation ports.ProductTranslation,
	logger *slog.Logger,
) (ports.CapabilityOfferRepo, *usecases.RecomputeCapabilityOffers, []cacheConsumer, func(), error) {
	pathCap, capacity, consumers, err := startCapabilityOfferCaches(ctx, brokers, logger)
	if err != nil {
		return nil, nil, nil, func() {}, err
	}

	logger.Info("capability offer repository wired", "backend", "postgres")
	offers := postgres.NewCapabilityOfferRepo(pool)
	recompute := &usecases.RecomputeCapabilityOffers{
		Translation: translation,
		Inventory:   wireInventoryClient(),
		PathCap:     pathCap,
		Capacity:    capacity,
		Offers:      offers,
		Clock:       system.Clock{},
		Logger:      logger,
		SiteId:      siteId(),
	}

	closeFn := func() { closeCaches(pathCap, capacity) }
	return offers, recompute, consumers, closeFn, nil
}

// startCapabilityOfferCaches dials both Kafka-fed caches, starts their
// Run goroutines, and blocks until both have replayed their topic's full
// history. See wireCapabilityOffer's own doc comment for why boot blocks
// here rather than serving traffic against an incomplete read model.
func startCapabilityOfferCaches(ctx context.Context, brokers []string, logger *slog.Logger) (*processpathcache.Consumer, *pathcapacitycache.Consumer, []cacheConsumer, error) {
	pathCap, capacity, err := dialCapabilityOfferCaches(ctx, brokers, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	pathCapDone := runCacheConsumer(ctx, logger, "process-path capability cache consumer stopped", pathCap.Run)
	capacityDone := runCacheConsumer(ctx, logger, "path capacity cache consumer stopped", capacity.Run)

	if err := waitCachesReady(ctx, logger, pathCap, capacity); err != nil {
		return nil, nil, nil, err
	}

	return pathCap, capacity, []cacheConsumer{pathCapDone, capacityDone}, nil
}

// dialCapabilityOfferCaches constructs both consumers.
//
// Retried for the same reason the Postgres dials are: each NewConsumer
// call dials the broker synchronously to capture the readiness watermark
// before any consuming begins, and that dial is exactly this fleet's
// known ~10s post-start first-outbound-dial reset (Istio native
// sidecars; docs/adr/0011-boot-retry-for-istio-first-dial-reset.md).
func dialCapabilityOfferCaches(ctx context.Context, brokers []string, logger *slog.Logger) (*processpathcache.Consumer, *pathcapacitycache.Consumer, error) {
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

// cacheConsumer is one running cache consumer: done closes once its Run has
// returned, and ctx is the context that Run runs on. serve() judges a stop
// against THAT context: whether the consumer was asked to stop or died on its
// own is only known by the context the consumer itself obeys.
type cacheConsumer struct {
	done chan struct{}
	ctx  context.Context
}

// runCacheConsumer runs one cache consumer in a goroutine and returns the
// handle serve() supervises.
func runCacheConsumer(ctx context.Context, logger *slog.Logger, stoppedMsg string, run func(context.Context) error) cacheConsumer {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error(stoppedMsg, "err", err)
		}
	}()
	return cacheConsumer{done: done, ctx: ctx}
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

func closeCaches(pathCap *processpathcache.Consumer, capacity *pathcapacitycache.Consumer) {
	if pathCap != nil {
		_ = pathCap.Close()
	}
	if capacity != nil {
		_ = capacity.Close()
	}
}

// runRecompute drives RecomputeCapabilityOffers on a ticker, exactly
// mirroring runSweep's shape.
func runRecompute(ctx context.Context, recompute *usecases.RecomputeCapabilityOffers, logger *slog.Logger) {
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
