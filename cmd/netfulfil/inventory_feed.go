package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// inventoryWorker is the inventory feed (ADR 0027 part 2) ready to be driven:
// the use case and how often. A nil *inventoryWorker means there is nothing to
// run.
type inventoryWorker struct {
	feed  *usecases.FeedInventory
	every time.Duration
}

// wireInventoryFeed builds the inventory feed worker, or returns nil with the
// reason logged. Only the live gateway has a network to feed: stub mode never
// starts it. The feed reads local data only: the capability offers the last
// recompute pass persisted, and the product dictionary (which SKUs each site
// lists). It feeds the sites the gateway itself converges onto nodes, the same
// ones the node profile is declared for.
func wireInventoryFeed(
	logger *slog.Logger,
	mode retailnetwork.Mode,
	gateway ports.NetworkGateway,
	catalogue ports.FeedCatalogue,
	offers ports.CapabilityOfferRepo,
) *inventoryWorker {
	if mode != retailnetwork.ModeLive {
		return nil
	}
	every, enabled := inventoryInterval(logger)
	if !enabled {
		logger.Warn("inventory feed disabled by NETWORK_INVENTORY_INTERVAL=0: the network keeps whatever stock it was last told, and sells against it")
		return nil
	}
	withSites, ok := gateway.(sitesOf)
	if !ok || len(withSites.Sites()) == 0 {
		logger.Warn("inventory feed not started: the gateway converges no site")
		return nil
	}
	var sites []shared.SiteId
	for _, s := range withSites.Sites() {
		sites = append(sites, shared.SiteId(s))
	}
	logger.Info("inventory feed wired", "interval", every, "sites", len(sites))
	return &inventoryWorker{
		every: every,
		feed: &usecases.FeedInventory{
			Feeder:    gateway,
			Catalogue: catalogue,
			Offers:    offers,
			Logger:    logger,
			Sites:     sites,
		},
	}
}

// startInventoryWorker runs the feed until ctx is done: once at boot, so a
// restart does not wait a whole interval, then on every tick. The returned
// channel closes once the goroutine has returned, so shutdown can wait for an
// in-flight feed instead of cutting it off; for a nil worker it is already
// closed.
func startInventoryWorker(ctx context.Context, logger *slog.Logger, w *inventoryWorker) chan struct{} {
	done := make(chan struct{})
	if w == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		runFeedInventory(ctx, w, logger)
	}()
	return done
}

// runFeedInventory drives FeedInventory. A pass never fails as a whole: each
// site's outcome is logged by the use case and the gateway, so this only
// reports a pass that changed something on the network. The gateway writes the
// ledger row before the call, so a feed cut off by shutdown is resumed under
// the same key by the next boot.
func runFeedInventory(ctx context.Context, w *inventoryWorker, logger *slog.Logger) {
	pass := func() {
		res := w.feed.Execute(ctx)
		sent, confirmed := res.Outcomes[contract.FeedSent], res.Outcomes[contract.FeedConfirmed]
		if sent > 0 || confirmed > 0 {
			logger.Info("inventory fed", "sent", sent, "confirmed", confirmed)
		}
	}
	pass()
	ticker := time.NewTicker(w.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pass()
		}
	}
}
