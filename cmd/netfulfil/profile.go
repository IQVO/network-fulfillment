package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// profileWorker is the node profile declaration (ADR 0027) ready to be driven:
// the use case and how often. A nil *profileWorker means there is nothing to run.
type profileWorker struct {
	declare *usecases.DeclareNodeProfiles
	every   time.Duration
}

// sitesOf is what the live gateway exposes about the sites it converges.
type sitesOf interface{ Sites() []string }

// wireNodeProfiles builds the node profile worker, or returns nil with the
// reason logged. Only the live gateway has a network to declare to: stub mode
// never starts it. It reads the same two caches RecomputeCapabilityOffers
// does (the CPT schedule with cycle times, and the remaining path capacity),
// and declares for the sites the gateway itself converges onto nodes.
func wireNodeProfiles(
	logger *slog.Logger,
	mode retailnetwork.Mode,
	gateway ports.NetworkGateway,
	pathCap ports.ProcessPathCapability,
	capacity ports.PathCapacity,
) *profileWorker {
	if mode != retailnetwork.ModeLive {
		return nil
	}
	every, enabled := profileInterval(logger)
	if !enabled {
		logger.Warn("node profile declaration disabled by NETWORK_PROFILE_INTERVAL=0: the network keeps whatever profile it has, and judges our acknowledgements by it")
		return nil
	}
	withSites, ok := gateway.(sitesOf)
	if !ok || len(withSites.Sites()) == 0 {
		logger.Warn("node profile declaration not started: the gateway converges no site")
		return nil
	}
	var sites []shared.SiteId
	for _, s := range withSites.Sites() {
		sites = append(sites, shared.SiteId(s))
	}
	history, _ := gateway.(ports.ProfileHistory) // nil is valid: no memory of ceilings
	logger.Info("node profile declaration wired", "interval", every, "sites", len(sites))
	return &profileWorker{
		every: every,
		declare: &usecases.DeclareNodeProfiles{
			Gateway:  gateway,
			PathCap:  pathCap,
			Capacity: capacity,
			Clock:    system.Clock{},
			Logger:   logger,
			History:  history,
			Metrics:  telemetry.NewNodeProfileMetrics(nil),
			Sites:    sites,
		},
	}
}

// startProfileWorker runs the declaration until ctx is done: once at boot, so a
// restart does not wait a whole interval, then on every tick. The returned
// channel closes once the goroutine has returned, so shutdown can wait for an
// in-flight declaration instead of cutting it off; for a nil worker it is
// already closed.
func startProfileWorker(ctx context.Context, logger *slog.Logger, w *profileWorker) chan struct{} {
	done := make(chan struct{})
	if w == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		runDeclareProfiles(ctx, w, logger)
	}()
	return done
}

// runDeclareProfiles drives DeclareNodeProfiles. A pass never fails as a whole:
// each site's outcome is counted and logged by the use case and the gateway, so
// this only reports a pass that changed something on the network.
func runDeclareProfiles(ctx context.Context, w *profileWorker, logger *slog.Logger) {
	pass := func() {
		res := w.declare.Execute(ctx)
		declared, confirmed := res.Outcomes[contract.ProfileDeclared], res.Outcomes[contract.ProfileConfirmed]
		if declared > 0 || confirmed > 0 {
			logger.Info("node profile declared", "declared", declared, "confirmed", confirmed)
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
