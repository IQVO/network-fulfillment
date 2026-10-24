package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// newHTTPServer builds the REST server around the inbound adapter's
// routes, on the address from PORT, with a bounded header-read timeout.
func newHTTPServer(api *inboundhttp.Server) *http.Server {
	return &http.Server{
		Addr:              addr(),
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// serve runs the inbound poller, the acknowledgement sweep and the HTTP
// server until SIGINT/SIGTERM arrives (exit code 0) or until the relay or a
// Kafka cache consumer dies on its own (exit code 1, so the pod restarts),
// then gives in-flight requests a bounded grace period before the process
// winds down.
func serve(
	ctx context.Context,
	logger *slog.Logger,
	srv *http.Server,
	sweep *usecases.SweepAcknowledgementDeadlines,
	rejectOverdue *usecases.RejectOverdueOrders,
	orphanHolds *usecases.SweepOrphanedHolds,
	reconcile *usecases.ReconcileSubmittedOrders,
	inbound *poller.Poller,
	relay *postgres.OutboxRelay,
	readiness *inboundhttp.Readiness,
	recompute *usecases.RecomputeCapabilityOffers,
	capabilityCaches []cacheConsumer,
) int {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runSweep(ctx, sweep, logger)
	go runRejectOverdue(ctx, rejectOverdue, logger)
	if orphanHolds != nil {
		go runSweepOrphanedHolds(ctx, orphanHolds, logger)
	}
	go runReconcile(ctx, reconcile, logger)
	go runRecompute(ctx, recompute, logger)

	pollerDone := startPoller(ctx, inbound)

	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	relayDone := startRelay(relayCtx, logger, relay)

	startHTTP(logger, srv)

	watched := []namedDone{{"outbox relay", relayDone, relayCtx}}
	for i, c := range capabilityCaches {
		watched = append(watched, namedDone{fmt.Sprintf("capability-offer cache consumer %d", i+1), c.done, c.ctx})
	}
	died := watchDeaths(ctx, watched)

	exitCode := 0
	select {
	case <-ctx.Done():
	case name := <-died:
		// The relay or a Kafka consumer stopped on its own while the process
		// was meant to be running: staying up would leave a pod that answers
		// HTTP but silently publishes or consumes nothing. Exit non-zero so
		// Kubernetes restarts it.
		logger.Error("a background component stopped on its own; shutting down", "component", name)
		exitCode = 1
	}
	stop() // cancels the poller and tickers when the cause was not a signal

	shutdownGracefully(logger, srv, readiness, stopRelay, relayDone, pollerDone, capabilityCaches)
	return exitCode
}

// namedDone pairs a long-lived goroutine's completion channel with a name
// for the log line, and with the context the goroutine itself runs on.
type namedDone struct {
	name string
	done <-chan struct{}
	ctx  context.Context
}

// watchDeaths reports, on the returned channel, the name of every watched
// goroutine that finishes while ctx is still live — that is, one that died
// on its own instead of being stopped by shutdown.
func watchDeaths(ctx context.Context, watched []namedDone) <-chan string {
	died := make(chan string, len(watched))
	for _, w := range watched {
		go func() {
			select {
			case <-w.done:
				if ctx.Err() == nil {
					died <- w.name
				}
			case <-ctx.Done():
			}
		}()
	}
	return died
}

// startPoller runs the inbound poller and returns a channel that closes
// once inbound.Run's goroutine has returned — mirroring the outbox
// relay's own relayDone — so graceful shutdown can wait for a REAL stop
// rather than merely firing the cancel and moving on. The poller itself
// has no in-flight "commit" step to finish (unlike a Kafka consumer's
// offset commit): its own watermark only advances after a fully
// successful pass (see poller.go's own doc comment), so an in-flight pass
// interrupted by shutdown simply is not counted as successful and is
// safely re-polled on the next boot.
func startPoller(ctx context.Context, inbound *poller.Poller) chan struct{} {
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		inbound.Run(ctx)
	}()
	return pollerDone
}

// startRelay runs the outbox relay (ADR 0003) alongside the HTTP server in
// the same process, draining outbox_events onto Kafka. The relay is always
// wired (the real-wiring ADR): there is no mode without one.
func startRelay(ctx context.Context, logger *slog.Logger, relay *postgres.OutboxRelay) chan struct{} {
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		logger.Info("outbox relay running", "topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic,
			"max_attempts", relay.MaxAttempts())
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("outbox relay stopped", "err", err)
		}
	}()
	return relayDone
}

// startHTTP serves srv in the background; a listener failure is fatal.
func startHTTP(logger *slog.Logger, srv *http.Server) {
	go func() {
		logger.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()
}

// shutdownGracefully performs the graceful shutdown (ADR 0004, ported from
// order-management's ADR 0025), in order:
//
//  1. Flip readiness to not-ready FIRST, before anything else
//     stops — a Kubernetes readinessProbe polling /readyz needs a
//     window to observe this and stop routing NEW traffic to this
//     pod before step 2 below ever closes the listener.
//  2. Stop accepting new HTTP connections and drain in-flight
//     requests, bounded by shutdownCtx.
//  3. Stop the outbox relay and the poller cleanly: cancel their
//     contexts (no new poll/relay pass starts after this) and wait,
//     bounded by the SAME shutdownCtx, for their goroutines to
//     actually finish in-flight work, rather than merely asking
//     them to stop and moving on.
//  4. Only THEN do the deferred closeOrders/closeEventPublisher
//     calls (registered earlier in run(), so by defer's LIFO order
//     they run AFTER this point, once every consumer/relay/poller
//     goroutine has already stopped touching the pool).
func shutdownGracefully(
	logger *slog.Logger,
	srv *http.Server,
	readiness *inboundhttp.Readiness,
	stopRelay context.CancelFunc,
	relayDone, pollerDone <-chan struct{},
	capabilityCaches []cacheConsumer,
) {
	readiness.SetNotReady()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	// Let the relay finish its in-flight pass so an event committed by a
	// request that completed just before shutdown is not stranded until
	// the next pod boots.
	stopRelay()
	waitStopped(shutdownCtx, logger, relayDone, "outbox relay did not stop before the shutdown deadline")

	// The poller's own context is serve's signal.NotifyContext ctx,
	// already cancelled by the SIGTERM/SIGINT that got us here, so
	// inbound.Run's own select has already seen it. This wait is purely
	// for the in-flight pass (if any) to finish before main returns.
	waitStopped(shutdownCtx, logger, pollerDone, "poller did not stop before the shutdown deadline")

	// capabilityCaches holds the two Kafka cache consumers.
	for _, c := range capabilityCaches {
		waitStopped(shutdownCtx, logger, c.done, "a capability-offer cache consumer did not stop before the shutdown deadline")
	}

	logger.Info("stopped")
}

// waitStopped blocks until done closes or the shutdown deadline passes,
// warning with msg in the latter case.
func waitStopped(shutdownCtx context.Context, logger *slog.Logger, done <-chan struct{}, msg string) {
	select {
	case <-done:
	case <-shutdownCtx.Done():
		logger.Warn(msg)
	}
}
