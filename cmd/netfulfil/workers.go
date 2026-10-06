package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// The background tickers driven by serve(). Each mirrors the same shape:
// tick, execute, log. (runRecompute lives with the capability-offer
// wiring in capability.go.)

// runSweep drives the acknowledgement-deadline sweep on a ticker. It
// only ever REPORTS a deadline at risk (ADR 0001 §6) — runRejectOverdue
// below is the separate path that frees the held inventory.
func runSweep(ctx context.Context, sweep *usecases.SweepAcknowledgementDeadlines, logger *slog.Logger) {
	ticker := time.NewTicker(sweepInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := sweep.Execute(ctx)
			if err != nil {
				logger.Error("acknowledgement sweep failed", "err", err)
				continue
			}
			if res.AtRisk > 0 {
				logger.Warn("acknowledgement deadlines at risk", "examined", res.Examined, "atRisk", res.AtRisk)
			}
		}
	}
}

// runRejectOverdue drives the actual rejection of orders whose
// acknowledgement window has closed unanswered, on its own ticker and
// its own code path from runSweep above (ADR 0001 §6: the sweep never
// mutates the aggregate; this is the separate path that does, with its
// own audit trail). An unanswered order holds real inventory
// reservations, so this is what keeps a missed SLA from quietly becoming
// permanently unsellable stock.
func runRejectOverdue(ctx context.Context, rejectOverdue *usecases.RejectOverdueOrders, logger *slog.Logger) {
	ticker := time.NewTicker(sweepInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := rejectOverdue.Execute(ctx)
			if err != nil {
				logger.Error("overdue-order rejection failed", "err", err)
				continue
			}
			if res.Rejected > 0 {
				logger.Warn("overdue orders rejected", "examined", res.Examined, "rejected", res.Rejected)
			}
		}
	}
}

// runReconcile drives the submitted-order reconciliation pass (ADR 0001
// §5) on its own ticker, reusing POLL_INTERVAL as the cadence: a
// submission is meaningless to re-check faster than new demand can even
// arrive, and this keeps the knob surface small rather than adding a
// third interval env var for a v1.
func runReconcile(ctx context.Context, reconcile *usecases.ReconcileSubmittedOrders, logger *slog.Logger) {
	ticker := time.NewTicker(pollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := reconcile.Execute(ctx)
			if err != nil {
				logger.Error("submitted-order reconciliation failed", "err", err)
				continue
			}
			if res.Confirmed > 0 || res.Failed > 0 {
				logger.Info("submitted orders reconciled", "examined", res.Examined, "confirmed", res.Confirmed, "failed", res.Failed, "pending", res.Pending)
			}
		}
	}
}
