package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// SweepOrphanedHolds frees the order-management holds nobody decided
// (ADR 0024, gap OM-0020, order-management ADR 0020's accepted deferral).
//
// A network-originated order is raised in order-management as a HELD order
// that reserves real inventory. If this context never releases or cancels
// it, the reservations stay forever, and order-management deliberately has no
// timeout of its own: the clock lives here. This pass finds the holds whose
// order is past acknowledgeBy + Grace and still undecided (the predicate is the
// domain's: NetworkOrder.HoldOrphaned) and cancels them through the EXISTING
// CancelHeldOrder, classifying order-management's answer:
//
//   - 2xx, or 404: the hold is gone. Record the outcome and report
//     OrphanedHoldCancelled in the same transaction.
//   - already released (409/422): NOT an orphan. Record it, never retry it.
//   - anything else (5xx, timeout, open breaker): leave it open and retry on the
//     next pass. Never mark it done.
//
// It never decides the protocol state of the order (RejectOverdueOrders owns
// that, with its own audit trail) and it reads nothing from order-management
// but the answer to the cancel. Like SweepAcknowledgementDeadlines and
// RejectOverdueOrders it is independent of the other two: a scheduler runs
// all three, none depends on another having run.
type SweepOrphanedHolds struct {
	Orders  ports.NetworkOrderRepo
	Planner ports.FulfillmentPlanner
	Events  ports.EventPublisher
	Clock   ports.Clock
	// UnitOfWork brackets Save+Publish atomically (transactional outbox).
	// Optional: nil means "no transactional backing" (in-memory dev mode).
	UnitOfWork ports.UnitOfWork
	// Metrics counts each hold's outcome. Optional.
	Metrics ports.OrphanHoldMetrics
	// Grace is added to acknowledgeBy before a hold counts as orphaned. It
	// lets RejectOverdueOrders do its normal work first, so this pass is a
	// backstop and never a second actor racing it. 0 means no grace.
	Grace time.Duration
}

// SweepOrphanedHoldsResult reports what one pass did.
type SweepOrphanedHoldsResult struct {
	// Examined is the number of open holds listed; the ones not yet orphaned
	// (still inside the window or the grace) are counted here and nowhere else.
	Examined        int
	Cancelled       int
	AlreadyReleased int
	Failed          int
}

func (uc *SweepOrphanedHolds) Execute(ctx context.Context) (SweepOrphanedHoldsResult, error) {
	now := uc.Clock.Now()

	open, err := uc.Orders.ListOpenHolds(ctx)
	if err != nil {
		return SweepOrphanedHoldsResult{}, err
	}

	res := SweepOrphanedHoldsResult{Examined: len(open)}
	for _, o := range open {
		if !o.HoldOrphaned(now, uc.Grace) {
			continue
		}
		outcome := uc.sweepOne(ctx, o, now)
		uc.count(ctx, outcome)
		if outcome == contract.HoldSweepCancelled {
			res.Cancelled++
		}
		if outcome == contract.HoldSweepAlreadyReleased {
			res.AlreadyReleased++
		}
		if outcome == contract.HoldSweepFailed {
			res.Failed++
		}
	}
	return res, nil
}

func (uc *SweepOrphanedHolds) count(ctx context.Context, outcome contract.HoldSweepOutcome) {
	if uc.Metrics != nil {
		uc.Metrics.Swept(ctx, outcome)
	}
}

// sweepOne frees one orphaned hold. One bad hold must not abort the pass: the
// rest are just as orphaned, and a failed one is retried on the next pass
// (order-management's cancel is idempotent).
func (uc *SweepOrphanedHolds) sweepOne(ctx context.Context, o *networkorder.NetworkOrder, now time.Time) contract.HoldSweepOutcome {
	err := uc.Planner.CancelHeldOrder(ctx, *o.LocalOrderId())
	if err == nil {
		return uc.settle(ctx, o, networkorder.HoldCancelled, shared.HoldAnswerCancelled, now)
	}
	if errors.Is(err, contract.ErrHeldOrderNotFound) {
		return uc.settle(ctx, o, networkorder.HoldCancelled, shared.HoldAnswerNotFound, now)
	}
	if errors.Is(err, contract.ErrHeldOrderAlreadyReleased) {
		return uc.settle(ctx, o, networkorder.HoldAlreadyReleased, "", now)
	}
	return contract.HoldSweepFailed
}

// settle records the outcome and, when the hold was cancelled, reports it in
// the same atomic scope. An already-released hold is recorded without an
// event: nothing was cancelled.
func (uc *SweepOrphanedHolds) settle(
	ctx context.Context,
	o *networkorder.NetworkOrder,
	outcome networkorder.HoldOutcome,
	answer shared.HoldAnswer,
	now time.Time,
) contract.HoldSweepOutcome {
	if err := o.SettleHold(outcome, now); err != nil {
		return contract.HoldSweepFailed
	}
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		if outcome != networkorder.HoldCancelled {
			return nil
		}
		return uc.Events.Publish(ctx, shared.OrphanedHoldCancelled{
			NetworkRef:    o.NetworkRef(),
			SiteId:        o.SiteId(),
			LocalOrderId:  *o.LocalOrderId(),
			AcknowledgeBy: o.AcknowledgeBy(),
			Answer:        answer,
			At:            now,
		})
	})
	if err != nil {
		return contract.HoldSweepFailed
	}
	if outcome == networkorder.HoldAlreadyReleased {
		return contract.HoldSweepAlreadyReleased
	}
	return contract.HoldSweepCancelled
}
