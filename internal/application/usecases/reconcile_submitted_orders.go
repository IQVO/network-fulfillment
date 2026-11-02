package usecases

import (
	"context"
	"fmt"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ReconcileSubmittedOrders settles every order left SUBMITTED by
// ReceiveNetworkDemand, by asking the gateway's transaction-status query
// whether the network has actually confirmed or refused what we told it
// (ADR 0001 §5). This is the use case that makes "submitted but
// unreconciled" a real, bounded, revisited state rather than a
// fire-and-forget assumption: a 200 on the original submit call is
// accepted-for-processing, never itself the completed commitment.
//
// It deliberately mirrors SweepAcknowledgementDeadlines' shape (one
// listing port call, one per-order step that must not abort the whole
// pass on a single failure) rather than inventing a new one.
type ReconcileSubmittedOrders struct {
	Orders  ports.NetworkOrderRepo
	Gateway ports.NetworkGateway
	Planner ports.FulfillmentPlanner
	Events  ports.EventPublisher
	Clock   ports.Clock
	// UnitOfWork brackets Save+Publish atomically (transactional
	// outbox), same convention as every other use case here. Optional.
	UnitOfWork ports.UnitOfWork
}

// ReconcileResult reports what one pass did.
type ReconcileResult struct {
	Examined  int
	Confirmed int
	Failed    int
	Pending   int
	// Resent counts orders whose answer the gateway had never sent and this
	// pass sent again (contract.SubmissionNotSent).
	Resent int
}

func (uc *ReconcileSubmittedOrders) Execute(ctx context.Context) (ReconcileResult, error) {
	submitted, err := uc.Orders.ListSubmitted(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}

	res := ReconcileResult{Examined: len(submitted)}
	for _, o := range submitted {
		status, err := uc.Gateway.SubmissionStatus(ctx, o.NetworkRef())
		if err != nil {
			// One bad lookup must not abort the pass, same reasoning as
			// the sweep: every other submitted order is still waiting on
			// a real verdict, and that is exactly what this pass exists
			// to chase down. Left for the next pass to retry.
			continue
		}
		switch status {
		case contract.SubmissionSuccess:
			if err := uc.confirm(ctx, o.NetworkRef()); err != nil {
				continue
			}
			res.Confirmed++
		case contract.SubmissionFailure:
			if err := uc.fail(ctx, o.NetworkRef()); err != nil {
				continue
			}
			res.Failed++
		case contract.SubmissionNotSent:
			// A SUBMITTED order is one we accepted: say so again. The next pass
			// reconciles whatever the network made of it.
			if err := uc.Gateway.SubmitAcknowledgement(ctx, o.NetworkRef(), contract.Acknowledgement{Accepted: true}); err != nil {
				continue
			}
			res.Resent++
		default:
			res.Pending++
		}
	}
	return res, nil
}

// confirm settles a submission the network has confirmed: the aggregate
// is saved ACKNOWLEDGED, and NetworkOrderAcknowledged published, in one
// atomic scope (ADR 0016) BEFORE the held order is released, so a crash
// between the two leaves a recoverable record (the order already reads
// ACKNOWLEDGED, its event already enqueued) rather than an ambiguous one.
// confirm settles one order on a fresh load, retrying the whole
// load-mutate-save cycle on a version conflict (migration 0013): the
// verdict was fetched above, but the row may have moved since — e.g. the
// overdue sweep rejected the order while we were asking the gateway. A
// reload that is no longer SUBMITTED fails ConfirmAcknowledgement's
// transition check and is swallowed by the pass loop, same as today.
func (uc *ReconcileSubmittedOrders) confirm(ctx context.Context, ref shared.NetworkRef) error {
	return withVersionRetry(func() error {
		o, err := uc.Orders.FindByRef(ctx, ref)
		if err != nil {
			return err
		}
		if o == nil {
			return fmt.Errorf("confirm %q: %w", ref, ErrOrderNotFound)
		}
		return uc.confirmOne(ctx, o)
	})
}

func (uc *ReconcileSubmittedOrders) confirmOne(ctx context.Context, o *networkorder.NetworkOrder) error {
	if err := o.ConfirmAcknowledgement(); err != nil {
		return err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		ack := shared.NetworkOrderAcknowledged{
			NetworkRef: o.NetworkRef(),
			SiteId:     o.SiteId(),
			ReceivedAt: o.ReceivedAt(),
			At:         uc.Clock.Now(),
		}
		if local := o.LocalOrderId(); local != nil {
			ack.LocalOrderId = *local
		}
		return uc.Events.Publish(ctx, ack)
	}); err != nil {
		return err
	}
	if local := o.LocalOrderId(); local != nil {
		return uc.Planner.ReleaseHeldOrder(ctx, *local)
	}
	return nil
}

// fail settles a submission the network itself refused: the hold is
// freed first, same ordering discipline as SweepAcknowledgementDeadlines
// and ReceiveNetworkDemand.reject use elsewhere in this package, because
// freeing the hold is the operationally important step and must not be
// skipped by a later failure.
// fail settles one refused order on a fresh load with the same conflict
// retry as confirm; freeHold is safe to repeat because order-management's
// cancel is idempotent and its already-released answer is handled below
// as a normal outcome.
func (uc *ReconcileSubmittedOrders) fail(ctx context.Context, ref shared.NetworkRef) error {
	return withVersionRetry(func() error {
		o, err := uc.Orders.FindByRef(ctx, ref)
		if err != nil {
			return err
		}
		if o == nil {
			return fmt.Errorf("fail %q: %w", ref, ErrOrderNotFound)
		}
		return uc.failOne(ctx, o)
	})
}

func (uc *ReconcileSubmittedOrders) failOne(ctx context.Context, o *networkorder.NetworkOrder) error {
	if err := freeHold(ctx, uc.Planner, o, uc.Clock.Now()); err != nil {
		return err
	}
	if err := o.Reject(); err != nil {
		return err
	}
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NetworkOrderRejected{
			NetworkRef: o.NetworkRef(),
			SiteId:     o.SiteId(),
			Reason:     shared.RejectionReasonSubmissionFailed,
			At:         uc.Clock.Now(),
		})
	})
}
