package usecases

import (
	"context"

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
			if err := uc.confirm(ctx, o); err != nil {
				continue
			}
			res.Confirmed++
		case contract.SubmissionFailure:
			if err := uc.fail(ctx, o); err != nil {
				continue
			}
			res.Failed++
		default:
			res.Pending++
		}
	}
	return res, nil
}

// confirm settles a submission the network has confirmed: the aggregate
// is saved ACKNOWLEDGED before the held order is released, so a crash
// between the two leaves a recoverable record (the order already reads
// ACKNOWLEDGED) rather than an ambiguous one.
func (uc *ReconcileSubmittedOrders) confirm(ctx context.Context, o *networkorder.NetworkOrder) error {
	if err := o.ConfirmAcknowledgement(); err != nil {
		return err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		return uc.Orders.Save(ctx, o)
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
func (uc *ReconcileSubmittedOrders) fail(ctx context.Context, o *networkorder.NetworkOrder) error {
	if local := o.LocalOrderId(); local != nil {
		if err := uc.Planner.CancelHeldOrder(ctx, *local); err != nil {
			return err
		}
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
