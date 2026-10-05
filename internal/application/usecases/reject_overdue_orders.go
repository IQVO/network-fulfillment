package usecases

import (
	"context"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// RejectOverdueOrders performs the actual rejection ADR 0001 §6 keeps
// deliberately separate from SweepAcknowledgementDeadlines: a miss is a
// reported fact first (AcknowledgementDeadlineAtRisk, re-fired every
// pass) and an irreversible rejection second, with its own audit trail
// (NetworkOrderRejected, reason=ACKNOWLEDGEMENT_DEADLINE_MISSED) and its
// own code path — a scheduler runs both use cases, but neither depends
// on the other having run.
//
// The only useful acts once a window has closed unanswered are to record
// the refusal and to stop holding inventory for demand nobody is going
// to fulfil, which is exactly what this use case does and
// SweepAcknowledgementDeadlines deliberately does not.
//
// It deliberately does NOT acknowledge late. An acknowledgement after
// the SLA instant is worse than none: the network has already re-sourced
// the order, and confirming it would commit us to a shipment nobody is
// expecting.
type RejectOverdueOrders struct {
	Orders  ports.NetworkOrderRepo
	Planner ports.FulfillmentPlanner
	Events  ports.EventPublisher
	Clock   ports.Clock
	// UnitOfWork brackets Save+Publish atomically (transactional
	// outbox). Optional: nil means "no transactional backing", the
	// in-memory / log-publisher dev configuration.
	UnitOfWork ports.UnitOfWork
}

// RejectOverdueResult reports what one pass did.
type RejectOverdueResult struct {
	Examined int
	Rejected int
}

func (uc *RejectOverdueOrders) Execute(ctx context.Context) (RejectOverdueResult, error) {
	now := uc.Clock.Now()

	unanswered, err := uc.Orders.ListUnanswered(ctx)
	if err != nil {
		return RejectOverdueResult{}, err
	}

	res := RejectOverdueResult{Examined: len(unanswered)}
	for _, o := range unanswered {
		if !o.AcknowledgementOverdue(now) {
			continue
		}
		if err := uc.rejectOne(ctx, o); err != nil {
			// One bad order must not abort the pass: the remaining
			// orders are still holding inventory, and they are exactly
			// what this use case exists to free. Left for the next
			// pass to retry — the window is already closed, so nothing
			// further is lost by waiting.
			continue
		}
		res.Rejected++
	}
	return res, nil
}

func (uc *RejectOverdueOrders) rejectOne(ctx context.Context, o *networkorder.NetworkOrder) error {
	// Free the local hold first. This is the step that actually matters
	// operationally — an unanswered order sitting on real inventory
	// reservations is stock we cannot sell to anyone else.
	if local := o.LocalOrderId(); local != nil {
		if err := uc.Planner.CancelHeldOrder(ctx, *local); err != nil {
			return err
		}
	}
	if err := o.Reject(); err != nil {
		return err
	}
	// Save and Publish commit together in one atomic scope
	// (transactional outbox), same discipline as ReceiveNetworkDemand:
	// the rejection record and its event can never diverge.
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NetworkOrderRejected{
			NetworkRef: o.NetworkRef(),
			SiteId:     o.SiteId(),
			Reason:     shared.RejectionReasonAcknowledgementDeadlineMissed,
			At:         uc.Clock.Now(),
		})
	})
}
