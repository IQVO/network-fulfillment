package usecases

import (
	"context"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// SweepAcknowledgementDeadlines finds orders whose 24h acknowledgement
// window has closed with no answer (ADR 0001 §6).
//
// A miss is a REPORTED FACT, not an error: the window has already
// closed, so there is nothing left to prevent and nothing to retry. The
// only useful acts are to record it and to stop holding inventory for
// demand we never answered — which is the orphaned-hold gap both ADRs
// named as their honest open consequence, closed here.
//
// It deliberately does NOT acknowledge late. An acknowledgement after
// the SLA instant is worse than none: the network has already re-sourced
// the order, and confirming it would commit us to a shipment nobody is
// expecting.
//
// Modelled on fulfillment-execution's SweepCPTMisses, which solves the
// structurally identical problem for internal cutoffs.
type SweepAcknowledgementDeadlines struct {
	Orders  ports.NetworkOrderRepo
	Planner ports.FulfillmentPlanner
	Events  ports.EventPublisher
	Clock   ports.Clock
}

// SweepResult reports what one pass did, so a caller (a scheduler, a
// test, or an operator endpoint) can see the outcome without reading
// logs.
type SweepResult struct {
	Examined int
	Missed   int
}

func (uc *SweepAcknowledgementDeadlines) Execute(ctx context.Context) (SweepResult, error) {
	now := uc.Clock.Now()

	unanswered, err := uc.Orders.ListUnanswered(ctx)
	if err != nil {
		return SweepResult{}, err
	}

	res := SweepResult{Examined: len(unanswered)}
	for _, o := range unanswered {
		if !o.AcknowledgementOverdue(now) {
			continue
		}
		if err := uc.missOne(ctx, o); err != nil {
			// One bad order must not abort the pass: the remaining
			// orders are still holding inventory, and they are exactly
			// what this sweep exists to free. The failure is left for
			// the next pass to retry — the window is already closed, so
			// nothing further is lost by waiting.
			continue
		}
		res.Missed++
	}
	return res, nil
}

func (uc *SweepAcknowledgementDeadlines) missOne(ctx context.Context, o *networkorder.NetworkOrder) error {
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
	if err := uc.Orders.Save(ctx, o); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NetworkOrderRejected{
		NetworkRef: o.NetworkRef(),
		SiteId:     o.SiteId(),
		Reason:     shared.RejectionReasonAcknowledgementDeadlineMissed,
		At:         uc.Clock.Now(),
	})
}
