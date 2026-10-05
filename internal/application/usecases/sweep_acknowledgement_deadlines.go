package usecases

import (
	"context"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// SweepAcknowledgementDeadlines finds orders whose 24h acknowledgement
// window has closed with no answer (ADR 0001 §6).
//
// It is a REPORTED FACT, not a state transition: per ADR 0001 §6, "the
// sweep never mutates the aggregate — it only raises
// AcknowledgementDeadlineAtRisk". Structurally this mirrors
// fulfillment-execution's SweepCPTMisses, which solves the identical
// problem for internal cutoffs the same way. RejectOverdueOrders is the
// separate use case (its own audit trail) that performs the actual
// rejection and frees the held inventory; a scheduler runs both, but
// they are independent code paths on purpose.
//
// Re-firing on every pass for as long as an order remains overdue is
// intended, not a bug to dedupe: the condition is still true.
type SweepAcknowledgementDeadlines struct {
	Orders ports.NetworkOrderRepo
	Events ports.EventPublisher
	Clock  ports.Clock
}

// SweepResult reports what one pass did, so a caller (a scheduler, a
// test, or an operator endpoint) can see the outcome without reading
// logs.
type SweepResult struct {
	Examined int
	AtRisk   int
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
		// A publish failure for one order must not abort the pass —
		// re-firing on the next pass recovers it, and the remaining
		// orders are just as overdue and just as worth reporting.
		if err := uc.Events.Publish(ctx, shared.AcknowledgementDeadlineAtRisk{
			NetworkRef:    o.NetworkRef(),
			SiteId:        o.SiteId(),
			AcknowledgeBy: o.AcknowledgeBy(),
			At:            now,
		}); err != nil {
			continue
		}
		res.AtRisk++
	}
	return res, nil
}
