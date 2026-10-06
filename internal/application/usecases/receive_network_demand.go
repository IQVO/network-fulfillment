// Package usecases holds this context's application services. They
// orchestrate the domain and the OUT ports; they contain no business
// rules of their own and no network vocabulary.
package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ErrOrderNotFound is returned when no NetworkOrder has the given ref.
var ErrOrderNotFound = errors.New("network order not found")

// ReceiveNetworkDemand turns one unit of inbound network demand into an
// answered NetworkOrder. It is the heart of this context and the place
// ADR 0001's boundary is either honoured or broken.
//
// The sequence, and why it is this order:
//
//  1. TRANSLATE every line's product id to a SKU. A single
//     untranslatable product rejects the whole order — we cannot
//     acknowledge in full what we cannot identify in full, and the
//     network's protocol has no partial acknowledgement.
//  2. RECORD receipt: Save the aggregate and Publish NetworkOrderReceived
//     in one atomic scope (ADR 0003, transactional outbox — see
//     saveAndPublishReceived). The store and the topic can never
//     disagree about whether we received this demand.
//  3. RAISE A HELD ORDER in order-management and ask it whether the
//     deadline is feasible. Held, because we must know whether we CAN
//     fulfil before answering, and must not have put work on the floor
//     for demand we may still reject.
//  4. RECORD the answer: the state transition, its Save and its Publish
//     commit together in the SAME atomic scope, again via the outbox —
//     so the answer given to the network in step 5 is always backed by
//     a durably enqueued event, never a row with no corresponding event.
//  5. ANSWER the network: acknowledge if feasible, reject if not — only
//     once step 4 has durably committed.
//  6. COMMIT: release the held order on acknowledgement, cancel it on
//     rejection, so inventory reservations never outlive the decision.
//
// Feasibility is ASKED, never computed here (ADR 0001 §7). This use case
// contains no arithmetic on cutoffs, capacities or cycle times; the
// promise math lives in exactly one place in this fleet and it is
// order-management.
type ReceiveNetworkDemand struct {
	Orders      ports.NetworkOrderRepo
	Gateway     ports.NetworkGateway
	Planner     ports.FulfillmentPlanner
	Translation ports.ProductTranslation
	Events      ports.EventPublisher
	Clock       ports.Clock
	// UnitOfWork brackets every Save+Publish pair this use case makes,
	// atomically (transactional outbox). Optional: a nil value means "no
	// transactional backing" and the two calls run back to back, which
	// is exactly the in-memory / log-publisher dev configuration.
	UnitOfWork ports.UnitOfWork
}

func (uc *ReceiveNetworkDemand) Execute(ctx context.Context, demand contract.InboundDemand) (*networkorder.NetworkOrder, error) {
	now := uc.Clock.Now()

	// Idempotency: the inbound side is a POLL (ADR 0001 §5), so the same
	// demand WILL be delivered again — after an outage, after a partial
	// batch, or simply because the network still lists it as
	// outstanding. Re-answering would send a duplicate acknowledgement
	// for an order we already committed to.
	if existing, err := uc.Orders.FindByRef(ctx, demand.NetworkRef); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}

	lines, translationErr := uc.translate(ctx, demand)

	// An untranslatable product is a business fact, not a failure to
	// retry: reject inside the window rather than let the clock run out
	// in silence. We still record the order, because "we refused this,
	// and why" is exactly the kind of fact this context exists to own.
	if translationErr != nil {
		if !errors.Is(translationErr, shared.ErrUnknownProduct) {
			return nil, translationErr
		}
		return uc.rejectUntranslatable(ctx, demand, now)
	}

	o, err := networkorder.Receive(demand.NetworkRef, demand.SiteId, demand.RequiredShipBy, lines, now)
	if err != nil {
		return nil, err
	}
	if err := uc.saveAndPublishReceived(ctx, o, len(lines), now); err != nil {
		return nil, err
	}

	result, err := uc.Planner.RaiseHeldOrder(ctx, contract.HeldOrderRequest{
		SiteId:         o.SiteId(),
		RequiredShipBy: o.RequiredShipBy(),
		Lines:          o.SKUQuantities(),
	})
	if err != nil {
		return nil, fmt.Errorf("raise held order: %w", err)
	}

	if !result.Feasible {
		return uc.reject(ctx, o, &result.LocalOrderId, shared.RejectionReasonInfeasibleDeadline)
	}
	return uc.acknowledge(ctx, o, result)
}

// saveAndPublishReceived persists o and publishes NetworkOrderReceived in
// one atomic scope: the aggregate's very first row and the fact "the
// network sent us this, and the acknowledgement clock is now running"
// commit together or not at all. lineCount is passed in separately from
// o.Lines() rather than derived from it because ReceiveUntranslatable's
// order is deliberately lineless (see its own doc comment) — the event
// still needs to say "zero lines translated", not "translation was never
// attempted".
func (uc *ReceiveNetworkDemand) saveAndPublishReceived(ctx context.Context, o *networkorder.NetworkOrder, lineCount int, now time.Time) error {
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NetworkOrderReceived{
			NetworkRef:     o.NetworkRef(),
			SiteId:         o.SiteId(),
			RequiredShipBy: o.RequiredShipBy(),
			AcknowledgeBy:  o.AcknowledgeBy(),
			LineCount:      lineCount,
			At:             now,
		})
	})
}

// translate maps every line into our own vocabulary, failing on the
// first product we cannot identify.
func (uc *ReceiveNetworkDemand) translate(ctx context.Context, demand contract.InboundDemand) ([]networkorder.Line, error) {
	lines := make([]networkorder.Line, 0, len(demand.Lines))
	for _, l := range demand.Lines {
		sku, err := uc.Translation.ToSKU(ctx, l.NetworkProductId)
		if err != nil {
			return nil, err
		}
		line, err := networkorder.NewLine(l.NetworkLineRef, l.NetworkProductId, sku, l.Quantity)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// rejectUntranslatable records and refuses demand containing a product
// we have no mapping for. The order is built with a synthetic single
// line carrying the ORIGINAL network identifiers, so the refusal is
// auditable against what the network actually sent.
func (uc *ReceiveNetworkDemand) rejectUntranslatable(ctx context.Context, demand contract.InboundDemand, now time.Time) (*networkorder.NetworkOrder, error) {
	o, err := networkorder.ReceiveUntranslatable(demand.NetworkRef, demand.SiteId, demand.RequiredShipBy, now)
	if err != nil {
		return nil, err
	}
	if err := uc.saveAndPublishReceived(ctx, o, 0, now); err != nil {
		return nil, err
	}
	return uc.reject(ctx, o, nil, shared.RejectionReasonUntranslatableSKU)
}

// reject answers the network in the negative and releases whatever we
// were holding.
//
// CancelHeldOrder runs before the aggregate is even mutated, deliberately:
// if the submission fails we will retry it and reach here again, whereas
// a cancel skipped on an error path leaves inventory reserved for demand
// we have already refused — the orphaned-hold failure both ADRs flagged
// as their honest open gap. The state transition, its Save and its
// Publish then commit together in one atomic scope: the network is only
// ever told "no" (SubmitAcknowledgement below) once that record is
// durable, so a submission failure can safely be retried without risking
// a duplicate or contradictory event.
func (uc *ReceiveNetworkDemand) reject(ctx context.Context, o *networkorder.NetworkOrder, local *shared.LocalOrderId, reason shared.RejectionReason) (*networkorder.NetworkOrder, error) {
	if local != nil {
		if err := uc.Planner.CancelHeldOrder(ctx, *local); err != nil {
			return nil, fmt.Errorf("cancel held order: %w", err)
		}
	}
	if err := o.Reject(); err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NetworkOrderRejected{
			NetworkRef: o.NetworkRef(),
			SiteId:     o.SiteId(),
			Reason:     reason,
			At:         uc.Clock.Now(),
		})
	}); err != nil {
		return nil, err
	}
	if err := uc.Gateway.SubmitAcknowledgement(ctx, o.NetworkRef(), false); err != nil {
		return nil, fmt.Errorf("submit rejection: %w", err)
	}
	return o, nil
}

// acknowledge commits us to the order and puts the work on the floor.
//
// The state transition, its Save and its Publish (NetworkOrderSubmitted —
// the order moves to SUBMITTED, ADR 0016) commit together in one
// atomic scope BEFORE the network is told or the hold released: a crash
// before that scope commits leaves nothing behind for a re-poll to answer
// twice, and the network is never told yes for a fact that is not yet
// durably recorded. Release is still the LAST step of all: it is the
// irreversible one, and nothing reaches the floor until every fallible
// step before it — including the record and the network submission — has
// already succeeded.
func (uc *ReceiveNetworkDemand) acknowledge(ctx context.Context, o *networkorder.NetworkOrder, result contract.HeldOrderResult) (*networkorder.NetworkOrder, error) {
	if err := o.Submit(); err != nil {
		return nil, err
	}
	if err := o.LinkLocalOrder(result.LocalOrderId); err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NetworkOrderSubmitted{
			NetworkRef:   o.NetworkRef(),
			SiteId:       o.SiteId(),
			LocalOrderId: result.LocalOrderId,
			ReceivedAt:   o.ReceivedAt(),
			At:           uc.Clock.Now(),
		})
	}); err != nil {
		return nil, err
	}
	if err := uc.Gateway.SubmitAcknowledgement(ctx, o.NetworkRef(), true); err != nil {
		return nil, fmt.Errorf("submit acknowledgement: %w", err)
	}
	// Deliberately NOT released here. ADR 0001 §5 models this submission
	// as pending until a later transaction-status query reconciles it;
	// releasing now would commit real work to the floor for a
	// submission that is merely accepted-for-processing, not yet a
	// settled commitment. ReconcileSubmittedOrders performs the release,
	// once SubmissionStatus actually confirms it.
	return o, nil
}
