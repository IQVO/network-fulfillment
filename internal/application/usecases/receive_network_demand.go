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
//  2. RAISE A HELD ORDER in order-management and ask it whether the
//     deadline is feasible. Held, because we must know whether we CAN
//     fulfil before answering, and must not have put work on the floor
//     for demand we may still reject.
//  3. ANSWER the network: acknowledge if feasible, reject if not.
//  4. COMMIT: release the held order on acknowledgement, cancel it on
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
	if err := uc.publishReceived(ctx, o, len(lines), now); err != nil {
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

// publishReceived raises NetworkOrderReceived. lineCount is passed in
// separately from o.Lines() rather than derived from it because
// ReceiveUntranslatable's order is deliberately lineless (see its own doc
// comment) — the event still needs to say "zero lines translated", not
// "translation was never attempted".
func (uc *ReceiveNetworkDemand) publishReceived(ctx context.Context, o *networkorder.NetworkOrder, lineCount int, now time.Time) error {
	return uc.Events.Publish(ctx, shared.NetworkOrderReceived{
		NetworkRef:     o.NetworkRef(),
		SiteId:         o.SiteId(),
		RequiredShipBy: o.RequiredShipBy(),
		AcknowledgeBy:  o.AcknowledgeBy(),
		LineCount:      lineCount,
		At:             now,
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
	if err := uc.publishReceived(ctx, o, 0, now); err != nil {
		return nil, err
	}
	return uc.reject(ctx, o, nil, shared.RejectionReasonUntranslatableSKU)
}

// reject answers the network in the negative and releases whatever we
// were holding.
//
// The local order is cancelled BEFORE the network is told, deliberately:
// if the submission fails we will retry it and reach here again, whereas
// a cancel skipped on an error path leaves inventory reserved for demand
// we have already refused — the orphaned-hold failure both ADRs flagged
// as their honest open gap.
func (uc *ReceiveNetworkDemand) reject(ctx context.Context, o *networkorder.NetworkOrder, local *shared.LocalOrderId, reason shared.RejectionReason) (*networkorder.NetworkOrder, error) {
	if local != nil {
		if err := uc.Planner.CancelHeldOrder(ctx, *local); err != nil {
			return nil, fmt.Errorf("cancel held order: %w", err)
		}
	}
	if err := o.Reject(); err != nil {
		return nil, err
	}
	if err := uc.Orders.Save(ctx, o); err != nil {
		return nil, err
	}
	if err := uc.Gateway.SubmitAcknowledgement(ctx, o.NetworkRef(), false); err != nil {
		return nil, fmt.Errorf("submit rejection: %w", err)
	}
	if err := uc.Events.Publish(ctx, shared.NetworkOrderRejected{
		NetworkRef: o.NetworkRef(),
		SiteId:     o.SiteId(),
		Reason:     reason,
		At:         uc.Clock.Now(),
	}); err != nil {
		return nil, fmt.Errorf("publish rejection: %w", err)
	}
	return o, nil
}

// acknowledge commits us to the order and puts the work on the floor.
//
// Order of operations is the opposite of reject's, and for the same
// reason — the safe side of a failure. The local order is linked and
// SAVED before the network is told, so a crash between the two leaves a
// recoverable record rather than an acknowledgement the network believes
// and we have no trace of. Release comes last: it is the irreversible
// step, and nothing should reach the floor until everything that can
// fail has.
func (uc *ReceiveNetworkDemand) acknowledge(ctx context.Context, o *networkorder.NetworkOrder, result contract.HeldOrderResult) (*networkorder.NetworkOrder, error) {
	if err := o.Acknowledge(); err != nil {
		return nil, err
	}
	if err := o.LinkLocalOrder(result.LocalOrderId); err != nil {
		return nil, err
	}
	if err := uc.Orders.Save(ctx, o); err != nil {
		return nil, err
	}
	if err := uc.Gateway.SubmitAcknowledgement(ctx, o.NetworkRef(), true); err != nil {
		return nil, fmt.Errorf("submit acknowledgement: %w", err)
	}
	if err := uc.Planner.ReleaseHeldOrder(ctx, result.LocalOrderId); err != nil {
		return nil, fmt.Errorf("release held order: %w", err)
	}
	if err := uc.Events.Publish(ctx, shared.NetworkOrderAcknowledged{
		NetworkRef:   o.NetworkRef(),
		SiteId:       o.SiteId(),
		LocalOrderId: result.LocalOrderId,
		ReceivedAt:   o.ReceivedAt(),
		At:           uc.Clock.Now(),
	}); err != nil {
		return nil, fmt.Errorf("publish acknowledgement: %w", err)
	}
	return o, nil
}
