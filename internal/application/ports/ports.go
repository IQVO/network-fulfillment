// Package ports declares this context's OUT ports. Per the harness
// architecture fitness test, it contains interfaces and nothing else —
// no struct, no function, no constant.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// NetworkOrderRepo persists NetworkOrder aggregates.
//
// FindByRef returns (nil, nil) when no order has this ref: "not found"
// is the application's concern, not the repository's — the same
// convention every sibling service in this fleet follows.
type NetworkOrderRepo interface {
	Save(ctx context.Context, o *networkorder.NetworkOrder) error
	FindByRef(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error)
	ListUnanswered(ctx context.Context) ([]*networkorder.NetworkOrder, error)
	// ListAll returns every order regardless of state, for the read-only
	// MCP list_network_orders tool (internal/adapters/inbound/mcp) and
	// operator tooling. ListUnanswered above remains the one the sweep
	// and the OLTP HTTP surface use; this is a separate, explicitly wider
	// query rather than a state filter bolted onto it, so a caller that
	// wants "just the working set" cannot be silently widened by a
	// future change here.
	ListAll(ctx context.Context) ([]*networkorder.NetworkOrder, error)
}

// NetworkGateway is the ONLY route to the external network. Every call
// to it goes through one adapter with a NETWORK_MODE switch (ADR 0001
// §4), so that stub and sandbox are reachable without credentials and a
// live call is impossible to make by accident.
//
// It is expressed entirely in OUR vocabulary. The Amazon wire shapes —
// purchase orders, ASINs, acknowledgement codes, selling parties — live
// inside the implementing adapter and are not nameable from here.
type NetworkGateway interface {
	// PollDemand fetches demand the network has for us since a given
	// instant. Inbound is a poll, not a push (ADR 0001 §5).
	PollDemand(ctx context.Context, since time.Time) ([]contract.InboundDemand, error)

	// SubmitAcknowledgement tells the network we will fulfil the order
	// in full, or will not fulfil it at all. accepted=false is a
	// rejection; the protocol has no middle answer.
	SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, accepted bool) error

	// SubmitShipmentConfirmation tells the network the order has
	// shipped.
	SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef) error
}

// FulfillmentPlanner is order-management, seen from here through the
// narrowest possible keyhole.
//
// ADR 0001 §7: feasibility is ASKED of order-management and never
// recomputed in this context. The promise math lives in exactly one
// place in this fleet, and it is not here.
type FulfillmentPlanner interface {
	// RaiseHeldOrder creates an order in order-management that is
	// allocated but deliberately NOT released (its ADR 0020
	// releaseOnAllocation=false), returning the local order id and
	// whether the required ship-by deadline is feasible.
	//
	// Held is the whole point: this context must know whether it CAN
	// fulfil before it answers the network, and must not have put work
	// on the floor for demand it may still reject.
	RaiseHeldOrder(ctx context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error)

	// ReleaseHeldOrder commits a previously held order to the floor,
	// called once the network has been acknowledged.
	ReleaseHeldOrder(ctx context.Context, id shared.LocalOrderId) error

	// CancelHeldOrder abandons a held order, freeing its inventory
	// reservations, when we reject the demand instead.
	CancelHeldOrder(ctx context.Context, id shared.LocalOrderId) error
}

// ProductTranslation maps the network's product identity to ours. It is
// the Anti-Corruption Layer's dictionary, and the single place a
// NetworkProductId becomes a SKU.
type ProductTranslation interface {
	// ToSKU returns shared.ErrUnknownProduct when there is no mapping —
	// a business fact (we cannot sell what we cannot identify), whose
	// correct handling is to reject the order inside the acknowledgement
	// window rather than retry or crash.
	ToSKU(ctx context.Context, id shared.NetworkProductId) (shared.SKU, error)
}

// EventPublisher publishes this context's integration events.
type EventPublisher interface {
	Publish(ctx context.Context, event any) error
}

// Clock is the only source of time in the application layer, so the
// acknowledgement window and its sweep are testable without sleeping.
type Clock interface {
	Now() time.Time
}
