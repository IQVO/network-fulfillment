package ports

import (
	"context"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// PurchaseOrderIndex answers which NetworkRef the demand of one purchase order
// was filed under (ADR 0036). It is the persisted map of ADR 0026 Decision 4
// read in one direction: the NetworkRef is minted when a purchase order is first
// seen and never derived from its number, so a caller that knows only the number
// needs this lookup to reach the order.
//
// It is deliberately NOT a method of NetworkOrderRepo. The tables behind it
// belong to the retailnetwork adapter, whose store keeps the network's
// vocabulary inside that package; the number crosses this port as an opaque
// string and nothing here parses it.
type PurchaseOrderIndex interface {
	// FindNetworkRefByPONumber returns the NetworkRef minted for the purchase
	// order with this number. ok is false, with a nil error, when no purchase
	// order with that number was ever recorded: "not found" is the application's
	// concern, as with every read of this context.
	FindNetworkRefByPONumber(ctx context.Context, poNumber string) (ref shared.NetworkRef, ok bool, err error)
}
