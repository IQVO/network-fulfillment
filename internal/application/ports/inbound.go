package ports

import (
	"context"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// InboundGateway is the inbound half of the way to the external network
// (ADR 0028): seller shipments in, receipts out. It is a port of its own, not
// two more methods of NetworkGateway, so the demand loop's port and its six test
// doubles stay as ADR 0026 left them; the live and the stub gateway implement
// both. Like NetworkGateway it speaks only OUR vocabulary: no node id, no
// transaction id, no wire field.
type InboundGateway interface {
	// PollInboundShipments returns the shipments bound for one site that the
	// network has handed over after `since` (zero = from the start), oldest
	// first. Re-delivery is normal: the same shipment can come back, and every
	// consumer of it is idempotent on InboundShipment.Ref.
	PollInboundShipments(ctx context.Context, site shared.SiteId, since time.Time) ([]contract.InboundShipment, error)

	// SubmitInboundReceipt tells the network what the dock received. It is
	// idempotent per shipment: calling it again after a crash or a failed call
	// never creates a second receipt on the network.
	SubmitInboundReceipt(ctx context.Context, receipt contract.InboundReceipt) error
}
