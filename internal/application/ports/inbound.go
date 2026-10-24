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

// InboundShipmentRepo is the local record of the shipments the network handed
// over: shipment <-> ASN number <-> status, and the poll cursor per site. It is
// the loop's memory, so a restart neither forgets a shipment nor registers it
// twice.
type InboundShipmentRepo interface {
	// Record stores a polled shipment as PENDING under its deterministic ASN
	// number. A shipment already recorded is left exactly as it is (a replay of
	// the poll is a no-op) and recorded=false.
	Record(ctx context.Context, s contract.InboundShipment, asnNumber string, now time.Time) (recorded bool, err error)
	// Due returns the PENDING shipments whose next attempt is at or before now,
	// oldest first, at most limit.
	Due(ctx context.Context, now time.Time, limit int) ([]contract.InboundRecord, error)
	// MarkRegistered closes the shipment's intake: the ASN exists.
	MarkRegistered(ctx context.Context, shipmentRef string, now time.Time) error
	// MarkRetry keeps the shipment PENDING with the error and the next attempt.
	MarkRetry(ctx context.Context, shipmentRef, lastError string, nextAttempt time.Time) error
	// MarkRejected parks the shipment: a verdict about its data, not retried.
	MarkRejected(ctx context.Context, shipmentRef, lastError string) error
	// Cursor returns the instant the last poll of a site ended at (zero = never).
	Cursor(ctx context.Context, site shared.SiteId) (time.Time, error)
	// AdvanceCursor moves the cursor forward; it never moves it back.
	AdvanceCursor(ctx context.Context, site shared.SiteId, to time.Time) error
}

// GtinResolver answers which SKU a GTIN identifies (product-master
// GET /gtins/{gtin}). contract.ErrGtinNotFound means product-master has no such
// GTIN.
type GtinResolver interface {
	ResolveGtin(ctx context.Context, gtin string) (contract.GtinResolution, error)
}

// GtinCache keeps product-master's answers locally. Only answers are stored.
type GtinCache interface {
	CachedGtin(ctx context.Context, gtin string) (contract.GtinResolution, bool, error)
	CacheGtin(ctx context.Context, r contract.GtinResolution) error
}

// AsnRegistrar registers an ASN in inbound-receiving. It is idempotent per ASN
// number: an ASN that already exists is success, so a replay is a no-op. A
// refusal is a *contract.AsnRefusedError.
type AsnRegistrar interface {
	RegisterAsn(ctx context.Context, asn contract.AsnRegistration) error
}
