package contract

import (
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The shapes of the inbound loop (ADR 0028): sellers ship SSCC-labelled cartons
// to one of our sites, the network tells us through its Vendor API, and we tell
// it what the dock received. Everything is in OUR vocabulary: the network's node
// id never appears (a SiteId does), a quantity is always eaches, and a GTIN is
// the 14-digit string.

// CartonKind is how a carton is packed.
type CartonKind string

const (
	// CartonHomogeneousCase: one GTIN in a case pack.
	CartonHomogeneousCase CartonKind = "HOMOGENEOUS_CASE"
	// CartonMixed: several GTINs, a logistic unit with no pack GTIN.
	CartonMixed CartonKind = "MIXED"
)

// InboundShipment is one shipment the network says is on its way to a site, as
// the pallet-and-carton tree flattened to cartons (a carton on a pallet names the
// pallet's SSCC as ParentSSCC).
type InboundShipment struct {
	// Ref is the network's identifier of the shipment: opaque, stable, the key
	// of every record this context keeps about it.
	Ref string
	// OwnerRef is the seller whose goods these are (the fleet's ownerRef).
	OwnerRef  string
	SiteId    shared.SiteId
	ShippedAt time.Time
	Cartons   []InboundCarton
}

// InboundCarton is one logistic unit of an InboundShipment.
type InboundCarton struct {
	SSCC         string
	ParentSSCC   string
	Kind         CartonKind
	Contents     []InboundContent
	GrossWeightG int64
	Dims         *CartonDims
}

// CartonDims are a carton's declared dimensions in millimetres.
type CartonDims struct {
	LengthMm, WidthMm, HeightMm int64
}

// InboundContent is one GTIN in a carton.
type InboundContent struct {
	GTIN      string
	QtyEaches int64
	Lot       string
	Expiry    string // YYYY-MM-DD, or empty
}

// ReceiptStatus is what the dock found of one carton.
type ReceiptStatus string

const (
	ReceiptReceived   ReceiptStatus = "RECEIVED"
	ReceiptMissing    ReceiptStatus = "MISSING"
	ReceiptUnexpected ReceiptStatus = "UNEXPECTED"
	ReceiptDamaged    ReceiptStatus = "DAMAGED"
)

// InboundReceipt is what the dock received against one InboundShipment.
type InboundReceipt struct {
	ShipmentRef string
	SiteId      shared.SiteId
	ReceivedAt  time.Time
	Cartons     []ReceiptCarton
}

// ReceiptCarton is the finding for one carton.
type ReceiptCarton struct {
	SSCC            string
	Status          ReceiptStatus
	Items           []ReceiptItem
	ComplianceCodes []string
}

// ReceiptItem is the count of one GTIN in a received carton, in eaches.
type ReceiptItem struct {
	GTIN      string
	QtyEaches int64
}
