package contract

import (
	"errors"
	"fmt"
)

// The shapes of the fleet side of the inbound loop (ADR 0028): the ASN this
// context registers in inbound-receiving and the GTIN answers it needs from
// product-master to build it. Fleet vocabulary only: SKU, ASN number, eaches.

// ErrGtinNotFound: product-master does not know the GTIN. A business fact, not a
// transport failure: the shipment waits (recorded, retried with backoff) until
// the product is declared. A SKU is never guessed.
var ErrGtinNotFound = errors.New("product-master: GTIN not found")

// GtinResolution is what product-master says a GTIN identifies.
type GtinResolution struct {
	GTIN         string
	SKU          string
	Level        string // EACH, INNER, CASE or PALLET
	UnitsPerPack int
	Active       bool
}

// AsnRegistration is the request to register an ASN with hierarchical cartons.
type AsnRegistration struct {
	AsnNumber   string
	SupplierRef string
	OwnerRef    string
	Lines       []AsnLine
	Cartons     []AsnCarton
}

// AsnLine is the per-SKU roll-up of the cartons, in eaches.
type AsnLine struct {
	LineNo      int
	SKU         string
	ExpectedQty int64
}

// AsnCarton is a carton of the ASN.
type AsnCarton struct {
	SSCC         string
	ParentSSCC   string
	Kind         CartonKind
	Contents     []AsnCartonContent
	GrossWeightG int64
	Dims         *CartonDims
}

// AsnCartonContent is one SKU/GTIN in a carton, tied to its ASN line.
type AsnCartonContent struct {
	LineNo int
	SKU    string
	GTIN   string
	Qty    int64
	Lot    string
	Expiry string
}

// AsnRefusedError is inbound-receiving answering a registration with a problem.
// Retryable is true when waiting can change the answer (an SKU its product
// projection has not caught up with yet); false is a verdict about the data.
type AsnRefusedError struct {
	Problem   string
	Detail    string
	Retryable bool
}

func (e *AsnRefusedError) Error() string {
	return fmt.Sprintf("inbound-receiving refused the ASN: %s: %s", e.Problem, e.Detail)
}

// InboundRecord is the local record of a polled shipment.
type InboundRecord struct {
	Shipment  InboundShipment
	AsnNumber string
	Attempts  int
}

// InboundStatus is where a recorded shipment stands.
type InboundStatus string

const (
	InboundPending    InboundStatus = "PENDING"
	InboundRegistered InboundStatus = "REGISTERED"
	InboundRejected   InboundStatus = "REJECTED"
)
