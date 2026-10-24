package contract

import (
	"errors"
	"time"
)

// ErrReceiptRefused: the network refused the inbound receipt outright (a carton
// that is not on the manifest, a malformed body). Retrying the same receipt can
// never succeed, so the loop parks it and surfaces it at ERROR.
var ErrReceiptRefused = errors.New("the network refused the inbound receipt")

// The facts inbound-receiving publishes about a receipt, as this context keeps
// them (ADR 0028): a local projection keyed by ASN number, only for the ASNs
// this service registered.

// ReceivedContent is what was counted of one GTIN in a received carton.
type ReceivedContent struct {
	GTIN      string
	Qty       int64 // eaches
	Condition string
}

// ReceivedCartonFact is a carton the dock accepted. SSCC is empty for a carton
// that arrived without a readable label (it has only an internal LPN).
type ReceivedCartonFact struct {
	ReceiptID  string
	AsnNumber  string
	SSCC       string
	LPN        string
	Contents   []ReceivedContent
	ReceivedAt time.Time
}

// Carton discrepancy kinds as inbound-receiving names them.
const (
	FindingUnexpectedCarton = "UnexpectedCarton"
	FindingMissingCarton    = "MissingCarton"
)

// CartonFinding is one carton discrepancy raised on a receipt. SSCC is empty
// when the finding is not about a scanned SSCC; for a MissingCarton the manifest
// SSCC is in Expected.
type CartonFinding struct {
	ReceiptID string
	AsnNumber string
	SSCC      string
	Kind      string
	Expected  string
	Actual    string
	At        time.Time
}

// ReceiptClosedFact: receiving ended on the ASN.
type ReceiptClosedFact struct {
	ReceiptID string
	AsnNumber string
	ClosedAt  time.Time
}

// ReceiptFacts is everything known about a closed receipt, ready to be turned
// into the vendor receipt.
type ReceiptFacts struct {
	Shipment  InboundShipment
	ReceiptID string
	ClosedAt  time.Time
	Cartons   []ReceivedCartonFact
	Findings  []CartonFinding
	Attempts  int
}
