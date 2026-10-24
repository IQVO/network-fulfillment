package retailnetwork

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Store is the adapter's persistence seam (ADR 0026 Decision 4): the maps that
// let the fleet's identifiers and the network's identifiers meet, and the
// submission ledger that makes every write idempotent across a crash. It lives
// here, not in ports, because everything in it is the network's vocabulary
// (purchase order numbers, listings, node ids, transaction ids) that must not
// leave this package. The Postgres implementation is PGStore.
type Store interface {
	// NodeIDForSite returns the node registered for a site code.
	NodeIDForSite(ctx context.Context, siteCode string) (nodeID string, ok bool, err error)
	// SiteForNode is the reverse lookup used on every polled purchase order.
	SiteForNode(ctx context.Context, nodeID string) (siteCode string, ok bool, err error)
	// SaveNode records siteCode <-> nodeID. Idempotent; a different node for
	// an already-mapped site, or a different site for a mapped node, is an
	// error (ErrMappingConflict), never an overwrite.
	SaveNode(ctx context.Context, siteCode, nodeID string) error

	// SavePurchaseOrder records a purchase order the first time it is seen and
	// returns the NetworkRef minted for it; a purchase order seen again (the
	// network re-delivers) returns the SAME ref and changes nothing.
	SavePurchaseOrder(ctx context.Context, po PurchaseOrder) (shared.NetworkRef, error)
	// PurchaseOrder returns the purchase order recorded under ref.
	PurchaseOrder(ctx context.Context, ref shared.NetworkRef) (PurchaseOrder, bool, error)

	// Submission returns the ledger row of (kind, subject).
	Submission(ctx context.Context, kind SubmissionKind, subject string) (Submission, bool, error)
	// BeginSubmission inserts the ledger row if there is none and returns the
	// stored row: the first body and key written for (kind, subject) are the
	// only ones ever sent, whatever the caller passes the second time.
	BeginSubmission(ctx context.Context, s Submission) (Submission, error)
	// RecordTransaction stores the transaction id the network answered with and
	// moves a SENDING row to PROCESSING. A row that already has a transaction
	// id keeps it.
	RecordTransaction(ctx context.Context, kind SubmissionKind, subject, transactionID string) error
	// RecordOutcome stores a status (and the network's errors) for a row. A
	// terminal row (SUCCESS, FAILURE) is never changed to another status.
	RecordOutcome(ctx context.Context, kind SubmissionKind, subject string, status SubmissionStatus, errs string) error

	// SaveLabels records the labels issued for a purchase order. Idempotent
	// per package number.
	SaveLabels(ctx context.Context, ref shared.NetworkRef, labels []Label) error
	// Labels returns the labels of a purchase order, ordered by package number.
	Labels(ctx context.Context, ref shared.NetworkRef) ([]Label, error)
}

// ErrMappingConflict is returned when a site or node is already mapped to a
// different counterpart.
var ErrMappingConflict = errors.New("retailnetwork: identifier already mapped to a different counterpart")

// SubmissionKind is what a ledger row is about.
type SubmissionKind string

const (
	KindAcknowledgement      SubmissionKind = "ACKNOWLEDGEMENT"
	KindShipmentConfirmation SubmissionKind = "SHIPMENT_CONFIRMATION"
	KindNodeRegistration     SubmissionKind = "NODE_REGISTRATION"
	maxIdempotencyKeyLength                 = 128
)

// SubmissionStatus is the ledger's view of a write.
type SubmissionStatus string

const (
	// StatusSending: the row exists, no transaction id was received yet.
	StatusSending SubmissionStatus = "SENDING"
	// StatusProcessing: the network accepted the write (202) and has not
	// reported it settled.
	StatusProcessing SubmissionStatus = "PROCESSING"
	StatusSuccess    SubmissionStatus = "SUCCESS"
	StatusFailure    SubmissionStatus = "FAILURE"
)

// Terminal reports whether the network will never change its mind.
func (s SubmissionStatus) Terminal() bool { return s == StatusSuccess || s == StatusFailure }

// Submission is one row of the ledger.
type Submission struct {
	Kind           SubmissionKind
	Subject        string // the NetworkRef, or the site code for a node registration
	IdempotencyKey string
	Body           []byte // the exact request body, byte for byte
	TransactionID  string // empty until the 202 was read
	Status         SubmissionStatus
	Errors         string
}

// PurchaseOrder is what the adapter remembers of a purchase order: the
// network's identifiers next to the NetworkRef the fleet knows it by.
type PurchaseOrder struct {
	Ref            shared.NetworkRef // minted by the store on first sight
	PoNumber       string
	VendorID       string
	NodeID         string
	RequiredShipBy time.Time
	AckBy          time.Time
	Lines          []PurchaseOrderLine
}

// PurchaseOrderLine maps one line both ways: the network's line number and
// listing against the opaque references the ACL is given.
type PurchaseOrderLine struct {
	LineNo           int
	NetworkLineRef   shared.NetworkLineRef
	ListingID        string
	NetworkProductID shared.NetworkProductId
	Quantity         int
}

// Label is a label the network issued for one package.
type Label struct {
	PackageNo      int
	LabelRef       string
	TrackingNumber string
	Carrier        string
}
