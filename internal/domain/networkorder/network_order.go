package networkorder

import (
	"errors"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// State is the NetworkOrder lifecycle. It is deliberately small: this
// context tracks only what it must in order to honour the network's
// protocol, and delegates everything about the WORK to order-management
// and the contexts downstream of it.
//
//	NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED
//	    -> REJECTED (from NEW or SUBMITTED)
type State string

const (
	// StateNew is demand received from the network and not yet answered.
	// The acknowledgement clock is already running.
	StateNew State = "NEW"

	// StateSubmitted means we decided to accept the order in full and
	// told the network so, but that submission has not yet been
	// reconciled against the network's own transaction-status record
	// (ADR 0001 §5: outbound submissions are asynchronous, and a 200 on
	// the original call is accepted-for-processing, never itself a
	// completed commitment). Reached via Submit(); resolved to
	// ACKNOWLEDGED by ConfirmAcknowledgement() once reconciliation
	// reports success, or back to REJECTED via Reject() if it reports
	// failure. A ReceiveNetworkDemand caller that has no real
	// asynchronous submission to reconcile may use Acknowledge(), which
	// performs both steps at once.
	StateSubmitted State = "SUBMITTED"

	// StateAcknowledged means the network has confirmed it received and
	// accepted our acknowledgement of this order in full. It is a
	// settled commitment, not an intention or an in-flight submission.
	StateAcknowledged State = "ACKNOWLEDGED"

	// StateRejected means we told the network we will not fulfil it.
	// Terminal, and just as valid an answer as acknowledgement — a
	// deadline we cannot meet is better refused inside the window than
	// missed after it.
	StateRejected State = "REJECTED"

	// StateConfirmed means shipment has been confirmed to the network.
	// Terminal.
	StateConfirmed State = "CONFIRMED"
)

var (
	// ErrAlreadyAnswered rejects a second acknowledgement or rejection.
	// The network's protocol allows exactly one answer per order, and a
	// duplicate is a bug in us, not a retry: a retry of a LOST
	// acknowledgement is handled by the transaction-status
	// reconciliation (ADR 0001 §5), not by re-deciding.
	ErrAlreadyAnswered = errors.New("network order has already been acknowledged or rejected")

	// ErrConfirmBeforeAcknowledge enforces the network's ordering: a
	// shipment may not be confirmed for an order we never committed to.
	ErrConfirmBeforeAcknowledge = errors.New("cannot confirm shipment before acknowledgement")

	// ErrNotAcknowledged rejects attaching a local order to an order we
	// have not at least submitted an answer for. Raising local work for
	// demand we have not committed to is the mirror of the hold that
	// order-management ADR 0020 introduced, and the same failure it
	// prevents.
	ErrNotAcknowledged = errors.New("network order is not acknowledged")

	// ErrNotSubmitted rejects confirming an acknowledgement that was
	// never submitted: reconciliation has nothing to confirm for an
	// order still NEW, already settled, or already rejected.
	ErrNotSubmitted = errors.New("network order has not been submitted")

	// ErrLocalOrderAlreadyLinked rejects re-linking. The mapping is
	// one-to-one and permanent; overwriting it would orphan real
	// allocated work in order-management with nothing pointing at it.
	ErrLocalOrderAlreadyLinked = errors.New("network order already has a local order")
)

// Line is one line of network demand, already translated into OUR
// product vocabulary. The NetworkProductId is retained alongside the SKU
// because acknowledgements and shipment confirmations must be addressed
// back to the network in ITS terms — dropping it would make the outbound
// leg impossible without a reverse lookup.
type Line struct {
	networkLineRef   shared.NetworkLineRef
	networkProductId shared.NetworkProductId
	sku              shared.SKU
	quantity         int
}

func NewLine(ref shared.NetworkLineRef, productId shared.NetworkProductId, sku shared.SKU, quantity int) (Line, error) {
	if ref == "" {
		return Line{}, shared.ErrEmptyNetworkLineRef
	}
	if productId == "" {
		return Line{}, shared.ErrEmptyNetworkProductId
	}
	if sku == "" {
		return Line{}, shared.ErrUnknownProduct
	}
	if quantity <= 0 {
		return Line{}, shared.ErrNonPositiveQuantity
	}
	return Line{networkLineRef: ref, networkProductId: productId, sku: sku, quantity: quantity}, nil
}

func (l Line) NetworkLineRef() shared.NetworkLineRef     { return l.networkLineRef }
func (l Line) NetworkProductId() shared.NetworkProductId { return l.networkProductId }
func (l Line) SKU() shared.SKU                           { return l.sku }
func (l Line) Quantity() int                             { return l.quantity }

// NetworkOrder is demand that arrived from an external retail network,
// carrying a deadline we did not choose.
//
// The aggregate owns the network PROTOCOL — the answer, its deadline,
// and the mapping to local work. It owns none of the fulfillment: no
// allocation, no promise computation, no release. Those belong to
// order-management, and asking them of this aggregate would be the exact
// duplication ADR 0001 §7 forbids.
type NetworkOrder struct {
	networkRef     shared.NetworkRef
	siteId         shared.SiteId
	requiredShipBy time.Time
	acknowledgeBy  time.Time
	lines          []Line
	state          State
	localOrderId   *shared.LocalOrderId
	receivedAt     time.Time
}

// AcknowledgementWindow is how long the network gives us to answer. ADR
// 0001 §6 fixes it at 24h, matching Amazon Vendor Direct Fulfillment's
// published SLA. It is a domain constant rather than configuration
// because it is a fact about the counterparty's protocol, not a knob we
// may turn.
const AcknowledgementWindow = 24 * time.Hour

// Receive builds a NetworkOrder from demand the network sent us.
//
// receivedAt is passed in rather than read from a clock so the
// acknowledgement deadline is derived from when the demand actually
// ARRIVED, not from when this happens to be constructed — a distinction
// that matters when a poll returns a batch after an outage, where every
// order in it may already be near or past its window.
func Receive(
	ref shared.NetworkRef,
	siteId shared.SiteId,
	requiredShipBy time.Time,
	lines []Line,
	receivedAt time.Time,
) (*NetworkOrder, error) {
	if ref == "" {
		return nil, shared.ErrEmptyNetworkRef
	}
	if len(lines) == 0 {
		return nil, shared.ErrNoLines
	}
	return &NetworkOrder{
		networkRef:     ref,
		siteId:         siteId,
		requiredShipBy: requiredShipBy,
		acknowledgeBy:  receivedAt.Add(AcknowledgementWindow),
		lines:          lines,
		state:          StateNew,
		receivedAt:     receivedAt,
	}, nil
}

// ReceiveUntranslatable builds a NetworkOrder for demand we cannot
// translate into our own product vocabulary.
//
// Such an order exists ONLY to be rejected and recorded: "the network
// asked for something we do not stock, and here is when we refused it"
// is a fact this context owns, and dropping the demand silently would
// leave the network waiting on an answer that never comes.
//
// It is deliberately a separate constructor rather than Receive with
// empty lines. Receive's ErrNoLines invariant is real and protects every
// other path; the one legitimate lineless order is this one, and naming
// it makes that exception visible instead of weakening the rule for
// everybody.
func ReceiveUntranslatable(
	ref shared.NetworkRef,
	siteId shared.SiteId,
	requiredShipBy time.Time,
	receivedAt time.Time,
) (*NetworkOrder, error) {
	if ref == "" {
		return nil, shared.ErrEmptyNetworkRef
	}
	return &NetworkOrder{
		networkRef:     ref,
		siteId:         siteId,
		requiredShipBy: requiredShipBy,
		acknowledgeBy:  receivedAt.Add(AcknowledgementWindow),
		state:          StateNew,
		receivedAt:     receivedAt,
	}, nil
}

// Submit records our DECISION to accept the order in full and tells the
// network — a commitment-in-flight, not yet a settled one. ADR 0001 §5
// models outbound submissions as pending until a later transaction-status
// reconciliation confirms them; call ConfirmAcknowledgement once that
// reconciliation reports success, or Reject if it reports failure.
func (o *NetworkOrder) Submit() error {
	if o.state != StateNew {
		return ErrAlreadyAnswered
	}
	o.state = StateSubmitted
	return nil
}

// ConfirmAcknowledgement settles a submitted order once the network's own
// transaction-status record confirms it (ReconcileSubmittedOrders). It is
// the only route from SUBMITTED to ACKNOWLEDGED — the network's protocol
// has no partial acknowledgement, which is why there is no quantity
// argument here, same as Submit.
func (o *NetworkOrder) ConfirmAcknowledgement() error {
	if o.state != StateSubmitted {
		return ErrNotSubmitted
	}
	o.state = StateAcknowledged
	return nil
}

// Acknowledge commits us to fulfilling this order IN FULL in one step,
// skipping the SUBMITTED interim state. It exists for callers with no
// real asynchronous submission to reconcile (tests, and fixtures seeding
// an already-settled order); ReceiveNetworkDemand itself calls Submit and
// ConfirmAcknowledgement separately so a real reconciliation pass can
// land between them (ADR 0001 §5).
func (o *NetworkOrder) Acknowledge() error {
	if err := o.Submit(); err != nil {
		return err
	}
	return o.ConfirmAcknowledgement()
}

// Reject refuses the order. Called when the deadline is infeasible, a
// product cannot be translated, the acknowledgement window is about to
// close with no answer, or a submitted acknowledgement's reconciliation
// reports failure — a refusal is a far better outcome for both parties
// than silence.
func (o *NetworkOrder) Reject() error {
	if o.state != StateNew && o.state != StateSubmitted {
		return ErrAlreadyAnswered
	}
	o.state = StateRejected
	return nil
}

// LinkLocalOrder records order-management's OrderId for the work raised
// from this demand. Valid once we have at least SUBMITTED an answer:
// local work must never exist for demand we have not committed to, but it
// must be linkable before reconciliation settles the submission, because
// ReconcileSubmittedOrders needs it to release or cancel the hold.
func (o *NetworkOrder) LinkLocalOrder(id shared.LocalOrderId) error {
	if o.state != StateSubmitted && o.state != StateAcknowledged {
		return ErrNotAcknowledged
	}
	if o.localOrderId != nil {
		return ErrLocalOrderAlreadyLinked
	}
	o.localOrderId = &id
	return nil
}

// ConfirmShipment records that the shipment has been confirmed to the
// network, closing the order.
func (o *NetworkOrder) ConfirmShipment() error {
	if o.state != StateAcknowledged {
		return ErrConfirmBeforeAcknowledge
	}
	o.state = StateConfirmed
	return nil
}

// AcknowledgementOverdue reports whether the window has closed with the
// order still unanswered. The sweep (ADR 0001 §6) uses this; a true here
// is a REPORTED FACT about an SLA we missed, not an error to swallow.
func (o *NetworkOrder) AcknowledgementOverdue(now time.Time) bool {
	return o.state == StateNew && now.After(o.acknowledgeBy)
}

// SKUQuantities projects the lines into the only vocabulary
// order-management understands: our SKUs and quantities. No network
// identifier crosses this method's return type — it is the narrow
// doorway ADR 0001 §2 describes, and the reason the Amazon vocabulary
// cannot leak inward.
func (o *NetworkOrder) SKUQuantities() map[shared.SKU]int {
	out := make(map[shared.SKU]int, len(o.lines))
	for _, l := range o.lines {
		out[l.sku] += l.quantity
	}
	return out
}

func (o *NetworkOrder) NetworkRef() shared.NetworkRef { return o.networkRef }
func (o *NetworkOrder) SiteId() shared.SiteId         { return o.siteId }
func (o *NetworkOrder) RequiredShipBy() time.Time     { return o.requiredShipBy }
func (o *NetworkOrder) AcknowledgeBy() time.Time      { return o.acknowledgeBy }
func (o *NetworkOrder) State() State                  { return o.state }
func (o *NetworkOrder) ReceivedAt() time.Time         { return o.receivedAt }
func (o *NetworkOrder) LocalOrderId() *shared.LocalOrderId {
	return o.localOrderId
}

// Lines returns a COPY. The slice header would otherwise let a caller
// outside the aggregate append to or mutate the line set, which is the
// quietest way an aggregate boundary gets breached in Go.
func (o *NetworkOrder) Lines() []Line {
	out := make([]Line, len(o.lines))
	copy(out, o.lines)
	return out
}

// Rehydrate rebuilds a persisted NetworkOrder. Only a repository should
// call it: it bypasses every invariant Receive enforces, because the
// stored row already satisfied them when it was written.
func Rehydrate(
	ref shared.NetworkRef,
	siteId shared.SiteId,
	requiredShipBy, acknowledgeBy, receivedAt time.Time,
	lines []Line,
	state State,
	localOrderId *shared.LocalOrderId,
) *NetworkOrder {
	return &NetworkOrder{
		networkRef:     ref,
		siteId:         siteId,
		requiredShipBy: requiredShipBy,
		acknowledgeBy:  acknowledgeBy,
		receivedAt:     receivedAt,
		lines:          lines,
		state:          state,
		localOrderId:   localOrderId,
	}
}
