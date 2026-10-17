package networkorder

import (
	"errors"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The hold is the order-management order this context raised for the demand
// (ADR 0001 §7, held at intake). ADR 0024 records it as soon as it exists and
// gives it an outcome, so a hold nobody decided can be found and freed after
// the acknowledgement window instead of keeping inventory reserved forever.

// HoldOutcome says how an order-management hold ended, as far as THIS context
// has confirmed it. The zero value means the hold is open: raised, and neither
// a cancel nor a found-released has been confirmed.
type HoldOutcome string

const (
	// HoldCancelled: order-management confirmed the hold is gone (a 2xx on
	// the cancel, or a 404 because it no longer exists).
	HoldCancelled HoldOutcome = "CANCELLED"

	// HoldAlreadyReleased: order-management refused the cancel because the
	// order was already released (its BR6). Someone committed it to the
	// floor, so it is not an orphan and is never retried.
	HoldAlreadyReleased HoldOutcome = "ALREADY_RELEASED"
)

var (
	// ErrUnknownHoldOutcome rejects an outcome outside the vocabulary above.
	ErrUnknownHoldOutcome = errors.New("unknown hold outcome")

	// ErrNoHold rejects settling the hold of an order that has none recorded.
	ErrNoHold = errors.New("network order has no order-management hold")

	// ErrHoldAlreadySettled rejects a second outcome: the first one stands.
	ErrHoldAlreadySettled = errors.New("the order-management hold is already settled")
)

// ParseHoldOutcome validates a persisted outcome string. Matching is exact,
// like ParseState: the stored vocabulary is closed.
func ParseHoldOutcome(value string) (HoldOutcome, error) {
	if value == string(HoldCancelled) {
		return HoldCancelled, nil
	}
	if value == string(HoldAlreadyReleased) {
		return HoldAlreadyReleased, nil
	}
	return "", ErrUnknownHoldOutcome
}

// RecordHold stores the order-management order raised for this demand on an
// order that has not been answered yet (NEW). It is idempotent for the same
// id, because order-management replays the same order for a retried raise
// (its Idempotency-Key), and refuses a different id: the first hold would be
// orphaned with nothing pointing at it.
func (o *NetworkOrder) RecordHold(id shared.LocalOrderId) error {
	if o.state != StateNew {
		return ErrAlreadyAnswered
	}
	if o.localOrderId != nil {
		if *o.localOrderId == id {
			return nil
		}
		return ErrLocalOrderAlreadyLinked
	}
	o.localOrderId = &id
	return nil
}

// HoldOpen reports whether a hold is recorded and has no outcome yet.
func (o *NetworkOrder) HoldOpen() bool {
	return o.localOrderId != nil && o.holdOutcome == ""
}

// HoldOutcome is how the hold ended, or "" while it is open.
func (o *NetworkOrder) HoldOutcome() HoldOutcome { return o.holdOutcome }

// HoldSettledAt is when the outcome was recorded, or the zero time while the
// hold is open.
func (o *NetworkOrder) HoldSettledAt() time.Time { return o.holdSettledAt }

// SettleHold records how the hold ended. The first outcome stands.
func (o *NetworkOrder) SettleHold(outcome HoldOutcome, at time.Time) error {
	if o.localOrderId == nil {
		return ErrNoHold
	}
	if _, err := ParseHoldOutcome(string(outcome)); err != nil {
		return err
	}
	if o.holdOutcome != "" {
		return ErrHoldAlreadySettled
	}
	o.holdOutcome = outcome
	o.holdSettledAt = at
	return nil
}

// HoldOrphaned is the orphan predicate of ADR 0024 §3. A hold is orphaned when
// it is recorded, has no outcome, the order is NEW or REJECTED, and now is
// strictly past acknowledgeBy + grace.
//
// NEW before the window closes is still being decided and is never orphaned;
// NEW after it is not being decided any more (an answer after the SLA is worse
// than none); REJECTED is a decision already made. SUBMITTED, ACKNOWLEDGED and
// CONFIRMED are never orphaned: this context committed to them or is waiting
// for the network's verdict, and cancelling would contradict that.
//
// Written as an if chain, not a switch (see the mutation gate's note on case
// expressions).
func (o *NetworkOrder) HoldOrphaned(now time.Time, grace time.Duration) bool {
	if !o.HoldOpen() {
		return false
	}
	if o.state != StateNew && o.state != StateRejected {
		return false
	}
	return now.After(o.acknowledgeBy.Add(grace))
}

// CommittedLocalOrderId is the local order the READ surfaces (REST, MCP) may
// show: the hold of an order this context has committed to (SUBMITTED or
// later, never REJECTED). The id is recorded earlier (NEW) and kept later
// (REJECTED) for the hold sweep, but the published contract says localOrderId
// is absent until acknowledged, so a consumer must not see it for an order
// that was never committed.
func (o *NetworkOrder) CommittedLocalOrderId() *shared.LocalOrderId {
	if o.localOrderId == nil {
		return nil
	}
	if o.state != StateSubmitted && o.state != StateAcknowledged && o.state != StateConfirmed {
		return nil
	}
	id := *o.localOrderId
	return &id
}

// RestoreHold rebuilds a persisted hold outcome. Only a repository should
// call it, like Rehydrate: the stored row already satisfied the invariants.
func (o *NetworkOrder) RestoreHold(outcome HoldOutcome, settledAt time.Time) {
	o.holdOutcome = outcome
	o.holdSettledAt = settledAt
}
