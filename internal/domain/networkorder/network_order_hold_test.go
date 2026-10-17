package networkorder

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// holdOrder builds an order in state with an optional recorded hold and
// outcome, the way a repository would rehydrate it.
func holdOrder(t *testing.T, state State, id string, outcome HoldOutcome) *NetworkOrder {
	t.Helper()
	var local *shared.LocalOrderId
	if id != "" {
		l := shared.LocalOrderId(id)
		local = &l
	}
	o := Rehydrate("po-1", "site-1", testNow().Add(48*time.Hour), testNow().Add(AcknowledgementWindow), testNow(),
		[]Line{mustLine(t)}, state, local)
	if outcome != "" {
		o.RestoreHold(outcome, testNow().Add(time.Hour))
	}
	return o
}

func TestRecordHold_OnlyWhileNewAndOnlyOnce(t *testing.T) {
	o := mustReceive(t)
	if err := o.RecordHold("ord-1"); err != nil {
		t.Fatalf("RecordHold on a NEW order: %v", err)
	}
	if got := o.LocalOrderId(); got == nil || *got != "ord-1" {
		t.Fatalf("localOrderId = %v, want ord-1", got)
	}
	if !o.HoldOpen() {
		t.Fatal("a recorded hold with no outcome must read as open")
	}

	// The raise is replayed by the poller after a crash: the same id again is a no-op.
	if err := o.RecordHold("ord-1"); err != nil {
		t.Fatalf("recording the same hold twice must be idempotent, got %v", err)
	}
	// A different id would orphan the first hold with nothing pointing at it.
	if err := o.RecordHold("ord-2"); !errors.Is(err, ErrLocalOrderAlreadyLinked) {
		t.Fatalf("a second, different hold: err = %v, want ErrLocalOrderAlreadyLinked", err)
	}
}

func TestRecordHold_RefusesAnAnsweredOrder(t *testing.T) {
	o := mustReceive(t)
	if err := o.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := o.RecordHold("ord-1"); !errors.Is(err, ErrAlreadyAnswered) {
		t.Fatalf("RecordHold on SUBMITTED: err = %v, want ErrAlreadyAnswered", err)
	}
}

func TestLinkLocalOrder_IsIdempotentForTheHoldAlreadyRecorded(t *testing.T) {
	o := mustReceive(t)
	if err := o.RecordHold("ord-1"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	if err := o.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := o.LinkLocalOrder("ord-1"); err != nil {
		t.Fatalf("linking the hold already recorded must succeed, got %v", err)
	}
	if err := o.LinkLocalOrder("ord-2"); !errors.Is(err, ErrLocalOrderAlreadyLinked) {
		t.Fatalf("linking another order: err = %v, want ErrLocalOrderAlreadyLinked", err)
	}
}

func TestSettleHold_RecordsTheOutcomeOnce(t *testing.T) {
	at := testNow().Add(26 * time.Hour)

	noHold := mustReceive(t)
	if err := noHold.SettleHold(HoldCancelled, at); !errors.Is(err, ErrNoHold) {
		t.Fatalf("settling an order with no hold: err = %v, want ErrNoHold", err)
	}

	o := holdOrder(t, StateNew, "ord-1", "")
	if err := o.SettleHold(HoldOutcome("BOGUS"), at); !errors.Is(err, ErrUnknownHoldOutcome) {
		t.Fatalf("unknown outcome: err = %v, want ErrUnknownHoldOutcome", err)
	}
	if o.HoldOutcome() != "" {
		t.Fatal("a refused settle must leave the hold open")
	}
	if err := o.SettleHold(HoldCancelled, at); err != nil {
		t.Fatalf("SettleHold: %v", err)
	}
	if o.HoldOutcome() != HoldCancelled || !o.HoldSettledAt().Equal(at) {
		t.Fatalf("outcome/at = %q/%v, want CANCELLED/%v", o.HoldOutcome(), o.HoldSettledAt(), at)
	}
	if o.HoldOpen() {
		t.Fatal("a settled hold must not read as open")
	}
	if err := o.SettleHold(HoldAlreadyReleased, at); !errors.Is(err, ErrHoldAlreadySettled) {
		t.Fatalf("a second settle: err = %v, want ErrHoldAlreadySettled", err)
	}
	if o.HoldOutcome() != HoldCancelled {
		t.Fatal("a refused second settle must not overwrite the first outcome")
	}
}

func TestParseHoldOutcome(t *testing.T) {
	for _, ok := range []HoldOutcome{HoldCancelled, HoldAlreadyReleased} {
		got, err := ParseHoldOutcome(string(ok))
		if err != nil || got != ok {
			t.Fatalf("ParseHoldOutcome(%q) = %q, %v", ok, got, err)
		}
	}
	for _, bad := range []string{"", "cancelled", " CANCELLED", "RELEASED"} {
		if _, err := ParseHoldOutcome(bad); !errors.Is(err, ErrUnknownHoldOutcome) {
			t.Fatalf("ParseHoldOutcome(%q): err = %v, want ErrUnknownHoldOutcome", bad, err)
		}
	}
}

// TestHoldOrphaned pins the orphan predicate of ADR 0024 §3: the hold is
// recorded, has no outcome, the order is NEW or REJECTED, and now is
// strictly past acknowledgeBy + grace. SUBMITTED, ACKNOWLEDGED and
// CONFIRMED are never orphaned: NF committed to them or is waiting for
// the network's verdict.
func TestHoldOrphaned(t *testing.T) {
	ackBy := testNow().Add(AcknowledgementWindow)
	grace := time.Hour
	past := ackBy.Add(grace).Add(time.Nanosecond)
	cases := []struct {
		name    string
		state   State
		id      string
		outcome HoldOutcome
		now     time.Time
		want    bool
	}{
		{"NEW, hold open, past window+grace", StateNew, "ord-1", "", past, true},
		{"REJECTED, hold open, past window+grace", StateRejected, "ord-1", "", past, true},
		{"exactly at window+grace is not past it", StateNew, "ord-1", "", ackBy.Add(grace), false},
		{"inside the grace", StateNew, "ord-1", "", ackBy.Add(grace / 2), false},
		{"inside the window: still being decided", StateNew, "ord-1", "", ackBy.Add(-time.Minute), false},
		{"no hold recorded: nothing to find", StateNew, "", "", past, false},
		{"hold already cancelled", StateNew, "ord-1", HoldCancelled, past, false},
		{"hold already found released", StateRejected, "ord-1", HoldAlreadyReleased, past, false},
		{"SUBMITTED is awaiting the network", StateSubmitted, "ord-1", "", past, false},
		{"ACKNOWLEDGED is committed", StateAcknowledged, "ord-1", "", past, false},
		{"CONFIRMED is committed", StateConfirmed, "ord-1", "", past, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := holdOrder(t, tc.state, tc.id, tc.outcome)
			if got := o.HoldOrphaned(tc.now, grace); got != tc.want {
				t.Fatalf("HoldOrphaned = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHoldOrphaned_ZeroGraceMeansTheWindowAlone(t *testing.T) {
	o := holdOrder(t, StateNew, "ord-1", "")
	ackBy := o.AcknowledgeBy()
	if o.HoldOrphaned(ackBy, 0) {
		t.Fatal("exactly at acknowledgeBy is not past the window")
	}
	if !o.HoldOrphaned(ackBy.Add(time.Nanosecond), 0) {
		t.Fatal("one instant past acknowledgeBy with no grace must be orphaned")
	}
}

func TestRestoreHold_RoundTrips(t *testing.T) {
	at := testNow().Add(30 * time.Hour)
	o := holdOrder(t, StateRejected, "ord-1", "")
	o.RestoreHold(HoldAlreadyReleased, at)
	if o.HoldOutcome() != HoldAlreadyReleased || !o.HoldSettledAt().Equal(at) {
		t.Fatalf("outcome/at = %q/%v, want ALREADY_RELEASED/%v", o.HoldOutcome(), o.HoldSettledAt(), at)
	}
}
