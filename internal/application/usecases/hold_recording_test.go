package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ADR 0024 §1-2: the hold is recorded the moment it exists, and the paths
// that free it record the outcome in the same save as the rejection.

func (f *fixture) storedOrder(t *testing.T, ref shared.NetworkRef) *networkorder.NetworkOrder {
	t.Helper()
	o, err := f.orders.FindByRef(context.Background(), ref)
	if err != nil || o == nil {
		t.Fatalf("FindByRef(%s) = %v, %v", ref, o, err)
	}
	return o
}

// TestReceive_RecordsTheHoldBeforeDeciding_WhenTheCancelOfAnInfeasibleOrderFails:
// the raise made a hold, the cancel failed. Before ADR 0024 the order was
// NEW with no id, so nothing could ever free the hold once the window closed.
func TestReceive_RecordsTheHoldBeforeDeciding_WhenTheCancelOfAnInfeasibleOrderFails(t *testing.T) {
	f := newFixture(false)
	f.planner.cancelErr = errBoom

	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err == nil {
		t.Fatal("a failed cancel must surface so the poller retries")
	}

	o := f.storedOrder(t, "po-1")
	if o.State() != networkorder.StateNew {
		t.Fatalf("state = %v, want NEW (the decision never completed)", o.State())
	}
	if got := o.LocalOrderId(); got == nil || *got != "ord-1" {
		t.Fatalf("localOrderId = %v, want the hold ord-1 recorded at raise", got)
	}
	if !o.HoldOpen() {
		t.Fatal("the hold is still open: its cancel failed")
	}
}

func TestReceive_FeasibleOrderCarriesTheRecordedHoldIntoSubmitted(t *testing.T) {
	f := newFixture(true)

	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	o := f.storedOrder(t, "po-1")
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED", o.State())
	}
	if got := o.LocalOrderId(); got == nil || *got != "ord-1" {
		t.Fatalf("localOrderId = %v, want ord-1", got)
	}
	if !o.HoldOpen() {
		t.Fatal("an accepted order's hold is open: it is released later by reconciliation")
	}
}

func TestReceive_InfeasibleOrderSettlesTheHoldAsCancelled(t *testing.T) {
	f := newFixture(false)

	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	o := f.storedOrder(t, "po-1")
	if o.State() != networkorder.StateRejected {
		t.Fatalf("state = %v, want REJECTED", o.State())
	}
	if o.HoldOutcome() != networkorder.HoldCancelled {
		t.Fatalf("outcome = %q, want CANCELLED recorded with the rejection", o.HoldOutcome())
	}
	if got := o.LocalOrderId(); got == nil || *got != "ord-1" {
		t.Fatalf("a rejected order keeps the id of the hold it freed, got %v", got)
	}
}

func (f *fixture) seedOverdueNew(t *testing.T, ref shared.NetworkRef, id string, outcome networkorder.HoldOutcome) {
	t.Helper()
	local := shared.LocalOrderId(id)
	o := networkorder.Rehydrate(ref, "site-1", now().Add(48*time.Hour), now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
	if outcome != "" {
		o.RestoreHold(outcome, now().Add(-30*time.Minute))
	}
	if err := f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func (f *fixture) rejectOverdue() *usecases.RejectOverdueOrders {
	return &usecases.RejectOverdueOrders{Orders: f.orders, Planner: f.planner, Events: nopPublisher{}, Clock: fixedClock{t: now()}}
}

func TestRejectOverdue_CancelsTheRecordedHoldAndSettlesIt(t *testing.T) {
	f := newFixture(true)
	f.seedOverdueNew(t, "po-old", "ord-held", "")

	res, err := f.rejectOverdue().Execute(context.Background())
	if err != nil || res.Rejected != 1 {
		t.Fatalf("res=%+v err=%v, want Rejected=1", res, err)
	}
	if got := f.planner.calls; !equal(got, []string{"cancel"}) {
		t.Fatalf("planner calls = %v, want [cancel]", got)
	}
	o := f.storedOrder(t, "po-old")
	if o.State() != networkorder.StateRejected || o.HoldOutcome() != networkorder.HoldCancelled {
		t.Fatalf("state/outcome = %v/%q, want REJECTED/CANCELLED", o.State(), o.HoldOutcome())
	}
}

// TestRejectOverdue_SkipsTheCancelWhenTheSweepAlreadySettledTheHold: a 404 or
// 409 on cancel is a failure to RejectOverdueOrders, so the orphaned-hold
// sweep settles the hold; the rejection then goes through without cancelling again.
func TestRejectOverdue_SkipsTheCancelWhenTheHoldIsAlreadySettled(t *testing.T) {
	for _, outcome := range []networkorder.HoldOutcome{networkorder.HoldCancelled, networkorder.HoldAlreadyReleased} {
		t.Run(string(outcome), func(t *testing.T) {
			f := newFixture(true)
			f.seedOverdueNew(t, "po-old", "ord-held", outcome)

			res, err := f.rejectOverdue().Execute(context.Background())
			if err != nil || res.Rejected != 1 {
				t.Fatalf("res=%+v err=%v, want Rejected=1", res, err)
			}
			if len(f.planner.calls) != 0 {
				t.Fatalf("planner calls = %v, want none: the hold is already settled", f.planner.calls)
			}
			if got := f.storedOrder(t, "po-old").HoldOutcome(); got != outcome {
				t.Fatalf("outcome = %q, want %q kept", got, outcome)
			}
		})
	}
}

func TestReconcile_FailureSettlesTheHoldAsCancelled(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rc := f.reconcile()
	rc.Gateway = &fakeReconcileGateway{StubGateway: f.gateway, status: contract.SubmissionFailure}

	if res, err := rc.Execute(context.Background()); err != nil || res.Failed != 1 {
		t.Fatalf("res=%+v err=%v, want Failed=1", res, err)
	}
	o := f.storedOrder(t, "po-1")
	if o.State() != networkorder.StateRejected || o.HoldOutcome() != networkorder.HoldCancelled {
		t.Fatalf("state/outcome = %v/%q, want REJECTED/CANCELLED", o.State(), o.HoldOutcome())
	}
}
