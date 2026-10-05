package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func (f *fixture) reconcile() *usecases.ReconcileSubmittedOrders {
	return &usecases.ReconcileSubmittedOrders{
		Orders:  f.orders,
		Gateway: f.gateway,
		Planner: f.planner,
		Events:  nopPublisher{},
		Clock:   fixedClock{t: now()},
	}
}

// TestReceive_FeasibleDemandIsSubmittedNotYetReleased is the ADR 0001 §5
// fix pinned: ReceiveNetworkDemand must stop at SUBMITTED and must NOT
// release the held order until a later reconciliation pass actually
// confirms the network's own record of the submission.
func TestReceive_FeasibleDemandIsSubmittedNotYetReleased(t *testing.T) {
	f := newFixture(true)

	o, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED", o.State())
	}

	accepted, submitted := f.gateway.Acknowledgement("po-1")
	if !submitted || !accepted {
		t.Fatalf("acknowledgement submitted=%v accepted=%v, want true/true", submitted, accepted)
	}

	// Release is deferred to reconciliation: nothing may reach the
	// floor until the submission is actually confirmed, not merely
	// accepted-for-processing.
	want := []string{"raise"}
	if got := f.planner.calls; !equal(got, want) {
		t.Fatalf("planner calls = %v, want %v (no release before reconciliation)", got, want)
	}
}

// TestReconcile_SuccessConfirmsAndReleases drives the full two-phase path:
// Execute leaves SUBMITTED, a reconciliation pass that sees SUCCESS
// settles the order and only THEN releases the hold.
func TestReconcile_SuccessConfirmsAndReleases(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.planner.calls = nil

	res, err := f.reconcile().Execute(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Examined != 1 || res.Confirmed != 1 {
		t.Fatalf("result = %+v, want Examined=1 Confirmed=1", res)
	}

	o, _ := f.orders.FindByRef(context.Background(), "po-1")
	if o == nil || o.State() != networkorder.StateAcknowledged {
		t.Fatalf("order state = %v, want ACKNOWLEDGED", o)
	}
	if got := f.planner.calls; !equal(got, []string{"release"}) {
		t.Fatalf("planner calls = %v, want [release]", got)
	}
}

// fakeReconcileGateway wraps the stub gateway's PollDemand/Submit* calls
// (delegated through embedding) but forces a fixed SubmissionStatus
// answer, so the failure path can be driven without depending on the
// stub's own always-succeeds reconciliation.
type fakeReconcileGateway struct {
	*network.StubGateway
	status contract.SubmissionStatusValue
}

func (g *fakeReconcileGateway) SubmissionStatus(context.Context, shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	return g.status, nil
}

func TestReconcile_FailureRejectsAndCancelsTheHold(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.planner.calls = nil

	rc := f.reconcile()
	rc.Gateway = &fakeReconcileGateway{StubGateway: f.gateway, status: contract.SubmissionFailure}

	res, err := rc.Execute(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Examined != 1 || res.Failed != 1 {
		t.Fatalf("result = %+v, want Examined=1 Failed=1", res)
	}

	o, _ := f.orders.FindByRef(context.Background(), "po-1")
	if o == nil || o.State() != networkorder.StateRejected {
		t.Fatalf("order state = %v, want REJECTED", o)
	}
	// Cancel must happen, freeing the hold the network itself refused.
	if got := f.planner.calls; !equal(got, []string{"cancel"}) {
		t.Fatalf("planner calls = %v, want [cancel]", got)
	}
}

func TestReconcile_PendingLeavesTheOrderAlone(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.planner.calls = nil

	rc := f.reconcile()
	rc.Gateway = &fakeReconcileGateway{StubGateway: f.gateway, status: contract.SubmissionPending}

	res, err := rc.Execute(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Examined != 1 || res.Pending != 1 {
		t.Fatalf("result = %+v, want Examined=1 Pending=1", res)
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none while pending", f.planner.calls)
	}

	after, _ := f.orders.FindByRef(context.Background(), "po-1")
	if after.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want still SUBMITTED", after.State())
	}
}
