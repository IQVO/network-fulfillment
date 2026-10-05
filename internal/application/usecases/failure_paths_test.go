package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Failure paths. These matter more than the happy path: every one of
// them decides whether inventory stays reserved for demand nobody is
// going to fulfil.

type failingRepo struct {
	saveErr error
	listErr error
	findErr error
}

func (r *failingRepo) Save(context.Context, *networkorder.NetworkOrder) error { return r.saveErr }
func (r *failingRepo) FindByRef(context.Context, shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	return nil, r.findErr
}
func (r *failingRepo) ListUnanswered(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, r.listErr
}
func (r *failingRepo) ListSubmitted(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, r.listErr
}
func (r *failingRepo) ListAll(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, r.listErr
}

func TestReceive_RepositoryLookupFailureAborts(t *testing.T) {
	f := newFixture(true)
	uc := f.receive()
	uc.Orders = &failingRepo{findErr: errBoom}

	// Aborting is correct: if we cannot tell whether we already answered
	// this order, answering again risks a duplicate acknowledgement.
	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}
}

func TestReceive_SaveFailureLeavesNoAcknowledgementOnTheNetwork(t *testing.T) {
	f := newFixture(true)
	uc := f.receive()
	uc.Orders = &failingRepo{saveErr: errBoom}

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}

	// The ordering rule, asserted: we save BEFORE telling the network,
	// so a crash leaves a recoverable record rather than an
	// acknowledgement the network believes and we have no trace of.
	if _, submitted := f.gateway.Acknowledgement("po-1"); submitted {
		t.Fatal("no acknowledgement may reach the network when the save failed")
	}
	// And nothing was released onto the floor.
	for _, c := range f.planner.calls {
		if c == "release" {
			t.Fatal("nothing may be released when the save failed")
		}
	}
}

func TestReconcile_ReleaseFailureIsReportedButOrderStaysAcknowledged(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.planner.releaseErr = errBoom

	rc := &usecases.ReconcileSubmittedOrders{
		Orders: f.orders, Gateway: f.gateway, Planner: f.planner,
		Events: nopPublisher{}, Clock: fixedClock{t: now()},
	}
	if _, err := rc.Execute(context.Background()); err != nil {
		t.Fatalf("reconcile must not abort the whole pass on one release failure: %v", err)
	}

	// The acknowledgement is already settled and the record is saved, so
	// this is recoverable: the order is ACKNOWLEDGED with a local order
	// that is still held, and a retry (the next reconciliation pass) can
	// release it. What must NOT happen is the failure being swallowed
	// silently or the order being left SUBMITTED.
	o, _ := f.orders.FindByRef(context.Background(), "po-1")
	if o == nil || o.State() != networkorder.StateAcknowledged {
		t.Fatalf("order state = %v, want ACKNOWLEDGED and persisted", o)
	}
}

func TestReceive_CancelFailureOnRejectionAborts(t *testing.T) {
	f := newFixture(false) // infeasible -> rejection path
	f.planner.cancelErr = errBoom

	_, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1"))
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}

	// Surfacing the error is the whole point: the hold is still in
	// place, so silently telling the network "rejected" would strand
	// that inventory with nothing left to trigger a cleanup.
	if _, submitted := f.gateway.Acknowledgement("po-1"); submitted {
		t.Fatal("no rejection may be submitted while the hold is still in place")
	}
}

func TestRejectOverdue_RepositoryFailureIsReported(t *testing.T) {
	f := newFixture(true)
	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders: &failingRepo{listErr: errBoom}, Planner: f.planner,
		Events: nopPublisher{}, Clock: fixedClock{t: now()},
	}
	if _, err := rejectOverdue.Execute(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
}

func TestRejectOverdue_OneFailingOrderDoesNotAbortThePass(t *testing.T) {
	f := newFixture(true)
	f.planner.cancelErr = errBoom

	// Two overdue orders, both holding inventory. The cancel fails for
	// both, but the pass must still EXAMINE both rather than stopping at
	// the first — the remaining orders are exactly what this use case
	// exists to free.
	for _, ref := range []shared.NetworkRef{"po-a", "po-b"} {
		local := shared.LocalOrderId("ord-" + string(ref))
		stuck := networkorder.Rehydrate(ref, "site-1", now().Add(48*time.Hour),
			now().Add(-time.Hour), now().Add(-25*time.Hour),
			[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
		if err := f.orders.Save(context.Background(), stuck); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders: f.orders, Planner: f.planner, Events: nopPublisher{},
		Clock: fixedClock{t: now()},
	}
	res, err := rejectOverdue.Execute(context.Background())
	if err != nil {
		t.Fatalf("rejectOverdue must not abort on a single failing order: %v", err)
	}
	if res.Examined != 2 {
		t.Fatalf("examined = %d, want 2", res.Examined)
	}
	if res.Rejected != 0 {
		t.Fatalf("rejected = %d, want 0 — nothing was successfully rejected", res.Rejected)
	}
	if len(f.planner.calls) != 2 {
		t.Fatalf("planner calls = %v, want one cancel attempt per overdue order", f.planner.calls)
	}
}

func TestReceiveUntranslatable_RejectsEmptyRef(t *testing.T) {
	if _, err := networkorder.ReceiveUntranslatable("", "site-1", now(), now()); !errors.Is(err, shared.ErrEmptyNetworkRef) {
		t.Fatalf("err = %v, want ErrEmptyNetworkRef", err)
	}
}
