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

func (f *fixture) deferShipment() *usecases.DeferShipmentConfirmation {
	return &usecases.DeferShipmentConfirmation{Orders: f.orders, Clock: fixedClock{t: now()}}
}

// submittedOrder seeds one order through the real ReceiveNetworkDemand and
// leaves it SUBMITTED: the network has not settled the acknowledgement yet,
// which is exactly the window a load can depart in.
func submittedOrder(t *testing.T, f *fixture, ref shared.NetworkRef) *networkorder.NetworkOrder {
	t.Helper()
	if _, err := f.receive().Execute(context.Background(), demand(ref, "ASIN-1")); err != nil {
		t.Fatalf("seed receive: %v", err)
	}
	o, err := f.orders.FindByRef(context.Background(), ref)
	if err != nil || o == nil {
		t.Fatalf("seed find: %v %v", o, err)
	}
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("seed state = %v, want SUBMITTED", o.State())
	}
	return o
}

func TestDeferShipmentConfirmation_RecordsTheShipmentOnASubmittedOrder(t *testing.T) {
	f := newFixture(true)
	submittedOrder(t, f, "po-1")

	if err := f.deferShipment().Execute(context.Background(), "po-1", "load-7", 3); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	o, _ := f.orders.FindByRef(context.Background(), "po-1")
	got, ok := o.PendingShipment()
	if !ok || got.ShipmentRef != "load-7" || got.PackageCount != 3 || !got.NotedAt.Equal(now()) {
		t.Fatalf("pending = %+v ok=%v, want {load-7 3 %v}", got, ok, now())
	}
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v: deferring must leave the order SUBMITTED", o.State())
	}
}

func TestDeferShipmentConfirmation_ASecondEarlyEventForTheSameLoadIsANoOp(t *testing.T) {
	f := newFixture(true)
	o := submittedOrder(t, f, "po-1")
	uc := f.deferShipment()
	ctx := context.Background()

	if err := uc.Execute(ctx, "po-1", "load-7", 2); err != nil {
		t.Fatalf("first: %v", err)
	}
	versionAfterFirst := o.Version()
	if err := uc.Execute(ctx, "po-1", "load-7", 9); err != nil {
		t.Fatalf("a redelivered early event must succeed, got %v", err)
	}
	// A different load while one is pending: the first wins, still a success.
	if err := uc.Execute(ctx, "po-1", "load-8", 4); err != nil {
		t.Fatalf("a second load while one is pending must succeed, got %v", err)
	}

	got, _ := o.PendingShipment()
	if got.ShipmentRef != "load-7" || got.PackageCount != 2 {
		t.Fatalf("pending = %+v, want the first shipment {load-7 2}", got)
	}
	if o.Version() != versionAfterFirst {
		t.Fatalf("version %d -> %d: a no-op must not write the row again", versionAfterFirst, o.Version())
	}
}

func TestDeferShipmentConfirmation_EveryOtherStateKeepsTheConfirmBeforeAcknowledgeError(t *testing.T) {
	ctx := context.Background()
	for _, state := range []networkorder.State{
		networkorder.StateNew, networkorder.StateAcknowledged, networkorder.StateRejected, networkorder.StateConfirmed,
	} {
		f := newFixture(true)
		local := shared.LocalOrderId("ord-1")
		o := networkorder.Rehydrate("po-1", "site-1", now().Add(48*time.Hour), now().Add(networkorder.AcknowledgementWindow), now(),
			[]networkorder.Line{mustLine(t)}, state, &local)
		if err := f.orders.Save(ctx, o); err != nil {
			t.Fatalf("%s: seed: %v", state, err)
		}

		err := f.deferShipment().Execute(ctx, "po-1", "load-7", 1)
		if !errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge) {
			t.Errorf("%s: err = %v, want ErrConfirmBeforeAcknowledge (the retry-then-DLQ path stays)", state, err)
		}
		if _, ok := o.PendingShipment(); ok {
			t.Errorf("%s: nothing may be recorded", state)
		}
	}
}

func TestDeferShipmentConfirmation_UnknownOrderAndRepoErrors(t *testing.T) {
	f := newFixture(true)
	if err := f.deferShipment().Execute(context.Background(), "nope", "load-7", 1); !errors.Is(err, usecases.ErrOrderNotFound) {
		t.Fatalf("unknown order: err = %v, want ErrOrderNotFound", err)
	}

	failing := f.deferShipment()
	failing.Orders = &failingRepo{findErr: errBoom}
	if err := failing.Execute(context.Background(), "po-1", "load-7", 1); !errors.Is(err, errBoom) {
		t.Fatalf("find error: err = %v, want errBoom", err)
	}
}

// A writer that won the row between our load and our save (the reconcile pass
// settling the order, a second early event) must not lose the fact: the whole
// load-note-save cycle reruns on the fresh row.
func TestDeferShipmentConfirmation_RetriesOnVersionConflict(t *testing.T) {
	f := newFixture(true)
	submittedOrder(t, f, "po-1")
	repo := &rowSimulatingRepo{NetworkOrderRepo: f.orders, ref: "po-1", conflicts: 1}
	uc := f.deferShipment()
	uc.Orders = repo

	if err := uc.Execute(context.Background(), "po-1", "load-7", 2); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if repo.saves != 2 {
		t.Fatalf("saves = %d, want 2 (one refused, one landed)", repo.saves)
	}
	o, _ := f.orders.FindByRef(context.Background(), "po-1")
	if got, ok := o.PendingShipment(); !ok || got.ShipmentRef != "load-7" {
		t.Fatalf("pending = %+v ok=%v, want load-7 recorded by the retry", got, ok)
	}
}
