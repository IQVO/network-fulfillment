package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// shipFlow is the whole deferred-confirmation chain on the memory adapters: a
// spying gateway to count the network calls, a recording publisher for the
// events, and the three use cases that take part.
type shipFlow struct {
	f       *fixture
	gateway *spyGateway
	pub     *recordingPublisher
	confirm *usecases.ConfirmNetworkOrderShipment
}

func newShipFlow() *shipFlow {
	f := newFixture(true)
	s := &shipFlow{f: f, gateway: &spyGateway{StubGateway: f.gateway}, pub: &recordingPublisher{}}
	s.confirm = f.confirmShipment()
	s.confirm.Gateway = s.gateway
	s.confirm.Events = s.pub
	return s
}

func (s *shipFlow) confirmPending() *usecases.ConfirmPendingShipments {
	return &usecases.ConfirmPendingShipments{Orders: s.f.orders, Confirm: s.confirm}
}

func (s *shipFlow) shipmentConfirmedEvents() int {
	n := 0
	for _, e := range s.pub.events {
		if _, ok := e.(shared.NetworkOrderShipmentConfirmed); ok {
			n++
		}
	}
	return n
}

func (s *shipFlow) state(t *testing.T, ref shared.NetworkRef) networkorder.State {
	t.Helper()
	o, err := s.f.orders.FindByRef(context.Background(), ref)
	if err != nil || o == nil {
		t.Fatalf("find %s: %v %v", ref, o, err)
	}
	return o.State()
}

// seed stores an order in state with a pending shipment, the way a repository
// would hand it back.
func (s *shipFlow) seed(t *testing.T, ref shared.NetworkRef, state networkorder.State, shipmentRef string, count int, noted time.Time) {
	t.Helper()
	local := shared.LocalOrderId("ord-" + string(ref))
	o := networkorder.Rehydrate(ref, "site-1", now().Add(48*time.Hour), now().Add(networkorder.AcknowledgementWindow), now(),
		[]networkorder.Line{mustLine(t)}, state, &local)
	o.RestorePendingShipment(networkorder.PendingShipment{ShipmentRef: shipmentRef, PackageCount: count, NotedAt: noted})
	if err := s.f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("seed %s: %v", ref, err)
	}
}

// wantPass runs one confirm-pending pass and checks its counters.
func (s *shipFlow) wantPass(t *testing.T, label string, want usecases.ConfirmPendingShipmentsResult) {
	t.Helper()
	res, err := s.confirmPending().Execute(context.Background())
	if err != nil || res != want {
		t.Fatalf("%s: %+v %v, want %+v", label, res, err, want)
	}
}

// wantConfirmedOnce checks everything the network and the event stream saw for
// the one order the flow confirmed: CONFIRMED, the fact used, one label request
// for packages, one submission, one NetworkOrderShipmentConfirmed.
func (s *shipFlow) wantConfirmedOnce(t *testing.T, ref shared.NetworkRef, packages int) {
	t.Helper()
	if got := s.state(t, ref); got != networkorder.StateConfirmed {
		t.Fatalf("state = %v, want CONFIRMED", got)
	}
	if got := s.shipmentConfirmedEvents(); got != 1 {
		t.Fatalf("NetworkOrderShipmentConfirmed published %d times, want exactly 1", got)
	}
	if len(s.gateway.labelReqs) != 1 || s.gateway.labelReqs[0] != packages {
		t.Fatalf("labels requested = %v, want one request for %d packages", s.gateway.labelReqs, packages)
	}
	if len(s.gateway.shipped) != 1 {
		t.Fatalf("shipment confirmations sent to the network = %d, want 1", len(s.gateway.shipped))
	}
	o, _ := s.f.orders.FindByRef(context.Background(), ref)
	if _, ok := o.PendingShipment(); ok {
		t.Fatal("the pending shipment must be cleared once used")
	}
}

func TestConfirmPendingShipments_EarlyEventThenReconcileThenSweepConfirmsOnce(t *testing.T) {
	s := newShipFlow()
	ctx := context.Background()
	submittedOrder(t, s.f, "po-1")

	// The load departs while the order is still SUBMITTED; a sweep before the
	// acknowledgement settles skips the order.
	if err := s.f.deferShipment().Execute(ctx, "po-1", "load-7", 3); err != nil {
		t.Fatalf("defer: %v", err)
	}
	s.wantPass(t, "sweep while SUBMITTED", usecases.ConfirmPendingShipmentsResult{Examined: 1})
	if got := s.state(t, "po-1"); got != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED still", got)
	}

	// The network settles the acknowledgement; the next sweep confirms, with the
	// noted package count.
	if _, err := s.f.reconcile().Execute(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s.wantPass(t, "sweep after ACKNOWLEDGED", usecases.ConfirmPendingShipmentsResult{Examined: 1, Confirmed: 1})
	s.wantConfirmedOnce(t, "po-1", 3)

	// A further sweep finds nothing and confirms nothing again.
	s.wantPass(t, "third sweep", usecases.ConfirmPendingShipmentsResult{})
	if got := s.shipmentConfirmedEvents(); got != 1 {
		t.Fatalf("events after the third sweep = %d, want still 1", got)
	}
}

func TestConfirmPendingShipments_ASecondEarlyEventForTheSameLoadConfirmsOnce(t *testing.T) {
	s := newShipFlow()
	ctx := context.Background()
	submittedOrder(t, s.f, "po-1")
	for range 2 {
		if err := s.f.deferShipment().Execute(ctx, "po-1", "load-7", 0); err != nil {
			t.Fatalf("defer: %v", err)
		}
	}
	if _, err := s.f.reconcile().Execute(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := s.confirmPending().Execute(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if got := s.shipmentConfirmedEvents(); got != 1 {
		t.Fatalf("events = %d, want 1", got)
	}
	// Count 0 was noted: the confirm path asks for the single-package placeholder.
	if len(s.gateway.labelReqs) != 1 || s.gateway.labelReqs[0] != 1 {
		t.Fatalf("labels requested = %v, want [1] (the placeholder for an unknown count)", s.gateway.labelReqs)
	}
}

func TestConfirmPendingShipments_SubmittedOrdersAreSkippedAndKeepTheirFact(t *testing.T) {
	s := newShipFlow()
	s.seed(t, "po-1", networkorder.StateSubmitted, "load-7", 2, now())

	res, err := s.confirmPending().Execute(context.Background())
	if err != nil || res != (usecases.ConfirmPendingShipmentsResult{Examined: 1}) {
		t.Fatalf("pass: %+v %v, want examined 1 and nothing else", res, err)
	}
	o, _ := s.f.orders.FindByRef(context.Background(), "po-1")
	if _, ok := o.PendingShipment(); !ok {
		t.Fatal("a skipped order must keep its pending shipment")
	}
	if len(s.gateway.labelReqs) != 0 || len(s.pub.events) != 0 {
		t.Fatalf("a SUBMITTED order must cause no network call or event: labels %v events %v", s.gateway.labelReqs, s.pub.events)
	}
}

func TestConfirmPendingShipments_DropsAStaleFactOnAnOrderThatNeedsNoConfirmation(t *testing.T) {
	for _, state := range []networkorder.State{networkorder.StateConfirmed, networkorder.StateRejected} {
		s := newShipFlow()
		s.seed(t, "po-1", state, "load-7", 2, now())

		res, err := s.confirmPending().Execute(context.Background())
		if err != nil || res != (usecases.ConfirmPendingShipmentsResult{Examined: 1, Dropped: 1}) {
			t.Fatalf("%s: pass: %+v %v, want examined 1, dropped 1", state, res, err)
		}
		o, _ := s.f.orders.FindByRef(context.Background(), "po-1")
		if _, ok := o.PendingShipment(); ok {
			t.Errorf("%s: the stale pending shipment must be dropped", state)
		}
		if o.State() != state {
			t.Errorf("%s: dropping changed the state to %v", state, o.State())
		}
		if len(s.gateway.labelReqs) != 0 || len(s.pub.events) != 0 {
			t.Errorf("%s: dropping must not touch the network or publish: labels %v events %v", state, s.gateway.labelReqs, s.pub.events)
		}
	}
}

// refFailingGateway refuses the label for one order only, so the pass has a
// failure to survive.
type refFailingGateway struct {
	*spyGateway
	failRef shared.NetworkRef
}

func (g refFailingGateway) RequestLabel(ctx context.Context, ref shared.NetworkRef, n int) ([]contract.LabelResult, error) {
	if ref == g.failRef {
		return nil, errBoom
	}
	return g.spyGateway.RequestLabel(ctx, ref, n)
}

func TestConfirmPendingShipments_OneFailureIsCountedAndNeverAbortsThePass(t *testing.T) {
	s := newShipFlow()
	s.confirm.Gateway = refFailingGateway{spyGateway: s.gateway, failRef: "po-1"}
	s.seed(t, "po-1", networkorder.StateAcknowledged, "load-7", 1, now())
	s.seed(t, "po-2", networkorder.StateAcknowledged, "load-8", 1, now().Add(time.Minute))
	ctx := context.Background()

	res, err := s.confirmPending().Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res != (usecases.ConfirmPendingShipmentsResult{Examined: 2, Confirmed: 1, Failed: 1}) {
		t.Fatalf("result = %+v, want examined 2, confirmed 1, failed 1", res)
	}
	if got := s.state(t, "po-2"); got != networkorder.StateConfirmed {
		t.Fatalf("po-2 = %v, want CONFIRMED despite po-1 failing", got)
	}
	o, _ := s.f.orders.FindByRef(ctx, "po-1")
	if _, ok := o.PendingShipment(); !ok || o.State() != networkorder.StateAcknowledged {
		t.Fatal("the failed order must keep its pending shipment and stay ACKNOWLEDGED for the next pass")
	}
}

func TestConfirmPendingShipments_AFailedDropIsCountedToo(t *testing.T) {
	s := newShipFlow()
	s.seed(t, "po-1", networkorder.StateRejected, "load-7", 1, now())
	uc := s.confirmPending()
	uc.Orders = &saveFailingRepo{NetworkOrderRepo: s.f.orders, err: errBoom}

	res, err := uc.Execute(context.Background())
	if err != nil || res != (usecases.ConfirmPendingShipmentsResult{Examined: 1, Failed: 1}) {
		t.Fatalf("pass: %+v %v, want examined 1, failed 1", res, err)
	}
}

func TestConfirmPendingShipments_ListFailureIsReturned(t *testing.T) {
	s := newShipFlow()
	uc := s.confirmPending()
	uc.Orders = &failingRepo{listErr: errBoom}
	if _, err := uc.Execute(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
}

// saveFailingRepo lets every read through and refuses every write.
type saveFailingRepo struct {
	ports.NetworkOrderRepo
	err error
}

func (r *saveFailingRepo) Save(context.Context, *networkorder.NetworkOrder) error { return r.err }
