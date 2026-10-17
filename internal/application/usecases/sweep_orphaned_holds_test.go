package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ADR 0024: the orphaned-hold sweep. These tests drive the use case with a
// fake order-management port (holdPlanner) and the in-memory repository.

const holdGrace = time.Hour

// holdPlanner is the order-management fake of the sweep tests. The sweep
// may only ever cancel: any other call is recorded and fails the test.
type holdPlanner struct {
	cancelled []shared.LocalOrderId
	cancelErr error
	other     []string
}

func (p *holdPlanner) RaiseHeldOrder(context.Context, contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	p.other = append(p.other, "raise")
	return contract.HeldOrderResult{}, errors.New("the sweep must never raise a hold")
}

func (p *holdPlanner) ReleaseHeldOrder(context.Context, shared.LocalOrderId) error {
	p.other = append(p.other, "release")
	return errors.New("the sweep must never release a hold")
}

func (p *holdPlanner) CancelHeldOrder(_ context.Context, id shared.LocalOrderId) error {
	p.cancelled = append(p.cancelled, id)
	return p.cancelErr
}

type holdMetrics struct{ outcomes []contract.HoldSweepOutcome }

func (m *holdMetrics) Swept(_ context.Context, outcome contract.HoldSweepOutcome) {
	m.outcomes = append(m.outcomes, outcome)
}

type sweepFx struct {
	orders  *memory.NetworkOrderRepo
	planner *holdPlanner
	pub     *scopedPublisher
	uow     *recordingUnitOfWork
	metrics *holdMetrics
}

func newSweepFx() *sweepFx {
	return &sweepFx{
		orders:  memory.NewNetworkOrderRepo(),
		planner: &holdPlanner{},
		pub:     &scopedPublisher{},
		uow:     &recordingUnitOfWork{},
		metrics: &holdMetrics{},
	}
}

func (f *sweepFx) uc(grace time.Duration) *usecases.SweepOrphanedHolds {
	return &usecases.SweepOrphanedHolds{
		Orders:     f.orders,
		Planner:    f.planner,
		Events:     f.pub,
		Clock:      fixedClock{t: now()},
		UnitOfWork: f.uow,
		Metrics:    f.metrics,
		Grace:      grace,
	}
}

// seed stores an order in state with the recorded hold id (empty = none) and
// an acknowledgement deadline ackBy.
func (f *sweepFx) seed(t *testing.T, ref shared.NetworkRef, state networkorder.State, id string, ackBy time.Time) *networkorder.NetworkOrder {
	t.Helper()
	var local *shared.LocalOrderId
	if id != "" {
		l := shared.LocalOrderId(id)
		local = &l
	}
	o := networkorder.Rehydrate(ref, "site-1", now().Add(48*time.Hour), ackBy, ackBy.Add(-networkorder.AcknowledgementWindow),
		[]networkorder.Line{mustLine(t)}, state, local)
	if err := f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("seed Save: %v", err)
	}
	return o
}

// pastGrace is an acknowledgement deadline that closed 2h ago: past a 1h grace.
func pastGrace() time.Time { return now().Add(-2 * time.Hour) }

func (f *sweepFx) stored(t *testing.T, ref shared.NetworkRef) *networkorder.NetworkOrder {
	t.Helper()
	o, err := f.orders.FindByRef(context.Background(), ref)
	if err != nil || o == nil {
		t.Fatalf("FindByRef(%s) = %v, %v", ref, o, err)
	}
	return o
}

func TestSweepOrphanedHolds_CancelsAnOrphanedHoldAndReportsIt(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-1", networkorder.StateNew, "ord-1", pastGrace())

	res, err := f.uc(holdGrace).Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Examined != 1 || res.Cancelled != 1 || res.Failed != 0 || res.AlreadyReleased != 0 {
		t.Fatalf("result = %+v, want Examined=1 Cancelled=1", res)
	}
	if len(f.planner.cancelled) != 1 || f.planner.cancelled[0] != "ord-1" {
		t.Fatalf("cancelled = %v, want [ord-1]", f.planner.cancelled)
	}
	if len(f.planner.other) != 0 {
		t.Fatalf("the sweep made calls other than cancel: %v", f.planner.other)
	}

	o := f.stored(t, "po-1")
	if o.HoldOutcome() != networkorder.HoldCancelled || !o.HoldSettledAt().Equal(now()) {
		t.Fatalf("outcome/at = %q/%v, want CANCELLED/%v", o.HoldOutcome(), o.HoldSettledAt(), now())
	}
	// The sweep frees the hold; it never decides the protocol state (that is RejectOverdueOrders').
	if o.State() != networkorder.StateNew {
		t.Fatalf("state = %v, want the sweep to leave it NEW", o.State())
	}

	if len(f.pub.events) != 1 {
		t.Fatalf("events = %d, want exactly OrphanedHoldCancelled: %+v", len(f.pub.events), f.pub.events)
	}
	ev, ok := f.pub.events[0].(shared.OrphanedHoldCancelled)
	if !ok {
		t.Fatalf("event = %T, want shared.OrphanedHoldCancelled", f.pub.events[0])
	}
	want := shared.OrphanedHoldCancelled{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1",
		AcknowledgeBy: pastGrace(), Answer: shared.HoldAnswerCancelled, At: now()}
	if ev != want {
		t.Fatalf("event = %+v, want %+v", ev, want)
	}
	// The settle and the report commit together (transactional outbox).
	if !f.pub.inScope[0] || f.uow.committed != 1 {
		t.Fatalf("publish inScope=%v committed=%d, want inside one committed unit of work", f.pub.inScope, f.uow.committed)
	}
	if len(f.metrics.outcomes) != 1 || f.metrics.outcomes[0] != contract.HoldSweepCancelled {
		t.Fatalf("metrics = %v, want [cancelled]", f.metrics.outcomes)
	}
}

func TestSweepOrphanedHolds_NotFoundMeansTheHoldIsGone(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-1", networkorder.StateNew, "ord-1", pastGrace())
	f.planner.cancelErr = fmt.Errorf("order-management DELETE /orders/ord-1: status 404: %w", contract.ErrHeldOrderNotFound)

	res, err := f.uc(holdGrace).Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Cancelled != 1 || res.Failed != 0 {
		t.Fatalf("result = %+v, want Cancelled=1 (a 404 is done)", res)
	}
	if got := f.stored(t, "po-1").HoldOutcome(); got != networkorder.HoldCancelled {
		t.Fatalf("outcome = %q, want CANCELLED", got)
	}
	ev := f.pub.events[0].(shared.OrphanedHoldCancelled)
	if ev.Answer != shared.HoldAnswerNotFound {
		t.Fatalf("answer = %q, want NOT_FOUND", ev.Answer)
	}
}

func TestSweepOrphanedHolds_AlreadyReleasedIsNotAnOrphanAndIsNeverRetried(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-1", networkorder.StateNew, "ord-1", pastGrace())
	f.planner.cancelErr = fmt.Errorf("order-management DELETE /orders/ord-1: status 409: %w", contract.ErrHeldOrderAlreadyReleased)
	uc := f.uc(holdGrace)

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.AlreadyReleased != 1 || res.Cancelled != 0 || res.Failed != 0 {
		t.Fatalf("result = %+v, want AlreadyReleased=1", res)
	}
	if got := f.stored(t, "po-1").HoldOutcome(); got != networkorder.HoldAlreadyReleased {
		t.Fatalf("outcome = %q, want ALREADY_RELEASED recorded", got)
	}
	if len(f.pub.events) != 0 {
		t.Fatalf("nothing was cancelled, so nothing may be reported: %+v", f.pub.events)
	}
	if len(f.metrics.outcomes) != 1 || f.metrics.outcomes[0] != contract.HoldSweepAlreadyReleased {
		t.Fatalf("metrics = %v, want [already_released]", f.metrics.outcomes)
	}

	// Never retried: the next pass must not call order-management again.
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if len(f.planner.cancelled) != 1 {
		t.Fatalf("cancel calls = %d, want 1 (an order found released is never retried)", len(f.planner.cancelled))
	}
}

func TestSweepOrphanedHolds_OrderManagementFailuresAreRetriedNeverMarkedDone(t *testing.T) {
	failures := map[string]error{
		"5xx":             errors.New("order-management DELETE /orders/ord-1: status 503"),
		"timeout":         fmt.Errorf("Delete \"http://om/orders/ord-1\": %w", context.DeadlineExceeded),
		"open breaker":    fmt.Errorf("%w: gobreaker: circuit breaker is open", ordermanagement.ErrCircuitOpen),
		"transport error": errors.New("dial tcp: connection refused"),
		"unexpected 4xx":  errors.New("order-management DELETE /orders/ord-1: status 400"),
	}
	for name, cancelErr := range failures {
		t.Run(name, func(t *testing.T) {
			f := newSweepFx()
			f.seed(t, "po-1", networkorder.StateNew, "ord-1", pastGrace())
			f.planner.cancelErr = cancelErr
			uc := f.uc(holdGrace)

			res, err := uc.Execute(context.Background())
			if err != nil {
				t.Fatalf("one failing hold must not fail the pass: %v", err)
			}
			if res.Failed != 1 || res.Cancelled != 0 || res.AlreadyReleased != 0 {
				t.Fatalf("result = %+v, want Failed=1", res)
			}
			if got := f.stored(t, "po-1").HoldOutcome(); got != "" {
				t.Fatalf("outcome = %q, want the hold left open", got)
			}
			if len(f.pub.events) != 0 {
				t.Fatalf("nothing may be reported for a cancel that did not happen: %+v", f.pub.events)
			}
			if len(f.metrics.outcomes) != 1 || f.metrics.outcomes[0] != contract.HoldSweepFailed {
				t.Fatalf("metrics = %v, want [failed]", f.metrics.outcomes)
			}

			// Order-management recovers: the next pass retries and completes.
			f.planner.cancelErr = nil
			res, err = uc.Execute(context.Background())
			if err != nil || res.Cancelled != 1 {
				t.Fatalf("retry: res=%+v err=%v, want Cancelled=1", res, err)
			}
			if len(f.planner.cancelled) != 2 {
				t.Fatalf("cancel calls = %d, want 2 (one failed, one retried)", len(f.planner.cancelled))
			}
		})
	}
}

// TestSweepOrphanedHolds_NeverCancelsAnOrderStillBeingDecided is the guard
// rail of ADR 0024 §3: the decision uses the NF state and the clock, never
// a timer alone. Every order below carries a recorded hold and has no
// outcome, and none may be cancelled.
func TestSweepOrphanedHolds_NeverCancelsAnOrderStillBeingDecided(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-new-in-window", networkorder.StateNew, "ord-a", now().Add(time.Hour))
	f.seed(t, "po-new-in-grace", networkorder.StateNew, "ord-b", now().Add(-30*time.Minute))
	f.seed(t, "po-new-at-grace-edge", networkorder.StateNew, "ord-c", now().Add(-holdGrace))
	f.seed(t, "po-submitted", networkorder.StateSubmitted, "ord-d", pastGrace())
	f.seed(t, "po-acknowledged", networkorder.StateAcknowledged, "ord-e", pastGrace())
	f.seed(t, "po-confirmed", networkorder.StateConfirmed, "ord-f", pastGrace())
	f.seed(t, "po-new-no-hold", networkorder.StateNew, "", pastGrace())

	res, err := f.uc(holdGrace).Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Cancelled != 0 || res.Failed != 0 || res.AlreadyReleased != 0 {
		t.Fatalf("result = %+v, want nothing acted on", res)
	}
	if len(f.planner.cancelled) != 0 || len(f.planner.other) != 0 {
		t.Fatalf("order-management was called: cancel=%v other=%v", f.planner.cancelled, f.planner.other)
	}
	if len(f.pub.events) != 0 {
		t.Fatalf("events = %+v, want none", f.pub.events)
	}
}

func TestSweepOrphanedHolds_SweepsARejectedOrderWhoseHoldWasNeverFreed(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-rejected", networkorder.StateRejected, "ord-1", pastGrace())

	res, err := f.uc(holdGrace).Execute(context.Background())
	if err != nil || res.Cancelled != 1 {
		t.Fatalf("res=%+v err=%v, want Cancelled=1", res, err)
	}
	o := f.stored(t, "po-rejected")
	if o.State() != networkorder.StateRejected || o.HoldOutcome() != networkorder.HoldCancelled {
		t.Fatalf("state/outcome = %v/%q, want REJECTED/CANCELLED", o.State(), o.HoldOutcome())
	}
}

func TestSweepOrphanedHolds_SecondPassIsANoOp(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-1", networkorder.StateNew, "ord-1", pastGrace())
	uc := f.uc(holdGrace)

	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if res != (usecases.SweepOrphanedHoldsResult{}) {
		t.Fatalf("second pass result = %+v, want all zero", res)
	}
	if len(f.planner.cancelled) != 1 || len(f.pub.events) != 1 {
		t.Fatalf("cancel calls=%d events=%d, want 1 and 1: the second pass must change nothing",
			len(f.planner.cancelled), len(f.pub.events))
	}
}

func TestSweepOrphanedHolds_ZeroGraceActsAsSoonAsTheWindowCloses(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-1", networkorder.StateNew, "ord-1", now().Add(-time.Minute))

	res, err := f.uc(0).Execute(context.Background())
	if err != nil || res.Cancelled != 1 {
		t.Fatalf("res=%+v err=%v, want Cancelled=1 with no grace", res, err)
	}
}

func TestSweepOrphanedHolds_SaveFailureReportsNothingAndCountsFailed(t *testing.T) {
	f := newSweepFx()
	f.seed(t, "po-1", networkorder.StateNew, "ord-1", pastGrace())
	uc := f.uc(holdGrace)
	uc.Orders = &saveFailingHoldRepo{NetworkOrderRepo: f.orders}

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Failed != 1 || res.Cancelled != 0 {
		t.Fatalf("result = %+v, want Failed=1: the settle was not durable", res)
	}
	if len(f.pub.events) != 0 {
		t.Fatalf("events = %+v, want none: the report commits with the settle or not at all", f.pub.events)
	}
}

func TestSweepOrphanedHolds_ListFailureIsReturned(t *testing.T) {
	f := newSweepFx()
	uc := f.uc(holdGrace)
	uc.Orders = &listFailingHoldRepo{NetworkOrderRepo: f.orders}

	if _, err := uc.Execute(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
}

// saveFailingHoldRepo and listFailingHoldRepo wrap the in-memory repository.
type saveFailingHoldRepo struct{ ports.NetworkOrderRepo }

func (r *saveFailingHoldRepo) Save(context.Context, *networkorder.NetworkOrder) error { return errBoom }

type listFailingHoldRepo struct{ ports.NetworkOrderRepo }

func (r *listFailingHoldRepo) ListOpenHolds(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, errBoom
}
