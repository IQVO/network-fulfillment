package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var errBoom = errors.New("boom")

func now() time.Time { return time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC) }

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, any) error { return nil }

// fakePlanner records what order-management was asked to do, in order.
// The ORDER is the point: several of this use case's rules are about
// which irreversible step happens after which fallible one.
type fakePlanner struct {
	calls []string

	feasible bool
	raiseErr error

	releaseErr error
	cancelErr  error

	lastRequest contract.HeldOrderRequest
}

func (p *fakePlanner) RaiseHeldOrder(_ context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	p.calls = append(p.calls, "raise")
	p.lastRequest = req
	if p.raiseErr != nil {
		return contract.HeldOrderResult{}, p.raiseErr
	}
	return contract.HeldOrderResult{LocalOrderId: "ord-1", Feasible: p.feasible}, nil
}

func (p *fakePlanner) ReleaseHeldOrder(_ context.Context, _ shared.LocalOrderId) error {
	p.calls = append(p.calls, "release")
	return p.releaseErr
}

func (p *fakePlanner) CancelHeldOrder(_ context.Context, _ shared.LocalOrderId) error {
	p.calls = append(p.calls, "cancel")
	return p.cancelErr
}

type fixture struct {
	orders      *memory.NetworkOrderRepo
	gateway     *network.StubGateway
	planner     *fakePlanner
	translation *memory.ProductTranslation
}

func newFixture(feasible bool) *fixture {
	tr := memory.NewProductTranslation()
	tr.Add("ASIN-1", "SKU-1")
	tr.Add("ASIN-2", "SKU-2")
	return &fixture{
		orders:      memory.NewNetworkOrderRepo(),
		gateway:     network.NewStubGateway(nil),
		planner:     &fakePlanner{feasible: feasible},
		translation: tr,
	}
}

func (f *fixture) receive() *usecases.ReceiveNetworkDemand {
	return &usecases.ReceiveNetworkDemand{
		Orders:      f.orders,
		Gateway:     f.gateway,
		Planner:     f.planner,
		Translation: f.translation,
		Events:      nopPublisher{},
		Clock:       fixedClock{t: now()},
	}
}

func demand(ref shared.NetworkRef, productIds ...shared.NetworkProductId) contract.InboundDemand {
	lines := make([]contract.InboundLine, 0, len(productIds))
	for i, id := range productIds {
		lines = append(lines, contract.InboundLine{
			NetworkLineRef:   shared.NetworkLineRef(string(rune('a' + i))),
			NetworkProductId: id,
			Quantity:         1,
		})
	}
	return contract.InboundDemand{
		NetworkRef:     ref,
		SiteId:         "site-1",
		RequiredShipBy: now().Add(48 * time.Hour),
		Lines:          lines,
	}
}

func TestReceive_FeasibleDemandIsAcknowledgedAndReleased(t *testing.T) {
	f := newFixture(true)

	o, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// SUBMITTED, not yet ACKNOWLEDGED: ADR 0001 §5 models this
	// submission as pending until a later reconciliation pass confirms
	// it (see TestReceive_FeasibleDemandIsSubmittedNotYetReleased and
	// TestReconcile_SuccessConfirmsAndReleases in
	// reconcile_submitted_orders_test.go for the rest of this path).
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED", o.State())
	}

	accepted, submitted := f.gateway.Acknowledgement("po-1")
	if !submitted || !accepted {
		t.Fatalf("acknowledgement submitted=%v accepted=%v, want true/true", submitted, accepted)
	}

	// Release is deferred to reconciliation, not performed here: nothing
	// may reach the floor until the submission is actually confirmed,
	// not merely accepted-for-processing.
	want := []string{"raise"}
	if got := f.planner.calls; !equal(got, want) {
		t.Fatalf("planner calls = %v, want %v", got, want)
	}
}

func TestReceive_InfeasibleDemandIsRejectedAndTheHoldIsFreed(t *testing.T) {
	f := newFixture(false)

	o, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o.State() != networkorder.StateRejected {
		t.Fatalf("state = %v, want REJECTED", o.State())
	}

	accepted, submitted := f.gateway.Acknowledgement("po-1")
	if !submitted || accepted {
		t.Fatalf("acknowledgement submitted=%v accepted=%v, want true/false", submitted, accepted)
	}

	// Cancel must happen, and must precede the network submission: a
	// cancel skipped on an error path is the orphaned-hold failure both
	// ADRs named.
	want := []string{"raise", "cancel"}
	if got := f.planner.calls; !equal(got, want) {
		t.Fatalf("planner calls = %v, want %v", got, want)
	}
	if f.planner.calls[len(f.planner.calls)-1] == "release" {
		t.Fatal("an infeasible order must never be released")
	}
}

func TestReceive_UntranslatableProductIsRejectedWithoutRaisingAnOrder(t *testing.T) {
	f := newFixture(true)

	o, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-UNKNOWN"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o.State() != networkorder.StateRejected {
		t.Fatalf("state = %v, want REJECTED", o.State())
	}

	// We never asked order-management for anything: we cannot allocate
	// what we cannot identify, and raising an order would reserve
	// inventory for a line that has no SKU.
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}

	accepted, submitted := f.gateway.Acknowledgement("po-1")
	if !submitted || accepted {
		t.Fatalf("acknowledgement submitted=%v accepted=%v, want true/false", submitted, accepted)
	}
}

func TestReceive_PartiallyUntranslatableRejectsTheWholeOrder(t *testing.T) {
	f := newFixture(true)

	// One good line, one unknown. The network's protocol has no partial
	// acknowledgement, so a single untranslatable product refuses
	// everything.
	o, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1", "ASIN-UNKNOWN"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o.State() != networkorder.StateRejected {
		t.Fatalf("state = %v, want REJECTED", o.State())
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}
}

func TestReceive_IsIdempotentOnRepeatedPolls(t *testing.T) {
	f := newFixture(true)
	uc := f.receive()

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("first: %v", err)
	}
	before := len(f.planner.calls)

	// Inbound is a POLL: the same demand WILL arrive again. Re-answering
	// would send a duplicate acknowledgement for an order we already
	// committed to.
	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(f.planner.calls) != before {
		t.Fatalf("planner calls = %v, want unchanged after a repeat poll", f.planner.calls)
	}
}

func TestReceive_SendsOnlyOurVocabularyToOrderManagement(t *testing.T) {
	f := newFixture(true)

	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1", "ASIN-2")); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// The ACL's whole purpose, asserted: order-management is handed
	// SKUs, quantities, a site and a deadline. If a NetworkProductId
	// ever appears in this map, the Amazon vocabulary has leaked inward.
	req := f.planner.lastRequest
	for sku := range req.Lines {
		if sku != "SKU-1" && sku != "SKU-2" {
			t.Fatalf("unexpected key %q in the planner request — network vocabulary leaked inward", sku)
		}
	}
	if len(req.Lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(req.Lines))
	}
	if !req.RequiredShipBy.Equal(now().Add(48 * time.Hour)) {
		t.Fatalf("requiredShipBy = %v, want the network's deadline carried through", req.RequiredShipBy)
	}
}

func TestReceive_PlannerFailureLeavesNoAnswerOnTheNetwork(t *testing.T) {
	f := newFixture(true)
	f.planner.raiseErr = errBoom

	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}

	// Nothing was told to the network, so the next poll can retry
	// cleanly. Answering before we know we can fulfil would be a
	// commitment we might not keep.
	if _, submitted := f.gateway.Acknowledgement("po-1"); submitted {
		t.Fatal("no acknowledgement may be submitted when the planner failed")
	}
}

func TestRejectOverdue_FreesTheHoldOfAnOrderThatWasNeverAnswered(t *testing.T) {
	f := newFixture(true)

	// An order received a full window ago and never answered.
	o, err := networkorder.Receive("po-old", "site-1", now().Add(48*time.Hour),
		[]networkorder.Line{mustLine(t)}, now().Add(-25*time.Hour))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := o.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := o.ConfirmAcknowledgement(); err != nil {
		t.Fatalf("ConfirmAcknowledgement: %v", err)
	}
	if err := o.LinkLocalOrder("ord-held"); err != nil {
		t.Fatalf("LinkLocalOrder: %v", err)
	}
	// Put it back to NEW-with-a-local-order by rehydrating: the real
	// shape of a crash between raising the hold and answering.
	local := shared.LocalOrderId("ord-held")
	stuck := networkorder.Rehydrate("po-old", "site-1", now().Add(48*time.Hour),
		now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
	if err := f.orders.Save(context.Background(), stuck); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders:  f.orders,
		Planner: f.planner,
		Events:  nopPublisher{},
		Clock:   fixedClock{t: now()},
	}
	res, err := rejectOverdue.Execute(context.Background())
	if err != nil {
		t.Fatalf("rejectOverdue: %v", err)
	}
	if res.Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", res.Rejected)
	}

	// The operationally important part: the inventory is no longer held
	// for demand we never answered.
	if got := f.planner.calls; !equal(got, []string{"cancel"}) {
		t.Fatalf("planner calls = %v, want [cancel]", got)
	}

	after, _ := f.orders.FindByRef(context.Background(), "po-old")
	if after.State() != networkorder.StateRejected {
		t.Fatalf("state = %v, want REJECTED", after.State())
	}
}

// TestSweep_ReportsAtRiskWithoutMutatingOrCallingThePlanner pins ADR
// 0001 §6's actual rule: the sweep is a pure reporter. It takes no
// Planner at all (a compile-time guarantee, not just a runtime
// assertion) and must never call CancelHeldOrder or change state —
// TestRejectOverdue_FreesTheHoldOfAnOrderThatWasNeverAnswered above is
// the separate path that does that.
func TestSweep_ReportsAtRiskWithoutMutatingOrCallingThePlanner(t *testing.T) {
	f := newFixture(true)

	local := shared.LocalOrderId("ord-held")
	stuck := networkorder.Rehydrate("po-old", "site-1", now().Add(48*time.Hour),
		now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
	if err := f.orders.Save(context.Background(), stuck); err != nil {
		t.Fatalf("Save: %v", err)
	}

	var pub recordingEventPublisher
	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: f.orders, Events: &pub, Clock: fixedClock{t: now()},
	}
	res, err := sweep.Execute(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.AtRisk != 1 {
		t.Fatalf("atRisk = %d, want 1", res.AtRisk)
	}
	if len(pub.events) != 1 {
		t.Fatalf("published events = %d, want 1 (AcknowledgementDeadlineAtRisk)", len(pub.events))
	}
	if _, ok := pub.events[0].(shared.AcknowledgementDeadlineAtRisk); !ok {
		t.Fatalf("published event = %T, want shared.AcknowledgementDeadlineAtRisk", pub.events[0])
	}

	// Still NEW, still holding its local order: the sweep must not
	// have touched the aggregate at all.
	after, _ := f.orders.FindByRef(context.Background(), "po-old")
	if after.State() != networkorder.StateNew {
		t.Fatalf("state = %v, want unchanged NEW — the sweep must never mutate the aggregate (ADR 0001 §6)", after.State())
	}
	if got := after.LocalOrderId(); got == nil || *got != local {
		t.Fatalf("localOrderId = %v, want still %v — the sweep must not touch the hold", got, local)
	}
}

// recordingEventPublisher is a minimal ports.EventPublisher for
// asserting WHICH event type was published, not just that something was.
type recordingEventPublisher struct {
	events []any
}

func (p *recordingEventPublisher) Publish(_ context.Context, event any) error {
	p.events = append(p.events, event)
	return nil
}

func TestSweep_LeavesOrdersInsideTheirWindowAlone(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.planner.calls = nil

	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: f.orders, Events: nopPublisher{},
		Clock: fixedClock{t: now().Add(time.Hour)},
	}
	res, err := sweep.Execute(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.AtRisk != 0 {
		t.Fatalf("atRisk = %d, want 0", res.AtRisk)
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}
}

func mustLine(t *testing.T) networkorder.Line {
	t.Helper()
	l, err := networkorder.NewLine("line-1", "ASIN-1", "SKU-1", 1)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return l
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
