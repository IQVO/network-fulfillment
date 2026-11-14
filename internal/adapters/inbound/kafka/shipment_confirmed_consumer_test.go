package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	ce "github.com/cloudevents/sdk-go/v2/event"
)

// countingGateway wraps the StubGateway and counts the two network calls a
// confirmation makes, plus the package counts the labels were asked for —
// the exactly-once and package-count assertions read these, not the stub's
// own maps.
type countingGateway struct {
	*retailnetwork.StubGateway
	labels     map[shared.NetworkRef]int
	submits    map[shared.NetworkRef]int
	packageFor map[shared.NetworkRef][]int
}

func newCountingGateway() *countingGateway {
	return &countingGateway{
		StubGateway: retailnetwork.NewStubGateway(nil),
		labels:      map[shared.NetworkRef]int{},
		submits:     map[shared.NetworkRef]int{},
		packageFor:  map[shared.NetworkRef][]int{},
	}
}

func (g *countingGateway) RequestLabel(_ context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error) {
	g.labels[ref]++
	g.packageFor[ref] = append(g.packageFor[ref], packageCount)
	return g.StubGateway.RequestLabel(context.Background(), ref, packageCount)
}

func (g *countingGateway) SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef, shippedAt time.Time) error {
	g.submits[ref]++
	return g.StubGateway.SubmitShipmentConfirmation(ctx, ref, shippedAt)
}

type recordingEvents struct{ events []any }

func (p *recordingEvents) Publish(_ context.Context, e any) error {
	p.events = append(p.events, e)
	return nil
}

// fixedTime is the smallest ports.Clock: one instant, forever.
type fixedTime struct{ t time.Time }

func (c fixedTime) Now() time.Time { return c.t }

// acknowledgedWithLocalOrder seeds one ACKNOWLEDGED NetworkOrder whose
// demand became the given order-management OrderId — the state the
// confirmation path requires and exactly the mapping ShipmentConfirmed
// joins on.
func acknowledgedWithLocalOrder(t *testing.T, orders *memory.NetworkOrderRepo, ref shared.NetworkRef, localOrderId shared.LocalOrderId) {
	t.Helper()
	id := localOrderId
	o := networkorder.Rehydrate(
		ref, "site-1",
		time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC),
		nil, networkorder.StateAcknowledged, &id,
	)
	if err := orders.Save(context.Background(), o); err != nil {
		t.Fatalf("seed %s: %v", ref, err)
	}
}

// shipmentEvent builds a structured CloudEvents message the way
// outbound-shipping does, byte-shaped like its merged contract's example.
func shipmentEvent(t *testing.T, id string, loadId string, orders []map[string]any) []byte {
	t.Helper()
	e := ce.New("1.0")
	e.SetID(id)
	e.SetSource("/warehouse/outbound-shipping")
	e.SetType(TypeShipmentConfirmed)
	e.SetSubject(loadId)
	e.SetTime(time.Date(2026, 11, 6, 16, 2, 0, 0, time.UTC))
	if err := e.SetData("application/json", map[string]any{
		"load_id":     loadId,
		"site_id":     "SITE-1",
		"cpt_id":      "cpt-1600",
		"carrier":     "STDOUT",
		"trailer_id":  "trl-91",
		"seal_number": "SL-4471",
		"departed_at": "2026-11-06T16:01:30Z",
		"orders":      orders,
		"version":     6,
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type shipmentConsumerFixture struct {
	consumer      *ShipmentConfirmedConsumer
	orders        *memory.NetworkOrderRepo
	confirmations *memory.ShipmentConfirmationLog
	gateway       *countingGateway
	events        *recordingEvents
	confirm       *usecases.ConfirmNetworkOrderShipment
	logs          *bytes.Buffer
}

func newShipmentConsumerFixture(t *testing.T) *shipmentConsumerFixture {
	t.Helper()
	f := &shipmentConsumerFixture{
		orders:        memory.NewNetworkOrderRepo(),
		confirmations: memory.NewShipmentConfirmationLog(),
		gateway:       newCountingGateway(),
		events:        &recordingEvents{},
		logs:          &bytes.Buffer{},
	}
	f.confirm = &usecases.ConfirmNetworkOrderShipment{
		Orders:        f.orders,
		Gateway:       f.gateway,
		Events:        f.events,
		Clock:         fixedTime{time.Date(2026, 11, 6, 16, 5, 0, 0, time.UTC)},
		Confirmations: f.confirmations,
	}
	f.consumer = &ShipmentConfirmedConsumer{
		Orders:  f.orders,
		Confirm: f.confirm,
		Defer: &usecases.DeferShipmentConfirmation{
			Orders: f.orders,
			Clock:  fixedTime{time.Date(2026, 11, 6, 16, 5, 0, 0, time.UTC)},
		},
		Logger: slog.New(slog.NewTextHandler(f.logs, nil)),
		Now:    func() time.Time { return time.Date(2026, 11, 6, 16, 5, 0, 0, time.UTC) },
	}
	return f
}

func mustConfirm(t *testing.T, f *shipmentConsumerFixture, ref shared.NetworkRef) *networkorder.NetworkOrder {
	t.Helper()
	o, err := f.orders.FindByRef(context.Background(), ref)
	if err != nil {
		t.Fatalf("find %s: %v", ref, err)
	}
	if o == nil || o.State() != networkorder.StateConfirmed {
		t.Fatalf("order %s not CONFIRMED", ref)
	}
	return o
}

func TestShipmentConfirmedConsumer_ConfirmsTheMappedOrders(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")
	acknowledgedWithLocalOrder(t, f.orders, "po-2", "ord-88422")

	raw := shipmentEvent(t, "3f1d2c3b-4a5e-4b7c-8d9e-0a1b2c3d4e07", "load-7f3a",
		[]map[string]any{{"order_id": "ord-88421"}, {"order_id": "ord-88422"}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	mustConfirm(t, f, "po-1")
	mustConfirm(t, f, "po-2")
	for _, ref := range []shared.NetworkRef{"po-1", "po-2"} {
		if f.gateway.submits[ref] != 1 {
			t.Errorf("%s: network shipment confirmations = %d, want 1", ref, f.gateway.submits[ref])
		}
	}
	if len(f.events.events) != 2 {
		t.Fatalf("NetworkOrderShipmentConfirmed events = %d, want 2", len(f.events.events))
	}
	// ADR 0026's placeholder holds until the producer carries the count:
	// no package_count in the contract yet means one label per order.
	for _, ref := range []shared.NetworkRef{"po-1", "po-2"} {
		if got := f.gateway.packageFor[ref]; len(got) != 1 || got[0] != 1 {
			t.Errorf("%s: label package counts = %v, want [1]", ref, got)
		}
	}
	// The (load id, order id) ledger rows exist for both.
	for _, pair := range [][2]string{{"load-7f3a", "ord-88421"}, {"load-7f3a", "ord-88422"}} {
		ok, err := f.confirmations.Exists(context.Background(), pair[0], pair[1])
		if err != nil || !ok {
			t.Errorf("ledger(%s,%s) exists=%v err=%v, want true", pair[0], pair[1], ok, err)
		}
	}
}

func TestShipmentConfirmedConsumer_RedeliveryConfirmsNothingNew(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")

	raw := shipmentEvent(t, "evt-dup", "load-7f3a", []map[string]any{{"order_id": "ord-88421"}})
	for i := 0; i < 3; i++ { // at-least-once: the same message, three deliveries
		if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	mustConfirm(t, f, "po-1")
	if f.gateway.submits["po-1"] != 1 || f.gateway.labels["po-1"] != 1 {
		t.Fatalf("after 3 deliveries: submits=%d labels=%d, want 1/1", f.gateway.submits["po-1"], f.gateway.labels["po-1"])
	}
	if len(f.events.events) != 1 {
		t.Fatalf("events = %d, want 1", len(f.events.events))
	}
}

func TestShipmentConfirmedConsumer_EventThenManualEndpointConfirmExactlyOnce(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")

	raw := shipmentEvent(t, "evt-1", "load-7f3a", []map[string]any{{"order_id": "ord-88421"}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("event path: %v", err)
	}
	// The operator fallback fires for the same shipment afterwards (ADR
	// 0014 addendum: one release, both paths live). It must be a no-op.
	manual, err := f.confirm.Execute(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("manual path: %v", err)
	}
	if manual.State() != networkorder.StateConfirmed {
		t.Fatalf("manual path returned state %v, want CONFIRMED (the existing confirmation)", manual.State())
	}

	if f.gateway.submits["po-1"] != 1 || f.gateway.labels["po-1"] != 1 {
		t.Fatalf("across both paths: submits=%d labels=%d, want 1/1", f.gateway.submits["po-1"], f.gateway.labels["po-1"])
	}
	if len(f.events.events) != 1 {
		t.Fatalf("events across both paths = %d, want 1", len(f.events.events))
	}
}

func TestShipmentConfirmedConsumer_ManualThenEventConfirmExactlyOnce(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")

	if _, err := f.confirm.Execute(context.Background(), "po-1"); err != nil { // operator first
		t.Fatalf("manual path: %v", err)
	}
	raw := shipmentEvent(t, "evt-2", "load-7f3a", []map[string]any{{"order_id": "ord-88421"}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("event path: %v", err)
	}

	if f.gateway.submits["po-1"] != 1 || f.gateway.labels["po-1"] != 1 {
		t.Fatalf("across both paths: submits=%d labels=%d, want 1/1", f.gateway.submits["po-1"], f.gateway.labels["po-1"])
	}
	if len(f.events.events) != 1 {
		t.Fatalf("events across both paths = %d, want 1", len(f.events.events))
	}
}

func TestShipmentConfirmedConsumer_UnknownOrderIdIsSkippedAndLogged(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")

	// ord-777 has no NetworkOrder: a local order raised from customer
	// demand shares the load but is none of this context's business.
	raw := shipmentEvent(t, "evt-3", "load-7f3a",
		[]map[string]any{{"order_id": "ord-88421"}, {"order_id": "ord-777"}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("Handle: %v (an unknown order_id must not fail the message)", err)
	}

	mustConfirm(t, f, "po-1")
	if f.gateway.submits["po-1"] != 1 {
		t.Fatalf("submits = %d, want 1 (only the mapped order)", f.gateway.submits["po-1"])
	}
	if msg := f.logs.String(); !bytes.Contains([]byte(msg), []byte("ord-777")) || !bytes.Contains([]byte(msg), []byte("skipping")) {
		t.Errorf("skip log missing: %q", msg)
	}
}

func TestShipmentConfirmedConsumer_UnknownTypeIsIgnored(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")

	e := ce.New("1.0")
	e.SetID("evt-loadclosed")
	e.SetSource("/warehouse/outbound-shipping")
	e.SetType("com.warehouse.wms.outbound-shipping.outboundload.LoadClosed")
	e.SetSubject("load-7f3a")
	if err := e.SetData("application/json", map[string]any{"load_id": "load-7f3a", "package_count": 4}); err != nil {
		t.Fatal(err)
	}
	raw, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("unknown type must be a skip, got %v", err)
	}
	if o, _ := f.orders.FindByRef(context.Background(), "po-1"); o.State() != networkorder.StateAcknowledged {
		t.Fatalf("state = %v, want ACKNOWLEDGED (nothing confirmed)", o.State())
	}
	if len(f.events.events) != 0 || len(f.gateway.submits) != 0 {
		t.Fatalf("unknown type had effects: events=%d submits=%d", len(f.events.events), len(f.gateway.submits))
	}
}

func TestShipmentConfirmedConsumer_ReportsTheRealPackageCountWhenPresent(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")

	// The additive producer field is not in the merged contract yet; the
	// decode tolerates it (ADR 0014 addendum), and the day it ships the
	// real count flows without touching this consumer again.
	raw := shipmentEvent(t, "evt-4", "load-7f3a",
		[]map[string]any{{"order_id": "ord-88421", "package_count": 3}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	mustConfirm(t, f, "po-1")
	if got := f.gateway.packageFor["po-1"]; len(got) != 1 || got[0] != 3 {
		t.Fatalf("label package counts = %v, want [3]", got)
	}
}

func TestShipmentConfirmedConsumer_MalformedEventsAreDeadLetterClassified(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	acknowledgedWithLocalOrder(t, f.orders, "po-1", "ord-88421")
	ctx := context.Background()

	// Not a CloudEvent at all: deterministic, never retried, DLQ.
	if err := f.consumer.Handle(ctx, "load-7f3a", []byte(`{"flat":"envelope"}`)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("not-a-CloudEvent: err = %v, want ErrNotCloudEvent", err)
	}

	// A ShipmentConfirmed with no load id anywhere (payload and key both
	// empty) breaks the producer's contract.
	noLoad := shipmentEvent(t, "evt-5", "", []map[string]any{{"order_id": "ord-88421"}})
	if err := f.consumer.Handle(ctx, "", noLoad); !errors.Is(err, errBadShipmentEvent) {
		t.Errorf("no load_id: err = %v, want errBadShipmentEvent", err)
	}

	// An orders[] entry without order_id breaks the contract's required
	// field and must not silently confirm nothing for that load.
	noOrderID := shipmentEvent(t, "evt-6", "load-7f3a", []map[string]any{{"order_id": ""}})
	if err := f.consumer.Handle(ctx, "load-7f3a", noOrderID); !errors.Is(err, errBadShipmentEvent) {
		t.Errorf("no order_id: err = %v, want errBadShipmentEvent", err)
	}

	// None of the malformed messages confirmed anything.
	if o, _ := f.orders.FindByRef(ctx, "po-1"); o.State() != networkorder.StateAcknowledged {
		t.Fatalf("state = %v, want ACKNOWLEDGED", o.State())
	}
}

// seedOrder stores one order in state carrying order-management's id: the join
// the event makes.
func seedOrder(t *testing.T, orders *memory.NetworkOrderRepo, ref shared.NetworkRef, localOrderId shared.LocalOrderId, state networkorder.State) *networkorder.NetworkOrder {
	t.Helper()
	id := localOrderId
	o := networkorder.Rehydrate(
		ref, "site-1",
		time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC),
		nil, state, &id,
	)
	if err := orders.Save(context.Background(), o); err != nil {
		t.Fatalf("seed %s: %v", ref, err)
	}
	return o
}

// A load that departs while its order is still SUBMITTED is a race, not a
// broken contract: Handle returns nil, which handleWithDeadLetter commits
// without a dead-letter message, and the shipment waits on the order.
func TestShipmentConfirmedConsumer_AnEarlyShipmentIsDeferredNotDeadLettered(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	o := seedOrder(t, f.orders, "po-1", "ord-88421", networkorder.StateSubmitted)

	raw := shipmentEvent(t, "evt-early", "load-7f3a", []map[string]any{{"order_id": "ord-88421", "package_count": 3}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("Handle: an early shipment must be handled without error (so it is committed, not dead-lettered), got %v", err)
	}

	pending, ok := o.PendingShipment()
	if !ok || pending.ShipmentRef != "load-7f3a" || pending.PackageCount != 3 {
		t.Fatalf("pending = %+v ok=%v, want {load-7f3a 3}", pending, ok)
	}
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED until the acknowledgement settles", o.State())
	}
	if f.gateway.labels["po-1"] != 0 || f.gateway.submits["po-1"] != 0 || len(f.events.events) != 0 {
		t.Fatalf("deferring must touch neither the network nor the event stream: labels %d submits %d events %d",
			f.gateway.labels["po-1"], f.gateway.submits["po-1"], len(f.events.events))
	}
	for _, want := range []string{"deferred", "ce_id=evt-early", "load_id=load-7f3a", "order_id=ord-88421"} {
		if !strings.Contains(f.logs.String(), want) {
			t.Errorf("the Info log lacks %q: %s", want, f.logs.String())
		}
	}

	// At-least-once: the redelivered event changes nothing and still succeeds.
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
}

// A mixed load: the order that is ready is confirmed, the early one deferred,
// and neither blocks the other.
func TestShipmentConfirmedConsumer_AnEarlyOrderDoesNotBlockTheRestOfTheLoad(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	early := seedOrder(t, f.orders, "po-1", "ord-1", networkorder.StateSubmitted)
	acknowledgedWithLocalOrder(t, f.orders, "po-2", "ord-2")

	raw := shipmentEvent(t, "evt-mixed", "load-9", []map[string]any{{"order_id": "ord-1"}, {"order_id": "ord-2"}})
	if err := f.consumer.Handle(context.Background(), "load-9", raw); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if _, ok := early.PendingShipment(); !ok {
		t.Fatal("po-1 must carry the deferred shipment")
	}
	mustConfirm(t, f, "po-2")
}

// NEW and REJECTED are a shipped order the network never took: an operator's
// anomaly. The confirm error stands, so handleWithDeadLetter retries and then
// dead-letters exactly as before.
func TestShipmentConfirmedConsumer_AnOrderTheNetworkNeverTookStillFails(t *testing.T) {
	for _, state := range []networkorder.State{networkorder.StateNew, networkorder.StateRejected} {
		f := newShipmentConsumerFixture(t)
		o := seedOrder(t, f.orders, "po-1", "ord-1", state)

		raw := shipmentEvent(t, "evt-anomaly", "load-7f3a", []map[string]any{{"order_id": "ord-1"}})
		err := f.consumer.Handle(context.Background(), "load-7f3a", raw)
		if !errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge) {
			t.Errorf("%s: err = %v, want ErrConfirmBeforeAcknowledge (retried, then dead-lettered)", state, err)
		}
		if _, ok := o.PendingShipment(); ok {
			t.Errorf("%s: nothing may be recorded", state)
		}
	}
}

// A consumer built without the Defer collaborator keeps the old behaviour
// instead of dereferencing nil in its goroutine.
func TestShipmentConfirmedConsumer_WithoutADeferrerAnEarlyShipmentFailsAsBefore(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	f.consumer.Defer = nil
	seedOrder(t, f.orders, "po-1", "ord-1", networkorder.StateSubmitted)

	raw := shipmentEvent(t, "evt-1", "load-7f3a", []map[string]any{{"order_id": "ord-1"}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); !errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge) {
		t.Fatalf("err = %v, want ErrConfirmBeforeAcknowledge", err)
	}
}

// Anything but the ordering race while deferring is an ordinary failure.
func TestShipmentConfirmedConsumer_ADeferFailureIsReturnedForTheRetry(t *testing.T) {
	f := newShipmentConsumerFixture(t)
	seedOrder(t, f.orders, "po-1", "ord-1", networkorder.StateSubmitted)
	boom := errors.New("storage is down")
	f.consumer.Defer.Orders = failingSaveRepo{NetworkOrderRepo: f.orders, err: boom}

	raw := shipmentEvent(t, "evt-1", "load-7f3a", []map[string]any{{"order_id": "ord-1"}})
	if err := f.consumer.Handle(context.Background(), "load-7f3a", raw); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the storage error", err)
	}
}

type failingSaveRepo struct {
	ports.NetworkOrderRepo
	err error
}

func (r failingSaveRepo) Save(context.Context, *networkorder.NetworkOrder) error { return r.err }
