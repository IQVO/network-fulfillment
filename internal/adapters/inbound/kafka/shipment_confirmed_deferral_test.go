package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The deferral of ADR 0014 addendum D7: a ShipmentConfirmed whose order is still
// SUBMITTED is recorded on the order and committed, never dead-lettered.

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
