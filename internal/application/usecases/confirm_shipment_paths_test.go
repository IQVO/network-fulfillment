package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// These tests prove the ONE-use-case rule of the ADR 0014 addendum: the
// event path (ExecuteForShipment, load id as the shipment reference) and
// the manual endpoint path (Execute, NetworkRef as the shipment reference)
// both confirm the same shipment exactly once — one label request, one
// network submission, one NetworkOrderShipmentConfirmed — whichever order
// they arrive in, and however often each is retried.

func spyConfirmFixture(t *testing.T) (*usecases.ConfirmNetworkOrderShipment, *spyGateway, *recordingPublisher) {
	t.Helper()
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1") // links local order ord-1
	gw := newSpyGateway()
	pub := &recordingPublisher{}
	uc := f.confirmShipment()
	uc.Gateway = gw
	uc.Events = pub
	return uc, gw, pub
}

func countCalls(g *spyGateway, name string) int {
	n := 0
	for _, c := range g.calls {
		if c == name {
			n++
		}
	}
	return n
}

func TestConfirmShipment_EventThenManualPathConfirmExactlyOnce(t *testing.T) {
	uc, gw, pub := spyConfirmFixture(t)
	ctx := context.Background()

	// The event path: outbound-shipping's load load-7f3a carried ord-1.
	if _, err := uc.ExecuteForShipment(ctx, "po-1", "load-7f3a", 0); err != nil {
		t.Fatalf("event path: %v", err)
	}
	// The operator fallback fires for the same order afterwards; a
	// duplicate submission must be a no-op returning the existing
	// confirmation, never a second submission to the network.
	manual, err := uc.Execute(ctx, "po-1")
	if err != nil {
		t.Fatalf("manual path: %v", err)
	}
	if manual.State() != networkorder.StateConfirmed {
		t.Fatalf("manual path returned %v, want the existing CONFIRMED", manual.State())
	}
	// ...and the event redelivered once more for good measure.
	if _, err := uc.ExecuteForShipment(ctx, "po-1", "load-7f3a", 0); err != nil {
		t.Fatalf("event redelivery: %v", err)
	}

	if got := countCalls(gw, "ship"); got != 1 {
		t.Fatalf("network shipment confirmations = %d, want exactly 1", got)
	}
	if got := countCalls(gw, "label"); got != 1 {
		t.Fatalf("label requests = %d, want exactly 1", got)
	}
	if len(pub.events) != 1 {
		t.Fatalf("NetworkOrderShipmentConfirmed events = %d, want exactly 1", len(pub.events))
	}
}

func TestConfirmShipment_ManualThenEventPathConfirmExactlyOnce(t *testing.T) {
	uc, gw, pub := spyConfirmFixture(t)
	ctx := context.Background()

	if _, err := uc.Execute(ctx, "po-1"); err != nil { // operator first
		t.Fatalf("manual path: %v", err)
	}
	if _, err := uc.ExecuteForShipment(ctx, "po-1", "load-7f3a", 0); err != nil {
		t.Fatalf("event path after the operator: %v", err)
	}

	if got := countCalls(gw, "ship"); got != 1 {
		t.Fatalf("network shipment confirmations = %d, want exactly 1", got)
	}
	if got := countCalls(gw, "label"); got != 1 {
		t.Fatalf("label requests = %d, want exactly 1", got)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want exactly 1", len(pub.events))
	}
}

func TestConfirmShipment_SecondLoadNamingAConfirmedOrderIsNoOp(t *testing.T) {
	uc, gw, _ := spyConfirmFixture(t)
	ctx := context.Background()

	if _, err := uc.ExecuteForShipment(ctx, "po-1", "load-1", 0); err != nil {
		t.Fatalf("first load: %v", err)
	}
	// A different shipment naming the same order (a re-published or split
	// load): the order is terminally CONFIRMED, so this is a no-op that
	// must not claim load-2 as this order's confirming shipment.
	o, err := uc.ExecuteForShipment(ctx, "po-1", "load-2", 0)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if o.State() != networkorder.StateConfirmed {
		t.Fatalf("state = %v, want CONFIRMED", o.State())
	}
	if got := countCalls(gw, "ship"); got != 1 {
		t.Fatalf("network shipment confirmations = %d, want exactly 1", got)
	}
}

func TestConfirmShipment_LedgerKeyIsShipmentRefAndOrderId(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")
	ctx := context.Background()

	uc := f.confirmShipment()
	if _, err := uc.ExecuteForShipment(ctx, "po-1", "load-7f3a", 2); err != nil {
		t.Fatalf("ExecuteForShipment: %v", err)
	}

	// The key is (load id, the order-management OrderId the event joined
	// on) — not the NetworkRef, and not the CloudEvents id.
	for _, key := range [][2]string{{"load-7f3a", "ord-1"}} {
		ok, err := f.confirmations.Exists(ctx, key[0], key[1])
		if err != nil || !ok {
			t.Errorf("ledger(%v) exists=%v err=%v, want true", key, ok, err)
		}
	}
	for _, key := range [][2]string{{"load-7f3a", "po-1"}, {"po-1", "ord-1"}} {
		ok, err := f.confirmations.Exists(ctx, key[0], key[1])
		if err != nil {
			t.Fatalf("Exists(%v): %v", key, err)
		}
		if ok {
			t.Errorf("ledger(%v) exists, want false: the key is load id + order id", key)
		}
	}
}

func TestConfirmShipment_PackageCountFallsBackToSinglePackage(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")

	gw := newSpyGateway()
	uc := f.confirmShipment()
	uc.Gateway = gw
	if _, err := uc.ExecuteForShipment(context.Background(), "po-1", "load-7f3a", 0); err != nil {
		t.Fatalf("ExecuteForShipment: %v", err)
	}
	if len(gw.labelReqs) != 1 || gw.labelReqs[0] != 1 {
		t.Fatalf("label package counts = %v, want [1] (ADR 0026 placeholder)", gw.labelReqs)
	}
}
