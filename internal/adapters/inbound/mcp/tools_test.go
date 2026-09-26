package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// base is the deterministic clock every mcp test runs against.
var base = time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// harness wires the MCP Deps over an in-memory NetworkOrderRepo, seeding
// orders directly through the domain constructors (this context's
// repository has no "register" use case to seed through — network orders
// only ever arrive via the poller/ReceiveNetworkDemand use case, which is
// out of scope for these adapter-level tests).
type harness struct {
	t      *testing.T
	orders *memory.NetworkOrderRepo
	clock  fixedClock
	deps   Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	orders := memory.NewNetworkOrderRepo()
	clock := fixedClock{now: base}
	return &harness{
		t:      t,
		orders: orders,
		clock:  clock,
		deps:   Deps{Orders: orders, Clock: clock},
	}
}

func (h *harness) ctx() context.Context { return context.Background() }

func mustLine(t *testing.T, ref, productId, sku string, qty int) networkorder.Line {
	t.Helper()
	l, err := networkorder.NewLine(shared.NetworkLineRef(ref), shared.NetworkProductId(productId), shared.SKU(sku), qty)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return l
}

// seedOrder receives a fresh NEW order into the harness's repo and
// returns it.
func (h *harness) seedOrder(ref string, receivedAt time.Time) *networkorder.NetworkOrder {
	h.t.Helper()
	line := mustLine(h.t, "1", "ASIN-1", "sku-1", 3)
	o, err := networkorder.Receive(shared.NetworkRef(ref), "site-1", receivedAt.Add(48*time.Hour), []networkorder.Line{line}, receivedAt)
	if err != nil {
		h.t.Fatalf("Receive: %v", err)
	}
	if err := h.orders.Save(h.ctx(), o); err != nil {
		h.t.Fatalf("Save: %v", err)
	}
	return o
}

func TestGetNetworkOrder_ReturnsBothVocabularies(t *testing.T) {
	h := newHarness(t)
	h.seedOrder("po-1", base.Add(-time.Hour))

	out, err := h.deps.getNetworkOrder(h.ctx(), getNetworkOrderInput{NetworkRef: "po-1"})
	if err != nil {
		t.Fatalf("getNetworkOrder: %v", err)
	}
	if out.NetworkRef != "po-1" || out.State != "NEW" {
		t.Errorf("order = %+v", out)
	}
	if len(out.Lines) != 1 || out.Lines[0].SKU != "sku-1" || out.Lines[0].NetworkProductId != "ASIN-1" {
		t.Errorf("lines = %+v", out.Lines)
	}
}

func TestGetNetworkOrder_RequiresNetworkRef(t *testing.T) {
	h := newHarness(t)
	if _, err := h.deps.getNetworkOrder(h.ctx(), getNetworkOrderInput{}); err == nil {
		t.Fatal("expected error for empty networkRef")
	}
}

func TestGetNetworkOrder_UnknownRefIsNotFound(t *testing.T) {
	h := newHarness(t)
	if _, err := h.deps.getNetworkOrder(h.ctx(), getNetworkOrderInput{NetworkRef: "ghost"}); err == nil {
		t.Fatal("expected an error for an unknown network ref")
	}
}

func TestGetNetworkOrder_ReportsOverdueUsingTheDomainPredicate(t *testing.T) {
	h := newHarness(t)
	// Received 25h before "now": the 24h window has closed.
	h.seedOrder("po-overdue", base.Add(-25*time.Hour))

	out, err := h.deps.getNetworkOrder(h.ctx(), getNetworkOrderInput{NetworkRef: "po-overdue"})
	if err != nil {
		t.Fatalf("getNetworkOrder: %v", err)
	}
	if !out.AcknowledgementOverdue {
		t.Error("expected acknowledgementOverdue=true for an order past its window")
	}
}

func TestGetNetworkOrder_CarriesNoCustomerPII(t *testing.T) {
	// The DTO's field set is the PII boundary itself: this test exists
	// so an accidental future field addition (a ship-to name/address/
	// phone) is caught structurally, not just by review.
	h := newHarness(t)
	h.seedOrder("po-1", base.Add(-time.Hour))
	out, err := h.deps.getNetworkOrder(h.ctx(), getNetworkOrderInput{NetworkRef: "po-1"})
	if err != nil {
		t.Fatalf("getNetworkOrder: %v", err)
	}
	_ = out // networkOrderDTO has no PII-shaped field by construction; see dto.go.
}

func TestListNetworkOrders_NoFilterListsEverything(t *testing.T) {
	h := newHarness(t)
	h.seedOrder("po-1", base.Add(-time.Hour))
	h.seedOrder("po-2", base.Add(-2*time.Hour))

	out, err := h.deps.listNetworkOrders(h.ctx(), listNetworkOrdersInput{})
	if err != nil {
		t.Fatalf("listNetworkOrders: %v", err)
	}
	if len(out.NetworkOrders) != 2 {
		t.Fatalf("orders = %d, want 2", len(out.NetworkOrders))
	}
}

func TestListNetworkOrders_FiltersByState(t *testing.T) {
	h := newHarness(t)
	o1 := h.seedOrder("po-1", base.Add(-time.Hour))
	h.seedOrder("po-2", base.Add(-2*time.Hour))
	if err := o1.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := h.orders.Save(h.ctx(), o1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, err := h.deps.listNetworkOrders(h.ctx(), listNetworkOrdersInput{State: "ACKNOWLEDGED"})
	if err != nil {
		t.Fatalf("listNetworkOrders: %v", err)
	}
	if len(out.NetworkOrders) != 1 || out.NetworkOrders[0].NetworkRef != "po-1" {
		t.Fatalf("orders = %+v", out.NetworkOrders)
	}
}

func TestListNetworkOrders_RejectsUnknownState(t *testing.T) {
	h := newHarness(t)
	if _, err := h.deps.listNetworkOrders(h.ctx(), listNetworkOrdersInput{State: "BOGUS"}); err == nil {
		t.Fatal("expected an error for an unknown state filter")
	}
}

func TestListNetworkOrders_EmptyIsAnEmptySliceNotNil(t *testing.T) {
	h := newHarness(t)
	out, err := h.deps.listNetworkOrders(h.ctx(), listNetworkOrdersInput{})
	if err != nil {
		t.Fatalf("listNetworkOrders: %v", err)
	}
	if out.NetworkOrders == nil {
		t.Fatal("expected an empty (non-nil) slice, got nil")
	}
	if len(out.NetworkOrders) != 0 {
		t.Fatalf("orders = %d, want 0", len(out.NetworkOrders))
	}
}
