package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func newOrder(t *testing.T, ref string, receivedAt time.Time) *networkorder.NetworkOrder {
	t.Helper()
	line, err := networkorder.NewLine("1", "ASIN-1", "sku-1", 1)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	o, err := networkorder.Receive(shared.NetworkRef(ref), "site-1", receivedAt.Add(48*time.Hour), []networkorder.Line{line}, receivedAt)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return o
}

// apis/openapi.yaml: the unanswered list is "soonest deadline first" - the
// same order the Postgres adapter returns (ORDER BY acknowledge_by).
func TestNetworkOrderRepo_ListUnansweredIsSoonestDeadlineFirst(t *testing.T) {
	repo := memory.NewNetworkOrderRepo()
	base := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// Saved out of deadline order, with refs that sort differently from
	// their deadlines, and a tie on the deadline broken by ref.
	for _, o := range []*networkorder.NetworkOrder{
		newOrder(t, "po-b", base.Add(2*time.Hour)),
		newOrder(t, "po-z", base),
		newOrder(t, "po-a", base.Add(2*time.Hour)),
		newOrder(t, "po-m", base.Add(time.Hour)),
	} {
		if err := repo.Save(ctx, o); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	got, err := repo.ListUnanswered(ctx)
	if err != nil {
		t.Fatalf("ListUnanswered: %v", err)
	}
	want := []shared.NetworkRef{"po-z", "po-m", "po-a", "po-b"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, ref := range want {
		if got[i].NetworkRef() != ref {
			t.Fatalf("position %d = %s, want %s (full order must be deadline, then ref)", i, got[i].NetworkRef(), ref)
		}
	}
}

// pendingOrder is an order in state carrying an early shipment noted at noted.
func pendingOrder(t *testing.T, ref string, state networkorder.State, noted time.Time) *networkorder.NetworkOrder {
	t.Helper()
	base := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	o := networkorder.Rehydrate(shared.NetworkRef(ref), "site-1", base.Add(48*time.Hour), base.Add(24*time.Hour), base,
		[]networkorder.Line{mustLine(t)}, state, nil)
	o.RestorePendingShipment(networkorder.PendingShipment{ShipmentRef: "load-" + ref, NotedAt: noted})
	return o
}

func mustLine(t *testing.T) networkorder.Line {
	t.Helper()
	line, err := networkorder.NewLine("1", "ASIN-1", "sku-1", 1)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return line
}

// ListPendingShipments mirrors the Postgres query: only orders carrying a
// pending shipment, those no longer SUBMITTED first (a SUBMITTED one is skipped
// by the pass, so it must never crowd out an order that can be settled), then
// oldest noted first, ties by ref, cut at the limit.
func TestNetworkOrderRepo_ListPendingShipmentsIsSettleableFirstThenOldestFirst(t *testing.T) {
	repo := memory.NewNetworkOrderRepo()
	base := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()

	for _, o := range []*networkorder.NetworkOrder{
		pendingOrder(t, "po-submitted-old", networkorder.StateSubmitted, base),
		pendingOrder(t, "po-ack-late", networkorder.StateAcknowledged, base.Add(3*time.Hour)),
		pendingOrder(t, "po-ack-early-b", networkorder.StateAcknowledged, base.Add(time.Hour)),
		pendingOrder(t, "po-ack-early-a", networkorder.StateAcknowledged, base.Add(time.Hour)),
		pendingOrder(t, "po-rejected", networkorder.StateRejected, base.Add(2*time.Hour)),
		newOrder(t, "po-no-pending", base),
	} {
		if err := repo.Save(ctx, o); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	got, err := repo.ListPendingShipments(ctx, 100)
	if err != nil {
		t.Fatalf("ListPendingShipments: %v", err)
	}
	want := []shared.NetworkRef{"po-ack-early-a", "po-ack-early-b", "po-rejected", "po-ack-late", "po-submitted-old"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (the order without a pending shipment must not be listed)", len(got), len(want))
	}
	for i, ref := range want {
		if got[i].NetworkRef() != ref {
			t.Fatalf("position %d = %s, want %s", i, got[i].NetworkRef(), ref)
		}
	}

	limited, err := repo.ListPendingShipments(ctx, 2)
	if err != nil || len(limited) != 2 || limited[0].NetworkRef() != "po-ack-early-a" {
		t.Fatalf("limit 2: %v %v, want the first two of the same order", limited, err)
	}
	if none, err := repo.ListPendingShipments(ctx, 0); err != nil || len(none) != 0 {
		t.Fatalf("limit 0: %v %v, want nothing", none, err)
	}
}
