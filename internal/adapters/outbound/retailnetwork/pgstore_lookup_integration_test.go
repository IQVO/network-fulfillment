//go:build integration

package retailnetwork_test

import (
	"context"
	"testing"
)

// FindNetworkRefByPONumber is the lookup of ADR 0036: the reverse of
// PurchaseOrder, read from the same map (migration 0006) SavePurchaseOrder
// writes. It reads and never mints: asking about a number must not create it.
func TestPGStore_FindNetworkRefByPONumberReadsTheMapAndNeverWritesIt(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	rows := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_purchase_orders`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	if ref, ok, err := store.FindNetworkRefByPONumber(ctx, "PO-7f3c"); err != nil || ok || ref != "" {
		t.Fatalf("a number nothing recorded = (%q, %v, %v), want (\"\", false, nil)", ref, ok, err)
	}
	if n := rows(); n != 0 {
		t.Fatalf("looking up an unknown number wrote %d row(s): a read must never mint a ref", n)
	}

	first, err := store.SavePurchaseOrder(ctx, samplePO("PO-7f3c"))
	if err != nil {
		t.Fatalf("SavePurchaseOrder: %v", err)
	}
	second, err := store.SavePurchaseOrder(ctx, samplePO("PO-9a10"))
	if err != nil {
		t.Fatalf("SavePurchaseOrder: %v", err)
	}
	for po, want := range map[string]string{"PO-7f3c": string(first), "PO-9a10": string(second)} {
		got, ok, err := store.FindNetworkRefByPONumber(ctx, po)
		if err != nil || !ok || string(got) != want {
			t.Fatalf("%s = (%q, %v, %v), want (%q, true, nil)", po, got, ok, err, want)
		}
	}

	// A re-delivered purchase order is filed under the same ref, so the lookup
	// keeps answering it.
	again, err := store.SavePurchaseOrder(ctx, samplePO("PO-7f3c"))
	if err != nil || again != first {
		t.Fatalf("a re-delivery = (%q, %v), want the same ref %q", again, err, first)
	}
	if got, _, _ := store.FindNetworkRefByPONumber(ctx, "PO-7f3c"); got != first {
		t.Fatalf("after a re-delivery PO-7f3c = %q, want %q", got, first)
	}

	// Exact match only: not another case, not a prefix, not the ref itself.
	for _, po := range []string{"po-7f3c", "PO-7f3", "PO-7f3c ", "", string(first)} {
		if ref, ok, err := store.FindNetworkRefByPONumber(ctx, po); err != nil || ok || ref != "" {
			t.Fatalf("%q = (%q, %v, %v), want (\"\", false, nil)", po, ref, ok, err)
		}
	}

	// It is the reverse of PurchaseOrder.
	ref, _, _ := store.FindNetworkRefByPONumber(ctx, "PO-9a10")
	if po, ok, err := store.PurchaseOrder(ctx, ref); err != nil || !ok || po.PoNumber != "PO-9a10" {
		t.Fatalf("PurchaseOrder(%q) = (%+v, %v, %v), want the purchase order PO-9a10", ref, po, ok, err)
	}
	if n := rows(); n != 2 {
		t.Fatalf("network_purchase_orders holds %d row(s), want the 2 saved: the lookups wrote nothing", n)
	}
}
