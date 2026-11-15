package memory_test

import (
	"context"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
)

// The in-memory index behaves like the Postgres map (migration 0006): an
// unknown number is (_, false, nil), and a number keeps the first ref it was
// given, as `ON CONFLICT (po_number) DO NOTHING` does.
func TestPurchaseOrderIndex_FindsARegisteredNumberAndOnlyThat(t *testing.T) {
	idx := memory.NewPurchaseOrderIndex()
	ctx := context.Background()

	if ref, ok, err := idx.FindNetworkRefByPONumber(ctx, "PO-1"); err != nil || ok || ref != "" {
		t.Fatalf("an unregistered number = (%q, %v, %v), want (\"\", false, nil)", ref, ok, err)
	}
	idx.Register("PO-1", "NR-1")
	idx.Register("PO-2", "NR-2")

	for po, want := range map[string]string{"PO-1": "NR-1", "PO-2": "NR-2"} {
		ref, ok, err := idx.FindNetworkRefByPONumber(ctx, po)
		if err != nil || !ok || string(ref) != want {
			t.Fatalf("%s = (%q, %v, %v), want (%q, true, nil)", po, ref, ok, err, want)
		}
	}
	if _, ok, _ := idx.FindNetworkRefByPONumber(ctx, "NR-1"); ok {
		t.Fatal("a networkRef is not a purchase-order number: the index must not answer for it")
	}
}

func TestPurchaseOrderIndex_ANumberKeepsTheFirstRefItWasGiven(t *testing.T) {
	idx := memory.NewPurchaseOrderIndex()
	idx.Register("PO-1", "NR-first")
	idx.Register("PO-1", "NR-second")

	ref, ok, err := idx.FindNetworkRefByPONumber(context.Background(), "PO-1")
	if err != nil || !ok || ref != "NR-first" {
		t.Fatalf("PO-1 = (%q, %v, %v), want NR-first: a re-delivered purchase order finds the same ref", ref, ok, err)
	}
}
