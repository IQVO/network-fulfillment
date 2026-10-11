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
