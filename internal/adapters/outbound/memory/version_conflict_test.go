package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func order(t *testing.T, ref string) *networkorder.NetworkOrder {
	t.Helper()
	local := shared.LocalOrderId("ord-" + ref)
	return networkorder.Rehydrate(shared.NetworkRef(ref), "site-1",
		time.Now().Add(48*time.Hour), time.Now().Add(time.Hour), time.Now(),
		nil, networkorder.StateNew, &local)
}

// TestSaveRejectsStaleVersion mirrors the Postgres conditional UPDATE from
// migration 0013: a second instance of the same ref, loaded before another
// writer's save landed, must be refused instead of silently overwriting it.
func TestSaveRejectsStaleVersion(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewNetworkOrderRepo()

	winner := order(t, "po-race")
	if err := repo.Save(ctx, winner); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	stale := order(t, "po-race") // same ref, still at version 0
	err := repo.Save(ctx, stale)
	if !errors.Is(err, networkorder.ErrVersionConflict) {
		t.Fatalf("stale Save err = %v, want ErrVersionConflict", err)
	}

	stale.RestoreVersion(winner.Version())
	if err := repo.Save(ctx, stale); err != nil {
		t.Fatalf("Save after catching up: %v", err)
	}
	if got := stale.Version(); got != winner.Version()+1 {
		t.Fatalf("stale.Version() = %d, want %d", got, winner.Version()+1)
	}
}
