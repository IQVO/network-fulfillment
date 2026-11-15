//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// orderWithPending builds an order in state the way a repository hands it back,
// optionally carrying an early shipment (ADR 0014 addendum D7).
func orderWithPending(t *testing.T, ref shared.NetworkRef, state networkorder.State, shipmentRef string, count int, noted time.Time) *networkorder.NetworkOrder {
	t.Helper()
	received := time.Now().UTC().Truncate(time.Microsecond).Add(-2 * time.Hour)
	local := shared.LocalOrderId("ord-" + string(ref))
	o := networkorder.Rehydrate(ref, "site-1", received.Add(48*time.Hour), received.Add(networkorder.AcknowledgementWindow), received,
		[]networkorder.Line{mustLine(t, "1", "ASIN-AAA", "sku-aaa", 1)}, state, &local)
	if shipmentRef != "" {
		o.RestorePendingShipment(networkorder.PendingShipment{ShipmentRef: shipmentRef, PackageCount: count, NotedAt: noted})
	}
	return o
}

// TestNetworkOrderRepo_PendingShipmentRoundTripsAndIsHeldByTheSchema covers the
// three columns of migration 0015: the mapper writes and reads them, the upsert
// clears them, the optimistic version rule still binds, and the CHECKs refuse a
// half-written shipment.
func TestNetworkOrderRepo_PendingShipmentRoundTripsAndIsHeldByTheSchema(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()
	noted := time.Now().UTC().Truncate(time.Microsecond)

	t.Run("a noted shipment is stored and read back", func(t *testing.T) {
		ref := uniqueRef("pend-rt")
		if err := repo.Save(ctx, orderWithPending(t, ref, networkorder.StateSubmitted, "load-7", 3, noted)); err != nil {
			t.Fatalf("Save: %v", err)
		}
		got, err := repo.FindByRef(ctx, ref)
		if err != nil || got == nil {
			t.Fatalf("FindByRef = %v, %v", got, err)
		}
		p, ok := got.PendingShipment()
		if !ok || p.ShipmentRef != "load-7" || p.PackageCount != 3 || !p.NotedAt.Equal(noted) {
			t.Fatalf("pending = %+v ok=%v, want {load-7 3 %v}", p, ok, noted)
		}
	})

	t.Run("a zero package count survives the round trip", func(t *testing.T) {
		ref := uniqueRef("pend-zero")
		if err := repo.Save(ctx, orderWithPending(t, ref, networkorder.StateSubmitted, "load-8", 0, noted)); err != nil {
			t.Fatalf("Save: %v", err)
		}
		got, _ := repo.FindByRef(ctx, ref)
		if p, ok := got.PendingShipment(); !ok || p.PackageCount != 0 {
			t.Fatalf("pending = %+v ok=%v, want a stored fact with count 0", p, ok)
		}
	})

	t.Run("confirming clears the three columns", func(t *testing.T) {
		ref := uniqueRef("pend-clear")
		if err := repo.Save(ctx, orderWithPending(t, ref, networkorder.StateAcknowledged, "load-9", 2, noted)); err != nil {
			t.Fatalf("Save: %v", err)
		}
		o, _ := repo.FindByRef(ctx, ref)
		if err := o.ConfirmShipment(); err != nil {
			t.Fatalf("ConfirmShipment: %v", err)
		}
		if err := repo.Save(ctx, o); err != nil {
			t.Fatalf("Save confirmed: %v", err)
		}
		var nulls int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM network_orders
			WHERE network_ref = $1 AND state = 'CONFIRMED'
			  AND pending_shipment_ref IS NULL AND pending_package_count IS NULL AND pending_shipment_noted_at IS NULL`,
			string(ref)).Scan(&nulls); err != nil || nulls != 1 {
			t.Fatalf("cleared columns = %d, %v, want the three columns NULL on the CONFIRMED row", nulls, err)
		}
		again, _ := repo.FindByRef(ctx, ref)
		if _, ok := again.PendingShipment(); ok {
			t.Fatal("a cleared shipment must not come back")
		}
	})

	t.Run("a stale writer is still refused", func(t *testing.T) {
		ref := uniqueRef("pend-stale")
		if err := repo.Save(ctx, orderWithPending(t, ref, networkorder.StateSubmitted, "", 0, time.Time{})); err != nil {
			t.Fatalf("Save: %v", err)
		}
		stale, _ := repo.FindByRef(ctx, ref)
		winner, _ := repo.FindByRef(ctx, ref)
		if err := winner.ConfirmAcknowledgement(); err != nil {
			t.Fatalf("ConfirmAcknowledgement: %v", err)
		}
		if err := repo.Save(ctx, winner); err != nil {
			t.Fatalf("winner Save: %v", err)
		}
		if err := stale.NoteShipmentBeforeAcknowledge("load-1", 1, noted); err != nil {
			t.Fatalf("note: %v", err)
		}
		if err := repo.Save(ctx, stale); !errors.Is(err, networkorder.ErrVersionConflict) {
			t.Fatalf("stale Save err = %v, want ErrVersionConflict: the early shipment must not overwrite the acknowledgement", err)
		}
	})

	t.Run("the CHECKs refuse a half-written shipment", func(t *testing.T) {
		ref := uniqueRef("pend-check")
		if err := repo.Save(ctx, orderWithPending(t, ref, networkorder.StateSubmitted, "", 0, time.Time{})); err != nil {
			t.Fatalf("Save: %v", err)
		}
		for name, set := range map[string]string{
			"only the reference":  `pending_shipment_ref = 'load-1'`,
			"only the count":      `pending_package_count = 1`,
			"only the instant":    `pending_shipment_noted_at = now()`,
			"reference and count": `pending_shipment_ref = 'load-1', pending_package_count = 1`,
			"a negative count":    `pending_shipment_ref = 'load-1', pending_package_count = -1, pending_shipment_noted_at = now()`,
			"a blank reference":   `pending_shipment_ref = '', pending_package_count = 1, pending_shipment_noted_at = now()`,
		} {
			if _, err := pool.Exec(ctx, `UPDATE network_orders SET `+set+` WHERE network_ref = $1`, string(ref)); err == nil {
				t.Errorf("%s: the row was accepted, want a CHECK violation", name)
			}
		}
		if _, err := pool.Exec(ctx, `
			UPDATE network_orders
			SET pending_shipment_ref = 'load-1', pending_package_count = 0, pending_shipment_noted_at = now()
			WHERE network_ref = $1`, string(ref)); err != nil {
			t.Errorf("all three set (count 0): %v, want it accepted", err)
		}
	})
}

// TestNetworkOrderRepo_ListPendingShipments pins the query: only orders that
// carry a shipment, those no longer SUBMITTED first, then oldest noted first,
// ties by ref, cut at the limit.
func TestNetworkOrderRepo_ListPendingShipments(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	seed := func(ref shared.NetworkRef, state networkorder.State, shipment string, noted time.Time) {
		t.Helper()
		if err := repo.Save(ctx, orderWithPending(t, ref, state, shipment, 1, noted)); err != nil {
			t.Fatalf("Save %s: %v", ref, err)
		}
	}
	seed("po-submitted-oldest", networkorder.StateSubmitted, "load-1", base)
	seed("po-ack-late", networkorder.StateAcknowledged, "load-2", base.Add(3*time.Minute))
	seed("po-ack-early-b", networkorder.StateAcknowledged, "load-3", base.Add(time.Minute))
	seed("po-ack-early-a", networkorder.StateAcknowledged, "load-4", base.Add(time.Minute))
	seed("po-rejected", networkorder.StateRejected, "load-5", base.Add(2*time.Minute))
	seed("po-none", networkorder.StateAcknowledged, "", time.Time{})

	got, err := repo.ListPendingShipments(ctx, 100)
	if err != nil {
		t.Fatalf("ListPendingShipments: %v", err)
	}
	want := []shared.NetworkRef{"po-ack-early-a", "po-ack-early-b", "po-rejected", "po-ack-late", "po-submitted-oldest"}
	if len(got) != len(want) {
		t.Fatalf("listed %d orders, want %d (the one without a shipment must not be listed)", len(got), len(want))
	}
	for i, ref := range want {
		if got[i].NetworkRef() != ref {
			t.Fatalf("position %d = %s, want %s", i, got[i].NetworkRef(), ref)
		}
		if _, ok := got[i].PendingShipment(); !ok {
			t.Errorf("%s was listed without its pending shipment loaded", ref)
		}
	}

	limited, err := repo.ListPendingShipments(ctx, 2)
	if err != nil || len(limited) != 2 || limited[0].NetworkRef() != "po-ack-early-a" || limited[1].NetworkRef() != "po-ack-early-b" {
		t.Fatalf("limit 2: %d orders, %v, want the first two of the same order", len(limited), err)
	}
	if none, err := repo.ListPendingShipments(ctx, 0); err != nil || len(none) != 0 {
		t.Fatalf("limit 0: %d orders, %v, want none", len(none), err)
	}
}
