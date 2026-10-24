//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func inboundShipmentFixture(ref string, shipped time.Time) contract.InboundShipment {
	return contract.InboundShipment{
		Ref: ref, OwnerRef: "S-100", SiteId: "site-1", ShippedAt: shipped,
		Cartons: []contract.InboundCarton{{
			SSCC: "006141410000000012", ParentSSCC: "006141410000000036", Kind: contract.CartonHomogeneousCase,
			Dims:     &contract.CartonDims{LengthMm: 400, WidthMm: 300, HeightMm: 200},
			Contents: []contract.InboundContent{{GTIN: "00614141000036", QtyEaches: 12, Lot: "L-77", Expiry: "2027-03-01"}},
		}},
	}
}

func TestInboundShipmentRepo_RecordIsIdempotentAndRoundTripsTheTree(t *testing.T) {
	repo := postgres.NewInboundShipmentRepo(newDB(t))
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	ref := "SHP-" + uniqueSuffix(t)
	s := inboundShipmentFixture(ref, now.Add(-time.Hour).Truncate(time.Microsecond))

	if fresh, err := repo.Record(ctx, s, "NF-"+ref, now); err != nil || !fresh {
		t.Fatalf("first Record = %v, %v", fresh, err)
	}
	changed := s
	changed.OwnerRef = "S-OTHER"
	if fresh, err := repo.Record(ctx, changed, "NF-"+ref, now); err != nil || fresh {
		t.Fatalf("second Record = %v, %v: a replay must leave the row as it is", fresh, err)
	}
	due, err := repo.Due(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	var got *contract.InboundRecord
	for i := range due {
		if due[i].Shipment.Ref == ref {
			got = &due[i]
		}
	}
	if got == nil || got.AsnNumber != "NF-"+ref || got.Shipment.OwnerRef != "S-100" ||
		got.Shipment.Cartons[0].ParentSSCC != "006141410000000036" || got.Shipment.Cartons[0].Contents[0].Lot != "L-77" ||
		got.Shipment.Cartons[0].Dims == nil || got.Shipment.Cartons[0].Dims.HeightMm != 200 || !got.Shipment.ShippedAt.Equal(s.ShippedAt) {
		t.Fatalf("due record = %+v", got)
	}
}

func TestInboundShipmentRepo_StatusTransitionsAndBackoff(t *testing.T) {
	repo := postgres.NewInboundShipmentRepo(newDB(t))
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	refA, refB, refC := "A-"+uniqueSuffix(t), "B-"+uniqueSuffix(t), "C-"+uniqueSuffix(t)
	for _, ref := range []string{refA, refB, refC} {
		if _, err := repo.Record(ctx, inboundShipmentFixture(ref, now.Add(-time.Hour)), "NF-"+ref, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.MarkRetry(ctx, refA, "GTIN not found", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkRegistered(ctx, refB, now); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkRejected(ctx, refC, "duplicate-sscc"); err != nil {
		t.Fatal(err)
	}
	dueNow, _ := repo.Due(ctx, now, 100)
	for _, r := range dueNow {
		if r.Shipment.Ref == refA || r.Shipment.Ref == refB || r.Shipment.Ref == refC {
			t.Errorf("%s is due now, want none of them (backoff, registered, rejected)", r.Shipment.Ref)
		}
	}
	later, _ := repo.Due(ctx, now.Add(2*time.Minute), 100)
	var attempts int
	for _, r := range later {
		if r.Shipment.Ref == refA {
			attempts = r.Attempts
		}
		if r.Shipment.Ref == refB || r.Shipment.Ref == refC {
			t.Errorf("%s is due after the backoff, but it is closed", r.Shipment.Ref)
		}
	}
	if attempts != 1 {
		t.Errorf("attempts of the retried shipment = %d, want 1", attempts)
	}
	// A closed shipment is never reopened by a late retry or a second close.
	if err := repo.MarkRetry(ctx, refB, "late", now); err != nil {
		t.Fatal(err)
	}
	if due, _ := repo.Due(ctx, now.Add(time.Hour), 100); containsShipment(due, refB) {
		t.Error("a REGISTERED shipment became due again")
	}
}

func containsShipment(recs []contract.InboundRecord, ref string) bool {
	for _, r := range recs {
		if r.Shipment.Ref == ref {
			return true
		}
	}
	return false
}

func TestInboundShipmentRepo_DueIsOldestFirstAndBounded(t *testing.T) {
	repo := postgres.NewInboundShipmentRepo(newDB(t))
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	suffix := uniqueSuffix(t)
	for i, age := range []time.Duration{3 * time.Hour, 5 * time.Hour, 4 * time.Hour} {
		ref := string(rune('a'+i)) + "-" + suffix
		if _, err := repo.Record(ctx, inboundShipmentFixture(ref, now.Add(-age)), "NF-"+ref, now); err != nil {
			t.Fatal(err)
		}
	}
	due, err := repo.Due(ctx, now, 2)
	if err != nil || len(due) != 2 {
		t.Fatalf("due = %d, %v, want 2", len(due), err)
	}
	if !due[0].Shipment.ShippedAt.Before(due[1].Shipment.ShippedAt) {
		t.Errorf("not oldest first: %v then %v", due[0].Shipment.ShippedAt, due[1].Shipment.ShippedAt)
	}
}

func TestInboundShipmentRepo_CursorOnlyMovesForward(t *testing.T) {
	repo := postgres.NewInboundShipmentRepo(newDB(t))
	ctx := context.Background()
	site := "site-" + uniqueSuffix(t)
	if got, err := repo.Cursor(ctx, shared.SiteId(site)); err != nil || !got.IsZero() {
		t.Fatalf("fresh cursor = %v, %v", got, err)
	}
	t1 := time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC)
	for _, to := range []time.Time{t1, t1.Add(-time.Hour), t1.Add(time.Hour)} {
		if err := repo.AdvanceCursor(ctx, shared.SiteId(site), to); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := repo.Cursor(ctx, shared.SiteId(site)); !got.Equal(t1.Add(time.Hour)) {
		t.Errorf("cursor = %v, want %v (an earlier value never moves it back)", got, t1.Add(time.Hour))
	}
}

func TestInboundShipmentRepo_GtinCacheKeepsAnswers(t *testing.T) {
	repo := postgres.NewInboundShipmentRepo(newDB(t))
	ctx := context.Background()
	gtin := "00614141000036"
	if _, ok, err := repo.CachedGtin(ctx, gtin); err != nil || ok {
		t.Fatalf("empty cache = %v, %v", ok, err)
	}
	r := contract.GtinResolution{GTIN: gtin, SKU: "SKU-A", Level: "EACH", UnitsPerPack: 1, Active: true}
	if err := repo.CacheGtin(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Active = false // retired: the same (sku, level), only the flag changes
	if err := repo.CacheGtin(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, ok, err := repo.CachedGtin(ctx, gtin)
	if err != nil || !ok || got.SKU != "SKU-A" || got.Level != "EACH" || got.Active {
		t.Fatalf("cached = %+v, %v, %v", got, ok, err)
	}
}
