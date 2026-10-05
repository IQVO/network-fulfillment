//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestCapabilityOfferRepo_SaveAndListRoundTrip(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewCapabilityOfferRepo(pool)
	ctx := context.Background()

	sku := shared.SKU("sku-" + uniqueSuffix(t))
	now := time.Now().UTC().Truncate(time.Microsecond)

	o, err := capabilityoffer.New(sku, "site-1", 42, 100, capabilityoffer.BasisThroughputConstrained, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	var found *capabilityoffer.CapabilityOffer
	for i := range all {
		if all[i].SKU() == sku {
			found = &all[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("ListAll did not return the saved offer for sku %q", sku)
	}
	if found.AdvertisedQuantity() != 42 {
		t.Errorf("AdvertisedQuantity = %d, want 42", found.AdvertisedQuantity())
	}
	if found.Basis() != capabilityoffer.BasisThroughputConstrained {
		t.Errorf("Basis = %q, want THROUGHPUT_CONSTRAINED", found.Basis())
	}
	if found.SiteId() != "site-1" {
		t.Errorf("SiteId = %q, want site-1", found.SiteId())
	}
	if !found.ComputedAt().Equal(now) {
		t.Errorf("ComputedAt = %v, want %v", found.ComputedAt(), now)
	}
}

// TestCapabilityOfferRepo_SaveUpsertsOnSameKey asserts a recompute pass's
// Save genuinely replaces the one current snapshot for (sku, siteId),
// never appending a second row (CapabilityOffer's own doc comment: "a
// pure, replaceable snapshot, fully superseded by its own next
// recompute").
func TestCapabilityOfferRepo_SaveUpsertsOnSameKey(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewCapabilityOfferRepo(pool)
	ctx := context.Background()

	sku := shared.SKU("sku-" + uniqueSuffix(t))
	now := time.Now().UTC().Truncate(time.Microsecond)

	first, err := capabilityoffer.New(sku, "site-1", 10, 10, capabilityoffer.BasisPhysical, now)
	if err != nil {
		t.Fatalf("New (first): %v", err)
	}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("Save (first): %v", err)
	}

	second, err := capabilityoffer.New(sku, "site-1", 3, 3, capabilityoffer.BasisThroughputConstrained, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("New (second): %v", err)
	}
	if err := repo.Save(ctx, second); err != nil {
		t.Fatalf("Save (second): %v", err)
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	count := 0
	var latest capabilityoffer.CapabilityOffer
	for _, o := range all {
		if o.SKU() == sku {
			count++
			latest = o
		}
	}
	if count != 1 {
		t.Fatalf("rows for sku %q = %d, want 1 (upsert, not append)", sku, count)
	}
	if latest.AdvertisedQuantity() != 3 || latest.Basis() != capabilityoffer.BasisThroughputConstrained {
		t.Fatalf("latest = %+v, want AdvertisedQuantity=3 Basis=THROUGHPUT_CONSTRAINED", latest)
	}
}

// TestCapabilityOfferRepo_DistinctSitesAreDistinctRows asserts the
// primary key is genuinely composite: the same SKU at two sites must
// both survive a Save of the other.
func TestCapabilityOfferRepo_DistinctSitesAreDistinctRows(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewCapabilityOfferRepo(pool)
	ctx := context.Background()

	sku := shared.SKU("sku-" + uniqueSuffix(t))
	now := time.Now().UTC().Truncate(time.Microsecond)

	a, _ := capabilityoffer.New(sku, "site-a", 1, 1, capabilityoffer.BasisPhysical, now)
	b, _ := capabilityoffer.New(sku, "site-b", 2, 2, capabilityoffer.BasisPhysical, now)
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if err := repo.Save(ctx, b); err != nil {
		t.Fatalf("Save b: %v", err)
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	sites := map[shared.SiteId]int{}
	for _, o := range all {
		if o.SKU() == sku {
			sites[o.SiteId()] = o.AdvertisedQuantity()
		}
	}
	if sites["site-a"] != 1 || sites["site-b"] != 2 {
		t.Fatalf("sites = %v, want site-a=1 site-b=2", sites)
	}
}

func uniqueSuffix(t *testing.T) string {
	t.Helper()
	return time.Now().Format("150405.000000000")
}
