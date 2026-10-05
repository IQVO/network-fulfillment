package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
)

func TestCapabilityOfferRepo_SaveUpsertsByKey(t *testing.T) {
	repo := memory.NewCapabilityOfferRepo()
	now := time.Now().UTC()

	o1, err := capabilityoffer.New("sku-1", "site-1", 10, 10, capabilityoffer.BasisPhysical, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(context.Background(), o1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Re-save with a different quantity for the SAME (sku, site): must
	// replace, never append.
	o2, err := capabilityoffer.New("sku-1", "site-1", 5, 5, capabilityoffer.BasisPhysical, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(context.Background(), o2); err != nil {
		t.Fatalf("Save: %v", err)
	}

	all, err := repo.ListAll(context.Background())
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("len(all) = %d, want 1 (upsert, not append)", len(all))
	}
	if all[0].AdvertisedQuantity() != 5 {
		t.Fatalf("AdvertisedQuantity = %d, want 5 (the latest save)", all[0].AdvertisedQuantity())
	}
}

func TestCapabilityOfferRepo_DistinctSitesAreSeparateRows(t *testing.T) {
	repo := memory.NewCapabilityOfferRepo()
	now := time.Now().UTC()

	a, _ := capabilityoffer.New("sku-1", "site-1", 1, 1, capabilityoffer.BasisPhysical, now)
	b, _ := capabilityoffer.New("sku-1", "site-2", 2, 2, capabilityoffer.BasisPhysical, now)
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if err := repo.Save(context.Background(), b); err != nil {
		t.Fatalf("Save b: %v", err)
	}

	all, err := repo.ListAll(context.Background())
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len(all) = %d, want 2 (same SKU, different sites)", len(all))
	}
}

func TestCapabilityOfferRepo_ListAllOnEmptyRepoReturnsEmpty(t *testing.T) {
	repo := memory.NewCapabilityOfferRepo()
	all, err := repo.ListAll(context.Background())
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("len(all) = %d, want 0", len(all))
	}
}

func TestProductTranslation_KnownSKUsDeduplicates(t *testing.T) {
	tr := memory.NewProductTranslation()
	tr.Add("ASIN-1", "SKU-1")
	tr.Add("ASIN-2", "SKU-1") // two network products map to the same SKU
	tr.Add("ASIN-3", "SKU-2")

	skus, err := tr.KnownSKUs(context.Background())
	if err != nil {
		t.Fatalf("KnownSKUs: %v", err)
	}
	if len(skus) != 2 {
		t.Fatalf("len(skus) = %d, want 2 (deduplicated)", len(skus))
	}
}

func TestProductTranslation_KnownSKUsEmptyWhenNoneLoaded(t *testing.T) {
	tr := memory.NewProductTranslation()
	skus, err := tr.KnownSKUs(context.Background())
	if err != nil {
		t.Fatalf("KnownSKUs: %v", err)
	}
	if len(skus) != 0 {
		t.Fatalf("len(skus) = %d, want 0", len(skus))
	}
}
