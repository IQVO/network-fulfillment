package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

type fakeTranslation struct {
	skus []shared.SKU
	err  error
}

func (f *fakeTranslation) ToSKU(context.Context, shared.NetworkProductId) (shared.SKU, error) {
	return "", nil
}
func (f *fakeTranslation) KnownSKUs(context.Context) ([]shared.SKU, error) { return f.skus, f.err }

type fakeInventory struct {
	usable map[shared.SKU]int
	errFor map[shared.SKU]error
}

func (f *fakeInventory) UsableQuantity(_ context.Context, sku shared.SKU) (int, error) {
	if err, ok := f.errFor[sku]; ok {
		return 0, err
	}
	return f.usable[sku], nil
}

type fakePathCap struct {
	next  contract.NextCutoff
	found bool
}

func (f *fakePathCap) NextCutoff(shared.SiteId, time.Time) (contract.NextCutoff, bool) {
	return f.next, f.found
}

type fakeCapacity struct {
	remaining map[string]int
	known     map[string]bool
}

func (f *fakeCapacity) RemainingCapacity(pathId string, _ time.Time) (int, bool) {
	return f.remaining[pathId], f.known[pathId]
}

type fakeOffersRepo struct {
	saved   []capabilityoffer.CapabilityOffer
	saveErr error
}

func (f *fakeOffersRepo) Save(_ context.Context, o capabilityoffer.CapabilityOffer) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, o)
	return nil
}
func (f *fakeOffersRepo) ListAll(context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	return f.saved, nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestRecomputeCapabilityOffers_PhysicalBasisWhenNoSchedule(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 100}},
		PathCap:     &fakePathCap{found: false},
		Capacity:    &fakeCapacity{},
		Offers:      &fakeOffersRepo{},
		Clock:       fixedClock{now: now},
		SiteId:      "site-1",
	}
	res, err := u.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Examined != 1 || res.ThroughputConstrained != 0 {
		t.Fatalf("res = %+v, want Examined=1 ThroughputConstrained=0", res)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if len(repo.saved) != 1 {
		t.Fatalf("saved = %d, want 1", len(repo.saved))
	}
	if repo.saved[0].AdvertisedQuantity() != 100 || repo.saved[0].Basis() != capabilityoffer.BasisPhysical {
		t.Fatalf("saved offer = %+v, want 100/PHYSICAL", repo.saved[0])
	}
}

func TestRecomputeCapabilityOffers_ThrottlesOnThroughput(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	cutoff := now.Add(2 * time.Hour)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400}},
		PathCap: &fakePathCap{found: true, next: contract.NextCutoff{
			CutoffAt: cutoff,
			Paths:    []contract.EligiblePath{{PathId: "path-1"}},
		}},
		Capacity: &fakeCapacity{
			remaining: map[string]int{"path-1": 120},
			known:     map[string]bool{"path-1": true},
		},
		Offers: &fakeOffersRepo{},
		Clock:  fixedClock{now: now},
		SiteId: "site-1",
	}
	res, err := u.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.ThroughputConstrained != 1 {
		t.Fatalf("ThroughputConstrained = %d, want 1", res.ThroughputConstrained)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if repo.saved[0].AdvertisedQuantity() != 120 || repo.saved[0].Basis() != capabilityoffer.BasisThroughputConstrained {
		t.Fatalf("saved offer = %+v, want 120/THROUGHPUT_CONSTRAINED", repo.saved[0])
	}
}

func TestRecomputeCapabilityOffers_SumsMultipleEligiblePaths(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	cutoff := now.Add(time.Hour)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400}},
		PathCap: &fakePathCap{found: true, next: contract.NextCutoff{
			CutoffAt: cutoff,
			Paths:    []contract.EligiblePath{{PathId: "path-1"}, {PathId: "path-2"}},
		}},
		Capacity: &fakeCapacity{
			remaining: map[string]int{"path-1": 50, "path-2": 30},
			known:     map[string]bool{"path-1": true, "path-2": true},
		},
		Offers: &fakeOffersRepo{},
		Clock:  fixedClock{now: now},
		SiteId: "site-1",
	}
	if _, err := u.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if repo.saved[0].AdvertisedQuantity() != 80 {
		t.Fatalf("AdvertisedQuantity = %d, want 80 (50+30)", repo.saved[0].AdvertisedQuantity())
	}
}

func TestRecomputeCapabilityOffers_UnknownPathCapacityIsSkippedFromSum(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	cutoff := now.Add(time.Hour)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400}},
		PathCap: &fakePathCap{found: true, next: contract.NextCutoff{
			CutoffAt: cutoff,
			Paths:    []contract.EligiblePath{{PathId: "path-1"}, {PathId: "path-2"}},
		}},
		Capacity: &fakeCapacity{
			remaining: map[string]int{"path-1": 50},
			known:     map[string]bool{"path-1": true, "path-2": false},
		},
		Offers: &fakeOffersRepo{},
		Clock:  fixedClock{now: now},
		SiteId: "site-1",
	}
	if _, err := u.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if repo.saved[0].AdvertisedQuantity() != 50 {
		t.Fatalf("AdvertisedQuantity = %d, want 50 (path-2 unknown contributes nothing)", repo.saved[0].AdvertisedQuantity())
	}
}

func TestRecomputeCapabilityOffers_NoEligiblePathKnownIsPhysicalFallback(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	cutoff := now.Add(time.Hour)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400}},
		PathCap: &fakePathCap{found: true, next: contract.NextCutoff{
			CutoffAt: cutoff,
			Paths:    []contract.EligiblePath{{PathId: "path-1"}},
		}},
		Capacity: &fakeCapacity{known: map[string]bool{"path-1": false}},
		Offers:   &fakeOffersRepo{},
		Clock:    fixedClock{now: now},
		SiteId:   "site-1",
	}
	if _, err := u.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if repo.saved[0].Basis() != capabilityoffer.BasisPhysical || repo.saved[0].AdvertisedQuantity() != 400 {
		t.Fatalf("saved offer = %+v, want 400/PHYSICAL (no proven capacity figure)", repo.saved[0])
	}
}

func TestRecomputeCapabilityOffers_SkipsSKUOnInventoryError(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-bad", "sku-good"}},
		Inventory: &fakeInventory{
			usable: map[shared.SKU]int{"sku-good": 10},
			errFor: map[shared.SKU]error{"sku-bad": errors.New("boom")},
		},
		PathCap:  &fakePathCap{found: false},
		Capacity: &fakeCapacity{},
		Offers:   &fakeOffersRepo{},
		Clock:    fixedClock{now: now},
		SiteId:   "site-1",
	}
	res, err := u.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Examined != 2 {
		t.Fatalf("Examined = %d, want 2", res.Examined)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if len(repo.saved) != 1 || repo.saved[0].SKU() != "sku-good" {
		t.Fatalf("saved = %+v, want only sku-good", repo.saved)
	}
}

func TestRecomputeCapabilityOffers_SkipsSKUOnSaveError(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 10}},
		PathCap:     &fakePathCap{found: false},
		Capacity:    &fakeCapacity{},
		Offers:      &fakeOffersRepo{saveErr: errors.New("db down")},
		Clock:       fixedClock{now: now},
		SiteId:      "site-1",
	}
	res, err := u.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute returned an error (should swallow a single save failure): %v", err)
	}
	if res.Examined != 1 {
		t.Fatalf("Examined = %d, want 1", res.Examined)
	}
}

func TestRecomputeCapabilityOffers_PropagatesKnownSKUsError(t *testing.T) {
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{err: errors.New("dictionary unavailable")},
		Inventory:   &fakeInventory{},
		PathCap:     &fakePathCap{},
		Capacity:    &fakeCapacity{},
		Offers:      &fakeOffersRepo{},
		Clock:       fixedClock{now: time.Now()},
		SiteId:      "site-1",
	}
	if _, err := u.Execute(context.Background()); err == nil {
		t.Fatal("expected an error when KnownSKUs fails")
	}
}
