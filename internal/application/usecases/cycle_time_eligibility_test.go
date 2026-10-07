package usecases

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// cycleTimeOffer runs one recompute pass (now -> cutoff = now+2h, physical
// 1000, remaining capacity per path) and returns the single saved offer.
func cycleTimeOffer(t *testing.T, paths []contract.EligiblePath, remaining map[string]int) capabilityoffer.CapabilityOffer {
	t.Helper()
	now := time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)
	known := map[string]bool{}
	for id := range remaining {
		known[id] = true
	}
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 1000}},
		PathCap: &fakePathCap{found: true, next: contract.NextCutoff{
			CutoffAt: now.Add(2 * time.Hour),
			Paths:    paths,
		}},
		Capacity: &fakeCapacity{remaining: remaining, known: known},
		Offers:   &fakeOffersRepo{},
		Clock:    fixedClock{now: now},
		SiteId:   "site-1",
	}
	if _, err := u.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if len(repo.saved) != 1 {
		t.Fatalf("saved = %d, want 1", len(repo.saved))
	}
	return repo.saved[0]
}

// ADR 0017: a path is eligible for a cutoff only if
// CycleTimeP95 <= cutoff - now. The window here is exactly 2h.
func TestCycleTimeEligibility_Boundary(t *testing.T) {
	window := 2 * time.Hour
	tests := []struct {
		name      string
		cycle     time.Duration
		wantQty   int
		wantBasis capabilityoffer.Basis
	}{
		{"well inside the window", 30 * time.Minute, 100, capabilityoffer.BasisThroughputConstrained},
		{"exactly equal to the window is eligible (<=)", window, 100, capabilityoffer.BasisThroughputConstrained},
		{"one nanosecond over the window is ineligible", window + time.Nanosecond, 0, capabilityoffer.BasisThroughputConstrained},
		{"far over the window is ineligible", 6 * time.Hour, 0, capabilityoffer.BasisThroughputConstrained},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offer := cycleTimeOffer(t,
				[]contract.EligiblePath{{PathId: "path-1", CycleTimeP95: tt.cycle, CycleTimeKnown: true}},
				map[string]int{"path-1": 100})
			if offer.AdvertisedQuantity() != tt.wantQty || offer.Basis() != tt.wantBasis {
				t.Fatalf("offer = %d/%s, want %d/%s", offer.AdvertisedQuantity(), offer.Basis(), tt.wantQty, tt.wantBasis)
			}
		})
	}
}

// Missing or zero cycle time is "no data", not evidence: the path stays
// eligible (fail-open) and contributes its capacity.
func TestCycleTimeEligibility_MissingOrZeroStaysEligible(t *testing.T) {
	tests := []struct {
		name string
		path contract.EligiblePath
	}{
		{"cycle time unknown", contract.EligiblePath{PathId: "path-1", CycleTimeKnown: false}},
		{"cycle time unknown even if a stale value is present", contract.EligiblePath{PathId: "path-1", CycleTimeP95: 99 * time.Hour, CycleTimeKnown: false}},
		{"cycle time known but zero", contract.EligiblePath{PathId: "path-1", CycleTimeP95: 0, CycleTimeKnown: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offer := cycleTimeOffer(t, []contract.EligiblePath{tt.path}, map[string]int{"path-1": 100})
			if offer.AdvertisedQuantity() != 100 || offer.Basis() != capabilityoffer.BasisThroughputConstrained {
				t.Fatalf("offer = %d/%s, want 100/THROUGHPUT_CONSTRAINED (path stays eligible)", offer.AdvertisedQuantity(), offer.Basis())
			}
		})
	}
}

// An ineligible path's remaining capacity is dropped from the sum — and
// capacity is NOT scaled by cycle time: the eligible paths still count
// their full remaining capacity.
func TestCycleTimeEligibility_IneligiblePathDroppedFromSum_NoScaling(t *testing.T) {
	offer := cycleTimeOffer(t,
		[]contract.EligiblePath{
			{PathId: "fast", CycleTimeP95: time.Hour, CycleTimeKnown: true},
			{PathId: "slow", CycleTimeP95: 3 * time.Hour, CycleTimeKnown: true},
			{PathId: "nodata", CycleTimeKnown: false},
		},
		map[string]int{"fast": 50, "slow": 400, "nodata": 7})
	if offer.AdvertisedQuantity() != 57 {
		t.Fatalf("AdvertisedQuantity = %d, want 57 (fast 50 + nodata 7; slow excluded; no scaling)", offer.AdvertisedQuantity())
	}
}

// When every path is ruled out by cycle time, nothing can make the
// cutoff: the offer is a proven zero (throughput-constrained), NOT the
// physical fallback reserved for "no capacity figure observed".
func TestCycleTimeEligibility_AllPathsTooSlow_AdvertisesZero(t *testing.T) {
	offer := cycleTimeOffer(t,
		[]contract.EligiblePath{
			{PathId: "p1", CycleTimeP95: 3 * time.Hour, CycleTimeKnown: true},
			{PathId: "p2", CycleTimeP95: 5 * time.Hour, CycleTimeKnown: true},
		},
		map[string]int{"p1": 100, "p2": 100})
	if offer.AdvertisedQuantity() != 0 || offer.Basis() != capabilityoffer.BasisThroughputConstrained {
		t.Fatalf("offer = %d/%s, want 0/THROUGHPUT_CONSTRAINED", offer.AdvertisedQuantity(), offer.Basis())
	}
}

// A slow path whose capacity was never observed is still just excluded:
// with another observed eligible path the sum is unchanged.
func TestCycleTimeEligibility_SlowPathWithoutCapacityFigure(t *testing.T) {
	offer := cycleTimeOffer(t,
		[]contract.EligiblePath{
			{PathId: "ok", CycleTimeP95: time.Hour, CycleTimeKnown: true},
			{PathId: "slow-unobserved", CycleTimeP95: 9 * time.Hour, CycleTimeKnown: true},
		},
		map[string]int{"ok": 20})
	if offer.AdvertisedQuantity() != 20 {
		t.Fatalf("AdvertisedQuantity = %d, want 20", offer.AdvertisedQuantity())
	}
}
