package usecases

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ADR 0031: missing the next truck is not "cannot ship". The offer is computed
// against the first cutoff that some path can still make, walking the schedule
// forward at most feasibleCutoffHorizon (7 days) from now.

// feasibleNow is 08:16; the site's next cutoff is 09:00, 44 minutes away.
var feasibleNow = time.Date(2026, 10, 30, 8, 16, 0, 0, time.UTC)

// pickPath is the live shape of the defect: a PICK path whose p95 is 2h.
var pickPath = contract.EligiblePath{PathId: "pick", CycleTimeP95: 2 * time.Hour, CycleTimeKnown: true}

func cutoffAt(at time.Time, paths ...contract.EligiblePath) contract.NextCutoff {
	return contract.NextCutoff{CutoffAt: at, Paths: paths}
}

// feasibleOffer runs one pass over a schedule (first, then later...) with
// physical stock 400 and returns the single saved offer and the path cache.
func feasibleOffer(t *testing.T, capacity *fakeCapacity, first contract.NextCutoff, later ...contract.NextCutoff) (capabilityoffer.CapabilityOffer, *fakePathCap) {
	t.Helper()
	pathCap := &fakePathCap{found: true, next: first, later: later}
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400}},
		PathCap:     pathCap,
		Capacity:    capacity,
		Offers:      &fakeOffersRepo{},
		Clock:       fixedClock{now: feasibleNow},
		SiteId:      "site-1",
	}
	if _, err := u.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	repo := u.Offers.(*fakeOffersRepo)
	if len(repo.saved) != 1 {
		t.Fatalf("saved = %d, want 1", len(repo.saved))
	}
	return repo.saved[0], pathCap
}

func assertOffer(t *testing.T, o capabilityoffer.CapabilityOffer, qty int, basis capabilityoffer.Basis) {
	t.Helper()
	if o.AdvertisedQuantity() != qty || o.Basis() != basis {
		t.Fatalf("offer = %d/%s, want %d/%s", o.AdvertisedQuantity(), o.Basis(), qty, basis)
	}
}

// The live defect: 2h p95, 44 minutes to the 09:00 cutoff. The 13:00 cutoff
// can still be made, so its capacity (70), not the 09:00 one (30) and not a
// zero, is advertised.
func TestNextFeasibleCutoff_SlowPathAdvertisesTheNextCutoffsCapacity(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := time.Date(2026, 10, 30, 13, 0, 0, 0, time.UTC)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 30}, c2: {"pick": 70}}},
		cutoffAt(c1, pickPath), cutoffAt(c2, pickPath))
	assertOffer(t, offer, 70, capabilityoffer.BasisThroughputConstrained)
}

// When the feasible cutoff's capacity was never observed, the usual fallback
// applies to THAT cutoff: physical stock, not a zero.
func TestNextFeasibleCutoff_UnknownCapacityAtTheFeasibleCutoffIsPhysical(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := c1.Add(24 * time.Hour)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 30}}},
		cutoffAt(c1, pickPath), cutoffAt(c2, pickPath))
	assertOffer(t, offer, 400, capabilityoffer.BasisPhysical)
}

// The walk passes every infeasible cutoff, not just the first one, and uses
// only the eligible paths of the cutoff it lands on.
func TestNextFeasibleCutoff_SkipsEveryCutoffNoPathCanMake(t *testing.T) {
	fast := contract.EligiblePath{PathId: "fast", CycleTimeP95: 3 * time.Hour, CycleTimeKnown: true}
	slow := contract.EligiblePath{PathId: "slow", CycleTimeP95: 5 * time.Hour, CycleTimeKnown: true}
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(2*time.Hour + 30*time.Minute)
	c3 := feasibleNow.Add(4 * time.Hour)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{
			c1: {"fast": 11, "slow": 12},
			c2: {"fast": 13, "slow": 14},
			c3: {"fast": 15, "slow": 160},
		}},
		cutoffAt(c1, fast, slow), cutoffAt(c2, fast, slow), cutoffAt(c3, fast, slow))
	// c3 is 4h away: fast (3h) fits, slow (5h) does not.
	assertOffer(t, offer, 15, capabilityoffer.BasisThroughputConstrained)
}

// A feasible next cutoff is used as it is: no walk, the cache is asked once.
func TestNextFeasibleCutoff_FeasibleNextCutoffIsNotWalkedPast(t *testing.T) {
	c1 := feasibleNow.Add(3 * time.Hour)
	c2 := feasibleNow.Add(6 * time.Hour)
	offer, pathCap := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 30}, c2: {"pick": 70}}},
		cutoffAt(c1, pickPath), cutoffAt(c2, pickPath))
	assertOffer(t, offer, 30, capabilityoffer.BasisThroughputConstrained)
	if len(pathCap.asked) != 1 {
		t.Fatalf("NextCutoff asked %d times (%v), want once", len(pathCap.asked), pathCap.asked)
	}
}

// The cache's `after` is inclusive, so each step asks strictly after the
// cutoff it just ruled out; otherwise it would get the same cutoff back.
func TestNextFeasibleCutoff_EachStepAsksStrictlyAfterTheRuledOutCutoff(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(5 * time.Hour)
	_, pathCap := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c2: {"pick": 70}}},
		cutoffAt(c1, pickPath), cutoffAt(c2, pickPath))
	if len(pathCap.asked) != 2 {
		t.Fatalf("NextCutoff asked %d times (%v), want 2", len(pathCap.asked), pathCap.asked)
	}
	if !pathCap.asked[0].Equal(feasibleNow) {
		t.Fatalf("first ask after %v, want now %v", pathCap.asked[0], feasibleNow)
	}
	if !pathCap.asked[1].After(c1) || pathCap.asked[1].After(c1.Add(time.Minute)) {
		t.Fatalf("second ask after %v, want just after the ruled-out cutoff %v", pathCap.asked[1], c1)
	}
}

// Every cutoff in the schedule is infeasible: a proven zero.
func TestNextFeasibleCutoff_AllCutoffsInfeasibleIsAProvenZero(t *testing.T) {
	glacial := contract.EligiblePath{PathId: "pick", CycleTimeP95: 30 * time.Hour, CycleTimeKnown: true}
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(12 * time.Hour)
	c3 := feasibleNow.Add(24 * time.Hour)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 10}, c2: {"pick": 20}, c3: {"pick": 30}}},
		cutoffAt(c1, glacial), cutoffAt(c2, glacial), cutoffAt(c3, glacial))
	assertOffer(t, offer, 0, capabilityoffer.BasisThroughputConstrained)
}

// The walk never looks past 7 days from now: a cutoff exactly 7 days away is
// still considered, one a nanosecond later is not.
func TestNextFeasibleCutoff_TheSevenDayBoundIsRespected(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	tests := []struct {
		name      string
		later     time.Time
		wantQty   int
		wantBasis capabilityoffer.Basis
	}{
		{"exactly 7 days ahead is inside the bound", feasibleNow.Add(7 * 24 * time.Hour), 70, capabilityoffer.BasisThroughputConstrained},
		{"one nanosecond past 7 days is outside: a proven zero", feasibleNow.Add(7*24*time.Hour + time.Nanosecond), 0, capabilityoffer.BasisThroughputConstrained},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offer, _ := feasibleOffer(t,
				&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 30}, tt.later: {"pick": 70}}},
				cutoffAt(c1, pickPath), cutoffAt(tt.later, pickPath))
			assertOffer(t, offer, tt.wantQty, tt.wantBasis)
		})
	}
}

// stuckPathCap ignores `after` and always answers the same cutoff: a cache
// that does not advance must not make the walk spin.
type stuckPathCap struct {
	next  contract.NextCutoff
	asked int
}

func (s *stuckPathCap) NextCutoff(shared.SiteId, time.Time) (contract.NextCutoff, bool) {
	s.asked++
	return s.next, true
}

func (s *stuckPathCap) Schedule(shared.SiteId, time.Time) (contract.CutoffSchedule, bool) {
	return contract.CutoffSchedule{}, false
}

func TestNextFeasibleCutoff_ACacheThatDoesNotAdvanceEndsTheWalk(t *testing.T) {
	pathCap := &stuckPathCap{next: cutoffAt(feasibleNow.Add(44*time.Minute), pickPath)}
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400}},
		PathCap:     pathCap,
		Capacity:    &fakeCapacity{remaining: map[string]int{"pick": 30}, known: map[string]bool{"pick": true}},
		Offers:      &fakeOffersRepo{},
		Clock:       fixedClock{now: feasibleNow},
		SiteId:      "site-1",
	}
	if _, err := u.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertOffer(t, u.Offers.(*fakeOffersRepo).saved[0], 0, capabilityoffer.BasisThroughputConstrained)
	if pathCap.asked != 2 {
		t.Fatalf("NextCutoff asked %d times, want 2 (the walk stops when the cache does not advance)", pathCap.asked)
	}
}

// The feasible cutoff is resolved once per pass, not once per SKU: every SKU
// is advertised against the same cutoff.
func TestNextFeasibleCutoff_ResolvedOncePerPass(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(5 * time.Hour)
	pathCap := &fakePathCap{found: true, next: cutoffAt(c1, pickPath), later: []contract.NextCutoff{cutoffAt(c2, pickPath)}}
	u := &RecomputeCapabilityOffers{
		Translation: &fakeTranslation{skus: []shared.SKU{"sku-1", "sku-2", "sku-3"}},
		Inventory:   &fakeInventory{usable: map[shared.SKU]int{"sku-1": 400, "sku-2": 50, "sku-3": 90}},
		PathCap:     pathCap,
		Capacity:    &fakeCapacity{perCutoff: map[time.Time]map[string]int{c2: {"pick": 70}}},
		Offers:      &fakeOffersRepo{},
		Clock:       fixedClock{now: feasibleNow},
		SiteId:      "site-1",
	}
	res, err := u.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.ThroughputConstrained != 2 {
		t.Fatalf("ThroughputConstrained = %d, want 2 (sku-1 and sku-3)", res.ThroughputConstrained)
	}
	if len(pathCap.asked) != 2 {
		t.Fatalf("NextCutoff asked %d times, want 2 for the whole pass", len(pathCap.asked))
	}
	want := map[shared.SKU]int{"sku-1": 70, "sku-2": 50, "sku-3": 70}
	for _, o := range u.Offers.(*fakeOffersRepo).saved {
		if o.AdvertisedQuantity() != want[o.SKU()] {
			t.Fatalf("%s advertises %d, want %d", o.SKU(), o.AdvertisedQuantity(), want[o.SKU()])
		}
	}
}
