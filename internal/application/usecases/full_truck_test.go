package usecases

import (
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
)

// Leak B: one full truck must not zero the offers of the whole site. A cutoff
// whose paths have used up their capacity is "this truck is full", exactly like
// a cutoff no path can make is "misses this truck" (ADR 0031): the walk moves on
// to the next cutoff that still has room, so a later truck keeps the site
// sellable. Only when every cutoff inside the horizon is full is it a proven zero.

var fastPath = contract.EligiblePath{PathId: "pick", CycleTimeP95: 10 * time.Minute, CycleTimeKnown: true}

// The 09:00 truck is full (0 left, observed), the 13:00 truck has 70: the
// offer advertises the 70 that can still ship, not a zero.
func TestFullTruck_TheNextCutoffWithRoomIsAdvertisedNotAZero(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := time.Date(2026, 10, 30, 13, 0, 0, 0, time.UTC)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 0}, c2: {"pick": 70}}},
		cutoffAt(c1, fastPath), cutoffAt(c2, fastPath))
	assertOffer(t, offer, 70, capabilityoffer.BasisThroughputConstrained)
}

// Several full trucks in a row are all passed.
func TestFullTruck_EveryFullCutoffIsPassed(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(5 * time.Hour)
	c3 := feasibleNow.Add(24 * time.Hour)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 0}, c2: {"pick": 0}, c3: {"pick": 25}}},
		cutoffAt(c1, fastPath), cutoffAt(c2, fastPath), cutoffAt(c3, fastPath))
	assertOffer(t, offer, 25, capabilityoffer.BasisThroughputConstrained)
}

// Every cutoff inside the horizon is full: that is a proven zero.
func TestFullTruck_AllCutoffsFullIsAProvenZero(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(24 * time.Hour)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c1: {"pick": 0}, c2: {"pick": 0}}},
		cutoffAt(c1, fastPath), cutoffAt(c2, fastPath))
	assertOffer(t, offer, 0, capabilityoffer.BasisThroughputConstrained)
}

// A truck with room is not walked past, and a truck whose figure was never
// observed is not "full": it is the usual unknown fallback to physical stock.
func TestFullTruck_ARoomyOrUnobservedTruckIsNotSkipped(t *testing.T) {
	c1 := feasibleNow.Add(44 * time.Minute)
	c2 := feasibleNow.Add(5 * time.Hour)
	offer, _ := feasibleOffer(t,
		&fakeCapacity{perCutoff: map[time.Time]map[string]int{c2: {"pick": 70}}},
		cutoffAt(c1, fastPath), cutoffAt(c2, fastPath))
	assertOffer(t, offer, 400, capabilityoffer.BasisPhysical)
}
