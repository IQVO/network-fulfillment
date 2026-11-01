package processpathcache

import (
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// NextCutoff satisfies ports.ProcessPathCapability.
func (c *Consumer) NextCutoff(siteId shared.SiteId, after time.Time) (contract.NextCutoff, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sched, ok := c.schedules[siteId]
	if !ok || len(sched.Cutoffs) == 0 {
		return contract.NextCutoff{}, false
	}

	best, bestPaths, found := earliestCutoff(sched.Cutoffs, after, zoneOf(sched))
	if !found {
		return contract.NextCutoff{}, false
	}
	return contract.NextCutoff{CutoffAt: best, Paths: c.eligiblePaths(bestPaths)}, true
}

// earliestCutoff resolves each cutoff's NEXT occurrence on or after
// `after` within the next 7 days (every schedule repeats weekly at
// most), then returns the earliest one(s) -- more than one cutoff can
// tie on the same instant. Days are walked on the local calendar of loc,
// the schedule's zone, and the instant is UTC (zone.go, ADR 0031 §2).
func earliestCutoff(cutoffs []cutoffData, after time.Time, loc *time.Location) (time.Time, []cutoffData, bool) {
	var best time.Time
	var bestPaths []cutoffData
	found := false
	for day := 0; day < 8; day++ {
		for _, cutoff := range cutoffs {
			at, ok := cutoffOnDay(cutoff, after, day, loc)
			if !ok {
				continue
			}
			if !found || at.Before(best) {
				best = at
				found = true
				bestPaths = []cutoffData{cutoff}
			} else if at.Equal(best) {
				bestPaths = append(bestPaths, cutoff)
			}
		}
		if found {
			break
		}
	}
	return best, bestPaths, found
}

// eligiblePaths resolves the deduplicated, joined path list for a set of
// tied-earliest cutoffs. Caller holds c.mu (read or write).
func (c *Consumer) eligiblePaths(cutoffs []cutoffData) []contract.EligiblePath {
	seen := make(map[string]bool)
	var paths []contract.EligiblePath
	for _, cutoff := range cutoffs {
		for _, pathID := range cutoff.EligiblePathIds {
			if seen[pathID] {
				continue
			}
			seen[pathID] = true
			state := c.paths[pathID]
			paths = append(paths, contract.EligiblePath{
				PathId:         pathID,
				CycleTimeP95:   state.cycleTimeP95,
				CycleTimeKnown: state.cycleTimeKnown,
			})
		}
	}
	return paths
}
