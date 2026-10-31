package usecases

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// The derivation of a node profile from the local caches (ADR 0027 §3). Pure
// functions: no port is called here except the capacity read the caller hands in.

const (
	minutesPerDay = 24 * 60
	daysPerWeek   = 7
)

// slot is one distinct cutoff time of the weekly schedule. Two cutoffs of the
// schedule at the same local time are one slot: node-topology refuses two at the
// same time on a shared day, and "handover at 18:00" is one fact however many
// ship methods it serves.
type slot struct {
	localTime string
	days      []time.Weekday
	nextAt    time.Time
	paths     []contract.EligiblePath
}

// mergeSlots groups the schedule's slots by local time, ordered by it. Days are
// united, the next occurrence is the earliest known one, and the paths are united
// (one entry per path id, the first seen).
func mergeSlots(slots []contract.ScheduleSlot) []slot {
	byTime := map[string]*slot{}
	var order []string
	for _, s := range slots {
		m, ok := byTime[s.LocalTime]
		if !ok {
			m = &slot{localTime: s.LocalTime}
			byTime[s.LocalTime] = m
			order = append(order, s.LocalTime)
		}
		m.days = unionDays(m.days, s.Days)
		if !s.NextAt.IsZero() && (m.nextAt.IsZero() || s.NextAt.Before(m.nextAt)) {
			m.nextAt = s.NextAt
		}
		m.paths = unionPaths(m.paths, s.Paths)
	}
	sort.Strings(order)
	out := make([]slot, len(order))
	for i, t := range order {
		out[i] = *byTime[t]
	}
	return out
}

func unionDays(a, b []time.Weekday) []time.Weekday {
	out := slices.Clone(a)
	for _, d := range b {
		if !slices.Contains(out, d) {
			out = append(out, d)
		}
	}
	slices.Sort(out)
	return out
}

func unionPaths(a, b []contract.EligiblePath) []contract.EligiblePath {
	out := slices.Clone(a)
	for _, p := range b {
		if !slices.ContainsFunc(out, func(q contract.EligiblePath) bool { return q.PathId == p.PathId }) {
			out = append(out, p)
		}
	}
	return out
}

// longestGap is the longest time between two consecutive cutoff occurrences of
// the week, counting the wrap from the last of the week to the first of the next.
// With a single occurrence it is a whole week. It is the most generous window the
// schedule ever gives a path to finish its work in.
func longestGap(slots []slot) time.Duration {
	var at []int
	for _, s := range slots {
		minute := localMinute(s.localTime)
		for _, d := range s.days {
			at = append(at, int(d)*minutesPerDay+minute)
		}
	}
	slices.Sort(at)
	at = slices.Compact(at)
	if len(at) < 2 {
		return daysPerWeek * minutesPerDay * time.Minute
	}
	longest := at[0] + daysPerWeek*minutesPerDay - at[len(at)-1]
	for i := 1; i < len(at); i++ {
		longest = max(longest, at[i]-at[i-1])
	}
	return time.Duration(longest) * time.Minute
}

// localMinute is the minute of the day of an HH:mm the schedule cache has
// already normalised.
func localMinute(hhmm string) int {
	var h, m int
	_, _ = fmt.Sscanf(hhmm, "%d:%d", &h, &m)
	return h*60 + m
}

// handlingTime is the longest known, positive cycle-time-p95 among the paths
// eligible for any cutoff, whole minutes rounded up. ADR 0017's rule applied
// without a clock: a path counts only if its cycle time fits the longest window
// the schedule gives; a path with no cycle time stays out (no data is not
// evidence). Zero means no path qualifies: nothing can honestly be declared.
func handlingTime(slots []slot) time.Duration {
	window := longestGap(slots)
	var longest time.Duration
	seen := map[string]bool{}
	for _, s := range slots {
		for _, p := range s.paths {
			if seen[p.PathId] || !p.CycleTimeKnown || p.CycleTimeP95 <= 0 {
				continue
			}
			seen[p.PathId] = true
			if cycleTimeFits(p, window) {
				longest = max(longest, p.CycleTimeP95)
			}
		}
	}
	return ceilMinutes(longest)
}

// ceilMinutes rounds d up to whole minutes: the figure is a p95, so a declaration
// never promises less time than it needs, and a sub-minute wobble does not make a
// new revision.
func ceilMinutes(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return (d + time.Minute - 1) / time.Minute * time.Minute
}

// slotCeiling is what the capacity cache says the slot's next occurrence can
// take: the sum of the remaining admission capacity of the eligible paths whose
// figure is known (ADR 0017's fit against the time left to the occurrence).
// 0 means nothing is known.
func slotCeiling(s slot, now time.Time, remaining func(pathID string, cutoffAt time.Time) (int, bool)) int {
	if s.nextAt.IsZero() {
		return 0
	}
	window := s.nextAt.Sub(now)
	total := 0
	for _, p := range s.paths {
		if !cycleTimeFits(p, window) {
			continue
		}
		if units, known := remaining(p.PathId, s.nextAt); known {
			total += units
		}
	}
	return total
}

// slotKey identifies a slot across declarations: its time and its days.
func slotKey(localTime string, days []time.Weekday) string {
	parts := make([]string, len(days))
	for i, d := range days {
		parts[i] = fmt.Sprint(int(d))
	}
	return localTime + "|" + strings.Join(parts, ",")
}

// throughputPerDay is the most orders any one weekday's cutoffs can take
// together, counting only the ceilings that are known.
func throughputPerDay(cutoffs []contract.ProfileCutoff) int {
	var perDay [daysPerWeek]int
	for _, c := range cutoffs {
		for _, d := range c.Days {
			if d >= time.Sunday && d <= time.Saturday {
				perDay[d] += c.MaxOrders
			}
		}
	}
	return slices.Max(perDay[:])
}

// fingerprint identifies the declared content: equal content, equal fingerprint,
// whatever the order the caches listed it in.
func fingerprint(p contract.NodeProfile) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "tz=%s\nhandling=%d\nthroughput=%d\n", p.Timezone, int64(p.HandlingTime/time.Minute), p.ThroughputPerDay)
	cutoffs := slices.Clone(p.Cutoffs)
	sort.Slice(cutoffs, func(i, j int) bool { return cutoffs[i].LocalTime < cutoffs[j].LocalTime })
	for _, c := range cutoffs {
		_, _ = fmt.Fprintf(h, "cutoff=%s max=%d\n", slotKey(c.LocalTime, c.Days), c.MaxOrders)
	}
	return hex.EncodeToString(h.Sum(nil))
}
