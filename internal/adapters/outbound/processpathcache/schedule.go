package processpathcache

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// scheduleLookaheadDays is how far Schedule looks for a slot's next occurrence:
// every schedule repeats weekly at most, so a week and a day always contains it.
const scheduleLookaheadDays = 8

// Schedule satisfies ports.ProcessPathCapability (ADR 0027): the site's whole
// weekly schedule, where NextCutoff returns only the next cutoff. Each slot's
// NextAt is resolved by the same rule as NextCutoff, so it matches the
// (path, cutoff) keys of the capacity cache.
//
// A cutoff this cache cannot read (a malformed local_time, or no recognisable
// weekday) is left out rather than guessed: a profile built from the rest is
// incomplete, which the use case treats as "not ready", never as a schedule with
// a hole in it silently declared. found=false means no usable slot.
func (c *Consumer) Schedule(siteId shared.SiteId, after time.Time) (contract.CutoffSchedule, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sched, ok := c.schedules[siteId]
	if !ok {
		return contract.CutoffSchedule{}, false
	}
	out := contract.CutoffSchedule{Timezone: sched.Timezone}
	for _, cutoff := range sched.Cutoffs {
		local, valid := normalizeLocalTime(cutoff.LocalTime)
		days := parseWeekdays(cutoff.DaysOfWeek)
		if !valid || len(days) == 0 {
			continue
		}
		out.Slots = append(out.Slots, contract.ScheduleSlot{
			LocalTime: local,
			Days:      days,
			NextAt:    nextOccurrence(cutoff, after, zoneOf(sched)),
			Paths:     c.eligiblePaths([]cutoffData{cutoff}),
		})
	}
	if len(out.Slots) == 0 {
		return contract.CutoffSchedule{}, false
	}
	return out, true
}

// nextOccurrence is the first instant (UTC) on or after `after` at which
// cutoff applies within the lookahead, days walked on loc's local calendar,
// or the zero time.
func nextOccurrence(cutoff cutoffData, after time.Time, loc *time.Location) time.Time {
	for day := 0; day < scheduleLookaheadDays; day++ {
		if at, ok := cutoffOnDay(cutoff, after, day, loc); ok {
			return at
		}
	}
	return time.Time{}
}

// normalizeLocalTime reads H:mm or HH:mm and returns it as HH:mm.
func normalizeLocalTime(raw string) (string, bool) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return "", false
	}
	var h, m int
	if _, err := fmt.Sscanf(parts[0], "%d", &h); err != nil {
		return "", false
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &m); err != nil {
		return "", false
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return "", false
	}
	return fmt.Sprintf("%02d:%02d", h, m), true
}

// parseWeekdays reads the schedule's day names (the three-letter form the
// schedule publishes, or the full English name, any case) into distinct weekdays,
// Sunday first. A name it does not recognise is dropped.
func parseWeekdays(names []string) []time.Weekday {
	seen := map[time.Weekday]bool{}
	for _, name := range names {
		for w := time.Sunday; w <= time.Saturday; w++ {
			full := w.String()
			if strings.EqualFold(name, full) || strings.EqualFold(name, full[:3]) {
				seen[w] = true
			}
		}
	}
	days := make([]time.Weekday, 0, len(seen))
	for w := range seen {
		days = append(days, w)
	}
	sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
	return days
}
