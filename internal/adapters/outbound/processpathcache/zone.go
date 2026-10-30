package processpathcache

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// A CPTScheduleChanged cutoff carries a wall-clock local_time ("HH:mm") and
// days_of_week, both meant in the schedule's IANA timezone. This file turns
// them into UTC instants (ADR 0031 §2): the day walk happens on the LOCAL
// calendar of that zone, each instant is built with time.Date in the zone (so a
// DST change moves the UTC instant, never the local time), and the answer is
// returned in UTC, the form the capacity cache keys cutoffs by.
//
// The zone database is embedded by every binary that runs this cache
// (cmd/netfulfil/tzdata.go), so a zone resolves on any base image.

// resolveZone loads the schedule's zone. An unknown or missing zone cannot be
// guessed: it falls back to UTC (the reading this cache used before ADR 0031)
// and says so once, at the time the schedule is received.
func resolveZone(logger *slog.Logger, d scheduleData) *time.Location {
	name := strings.TrimSpace(d.Timezone)
	if name != "" && name != "Local" {
		if loc, err := time.LoadLocation(name); err == nil {
			return loc
		}
	}
	if logger != nil {
		logger.Warn("process-path capability cache: the CPT schedule's timezone is unknown or missing; its cutoffs are read as UTC",
			"site", d.SiteId, "timezone", d.Timezone)
	}
	return time.UTC
}

// zoneOf is the schedule's resolved zone, UTC when it was never resolved.
func zoneOf(d scheduleData) *time.Location {
	if d.loc == nil {
		return time.UTC
	}
	return d.loc
}

// cutoffOnDay resolves one cutoff on the local date that is `day` days after
// the local date of `after` in loc. ok=false when the cutoff does not apply on
// that local weekday, is malformed, or falls before `after`. The instant is UTC.
func cutoffOnDay(cutoff cutoffData, after time.Time, day int, loc *time.Location) (time.Time, bool) {
	y, m, d := after.In(loc).Date()
	// Noon is never inside a DST gap, so its weekday is the local date's.
	if !dayMatches(cutoff.DaysOfWeek, time.Date(y, m, d+day, 12, 0, 0, 0, loc).Weekday()) {
		return time.Time{}, false
	}
	h, mi, err := parseLocalTime(cutoff.LocalTime)
	if err != nil {
		return time.Time{}, false
	}
	at := time.Date(y, m, d+day, h, mi, 0, 0, loc)
	if at.Before(after) {
		return time.Time{}, false
	}
	return at.UTC(), true
}

func dayMatches(days []string, w time.Weekday) bool {
	want := w.String()[:3]
	for _, d := range days {
		if strings.EqualFold(d, want) {
			return true
		}
	}
	return false
}

// parseLocalTime reads "H:mm"/"HH:mm" into hour and minute.
func parseLocalTime(localTime string) (int, int, error) {
	parts := strings.Split(localTime, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("processpathcache: malformed local_time %q", localTime)
	}
	var h, m int
	if _, err := fmt.Sscanf(parts[0], "%d", &h); err != nil {
		return 0, 0, err
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &m); err != nil {
		return 0, 0, err
	}
	return h, m, nil
}
