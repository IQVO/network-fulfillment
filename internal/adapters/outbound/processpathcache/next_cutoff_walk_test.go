package processpathcache

import (
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// RecomputeCapabilityOffers walks the schedule cutoff by cutoff (ADR 0031) by
// asking NextCutoff just after the cutoff it ruled out. That walk relies on
// `after` being INCLUSIVE (a cutoff exactly at `after` is returned) and on a
// later `after` reaching the following cutoff, today or on a later day.
func TestNextCutoff_AfterIsInclusiveAndAdvancesToTheFollowingCutoff(t *testing.T) {
	c := newTestConsumer(nil)
	drive(c, []kafkago.Message{{Value: buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId:   "site-1",
		Timezone: "UTC",
		Cutoffs: []cutoffData{
			{CptId: "cpt-am", LocalTime: "09:00", DaysOfWeek: allDays(), ShipMethod: "GROUND", EligiblePathIds: []string{"pick"}},
			{CptId: "cpt-pm", LocalTime: "13:00", DaysOfWeek: allDays(), ShipMethod: "GROUND", EligiblePathIds: []string{"pick"}},
		},
	})}})

	am := time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC)
	pm := time.Date(2026, 10, 30, 13, 0, 0, 0, time.UTC)
	tomorrowAm := am.AddDate(0, 0, 1)
	tests := []struct {
		name  string
		after time.Time
		want  time.Time
	}{
		{"44 minutes before the morning cutoff", am.Add(-44 * time.Minute), am},
		{"exactly at the cutoff is inclusive", am, am},
		{"one nanosecond after reaches the afternoon cutoff", am.Add(time.Nanosecond), pm},
		{"just after the last cutoff of the day reaches tomorrow's first", pm.Add(time.Nanosecond), tomorrowAm},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, ok := c.NextCutoff("site-1", tt.after)
			if !ok {
				t.Fatal("expected a cutoff")
			}
			if !next.CutoffAt.Equal(tt.want) {
				t.Fatalf("NextCutoff(%v) = %v, want %v", tt.after, next.CutoffAt, tt.want)
			}
		})
	}
}
