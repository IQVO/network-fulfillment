package processpathcache

import (
	"slices"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func TestSchedule_NoScheduleReturnsNotFound(t *testing.T) {
	c := newTestConsumer(nil)
	if _, ok := c.Schedule("site-1", time.Now()); ok {
		t.Fatal("Schedule found a schedule with none cached")
	}
}

func TestSchedule_ReturnsTheWholeWeekWithNextOccurrencesAndPaths(t *testing.T) {
	c := newTestConsumer(nil)
	pathMsg := buildEvent(t, typeCreated, "path-1", pathData{PathId: "path-1", CycleTimeP95: "45m0s"})
	scheduleMsg := buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId:   "site-1",
		Timezone: "America/Sao_Paulo",
		Cutoffs: []cutoffData{
			{CptId: "cpt-1", LocalTime: "18:00", DaysOfWeek: []string{"Mon", "tue", "WED", "Thu", "Fri"}, EligiblePathIds: []string{"path-1"}},
			{CptId: "cpt-2", LocalTime: "10:30", DaysOfWeek: []string{"Sat"}, EligiblePathIds: []string{"path-2"}},
		},
	})
	drive(c, []kafkago.Message{{Value: pathMsg}, {Value: scheduleMsg}})

	after := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC) // a Monday
	got, ok := c.Schedule("site-1", after)
	if !ok {
		t.Fatal("Schedule found nothing")
	}
	if got.Timezone != "America/Sao_Paulo" || len(got.Slots) != 2 {
		t.Fatalf("schedule = %+v, want the zone and 2 slots", got)
	}
	checkSlot(t, "weekday", got.Slots[0], "18:00", []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday},
		time.Date(2026, 10, 5, 18, 0, 0, 0, time.UTC), true, 45*time.Minute)
	checkSlot(t, "saturday", got.Slots[1], "10:30", []time.Weekday{time.Saturday},
		time.Date(2026, 10, 10, 10, 30, 0, 0, time.UTC), false, 0)
}

// checkSlot asserts a slot's time, days, next occurrence and its one eligible
// path: whether that path's cycle time is known, and if so what it is.
func checkSlot(t *testing.T, label string, s contract.ScheduleSlot, localTime string, days []time.Weekday, next time.Time, cycleKnown bool, cycle time.Duration) {
	t.Helper()
	if s.LocalTime != localTime || !slices.Equal(s.Days, days) {
		t.Errorf("%s slot = %+v, want %s on %v", label, s, localTime, days)
	}
	if !s.NextAt.Equal(next) {
		t.Errorf("%s NextAt = %v, want %v", label, s.NextAt, next)
	}
	if len(s.Paths) != 1 || s.Paths[0].CycleTimeKnown != cycleKnown || s.Paths[0].CycleTimeP95 != cycle {
		t.Errorf("%s paths = %+v, want one path with known=%v cycle=%v", label, s.Paths, cycleKnown, cycle)
	}
}

func TestSchedule_DropsWhatItCannotRepresent(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleMsg := buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId:   "site-1",
		Timezone: "UTC",
		Cutoffs: []cutoffData{
			{CptId: "bad-time", LocalTime: "6pm", DaysOfWeek: allDays()},
			{CptId: "bad-hour", LocalTime: "25:00", DaysOfWeek: allDays()},
			{CptId: "no-days", LocalTime: "18:00", DaysOfWeek: []string{"Xyz"}},
			{CptId: "good", LocalTime: "6:05", DaysOfWeek: []string{"Mon", "Mon", "monday"}},
		},
	})
	drive(c, []kafkago.Message{{Value: scheduleMsg}})

	got, ok := c.Schedule("site-1", time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	if !ok || len(got.Slots) != 1 {
		t.Fatalf("schedule = %+v ok=%v, want only the one well-formed slot", got, ok)
	}
	if s := got.Slots[0]; s.LocalTime != "06:05" || len(s.Days) != 1 || s.Days[0] != time.Monday {
		t.Errorf("slot = %+v, want 06:05 on Monday once", s)
	}
}

func TestSchedule_AllSlotsUnusableIsNotFound(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleMsg := buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId:  "site-1",
		Cutoffs: []cutoffData{{CptId: "bad", LocalTime: "nope", DaysOfWeek: allDays()}},
	})
	drive(c, []kafkago.Message{{Value: scheduleMsg}})
	if _, ok := c.Schedule("site-1", time.Now()); ok {
		t.Fatal("Schedule found a schedule although no slot is usable")
	}
}
