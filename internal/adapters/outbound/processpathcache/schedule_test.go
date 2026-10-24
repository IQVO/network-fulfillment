package processpathcache

import (
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
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
	weekday, saturday := got.Slots[0], got.Slots[1]
	if weekday.LocalTime != "18:00" || len(weekday.Days) != 5 || weekday.Days[0] != time.Monday || weekday.Days[4] != time.Friday {
		t.Errorf("weekday slot = %+v, want 18:00 on Monday..Friday", weekday)
	}
	if want := time.Date(2026, 10, 5, 18, 0, 0, 0, time.UTC); !weekday.NextAt.Equal(want) {
		t.Errorf("weekday NextAt = %v, want %v", weekday.NextAt, want)
	}
	if len(weekday.Paths) != 1 || !weekday.Paths[0].CycleTimeKnown || weekday.Paths[0].CycleTimeP95 != 45*time.Minute {
		t.Errorf("weekday paths = %+v, want path-1 with 45m", weekday.Paths)
	}
	if saturday.LocalTime != "10:30" || len(saturday.Days) != 1 || saturday.Days[0] != time.Saturday {
		t.Errorf("saturday slot = %+v, want 10:30 on Saturday", saturday)
	}
	if want := time.Date(2026, 10, 10, 10, 30, 0, 0, time.UTC); !saturday.NextAt.Equal(want) {
		t.Errorf("saturday NextAt = %v, want %v", saturday.NextAt, want)
	}
	if len(saturday.Paths) != 1 || saturday.Paths[0].CycleTimeKnown {
		t.Errorf("saturday paths = %+v, want path-2 with no cycle time", saturday.Paths)
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
