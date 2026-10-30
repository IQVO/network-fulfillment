package processpathcache

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// A cutoff's local_time is a wall-clock time in the schedule's IANA zone. The
// cache resolves it there (days walked on the LOCAL date, DST-correct) and
// answers UTC instants; an unknown or missing zone falls back to UTC with one
// WARN per schedule received.

func scheduleIn(t *testing.T, c *Consumer, zone string, cutoffs ...cutoffData) {
	t.Helper()
	drive(c, []kafkago.Message{{Value: buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId: "site-1", Timezone: zone, Cutoffs: cutoffs,
	})}})
}

func cutoffEvery(id, local string, days ...string) cutoffData {
	if len(days) == 0 {
		days = allDays()
	}
	return cutoffData{CptId: id, LocalTime: local, DaysOfWeek: days, ShipMethod: "GROUND", EligiblePathIds: []string{"pick"}}
}

func utc(s string) time.Time {
	at, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return at
}

func expectNext(t *testing.T, c *Consumer, after, want time.Time) {
	t.Helper()
	next, ok := c.NextCutoff("site-1", after)
	if !ok {
		t.Fatalf("NextCutoff(%v): no cutoff", after)
	}
	if !next.CutoffAt.Equal(want) {
		t.Fatalf("NextCutoff(%v) = %v, want %v", after, next.CutoffAt.UTC(), want)
	}
	if next.CutoffAt.Location() != time.UTC {
		t.Fatalf("NextCutoff answered in %v, want a UTC instant", next.CutoffAt.Location())
	}
}

// The live case: site-1 in America/Chicago (CDT, UTC-5 on 2026-10-30), cutoffs
// 11:00 and 17:00 local. At 09:56Z the next cutoff is 11:00 CDT = 16:00Z, six
// hours away, not 11:00Z 64 minutes away.
func TestNextCutoff_ResolvesLocalTimeInTheSchedulesZone(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleIn(t, c, "America/Chicago", cutoffEvery("am", "11:00"), cutoffEvery("pm", "17:00"))

	expectNext(t, c, utc("2026-10-30T09:56:00Z"), utc("2026-10-30T16:00:00Z"))
	expectNext(t, c, utc("2026-10-30T16:00:00Z").Add(time.Nanosecond), utc("2026-10-30T22:00:00Z"))
	// After the 17:00 CDT cutoff the next one is tomorrow's 11:00 CDT.
	expectNext(t, c, utc("2026-10-30T22:00:00Z").Add(time.Nanosecond), utc("2026-10-31T16:00:00Z"))
}

// `after` in any location is the same instant.
func TestNextCutoff_AfterInAnotherLocationIsTheSameInstant(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleIn(t, c, "America/Chicago", cutoffEvery("am", "11:00"))
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	expectNext(t, c, utc("2026-10-30T09:56:00Z").In(tokyo), utc("2026-10-30T16:00:00Z"))
}

// DST in America/Chicago: it ends 2026-11-01 (CDT UTC-5 -> CST UTC-6) and starts
// 2027-03-14 (CST -> CDT). 11:00 local stays 11:00 local across both.
func TestNextCutoff_IsDSTCorrect(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleIn(t, c, "America/Chicago", cutoffEvery("am", "11:00"))
	tests := []struct {
		name        string
		after, want string
	}{
		{"the day before DST ends is CDT", "2026-10-31T12:00:00Z", "2026-10-31T16:00:00Z"},
		{"DST end day, asked before the 02:00 switch, is CST", "2026-11-01T06:00:00Z", "2026-11-01T17:00:00Z"},
		{"DST end day, asked after the previous cutoff", "2026-10-31T16:00:01Z", "2026-11-01T17:00:00Z"},
		{"the day before DST starts is CST", "2027-03-13T12:00:00Z", "2027-03-13T17:00:00Z"},
		{"DST start day, asked before the 02:00 switch, is CDT", "2027-03-14T06:00:00Z", "2027-03-14T16:00:00Z"},
		{"DST start day, asked after the previous cutoff", "2027-03-13T17:00:01Z", "2027-03-14T16:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectNext(t, c, utc(tt.after), utc(tt.want))
		})
	}
}

// days_of_week is read on the LOCAL date. At 2026-10-31T00:30Z it is still
// Friday 19:30 in Chicago, so a Friday-only 20:00 cutoff is 30 minutes away
// (2026-10-31T01:00Z), although the UTC date is already Saturday.
func TestNextCutoff_DaysOfWeekAreLocalAcrossUTCMidnight(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleIn(t, c, "America/Chicago", cutoffEvery("fri-pm", "20:00", "Fri"))
	expectNext(t, c, utc("2026-10-31T00:30:00Z"), utc("2026-10-31T01:00:00Z"))
	// And once it has passed, the next one is a week later (still CST/CDT-aware:
	// 2026-11-06 is CST, so 20:00 local = 02:00Z on the 7th).
	expectNext(t, c, utc("2026-10-31T01:00:00Z").Add(time.Nanosecond), utc("2026-11-07T02:00:00Z"))
}

// A UTC schedule resolves exactly as before.
func TestNextCutoff_UTCScheduleIsUnchanged(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleIn(t, c, "UTC", cutoffEvery("am", "11:00"), cutoffEvery("pm", "17:00"))
	expectNext(t, c, utc("2026-10-30T09:56:00Z"), utc("2026-10-30T11:00:00Z"))
	expectNext(t, c, utc("2026-10-30T11:00:00Z").Add(time.Nanosecond), utc("2026-10-30T17:00:00Z"))
}

// An unknown or missing zone is read as UTC, with one WARN per schedule
// received, however many lookups follow.
func TestNextCutoff_UnknownOrMissingZoneFallsBackToUTCWithOneWarn(t *testing.T) {
	for _, zone := range []string{"Mars/Olympus_Mons", ""} {
		t.Run("zone "+zone, func(t *testing.T) {
			var logs bytes.Buffer
			c := newTestConsumer(nil)
			c.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
			scheduleIn(t, c, zone, cutoffEvery("am", "11:00"))

			expectNext(t, c, utc("2026-10-30T09:56:00Z"), utc("2026-10-30T11:00:00Z"))
			expectNext(t, c, utc("2026-10-30T12:00:00Z"), utc("2026-10-31T11:00:00Z"))
			if _, ok := c.Schedule("site-1", utc("2026-10-30T09:56:00Z")); !ok {
				t.Fatal("Schedule: no slot")
			}
			if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
				t.Fatalf("WARN lines = %d, want exactly 1:\n%s", n, logs.String())
			}
			if !strings.Contains(logs.String(), "site-1") {
				t.Fatalf("the WARN does not name the site:\n%s", logs.String())
			}
		})
	}
}

// A known zone logs nothing.
func TestNextCutoff_KnownZoneDoesNotWarn(t *testing.T) {
	var logs bytes.Buffer
	c := newTestConsumer(nil)
	c.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	scheduleIn(t, c, "America/Chicago", cutoffEvery("am", "11:00"))
	expectNext(t, c, utc("2026-10-30T09:56:00Z"), utc("2026-10-30T16:00:00Z"))
	if logs.Len() != 0 {
		t.Fatalf("unexpected log output:\n%s", logs.String())
	}
}

// The node profile reads the same schedule: each slot keeps its LOCAL time and
// days (that is what is declared, with the zone), and its NextAt is the UTC
// instant of the next local occurrence, the key the capacity cache is read with.
func TestSchedule_SlotsKeepLocalTimesAndResolveNextAtInTheZone(t *testing.T) {
	c := newTestConsumer(nil)
	scheduleIn(t, c, "America/Chicago", cutoffEvery("am", "11:00"), cutoffEvery("fri-pm", "20:00", "Fri"))
	sched, ok := c.Schedule("site-1", utc("2026-10-31T00:30:00Z"))
	if !ok {
		t.Fatal("Schedule: no slot")
	}
	if sched.Timezone != "America/Chicago" {
		t.Fatalf("Timezone = %q, want America/Chicago", sched.Timezone)
	}
	got := map[string]time.Time{}
	for _, s := range sched.Slots {
		got[s.LocalTime] = s.NextAt
		if s.NextAt.Location() != time.UTC {
			t.Fatalf("slot %s NextAt in %v, want UTC", s.LocalTime, s.NextAt.Location())
		}
	}
	// Friday 19:30 CDT: today's 11:00 has passed, tomorrow's (Sat) is 16:00Z;
	// Friday's 20:00 is 30 minutes away.
	if want := utc("2026-10-31T16:00:00Z"); !got["11:00"].Equal(want) {
		t.Fatalf("11:00 NextAt = %v, want %v", got["11:00"], want)
	}
	if want := utc("2026-10-31T01:00:00Z"); !got["20:00"].Equal(want) {
		t.Fatalf("20:00 NextAt = %v, want %v", got["20:00"], want)
	}
}
