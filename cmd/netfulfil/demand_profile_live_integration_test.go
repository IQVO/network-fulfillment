//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/pathcapacitycache"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/processpathcache"
)

// The node profile (ADR 0027 part 1) through the REAL entry point: run(ctx) with
// NETWORK_MODE=live against a real Postgres and a real Kafka whose two cache
// topics carry a CPT schedule, a path with a cycle time and the path's
// remaining capacity, and a stateful fake of the Vendor API that judges a profile
// revision the way node-topology does. Named to sort before e2e_integration_test.go,
// whose last subtest stops the broker it shares with the tests after it.
//
// What it proves: the profile is declared once as revision 1 from what the
// caches say; a pass over unchanged caches sends nothing; a changed schedule is
// revision 2; and after a restart nothing is re-sent and the next change is
// revision 3, never a revision that was used.

const (
	profilePath       = "/vendor/nodes/N-1/profile"
	profilePathID     = "P-1"
	profileCycleTime  = "2m30s" // declared as 3 minutes: rounded UP, never less
	profileCeiling    = 30      // remaining capacity of P-1 at each cutoff
	profileTimezone   = "America/Sao_Paulo"
	pathCreatedType   = "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
	scheduleType      = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
	pathCapacityType  = "com.warehouse.wes.work-planning.workpool.PathCapacityChanged"
	profileDaysAhead  = 9
	profileEveryTick  = "1s"
	profileQuietTicks = 3500 * time.Millisecond // three passes at profileEveryTick, and then some
)

var (
	allDayNames   = []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	noSundayNames = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
)

// profileFeed publishes what process-path-management and wes-work-planning
// would publish for the site: the cutoffs sit 6 and 12 hours from now (UTC, which
// is how the cache reads a local_time), so no cutoff is ever too close for the
// path's cycle time to fit.
type profileFeed struct {
	t       *testing.T
	ctx     context.Context
	brokers []string
	seq     int
	times   []string // the two cutoff local times, in order
	now     time.Time
}

func newProfileFeed(t *testing.T, ctx context.Context, brokers []string) *profileFeed {
	t.Helper()
	now := time.Now().UTC()
	return &profileFeed{
		t: t, ctx: ctx, brokers: brokers, now: now,
		times: []string{now.Add(6 * time.Hour).Format("15:04"), now.Add(12 * time.Hour).Format("15:04")},
	}
}

func (f *profileFeed) message(eventType, subject string, data any) kafkago.Message {
	f.t.Helper()
	f.seq++
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(fmt.Sprintf("evt-profile-%d", f.seq))
	e.SetSource("/warehouse/integration-test")
	e.SetType(eventType)
	e.SetSubject(subject)
	e.SetTime(time.Now().UTC())
	if err := e.SetData("application/json", data); err != nil {
		f.t.Fatalf("SetData: %v", err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		f.t.Fatalf("marshal event: %v", err)
	}
	return kafkago.Message{Key: []byte(subject), Value: raw}
}

func (f *profileFeed) write(topic string, msgs ...kafkago.Message) {
	f.t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(f.brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	defer func() { _ = w.Close() }()
	if err := w.WriteMessages(f.ctx, msgs...); err != nil {
		f.t.Fatalf("publish to %s: %v", topic, err)
	}
}

// publishPath publishes the path with its cycle-time p95.
func (f *profileFeed) publishPath() {
	f.t.Helper()
	f.write(processpathcache.Topic, f.message(pathCreatedType, profilePathID,
		map[string]any{"path_id": profilePathID, "cycle_time_p95": profileCycleTime}))
}

// publishSchedule publishes the site's schedule (a snapshot: it replaces the last).
func (f *profileFeed) publishSchedule(days []string) {
	f.t.Helper()
	cutoffs := make([]map[string]any, len(f.times))
	for i, lt := range f.times {
		cutoffs[i] = map[string]any{
			"cpt_id": fmt.Sprintf("cpt-%d", i+1), "local_time": lt, "days_of_week": days,
			"ship_method": "GROUND", "eligible_path_ids": []string{profilePathID},
		}
	}
	f.write(processpathcache.Topic, f.message(scheduleType, liveSite,
		map[string]any{"site_id": liveSite, "timezone": profileTimezone, "cutoffs": cutoffs}))
}

// publishCapacity publishes the path's remaining capacity for every occurrence of
// both cutoffs from yesterday to a week and a half ahead, which covers whichever
// day is the next one for a schedule with or without Sunday.
func (f *profileFeed) publishCapacity() {
	f.t.Helper()
	var msgs []kafkago.Message
	for off := -1; off <= profileDaysAhead; off++ {
		for _, lt := range f.times {
			var h, m int
			_, _ = fmt.Sscanf(lt, "%d:%d", &h, &m)
			at := time.Date(f.now.Year(), f.now.Month(), f.now.Day()+off, h, m, 0, 0, time.UTC)
			msgs = append(msgs, f.message(pathCapacityType, profilePathID, map[string]any{
				"path_id": profilePathID, "cutoff_at": at.Format(time.RFC3339), "remaining_units": profileCeiling, "known": true,
			}))
		}
	}
	f.write(pathcapacitycache.Topic, msgs...)
}

// profileBody is the part of the PUT body this test reads.
type profileBody struct {
	VendorID            string                     `json:"vendorId"`
	ProfileRevision     int                        `json:"profileRevision"`
	Timezone            string                     `json:"timezone"`
	OperatingHoursByDay map[string]json.RawMessage `json:"operatingHoursByDay"`
	HandlingTimeMinutes int                        `json:"handlingTimeMinutes"`
	Cutoffs             []struct {
		LocalTime string   `json:"localTime"`
		MaxOrders *int     `json:"maxOrders"`
		Days      []string `json:"days"`
	} `json:"cutoffs"`
	ThroughputCap struct {
		Value    int    `json:"value"`
		TimeUnit string `json:"timeUnit"`
		Unit     string `json:"unit"`
	} `json:"throughputCap"`
}

func decodeProfile(t *testing.T, w fakeProfileWrite) profileBody {
	t.Helper()
	var b profileBody
	if err := json.Unmarshal([]byte(w.Body), &b); err != nil {
		t.Fatalf("profile body is not JSON: %v\n%s", err, w.Body)
	}
	return b
}

func waitForProfileWrites(t *testing.T, ctx context.Context, rig *liveRig, n int) {
	t.Helper()
	e2eWaitFor(t, ctx, 90*time.Second, fmt.Sprintf("%d node profile writes on the network", n), func() bool {
		return len(rig.network.profileWrites()) == n
	})
	// The write is accepted before the transaction is read; wait for the outcome too.
	e2eWaitFor(t, ctx, 30*time.Second, fmt.Sprintf("the network holds profile revision %d", n), func() bool {
		return rig.network.profileRevisionOf("N-1") == n
	})
}

// expectFirstProfile checks revision 1: what was declared, from what, and how it was kept.
func expectFirstProfile(t *testing.T, ctx context.Context, rig *liveRig, feed *profileFeed) {
	t.Helper()
	writes := rig.network.profileWrites()
	expectEqual(t, "profile writes", len(writes), 1)
	expectEqual(t, "idempotency key", writes[0].Key, "nf-profile-"+liveSite+"-1")
	b := decodeProfile(t, writes[0])
	expectEqual(t, "vendorId", b.VendorID, liveVendorID)
	expectEqual(t, "profileRevision", b.ProfileRevision, 1)
	expectEqual(t, "timezone", b.Timezone, profileTimezone)
	expectEqual(t, "handlingTimeMinutes", b.HandlingTimeMinutes, 3)
	expectEqual(t, "throughputCap", fmt.Sprintf("%d %s per %s", b.ThroughputCap.Value, b.ThroughputCap.Unit, b.ThroughputCap.TimeUnit), "60 ORDER per DAY")
	expectEqual(t, "cutoffs", len(b.Cutoffs), 2)
	times := slices.Sorted(slices.Values(feed.times))
	for i, c := range b.Cutoffs {
		expectEqual(t, "cutoff time", c.LocalTime, times[i])
		if c.MaxOrders == nil || *c.MaxOrders != profileCeiling {
			t.Fatalf("cutoff %s maxOrders = %v, want %d (the path's remaining capacity)", c.LocalTime, c.MaxOrders, profileCeiling)
		}
		expectEqual(t, "days of "+c.LocalTime, len(c.Days), 7)
	}
	expectEqual(t, "operating days", len(b.OperatingHoursByDay), 7)
	expectEqual(t, "ledger row", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'NODE_PROFILE' AND subject = $1`, liveSite+"#1#1"), "SUCCESS")
	expectEqual(t, "persisted revision", rig.count(t, ctx, `SELECT revision FROM network_node_profiles WHERE site_code = $1`, liveSite), 1)
}

// expectNothingResent waits for several passes and checks the network heard nothing more.
func expectNothingResent(t *testing.T, rig *liveRig, writes int) {
	t.Helper()
	time.Sleep(profileQuietTicks)
	expectEqual(t, "PUT requests", rig.network.requestCount(http.MethodPut, profilePath), writes)
	expectEqual(t, "profile writes", len(rig.network.profileWrites()), writes)
}

// expectRevision checks a later revision: its key, its content and its ledger row.
func expectRevision(t *testing.T, ctx context.Context, rig *liveRig, revision int, days []string) {
	t.Helper()
	writes := rig.network.profileWrites()
	expectEqual(t, "profile writes", len(writes), revision)
	w := writes[revision-1]
	expectEqual(t, "idempotency key", w.Key, fmt.Sprintf("nf-profile-%s-%d", liveSite, revision))
	expectEqual(t, "revision in the body", w.Revision, revision)
	b := decodeProfile(t, w)
	expectEqual(t, "profileRevision", b.ProfileRevision, revision)
	for _, c := range b.Cutoffs {
		if !slices.Equal(c.Days, lowerDays(days)) {
			t.Fatalf("cutoff %s days = %v, want %v", c.LocalTime, c.Days, lowerDays(days))
		}
	}
	expectEqual(t, "operating days", len(b.OperatingHoursByDay), len(days))
	expectEqual(t, "ledger row", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'NODE_PROFILE' AND subject = $1`, fmt.Sprintf("%s#%d#1", liveSite, revision)), "SUCCESS")
	expectEqual(t, "persisted revision", rig.count(t, ctx, `SELECT revision FROM network_node_profiles WHERE site_code = $1`, liveSite), revision)
	expectEqual(t, "revision the network holds", rig.network.profileRevisionOf("N-1"), revision)
}

// lowerDays turns the schedule's day names into the Vendor API's.
func lowerDays(days []string) []string {
	full := map[string]string{"Sun": "sunday", "Mon": "monday", "Tue": "tuesday", "Wed": "wednesday", "Thu": "thursday", "Fri": "friday", "Sat": "saturday"}
	out := make([]string, len(days))
	for i, d := range days {
		out[i] = full[d]
	}
	return out
}

// TestLiveProfile_DeclaredOnceChangedByTheScheduleAndContinuedAcrossARestart is the
// node profile end to end. The site is already a node on the network (adoption),
// so the demand loop maps it and the profile worker declares for it.
func TestLiveProfile_DeclaredOnceChangedByTheScheduleAndContinuedAcrossARestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startLiveRig(t, ctx)
	expectEqual(t, "adopted node", rig.network.addNode(liveSite), "N-1")
	feed := newProfileFeed(t, ctx, rig.env.brokers)
	feed.publishPath()
	feed.publishCapacity()
	feed.publishSchedule(allDayNames)

	env := map[string]string{"NETWORK_PROFILE_INTERVAL": profileEveryTick, "POLL_INTERVAL": "500ms"}
	stop := rig.start(t, ctx, env)
	waitForProfileWrites(t, ctx, rig, 1)

	t.Run("the first declaration is revision 1, built from the caches", func(t *testing.T) { expectFirstProfile(t, ctx, rig, feed) })
	t.Run("passes over unchanged caches send nothing", func(t *testing.T) { expectNothingResent(t, rig, 1) })

	feed.publishSchedule(noSundayNames)
	waitForProfileWrites(t, ctx, rig, 2)
	t.Run("a changed schedule is revision 2", func(t *testing.T) { expectRevision(t, ctx, rig, 2, noSundayNames) })

	mustStop(t, stop)
	stop = rig.start(t, ctx, env)
	t.Run("a restart re-sends nothing", func(t *testing.T) { expectNothingResent(t, rig, 2) })

	feed.publishSchedule(allDayNames)
	waitForProfileWrites(t, ctx, rig, 3)
	t.Run("after the restart the next change is revision 3, never a used one", func(t *testing.T) { expectRevision(t, ctx, rig, 3, allDayNames) })

	mustStop(t, stop)
}
