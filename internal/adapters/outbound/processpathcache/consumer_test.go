package processpathcache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// buildEvent constructs a valid CloudEvents 1.0 structured-mode message
// value for an arbitrary `type`/payload, so these tests can drive the
// consumer with process-path-management's own event vocabulary without
// depending on this repo's own cloudevents.New (which stamps THIS
// service's subdomain/context segment, not process-path-management's).
func buildEvent(t *testing.T, eventType, subject string, data any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID("evt-" + eventType)
	e.SetSource("/warehouse/process-path-management")
	e.SetType(eventType)
	e.SetSubject(subject)
	e.SetTime(time.Now().UTC())
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// fakeReader feeds a fixed slice of messages, then blocks until ctx is
// cancelled -- mirroring a real kafka.Reader whose ReadMessage blocks
// once it is caught up, so Consumer.Run's loop behaves identically in
// tests to production.
type fakeReader struct {
	msgs   []kafkago.Message
	i      int
	closed bool
}

func (r *fakeReader) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	if r.i < len(r.msgs) {
		m := r.msgs[r.i]
		r.i++
		return m, nil
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

func newTestConsumer(msgs []kafkago.Message) *Consumer {
	return &Consumer{
		Reader:    &fakeReader{msgs: msgs},
		schedules: make(map[shared.SiteId]scheduleData),
		paths:     make(map[string]pathState),
		readyCh:   make(chan struct{}),
	}
}

// drive runs Consumer.Run until every seeded message has been handled
// (polling handle()'s observable effects would be racy; instead this
// calls handle() directly for determinism and uses Run only in the
// dedicated readiness test below).
func drive(c *Consumer, msgs []kafkago.Message) {
	for _, m := range msgs {
		_ = c.handle(m)
	}
}

func TestNextCutoff_NoScheduleReturnsNotFound(t *testing.T) {
	c := newTestConsumer(nil)
	_, ok := c.NextCutoff("site-1", time.Now())
	if ok {
		t.Fatal("expected ok=false with no schedule cached")
	}
}

func TestConsumer_AppliesPathCreatedAndSchedule(t *testing.T) {
	c := newTestConsumer(nil)

	pathMsg := buildEvent(t, typeCreated, "path-1", pathData{PathId: "path-1", CycleTimeP95: "45m0s"})
	scheduleMsg := buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId:   "site-1",
		Timezone: "UTC",
		Cutoffs: []cutoffData{
			{CptId: "cpt-1", LocalTime: "18:00", DaysOfWeek: allDays(), ShipMethod: "GROUND", EligiblePathIds: []string{"path-1"}},
		},
	})

	drive(c, []kafkago.Message{{Value: pathMsg}, {Value: scheduleMsg}})

	after := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) // a Monday
	next, ok := c.NextCutoff("site-1", after)
	if !ok {
		t.Fatal("expected a cutoff to be found")
	}
	if len(next.Paths) != 1 {
		t.Fatalf("paths = %d, want 1", len(next.Paths))
	}
	if next.Paths[0].PathId != "path-1" {
		t.Errorf("PathId = %q, want path-1", next.Paths[0].PathId)
	}
	if !next.Paths[0].CycleTimeKnown || next.Paths[0].CycleTimeP95 != 45*time.Minute {
		t.Errorf("CycleTimeP95 = %v known=%v, want 45m known=true", next.Paths[0].CycleTimeP95, next.Paths[0].CycleTimeKnown)
	}
}

func TestConsumer_DeactivatedPathLosesCycleTimeButStaysEligible(t *testing.T) {
	c := newTestConsumer(nil)
	pathMsg := buildEvent(t, typeCreated, "path-1", pathData{PathId: "path-1", CycleTimeP95: "45m0s"})
	deactivatedMsg := buildEvent(t, typeDeactivated, "path-1", deactivatedData{PathId: "path-1"})
	scheduleMsg := buildEvent(t, typeCPTScheduleChanged, "site-1", scheduleData{
		SiteId: "site-1",
		Cutoffs: []cutoffData{
			{CptId: "cpt-1", LocalTime: "18:00", DaysOfWeek: allDays(), EligiblePathIds: []string{"path-1"}},
		},
	})
	drive(c, []kafkago.Message{{Value: pathMsg}, {Value: deactivatedMsg}, {Value: scheduleMsg}})

	after := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	next, ok := c.NextCutoff("site-1", after)
	if !ok {
		t.Fatal("expected a cutoff to still be found (deactivation only drops cycle-time knowledge)")
	}
	if len(next.Paths) != 1 || next.Paths[0].CycleTimeKnown {
		t.Fatalf("paths = %+v, want 1 path with CycleTimeKnown=false", next.Paths)
	}
}

func TestConsumer_IgnoresUnknownEventType(t *testing.T) {
	c := newTestConsumer(nil)
	msg := buildEvent(t, "com.warehouse.wes.process-path-management.processpath.SomethingElse", "path-1", map[string]string{"x": "y"})
	if err := c.handle(kafkago.Message{Value: msg}); err != nil {
		t.Fatalf("handle unknown type: %v", err)
	}
}

func TestConsumer_HandleRejectsNonCloudEvent(t *testing.T) {
	c := newTestConsumer(nil)
	err := c.handle(kafkago.Message{Value: []byte("not json")})
	if err == nil {
		t.Fatal("expected an error for a non-CloudEvents message")
	}
}

func TestConsumer_ReadyImmediatelyWhenTopicEmpty(t *testing.T) {
	c := newTestConsumer(nil)
	c.target = targetOffsets{} // newTargetOffsets' empty-topic result
	if len(c.target) == 0 {
		c.markReady()
	}
	if !c.Ready() {
		t.Fatal("expected Ready() = true for an empty target topic")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func TestConsumer_CheckReadyMarksReadyAtTargetOffset(t *testing.T) {
	c := newTestConsumer(nil)
	c.target = targetOffsets{0: 3} // last offset observed was 2 (target=last, i.e. exclusive-of-next)
	if c.Ready() {
		t.Fatal("should not be ready before reaching target")
	}
	c.checkReady(kafkago.Message{Partition: 0, Offset: 1})
	if c.Ready() {
		t.Fatal("should not be ready yet")
	}
	c.checkReady(kafkago.Message{Partition: 0, Offset: 2})
	if !c.Ready() {
		t.Fatal("expected Ready() = true once offset reaches target-1")
	}
}

func TestConsumer_RunStopsOnContextCancel(t *testing.T) {
	c := newTestConsumer(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestIsUnknownTopic(t *testing.T) {
	if !isUnknownTopic(kafkago.UnknownTopicOrPartition) {
		t.Fatal("expected isUnknownTopic(UnknownTopicOrPartition) = true")
	}
	if isUnknownTopic(errors.New("some other error")) {
		t.Fatal("expected isUnknownTopic(other) = false")
	}
}

func allDays() []string {
	return []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
}
