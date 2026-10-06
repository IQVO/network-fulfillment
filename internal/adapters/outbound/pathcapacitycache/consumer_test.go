package pathcapacitycache

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
)

func buildEvent(t *testing.T, eventType, subject string, data any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID("evt-" + eventType)
	e.SetSource("/warehouse/work-planning")
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

type fakeReader struct {
	msgs []kafkago.Message
	i    int
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

func (r *fakeReader) Close() error { return nil }

func newTestConsumer(msgs []kafkago.Message) *Consumer {
	return &Consumer{
		Reader:  &fakeReader{msgs: msgs},
		cache:   make(map[capacityKey]capacityValue),
		readyCh: make(chan struct{}),
	}
}

func TestRemainingCapacity_UnknownPathReturnsFalse(t *testing.T) {
	c := newTestConsumer(nil)
	_, known := c.RemainingCapacity("path-1", time.Now())
	if known {
		t.Fatal("expected known=false for a path never observed")
	}
}

func TestConsumer_AppliesPathCapacityChanged(t *testing.T) {
	c := newTestConsumer(nil)
	cutoff := time.Date(2026, 10, 5, 18, 0, 0, 0, time.UTC)
	msg := buildEvent(t, typePathCapacityChanged, "path-1", capacityData{
		PathId: "path-1", CutoffAt: cutoff.Format(time.RFC3339), RemainingUnits: 42, Known: true,
	})
	if err := c.handle(kafkago.Message{Value: msg}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	units, known := c.RemainingCapacity("path-1", cutoff)
	if !known || units != 42 {
		t.Fatalf("RemainingCapacity = (%d, %v), want (42, true)", units, known)
	}
}

func TestConsumer_KnownFalseIsReportedAsUnknown(t *testing.T) {
	c := newTestConsumer(nil)
	cutoff := time.Date(2026, 10, 5, 18, 0, 0, 0, time.UTC)
	msg := buildEvent(t, typePathCapacityChanged, "path-1", capacityData{
		PathId: "path-1", CutoffAt: cutoff.Format(time.RFC3339), RemainingUnits: 0, Known: false,
	})
	if err := c.handle(kafkago.Message{Value: msg}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	_, known := c.RemainingCapacity("path-1", cutoff)
	if known {
		t.Fatal("expected known=false when the event itself reports Known=false")
	}
}

func TestConsumer_IgnoresOtherEventFamilies(t *testing.T) {
	c := newTestConsumer(nil)
	msg := buildEvent(t, "com.warehouse.wes.work-planning.chargeforecast.ChargeForecastUpdated", "x", map[string]string{"a": "b"})
	if err := c.handle(kafkago.Message{Value: msg}); err != nil {
		t.Fatalf("handle unrelated type: %v", err)
	}
}

func TestConsumer_HandleRejectsNonCloudEvent(t *testing.T) {
	c := newTestConsumer(nil)
	if err := c.handle(kafkago.Message{Value: []byte("garbage")}); err == nil {
		t.Fatal("expected an error for a non-CloudEvents message")
	}
}

func TestConsumer_HandleRejectsMalformedCutoff(t *testing.T) {
	c := newTestConsumer(nil)
	msg := buildEvent(t, typePathCapacityChanged, "path-1", capacityData{
		PathId: "path-1", CutoffAt: "not-a-time", RemainingUnits: 1, Known: true,
	})
	if err := c.handle(kafkago.Message{Value: msg}); err == nil {
		t.Fatal("expected an error for a malformed cutoff_at")
	}
}

func TestConsumer_ReadyImmediatelyWhenTopicEmpty(t *testing.T) {
	c := newTestConsumer(nil)
	c.target = targetOffsets{}
	if len(c.target) == 0 {
		c.markReady()
	}
	if !c.Ready() {
		t.Fatal("expected Ready() = true for an empty target topic")
	}
}

func TestConsumer_CheckReadyMarksReadyAtTargetOffset(t *testing.T) {
	c := newTestConsumer(nil)
	c.target = targetOffsets{0: 3}
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
