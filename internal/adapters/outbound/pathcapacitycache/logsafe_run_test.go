package pathcapacitycache

import (
	"context"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// TestRunLogsNoEventDerivedLineBreaks: the message-handling-failure log of
// the consume loop must carry no line break from the event payload embedded
// in the handling error (a malformed cutoff_at carries attacker text).
func TestRunLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	msg := buildEvent(t, typePathCapacityChanged, "id", capacityData{
		PathId: "path-1", CutoffAt: "not-a-time" + crlfForged, RemainingUnits: 1, Known: true,
	})
	c := newTestConsumer([]kafkago.Message{{Value: msg}})
	c.Logger = slog.New(rec)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec.assertNoLineBreaks(t, 1)
}
