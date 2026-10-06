package eventwire_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/eventwire"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// unmappedEvent is a DomainEvent the wire package has no DTO for.
type unmappedEvent struct{ At time.Time }

func (unmappedEvent) EventName() string       { return "Unmapped" }
func (e unmappedEvent) OccurredAt() time.Time { return e.At }

func TestPayload_FieldNamesPerEvent(t *testing.T) {
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		event shared.DomainEvent
		keys  []string
	}{
		{shared.NetworkOrderReceived{NetworkRef: "n", At: at},
			[]string{"networkRef", "siteId", "requiredShipBy", "acknowledgeBy", "lineCount", "at"}},
		{shared.NetworkOrderAcknowledged{NetworkRef: "n", At: at},
			[]string{"networkRef", "siteId", "localOrderId", "receivedAt", "at"}},
		{shared.NetworkOrderRejected{NetworkRef: "n", At: at},
			[]string{"networkRef", "siteId", "reason", "at"}},
		{shared.NetworkOrderShipmentConfirmed{NetworkRef: "n", At: at},
			[]string{"networkRef", "siteId", "localOrderId", "at"}},
		{shared.AcknowledgementDeadlineAtRisk{NetworkRef: "n", At: at},
			[]string{"networkRef", "siteId", "acknowledgeBy", "at"}},
	}
	for _, tt := range tests {
		t.Run(tt.event.EventName(), func(t *testing.T) {
			p, err := eventwire.Payload(tt.event)
			if err != nil {
				t.Fatalf("Payload: %v", err)
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.keys) {
				t.Errorf("got %d members %v, want exactly %v", len(got), got, tt.keys)
			}
			for _, k := range tt.keys {
				if _, ok := got[k]; !ok {
					t.Errorf("member %q missing from %s", k, raw)
				}
			}
		})
	}
}

func TestPayload_UnmappedEventIsAnError(t *testing.T) {
	if _, err := eventwire.Payload(unmappedEvent{}); err == nil || !strings.Contains(err.Error(), "no wire payload") {
		t.Fatalf("err = %v, want no-wire-payload error", err)
	}
}
