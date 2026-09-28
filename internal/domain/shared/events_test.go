package shared

import (
	"testing"
	"time"
)

func TestDomainEvents_EventNameAndOccurredAt(t *testing.T) {
	now := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		event DomainEvent
		want  string
	}{
		{
			name: "NetworkOrderReceived",
			event: NetworkOrderReceived{
				NetworkRef: "po-1", SiteId: "site-1",
				RequiredShipBy: now.Add(48 * time.Hour), AcknowledgeBy: now.Add(24 * time.Hour),
				LineCount: 2, At: now,
			},
			want: "NetworkOrderReceived",
		},
		{
			name: "NetworkOrderAcknowledged",
			event: NetworkOrderAcknowledged{
				NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1",
				ReceivedAt: now.Add(-time.Hour), At: now,
			},
			want: "NetworkOrderAcknowledged",
		},
		{
			name: "NetworkOrderRejected",
			event: NetworkOrderRejected{
				NetworkRef: "po-1", SiteId: "site-1",
				Reason: RejectionReasonUntranslatableSKU, At: now,
			},
			want: "NetworkOrderRejected",
		},
		{
			name: "NetworkOrderShipmentConfirmed",
			event: NetworkOrderShipmentConfirmed{
				NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", At: now,
			},
			want: "NetworkOrderShipmentConfirmed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.EventName(); got != tt.want {
				t.Fatalf("EventName() = %q, want %q", got, tt.want)
			}
			if got := tt.event.OccurredAt(); !got.Equal(now) {
				t.Fatalf("OccurredAt() = %v, want %v", got, now)
			}
		})
	}
}

func TestRejectionReasons_AreDistinctValues(t *testing.T) {
	seen := map[RejectionReason]bool{}
	for _, r := range []RejectionReason{
		RejectionReasonUntranslatableSKU,
		RejectionReasonInfeasibleDeadline,
		RejectionReasonAcknowledgementDeadlineMissed,
	} {
		if seen[r] {
			t.Fatalf("duplicate RejectionReason value %q", r)
		}
		seen[r] = true
		if r == "" {
			t.Fatal("RejectionReason must not be empty")
		}
	}
}
