package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
)

// TestOutboxMaxAttempts pins OUTBOX_MAX_ATTEMPTS (gap fix D12, ADR 0021):
// unset means the fleet default of 20, 0 means unlimited, and an invalid or
// negative value falls back to the default with a warning instead of
// silently disabling the cap.
func TestOutboxMaxAttempts(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
		warn bool
	}{
		{name: "unset uses the default", raw: "", want: 20},
		{name: "explicit value", raw: "5", want: 5},
		{name: "zero means unlimited", raw: "0", want: 0},
		{name: "negative falls back", raw: "-1", want: 20, warn: true},
		{name: "garbage falls back", raw: "many", want: 20, warn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OUTBOX_MAX_ATTEMPTS", tt.raw)
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			if got := outboxMaxAttempts(logger); got != tt.want {
				t.Fatalf("outboxMaxAttempts(%q) = %d, want %d", tt.raw, got, tt.want)
			}
			if warned := strings.Contains(buf.String(), "level=WARN"); warned != tt.warn {
				t.Fatalf("warned = %v, want %v (log: %s)", warned, tt.warn, buf.String())
			}
		})
	}
}

// TestDefaultOutboxMaxAttemptsIsTwenty keeps the fleet default in one
// place: the postgres package constant cmd/netfulfil falls back to.
func TestDefaultOutboxMaxAttemptsIsTwenty(t *testing.T) {
	if postgres.DefaultOutboxMaxAttempts != 20 {
		t.Fatalf("DefaultOutboxMaxAttempts = %d, want 20 (fleet convention, ADR 0021)", postgres.DefaultOutboxMaxAttempts)
	}
}

// TestWireEventPublisher_OutboxRelayIsCapped proves the composition root
// hands the parsed cap to the relay it builds, not just that the parser
// works: with a pool, EVENT_PUBLISHER=kafka and OUTBOX_MAX_ATTEMPTS=7 the
// returned relay carries 7.
func TestWireEventPublisher_OutboxRelayIsCapped(t *testing.T) {
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "7")
	_, relay, closeFn := wireEventPublisher(&pgxpool.Pool{}, quietLogger())
	defer closeFn()
	if relay == nil {
		t.Fatal("outbox mode built no relay")
	}
	if got := relay.MaxAttempts(); got != 7 {
		t.Fatalf("relay.MaxAttempts() = %d, want 7 from OUTBOX_MAX_ATTEMPTS", got)
	}
}
