package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
)

// ADR 0024 §5: the orphaned-hold sweep's two env knobs.

func warnLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestOrphanHoldSweepEnabled(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
		warn bool
	}{
		{"unset is ON by default: a leaked hold is the worse failure", "", true, false},
		{"true", "true", true, false},
		{"TRUE is case-insensitive", "TRUE", true, false},
		{"false turns it off", "false", false, false},
		{"False is case-insensitive", "False", false, false},
		{"0 turns it off", "0", false, false},
		{"garbage keeps it ON and warns", "maybe", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ORPHAN_HOLD_SWEEP_ENABLED", tt.raw)
			logger, buf := warnLogger()
			if got := orphanHoldSweepEnabled(logger); got != tt.want {
				t.Fatalf("orphanHoldSweepEnabled(%q) = %v, want %v", tt.raw, got, tt.want)
			}
			if warned := strings.Contains(buf.String(), "level=WARN"); warned != tt.warn {
				t.Fatalf("warned = %v, want %v (log: %s)", warned, tt.warn, buf.String())
			}
		})
	}
}

func TestOrphanHoldGrace(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want time.Duration
		warn bool
	}{
		{"unset uses the documented default of one hour", "", time.Hour, false},
		{"explicit duration", "30m", 30 * time.Minute, false},
		{"zero means no grace and is valid", "0", 0, false},
		{"zero with a unit is valid too", "0s", 0, false},
		{"negative falls back and warns", "-5m", time.Hour, true},
		{"garbage falls back and warns", "soon", time.Hour, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ORPHAN_HOLD_GRACE", tt.raw)
			logger, buf := warnLogger()
			if got := orphanHoldGrace(logger); got != tt.want {
				t.Fatalf("orphanHoldGrace(%q) = %v, want %v", tt.raw, got, tt.want)
			}
			if warned := strings.Contains(buf.String(), "level=WARN"); warned != tt.warn {
				t.Fatalf("warned = %v, want %v (log: %s)", warned, tt.warn, buf.String())
			}
		})
	}
}

// TestWireOrphanHoldSweep proves the composition root hands the parsed knobs
// to the use case, and builds nothing when the sweep is switched off.
func TestWireOrphanHoldSweep(t *testing.T) {
	t.Setenv("ORPHAN_HOLD_GRACE", "90m")

	t.Setenv("ORPHAN_HOLD_SWEEP_ENABLED", "false")
	if got := wireOrphanHoldSweep(memory.NewNetworkOrderRepo(), nil, nil, nil, quietLogger()); got != nil {
		t.Fatal("ORPHAN_HOLD_SWEEP_ENABLED=false must wire no sweep")
	}

	t.Setenv("ORPHAN_HOLD_SWEEP_ENABLED", "true")
	got := wireOrphanHoldSweep(memory.NewNetworkOrderRepo(), nil, nil, nil, quietLogger())
	if got == nil {
		t.Fatal("ORPHAN_HOLD_SWEEP_ENABLED=true wired no sweep")
	}
	if got.Grace != 90*time.Minute {
		t.Fatalf("Grace = %v, want 90m from ORPHAN_HOLD_GRACE", got.Grace)
	}
	if got.Clock == nil || got.Orders == nil {
		t.Fatalf("sweep is missing collaborators: %+v", got)
	}
	if got.UnitOfWork == nil {
		t.Fatal("the sweep always runs in a transaction: the unit of work must be wired (Postgres is the only runtime)")
	}
	if got.Metrics == nil {
		t.Fatal("the sweep must carry its outcome counter")
	}
}
