package main

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewLoggerAcceptsWarning pins gap fix D22: LOG_LEVEL=warning means
// warn in every binary of this repository, never a silent fallback to info.
func TestNewLoggerAcceptsWarning(t *testing.T) {
	for _, level := range []string{"warn", "warning", "WARNING"} {
		l := newLogger(level)
		if l.Enabled(context.Background(), slog.LevelInfo) {
			t.Fatalf("LOG_LEVEL=%s must suppress info", level)
		}
		if !l.Enabled(context.Background(), slog.LevelWarn) {
			t.Fatalf("LOG_LEVEL=%s must keep warn", level)
		}
	}
}

// TestNewLoggerLevels pins the rest of the LOG_LEVEL vocabulary: unset or
// unknown means info.
func TestNewLoggerLevels(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"bogus", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"ERROR", slog.LevelError},
	}
	for _, tt := range tests {
		l := newLogger(tt.level)
		if !l.Enabled(context.Background(), tt.want) {
			t.Fatalf("LOG_LEVEL=%q: level %v must be enabled", tt.level, tt.want)
		}
		if l.Enabled(context.Background(), tt.want-1) {
			t.Fatalf("LOG_LEVEL=%q: level below %v must be suppressed", tt.level, tt.want)
		}
	}
}
