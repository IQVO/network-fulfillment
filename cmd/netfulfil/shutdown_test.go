package main

import (
	"context"
	"testing"
	"time"
)

func recvName(t *testing.T, ch <-chan string) (string, bool) {
	t.Helper()
	select {
	case name := <-ch:
		return name, true
	case <-time.After(200 * time.Millisecond):
		return "", false
	}
}

// TestWatchDeaths_ReportsAGoroutineThatStopsOnItsOwn pins the rule that the
// process exits non-zero when the relay or a consumer dies while the
// process is meant to be running.
func TestWatchDeaths_ReportsAGoroutineThatStopsOnItsOwn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alive := make(chan struct{})
	dying := make(chan struct{})
	died := watchDeaths(ctx, []namedDone{{"alive", alive}, {"dying", dying}})

	if name, ok := recvName(t, died); ok {
		t.Fatalf("nothing has stopped yet, got a death report for %q", name)
	}
	close(dying)
	name, ok := recvName(t, died)
	if !ok || name != "dying" {
		t.Fatalf("death report = %q (received=%v), want \"dying\"", name, ok)
	}
}

// TestWatchDeaths_IgnoresStopsCausedByShutdown: a goroutine that finishes
// because the context was cancelled (a signal) is a clean stop, not a death.
func TestWatchDeaths_IgnoresStopsCausedByShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	died := watchDeaths(ctx, []namedDone{{"relay", done}})

	cancel()
	close(done)
	if name, ok := recvName(t, died); ok {
		t.Fatalf("a stop after shutdown began must not be reported, got %q", name)
	}
}
