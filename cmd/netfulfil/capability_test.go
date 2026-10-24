package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// captured returns a logger writing to a buffer. Reading the buffer after the
// consumer's done channel closed is race-free: runCacheConsumer logs before it
// closes done.
func captured() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

const stoppedMsg = "test cache consumer stopped"

// TestRunCacheConsumer_LogsAConsumerThatDiesOnItsOwn: an error returned while
// the consumer's context is live is the reason the process is about to exit 1,
// so it is logged with the error.
func TestRunCacheConsumer_LogsAConsumerThatDiesOnItsOwn(t *testing.T) {
	logger, buf := captured()
	done := runCacheConsumer(context.Background(), logger, stoppedMsg, func(context.Context) error {
		return errors.New("coordinator not available")
	})
	<-done

	out := buf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, stoppedMsg) || !strings.Contains(out, "coordinator not available") {
		t.Fatalf("log = %q, want an ERROR %q naming the error", out, stoppedMsg)
	}
}

// TestRunCacheConsumer_LogsACanceledErrorWhileItsContextIsLive: a
// context.Canceled that does not come from the consumer's own context (a Kafka
// reader's internal context, a closed reader) is a real failure. It used to be
// swallowed silently, which hid the cause of an exit 1.
func TestRunCacheConsumer_LogsACanceledErrorWhileItsContextIsLive(t *testing.T) {
	logger, buf := captured()
	done := runCacheConsumer(context.Background(), logger, stoppedMsg, func(context.Context) error {
		return fmt.Errorf("fetching message: %w", context.Canceled)
	})
	<-done

	out := buf.String()
	if !strings.Contains(out, stoppedMsg) || !strings.Contains(out, "fetching message: context canceled") {
		t.Fatalf("log = %q, want %q naming the canceled error", out, stoppedMsg)
	}
}

// TestRunCacheConsumer_LogsACleanReturnWhileItsContextIsLive: a consumer that
// returns nil with nobody having asked it to stop is as dead as one that
// returned an error, and the process exits 1 because of it.
func TestRunCacheConsumer_LogsACleanReturnWhileItsContextIsLive(t *testing.T) {
	logger, buf := captured()
	done := runCacheConsumer(context.Background(), logger, stoppedMsg, func(context.Context) error { return nil })
	<-done

	if out := buf.String(); !strings.Contains(out, stoppedMsg) {
		t.Fatalf("log = %q, want %q for a consumer that returned with its context live", out, stoppedMsg)
	}
}

// TestRunCacheConsumer_IsSilentWhenItsContextStopsIt: a stop that its own
// context asked for logs nothing, whatever the reader returned on the way out.
func TestRunCacheConsumer_IsSilentWhenItsContextStopsIt(t *testing.T) {
	for name, result := range map[string]error{
		"nil":              nil,
		"canceled":         context.Canceled,
		"wrapped canceled": fmt.Errorf("fetching message: %w", context.Canceled),
	} {
		t.Run(name, func(t *testing.T) {
			logger, buf := captured()
			ctx, cancel := context.WithCancel(context.Background())
			done := runCacheConsumer(ctx, logger, stoppedMsg, func(ctx context.Context) error {
				<-ctx.Done()
				return result
			})
			cancel()
			<-done

			if out := buf.String(); out != "" {
				t.Fatalf("log = %q, want nothing for a stop the context asked for", out)
			}
		})
	}
}
