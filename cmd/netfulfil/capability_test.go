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
	c := runCacheConsumer(context.Background(), logger, stoppedMsg, func(context.Context) error {
		return errors.New("coordinator not available")
	})
	<-c.done

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
	c := runCacheConsumer(context.Background(), logger, stoppedMsg, func(context.Context) error {
		return fmt.Errorf("fetching message: %w", context.Canceled)
	})
	<-c.done

	out := buf.String()
	if !strings.Contains(out, stoppedMsg) || !strings.Contains(out, "fetching message: context canceled") {
		t.Fatalf("log = %q, want %q naming the canceled error", out, stoppedMsg)
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
			c := runCacheConsumer(ctx, logger, stoppedMsg, func(ctx context.Context) error {
				<-ctx.Done()
				return result
			})
			cancel()
			<-c.done

			if out := buf.String(); out != "" {
				t.Fatalf("log = %q, want nothing for a stop the context asked for", out)
			}
		})
	}
}

// TestRunCacheConsumer_HandsServeTheContextItRunsOn: serve() decides death
// against this context, so it must be the one the consumer obeys.
func TestRunCacheConsumer_HandsServeTheContextItRunsOn(t *testing.T) {
	logger, _ := captured()
	ctx, cancel := context.WithCancel(context.Background())
	c := runCacheConsumer(ctx, logger, stoppedMsg, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	if c.ctx != ctx {
		t.Fatal("the handle must carry the context the consumer runs on")
	}
	cancel()
	<-c.done
}

// TestCacheConsumerStoppedByShutdownIsNotADeath replays the failure end to end
// with the real consumer wrapper and the real watcher: the process context the
// consumer runs on is cancelled, the consumer returns nil (what both caches'
// Run does once its context is done), and serve()'s own context has not been
// cancelled yet. The run exited 1 ("a background component stopped on its
// own") on this interleaving, in about one clean shutdown in 200.
func TestCacheConsumerStoppedByShutdownIsNotADeath(t *testing.T) {
	logger, _ := captured()
	processCtx, cancelProcess := context.WithCancel(context.Background())
	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe() // the cancellation never reaches serve's context in this test

	c := runCacheConsumer(processCtx, logger, stoppedMsg, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	died := watchDeaths(serveCtx, []namedDone{{"capability-offer cache consumer 1", c.done, c.ctx}})

	cancelProcess()
	<-c.done
	if name, ok := recvName(t, died); ok {
		t.Fatalf("a consumer stopped by the process context was reported as dead: %q", name)
	}
}

// TestCacheConsumerThatReturnsWhileItsContextIsLiveIsADeath keeps the other
// half of the rule: the consumer ended on its own, so the process must exit.
func TestCacheConsumerThatReturnsWhileItsContextIsLiveIsADeath(t *testing.T) {
	logger, _ := captured()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := runCacheConsumer(ctx, logger, stoppedMsg, func(context.Context) error {
		return errors.New("broker went away")
	})
	died := watchDeaths(ctx, []namedDone{{"capability-offer cache consumer 2", c.done, c.ctx}})

	name, ok := recvName(t, died)
	if !ok || name != "capability-offer cache consumer 2" {
		t.Fatalf("death report = %q (received=%v), want the consumer", name, ok)
	}
}
