package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// bootRetries and bootRetryDelay bound the startup retry budget.
//
// ~31s total (1+2+4+8+16), comfortably past the ~10s first-dial reset and
// still far inside the liveness probe's own tolerance, so a genuinely
// unreachable database still fails the pod rather than hanging it.
const (
	bootRetries    = 5
	bootRetryDelay = time.Second
)

// retry runs op with exponential backoff, returning the LAST error so a
// permanent failure still reports its real cause rather than "timed out".
func retry(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return retryWithDelay(ctx, logger, what, bootRetryDelay, op)
}

// retryWithDelay is retry with the base delay injected, so tests can
// exercise the give-up path without sleeping out the real ~31s budget.
func retryWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	delay := base
	var err error
	for attempt := 1; attempt <= bootRetries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == bootRetries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, bootRetries, err)
}
