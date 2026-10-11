package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestSetup_NeverBlocksOnAMissingCollector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	closeTelemetry, err := Setup(ctx, "network-fulfillment", "test", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Setup waited on the Collector")
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	_ = closeTelemetry(closeCtx)
}

func TestEnvironment_DefaultsToLocal(t *testing.T) {
	t.Setenv("ENVIRONMENT", "")
	if got := Environment(); got != "local" {
		t.Fatalf("Environment() = %q, want local", got)
	}
	t.Setenv("ENVIRONMENT", "kind")
	if got := Environment(); got != "kind" {
		t.Fatalf("Environment() = %q, want kind", got)
	}
}
