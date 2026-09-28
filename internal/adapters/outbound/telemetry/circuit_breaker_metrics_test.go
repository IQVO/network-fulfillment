package telemetry_test

import (
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
)

// TestCircuitBreakerMetrics_SetStateUpdatesGaugePerDependency proves
// SetState records gobreaker's own state numbering verbatim, keyed by
// dependency, and that the gauge is actually served on the metrics'
// own dedicated registry (not global state).
func TestCircuitBreakerMetrics_SetStateUpdatesGaugePerDependency(t *testing.T) {
	metrics := telemetry.NewCircuitBreakerMetrics()

	metrics.SetState("order-management", 2)
	metrics.SetState("network-gateway", 0)

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != "circuit_breaker_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "dependency" {
					got[label.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}

	if got["order-management"] != 2 {
		t.Fatalf("order-management gauge = %v, want 2 (open)", got["order-management"])
	}
	if got["network-gateway"] != 0 {
		t.Fatalf("network-gateway gauge = %v, want 0 (closed)", got["network-gateway"])
	}
	if len(got) != 2 {
		t.Fatalf("got %d dependency series, want exactly 2: %v", len(got), got)
	}
}

// TestNewCircuitBreakerMetrics_UsesADedicatedRegistry proves two
// independent NewCircuitBreakerMetrics calls never collide -- each gets
// its own registry rather than sharing prometheus.DefaultRegisterer,
// which would panic on the second MustRegister of the same metric name.
func TestNewCircuitBreakerMetrics_UsesADedicatedRegistry(t *testing.T) {
	a := telemetry.NewCircuitBreakerMetrics()
	b := telemetry.NewCircuitBreakerMetrics()

	a.SetState("dep", 1)
	b.SetState("dep", 0)

	if a.Registry == b.Registry {
		t.Fatalf("two NewCircuitBreakerMetrics calls shared one registry, want dedicated registries")
	}
}
