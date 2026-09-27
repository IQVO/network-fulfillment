// Package telemetry holds network-fulfillment's Prometheus-facing
// metrics. Unlike order-management (which re-exports an OTel Int64Gauge
// through its OTel Collector pipeline), this repo has no OTel/otelchi
// wiring at all (see cmd/netfulfil-projector's own doc comment: "this
// process is trace-free... network-fulfillment has no
// observability/OTel package"), so this package goes straight to
// github.com/prometheus/client_golang against its own dedicated
// registry, served at GET /metrics (internal/adapters/inbound/http).
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Registry is this service's dedicated Prometheus registry. A
// dedicated registry (rather than the global prometheus.DefaultRegisterer)
// keeps this package's metric surface explicit and testable: a test can
// construct its own CircuitBreakerMetrics against a throwaway registry
// without touching global state, and NewCircuitBreakerMetrics is the
// only thing in this repo that registers anything.
func newRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

// circuitBreakerGaugeName follows the fleet's existing
// circuit_breaker_state{dependency="..."} convention (see
// order-management's ADR 0025 / this repo's ADR 0004), 0=closed,
// 1=half-open, 2=open — gobreaker's own numbering verbatim, no
// translation table.
const circuitBreakerGaugeName = "circuit_breaker_state"

// CircuitBreakerMetrics implements resilience.StateRecorder. It owns its
// own dedicated *prometheus.Registry (Registry field) so the composition
// root can serve it at GET /metrics without depending on package-level
// global state.
type CircuitBreakerMetrics struct {
	Registry *prometheus.Registry
	gauge    *prometheus.GaugeVec
}

// NewCircuitBreakerMetrics builds and registers the circuit_breaker_state
// gauge on a fresh, dedicated registry. This never fails (unlike
// order-management's OTel-instrument-name variant): client_golang's
// GaugeVec construction/registration on a brand-new registry has no
// failure mode worth propagating, so callers never need an error branch
// here — mirrors this repo's existing "telemetry that would rather run
// degraded than crash boot" posture (e.g. seedStubDemand only refuses
// boot on a genuine misconfiguration, never on an optional integration).
func NewCircuitBreakerMetrics() *CircuitBreakerMetrics {
	registry := newRegistry()
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: circuitBreakerGaugeName,
		Help: "Circuit breaker state per downstream dependency (0=closed, 1=half-open, 2=open).",
	}, []string{"dependency"})
	registry.MustRegister(gauge)
	return &CircuitBreakerMetrics{Registry: registry, gauge: gauge}
}

// SetState implements resilience.StateRecorder. state is gobreaker.State's
// own int value (0/1/2), recorded verbatim.
func (m *CircuitBreakerMetrics) SetState(dependency string, state int64) {
	m.gauge.WithLabelValues(dependency).Set(float64(state))
}
