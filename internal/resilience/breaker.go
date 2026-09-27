// Package resilience holds the small pieces of circuit-breaker/timeout
// policy shared by network-fulfillment's outbound cross-context HTTP
// client (ADR 0004, ported verbatim from order-management's ADR 0025 —
// see that repo's internal/resilience for the reference this mirrors):
// the shared trip condition, the Prometheus-gauge wiring contract, and
// the context-deadline-propagation helper. It is a top-level internal
// package (a sibling of internal/domain, internal/application), not a
// domain/application/adapter layer itself, so the hexagonal fitness
// tests place no restriction on who may import it — only the outbound
// ordermanagement adapter does today.
//
// network-fulfillment has exactly ONE real sync cross-context HTTP
// client (ordermanagement.Planner), so unlike order-management (which
// has two independent breakers, one per dependency) this repo builds
// exactly one — but the shared tuning/helpers below are kept as their
// own package anyway, both to mirror the reference design verbatim and
// because any FUTURE second sync HTTP dependency should reuse this same
// tuning rather than inventing its own.
package resilience

import (
	"time"

	gobreaker "github.com/sony/gobreaker/v2"
)

// Shared breaker tuning (ADR 0004), used by ordermanagement.BreakerClient
// via its own DefaultBreakerSettings helper:
//
//   - DefaultMaxRequests: only 1 probe request is let through per
//     half-open cycle, so a still-broken dependency is confirmed broken
//     again with minimal extra load.
//   - DefaultInterval: the closed-state window gobreaker uses to reset
//     its rolling Counts. Without this, a failure streak from an hour
//     ago could combine with a fresh one to trip the breaker on stale
//     history.
//   - DefaultTimeout: how long the breaker stays open before allowing a
//     half-open probe.
const (
	DefaultMaxRequests = 1
	DefaultInterval    = 30 * time.Second
	DefaultTimeout     = 30 * time.Second

	// minRequestVolumeForErrorRate guards ReadyToTrip's error-rate leg:
	// without a minimum sample size, one early failure (1 request, a
	// 100% error rate) would trip the breaker on its own — exactly what
	// the ConsecutiveFailures>=5 leg already exists to gate sensibly.
	// The error-rate leg only engages once there is enough traffic for
	// "over half failed" to mean something.
	minRequestVolumeForErrorRate = 10
)

// ReadyToTrip is the shared trip condition for every breaker in this
// service: open the breaker when either five consecutive requests have
// failed, or — given at least minRequestVolumeForErrorRate requests in
// the current closed-state window — more than half of them failed.
func ReadyToTrip(counts gobreaker.Counts) bool {
	if counts.ConsecutiveFailures >= 5 {
		return true
	}
	if counts.Requests < minRequestVolumeForErrorRate {
		return false
	}
	failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
	return failureRate > 0.5
}

// StateRecorder receives a breaker's state transitions so they can be
// exposed as the circuit_breaker.state gauge (re-published as
// Prometheus metric circuit_breaker_state{dependency="..."}). The
// single implementation is
// internal/adapters/outbound/telemetry.CircuitBreakerMetrics.
//
// state follows gobreaker.State's own numbering verbatim (0=closed,
// 1=half-open, 2=open), so RecordStateChange needs no translation table.
type StateRecorder interface {
	SetState(dependency string, state int64)
}

// RecordStateChange adapts a StateRecorder into the shape
// gobreaker.Settings.OnStateChange expects for dependency. A nil
// recorder is a documented no-op (mirrors this repo's nil-Logger
// convention elsewhere), so a test that does not care about the metric
// never needs to construct one.
func RecordStateChange(dependency string, recorder StateRecorder) func(name string, from, to gobreaker.State) {
	return func(_ string, _, to gobreaker.State) {
		if recorder == nil {
			return
		}
		recorder.SetState(dependency, int64(to))
	}
}
