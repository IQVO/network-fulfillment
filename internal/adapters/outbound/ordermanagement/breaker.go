// breaker.go wraps Planner with a circuit breaker (sony/gobreaker/v2,
// ADR 0004 — ported verbatim from order-management's ADR 0025) guarding
// every one of its three calls (RaiseHeldOrder, ReleaseHeldOrder,
// CancelHeldOrder) through the SAME breaker instance: one breaker per
// downstream DEPENDENCY (order-management), not one per HTTP verb,
// exactly mirroring order-management's own
// inventorystorage.BreakerClient shape for its two mutating methods.
//
// Unlike order-management's reference (which reuses each client's
// pre-existing permissive/fail-open OR fail-loud mode switch as the
// breaker's OPEN-state fallback), network-fulfillment's Planner has NO
// such mode switch to reuse: it is the only sync cross-context HTTP
// client in this repo, and its current behaviour on any failure
// (network error, non-2xx status, decode failure) is already to
// PROPAGATE the error unchanged — Planner.do returns a plain error, and
// every caller (ReceiveNetworkDemand, SweepAcknowledgementDeadlines)
// already treats a Planner failure as a failure of the whole use case,
// with no soft/permissive path anywhere in this call chain. So there is
// no pre-existing "fallback semantics" to inherit; per the same
// principle the reference ADR states (the breaker decides WHEN to fall
// back, not WHAT the fallback is — and inventing a NEW fallback here
// would be exactly the kind of new semantics that principle forbids),
// this breaker's OPEN-state behaviour is simply to propagate a
// wrapped error, identical in shape to what Planner already returns on
// a real failure. See ADR 0004 for the full reasoning and the
// alternative considered and rejected.
//
// This is also, deliberately, the ONLY outbound client in this service
// that never retries a call: RaiseHeldOrder (POST /orders),
// ReleaseHeldOrder (POST .../release) and CancelHeldOrder
// (DELETE /orders/{id}) are all mutations with no HTTP-level
// idempotency-key protection on this call path (order-management's own
// idempotency-key middleware protects ITS create endpoint from a
// caller's retry, but nothing here guarantees a blind retry of a LOST
// response is safe) — see the reference's identical
// "retry-only-on-reads" rule; this repo simply has zero read-only calls
// to apply it to.
package ordermanagement

import (
	"context"
	"errors"
	"fmt"
	"time"

	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	"github.com/claudioed/network-fulfillment/internal/resilience"
)

// DependencyName labels this breaker's Prometheus gauge series
// (circuit_breaker_state{dependency="order-management"}).
const DependencyName = "order-management"

// FulfillmentPlanner is the subset of ports.FulfillmentPlanner this
// package's own Planner satisfies, declared locally so BreakerClient can
// wrap it without importing the ports package's full name (avoids an
// import cycle risk if ports ever needed to reference this adapter,
// and mirrors order-management's identical local-interface pattern in
// its own breaker.go files).
type FulfillmentPlanner interface {
	RaiseHeldOrder(ctx context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error)
	ReleaseHeldOrder(ctx context.Context, id shared.LocalOrderId) error
	CancelHeldOrder(ctx context.Context, id shared.LocalOrderId) error
}

// BreakerClient wraps Planner with a circuit breaker guarding all three
// methods through the SAME breaker instance (one breaker per downstream
// dependency, not one per HTTP verb) — sharing a
// gobreaker.CircuitBreaker[any] because the three methods have different
// return types; see each Execute call site below for how it recovers
// its own concrete type.
type BreakerClient struct {
	breaker *gobreaker.CircuitBreaker[any]
	inner   FulfillmentPlanner
}

var _ ports.FulfillmentPlanner = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient wrapping inner. recorder is
// resilience.StateRecorder (typically
// telemetry.CircuitBreakerMetrics) — nil is a valid, documented no-op
// (see resilience.RecordStateChange), so a test that does not care
// about the metric never needs to construct one.
func NewBreakerClient(inner FulfillmentPlanner, recorder resilience.StateRecorder) *BreakerClient {
	return newBreakerClient(inner, recorder, resilience.DefaultTimeout)
}

// NewBreakerClientWithTimeout is NewBreakerClient with an explicit
// breaker cooldown (gobreaker.Settings.Timeout) instead of
// resilience.DefaultTimeout, so a half-open-recovery test does not have
// to sleep for the full production cooldown in real time. Production
// code should always use NewBreakerClient; this exists for tests.
func NewBreakerClientWithTimeout(inner FulfillmentPlanner, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return newBreakerClient(inner, recorder, cooldown)
}

func newBreakerClient(inner FulfillmentPlanner, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return &BreakerClient{
		breaker: gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
			Name:        DependencyName,
			MaxRequests: resilience.DefaultMaxRequests,
			Interval:    resilience.DefaultInterval,
			Timeout:     cooldown,
			ReadyToTrip: resilience.ReadyToTrip,
			// The caller giving up (request cancelled) is not this
			// dependency's fault; don't let it count as a failure
			// against the breaker either way.
			IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner: inner,
	}
}

// ErrCircuitOpen wraps gobreaker's own rejection so a caller (and its
// logs) sees this dependency's name without needing to know gobreaker's
// sentinel errors. There is no fallback behind this — see the package
// doc comment for why.
var ErrCircuitOpen = errors.New("order-management: circuit breaker open, call not attempted")

// RaiseHeldOrder derives its timeout from the inbound request's
// remaining deadline (capped at DefaultTimeout — see
// resilience.CallTimeout), then routes the call through the breaker.
func (c *BreakerClient) RaiseHeldOrder(ctx context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	callCtx, cancel := resilience.CallTimeout(ctx, resilience.DefaultTimeout)
	defer cancel()

	v, err := c.breaker.Execute(func() (any, error) {
		return c.inner.RaiseHeldOrder(callCtx, req)
	})
	if isBreakerRejection(err) {
		return contract.HeldOrderResult{}, fmt.Errorf("%w: %v", ErrCircuitOpen, err)
	}
	if err != nil {
		return contract.HeldOrderResult{}, err
	}
	return v.(contract.HeldOrderResult), nil
}

// ReleaseHeldOrder mirrors RaiseHeldOrder's breaker/timeout shape.
func (c *BreakerClient) ReleaseHeldOrder(ctx context.Context, id shared.LocalOrderId) error {
	callCtx, cancel := resilience.CallTimeout(ctx, resilience.DefaultTimeout)
	defer cancel()

	_, err := c.breaker.Execute(func() (any, error) {
		return nil, c.inner.ReleaseHeldOrder(callCtx, id)
	})
	if isBreakerRejection(err) {
		return fmt.Errorf("%w: %v", ErrCircuitOpen, err)
	}
	return err
}

// CancelHeldOrder mirrors RaiseHeldOrder's breaker/timeout shape.
func (c *BreakerClient) CancelHeldOrder(ctx context.Context, id shared.LocalOrderId) error {
	callCtx, cancel := resilience.CallTimeout(ctx, resilience.DefaultTimeout)
	defer cancel()

	_, err := c.breaker.Execute(func() (any, error) {
		return nil, c.inner.CancelHeldOrder(callCtx, id)
	})
	if isBreakerRejection(err) {
		return fmt.Errorf("%w: %v", ErrCircuitOpen, err)
	}
	return err
}

// isBreakerRejection reports whether err is gobreaker refusing to even
// attempt the call (open, or half-open and already at its probe limit)
// — the ONLY case ErrCircuitOpen should wrap; a real error FROM a call
// gobreaker did let through must propagate unchanged, exactly as it did
// before this breaker existed.
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
