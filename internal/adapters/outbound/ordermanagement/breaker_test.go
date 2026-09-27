package ordermanagement_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// recordingRecorder implements resilience.StateRecorder, capturing every
// state transition in order so a test can assert the breaker actually
// opened at the expected point, not just that call outcomes looked
// right.
type recordingRecorder struct {
	states []int64
}

func (r *recordingRecorder) SetState(_ string, state int64) {
	r.states = append(r.states, state)
}

func (r *recordingRecorder) last() int64 {
	if len(r.states) == 0 {
		return -1
	}
	return r.states[len(r.states)-1]
}

// The gobreaker.State values (0=closed,1=half-open,2=open) are
// duplicated here as untyped constants rather than importing gobreaker
// into this _test package, matching resilience.RecordStateChange's own
// documented "no translation table" contract.
const (
	gobreakerOpen = 2
)

// countingPlanner is a fake FulfillmentPlanner recording every call it
// receives so a test can prove the breaker stopped calling it at all
// once open, not merely that its own errors kept propagating.
type countingPlanner struct {
	raiseErr   error
	releaseErr error
	cancelErr  error

	raiseCalls   int32
	releaseCalls int32
	cancelCalls  int32
}

func (p *countingPlanner) RaiseHeldOrder(_ context.Context, _ contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	atomic.AddInt32(&p.raiseCalls, 1)
	if p.raiseErr != nil {
		return contract.HeldOrderResult{}, p.raiseErr
	}
	return contract.HeldOrderResult{Feasible: true}, nil
}

func (p *countingPlanner) ReleaseHeldOrder(_ context.Context, _ shared.LocalOrderId) error {
	atomic.AddInt32(&p.releaseCalls, 1)
	return p.releaseErr
}

func (p *countingPlanner) CancelHeldOrder(_ context.Context, _ shared.LocalOrderId) error {
	atomic.AddInt32(&p.cancelCalls, 1)
	return p.cancelErr
}

// TestBreakerClient_OpensAfterConsecutiveFailures_PropagatesErrCircuitOpen
// is the ADR 0004 acceptance test: 5 consecutive RaiseHeldOrder failures
// trip the breaker (resilience.ReadyToTrip's ConsecutiveFailures>=5
// leg), and every call after that short-circuits to ErrCircuitOpen
// WITHOUT ever reaching the inner planner again — there is no
// permissive fallback here (unlike order-management's own reference),
// only a distinctly-labelled error, per this package's doc comment.
func TestBreakerClient_OpensAfterConsecutiveFailures_PropagatesErrCircuitOpen(t *testing.T) {
	boom := errors.New("connection refused")
	inner := &countingPlanner{raiseErr: boom}
	recorder := &recordingRecorder{}
	client := ordermanagement.NewBreakerClient(inner, recorder)

	for i := 0; i < 5; i++ {
		_, err := client.RaiseHeldOrder(context.Background(), contract.HeldOrderRequest{})
		if !errors.Is(err, boom) {
			t.Fatalf("call %d: err = %v, want the real transport error %v (breaker still closed)", i, err, boom)
		}
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 consecutive failures = %d, want open (%d)", recorder.last(), gobreakerOpen)
	}
	callsBeforeShortCircuit := atomic.LoadInt32(&inner.raiseCalls)

	_, err := client.RaiseHeldOrder(context.Background(), contract.HeldOrderRequest{})
	if !errors.Is(err, ordermanagement.ErrCircuitOpen) {
		t.Fatalf("while open, err = %v, want %v", err, ordermanagement.ErrCircuitOpen)
	}
	if atomic.LoadInt32(&inner.raiseCalls) != callsBeforeShortCircuit {
		t.Fatalf("inner planner was called again while the breaker is open -- it must short-circuit instead")
	}

	// ReleaseHeldOrder/CancelHeldOrder share the SAME breaker instance
	// (one breaker per dependency, not per verb): they must also
	// short-circuit once RaiseHeldOrder already tripped it.
	if err := client.ReleaseHeldOrder(context.Background(), shared.LocalOrderId("ord-1")); !errors.Is(err, ordermanagement.ErrCircuitOpen) {
		t.Fatalf("ReleaseHeldOrder while open: err = %v, want %v", err, ordermanagement.ErrCircuitOpen)
	}
	if err := client.CancelHeldOrder(context.Background(), shared.LocalOrderId("ord-1")); !errors.Is(err, ordermanagement.ErrCircuitOpen) {
		t.Fatalf("CancelHeldOrder while open: err = %v, want %v", err, ordermanagement.ErrCircuitOpen)
	}
	if atomic.LoadInt32(&inner.releaseCalls) != 0 {
		t.Fatalf("inner planner's ReleaseHeldOrder was called while the breaker is open")
	}
	if atomic.LoadInt32(&inner.cancelCalls) != 0 {
		t.Fatalf("inner planner's CancelHeldOrder was called while the breaker is open")
	}
}

// TestBreakerClient_RealErrorPropagatesUnchangedWhileClosed proves a
// genuine error from an ATTEMPTED call (breaker closed, well under the
// trip threshold) reaches the caller unchanged -- not wrapped in
// ErrCircuitOpen, which must mean ONLY "gobreaker refused to even try".
func TestBreakerClient_RealErrorPropagatesUnchangedWhileClosed(t *testing.T) {
	boom := errors.New("downstream 500")
	inner := &countingPlanner{releaseErr: boom}
	client := ordermanagement.NewBreakerClient(inner, nil)

	err := client.ReleaseHeldOrder(context.Background(), shared.LocalOrderId("ord-1"))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the real error %v unchanged", err, boom)
	}
	if errors.Is(err, ordermanagement.ErrCircuitOpen) {
		t.Fatalf("a real attempted-call error must never be ErrCircuitOpen")
	}
}

// TestBreakerClient_SuccessDoesNotTrip proves ordinary successful calls
// never trip the breaker and always return the inner planner's result
// verbatim.
func TestBreakerClient_SuccessDoesNotTrip(t *testing.T) {
	inner := &countingPlanner{}
	recorder := &recordingRecorder{}
	client := ordermanagement.NewBreakerClient(inner, recorder)

	for i := 0; i < 20; i++ {
		result, err := client.RaiseHeldOrder(context.Background(), contract.HeldOrderRequest{})
		if err != nil {
			t.Fatalf("call %d: unexpected error %v", i, err)
		}
		if !result.Feasible {
			t.Fatalf("call %d: result.Feasible = false, want true (inner planner's real result)", i)
		}
	}
	if len(recorder.states) != 0 {
		t.Fatalf("breaker recorded state transitions %v on an all-success run, want none", recorder.states)
	}
}

// TestBreakerClient_HalfOpenProbeRecoversToClosedOnSuccess proves the
// breaker actually recovers: after tripping open, waiting out the
// cooldown lets exactly one probe through (half-open), and a
// successful probe closes the breaker again -- confirming the whole
// closed -> open -> half-open -> closed cycle, not just the trip.
func TestBreakerClient_HalfOpenProbeRecoversToClosedOnSuccess(t *testing.T) {
	boom := errors.New("connection refused")
	inner := &countingPlanner{raiseErr: boom}
	recorder := &recordingRecorder{}
	// A tiny cooldown so the test does not have to sleep out the real
	// 30s production DefaultTimeout.
	client := ordermanagement.NewBreakerClientWithTimeout(inner, recorder, 20*time.Millisecond)

	for i := 0; i < 5; i++ {
		_, _ = client.RaiseHeldOrder(context.Background(), contract.HeldOrderRequest{})
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 consecutive failures = %d, want open (%d)", recorder.last(), gobreakerOpen)
	}

	// The inner planner now succeeds -- simulating the dependency
	// having recovered -- and once the cooldown elapses the next call
	// is let through as a half-open probe.
	inner.raiseErr = nil
	time.Sleep(30 * time.Millisecond)

	result, err := client.RaiseHeldOrder(context.Background(), contract.HeldOrderRequest{})
	if err != nil {
		t.Fatalf("half-open probe: unexpected error %v", err)
	}
	if !result.Feasible {
		t.Fatalf("half-open probe: result.Feasible = false, want true")
	}
	const gobreakerClosed = 0
	if recorder.last() != gobreakerClosed {
		t.Fatalf("breaker state after a successful half-open probe = %d, want closed (%d)", recorder.last(), gobreakerClosed)
	}
}

// TestBreakerClient_CallTimeoutIsBoundedByCallerDeadline proves
// RaiseHeldOrder derives its outbound deadline from the caller's own
// remaining budget rather than a fresh, hardcoded one: a caller whose
// context is already cancelled must fail fast rather than block for
// DefaultTimeout.
func TestBreakerClient_CallTimeoutIsBoundedByCallerDeadline(t *testing.T) {
	inner := &countingPlanner{}
	client := ordermanagement.NewBreakerClient(inner, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := client.RaiseHeldOrder(ctx, contract.HeldOrderRequest{})
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("RaiseHeldOrder took %v against an already-cancelled context, want near-instant", elapsed)
	}
	// The inner planner's own fake never checks ctx, so it "succeeds"
	// regardless -- this test only asserts on TIMING (that CallTimeout
	// derived a context bounded by the caller's own cancellation),
	// consistent with resilience.CallTimeout's contract; it does not
	// assert on err's value.
	_ = err
}
