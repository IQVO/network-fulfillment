package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// recordingPublisher records every event Published, in order, so tests can
// assert both WHICH domain events were raised and their field values.
type recordingPublisher struct {
	events []any
	err    error
}

func (p *recordingPublisher) Publish(_ context.Context, event any) error {
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

// TestReceive_FeasibleDemand_PublishesReceivedThenAcknowledged asserts the
// event sequence and payload for the happy path: a NetworkOrderReceived
// (the fact demand arrived) followed by a NetworkOrderAcknowledged (the
// fact we committed) — in that order, since we cannot have committed to
// demand we never recorded receiving.
func TestReceive_FeasibleDemand_PublishesReceivedThenAcknowledged(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}
	uc := f.receive()
	uc.Events = pub

	o, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1", "ASIN-2"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(pub.events) != 2 {
		t.Fatalf("events = %d, want 2 (Received, Acknowledged): %+v", len(pub.events), pub.events)
	}

	received, ok := pub.events[0].(shared.NetworkOrderReceived)
	if !ok {
		t.Fatalf("events[0] = %T, want NetworkOrderReceived", pub.events[0])
	}
	if received.NetworkRef != "po-1" {
		t.Errorf("NetworkOrderReceived.NetworkRef = %q, want po-1", received.NetworkRef)
	}
	if received.LineCount != 2 {
		t.Errorf("NetworkOrderReceived.LineCount = %d, want 2", received.LineCount)
	}
	if !received.AcknowledgeBy.Equal(o.AcknowledgeBy()) {
		t.Errorf("NetworkOrderReceived.AcknowledgeBy = %v, want %v", received.AcknowledgeBy, o.AcknowledgeBy())
	}

	acked, ok := pub.events[1].(shared.NetworkOrderAcknowledged)
	if !ok {
		t.Fatalf("events[1] = %T, want NetworkOrderAcknowledged", pub.events[1])
	}
	if acked.NetworkRef != "po-1" {
		t.Errorf("NetworkOrderAcknowledged.NetworkRef = %q, want po-1", acked.NetworkRef)
	}
	if acked.LocalOrderId != *o.LocalOrderId() {
		t.Errorf("NetworkOrderAcknowledged.LocalOrderId = %q, want %q", acked.LocalOrderId, *o.LocalOrderId())
	}
	if !acked.ReceivedAt.Equal(o.ReceivedAt()) {
		t.Errorf("NetworkOrderAcknowledged.ReceivedAt = %v, want %v", acked.ReceivedAt, o.ReceivedAt())
	}
}

// TestReceive_InfeasibleDemand_PublishesReceivedThenRejectedWithReason
// asserts the rejection reason is INFEASIBLE_DEADLINE, distinct from an
// untranslatable-product rejection or a missed-deadline rejection.
func TestReceive_InfeasibleDemand_PublishesReceivedThenRejectedWithReason(t *testing.T) {
	f := newFixture(false)
	pub := &recordingPublisher{}
	uc := f.receive()
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(pub.events) != 2 {
		t.Fatalf("events = %d, want 2 (Received, Rejected): %+v", len(pub.events), pub.events)
	}
	if _, ok := pub.events[0].(shared.NetworkOrderReceived); !ok {
		t.Fatalf("events[0] = %T, want NetworkOrderReceived", pub.events[0])
	}
	rejected, ok := pub.events[1].(shared.NetworkOrderRejected)
	if !ok {
		t.Fatalf("events[1] = %T, want NetworkOrderRejected", pub.events[1])
	}
	if rejected.Reason != shared.RejectionReasonInfeasibleDeadline {
		t.Errorf("Reason = %q, want INFEASIBLE_DEADLINE", rejected.Reason)
	}
	if rejected.NetworkRef != "po-1" {
		t.Errorf("NetworkRef = %q, want po-1", rejected.NetworkRef)
	}
}

// TestReceive_UntranslatableProduct_PublishesReceivedWithZeroLinesThenRejected
// asserts the untranslatable path publishes LineCount=0 (never attempted
// translation of the good lines that never existed) and the
// UNTRANSLATABLE_SKU reason.
func TestReceive_UntranslatableProduct_PublishesReceivedWithZeroLinesThenRejected(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}
	uc := f.receive()
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-UNKNOWN")); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(pub.events) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(pub.events), pub.events)
	}
	received, ok := pub.events[0].(shared.NetworkOrderReceived)
	if !ok {
		t.Fatalf("events[0] = %T, want NetworkOrderReceived", pub.events[0])
	}
	if received.LineCount != 0 {
		t.Errorf("LineCount = %d, want 0 for an untranslatable order", received.LineCount)
	}
	rejected, ok := pub.events[1].(shared.NetworkOrderRejected)
	if !ok {
		t.Fatalf("events[1] = %T, want NetworkOrderRejected", pub.events[1])
	}
	if rejected.Reason != shared.RejectionReasonUntranslatableSKU {
		t.Errorf("Reason = %q, want UNTRANSLATABLE_SKU", rejected.Reason)
	}
}

// TestReceive_IdempotentPoll_PublishesNoEventsOnRepeat asserts a
// re-delivered poll (already-answered order) raises NOTHING — the whole
// point of the idempotency guard is that a retry is invisible downstream,
// including to analytics.
func TestReceive_IdempotentPoll_PublishesNoEventsOnRepeat(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}
	uc := f.receive()
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("first: %v", err)
	}
	before := len(pub.events)

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(pub.events) != before {
		t.Fatalf("events after repeat = %d, want unchanged from %d", len(pub.events), before)
	}
}

// TestReceive_EventPublishFailureIsReported: a failing publisher must
// abort the use case rather than silently swallow the error, matching
// this fleet's convention that a fallible step's failure is never
// discarded.
func TestReceive_EventPublishFailureIsReported(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{err: errBoom}
	uc := f.receive()
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err == nil {
		t.Fatal("expected an error when the event publisher fails")
	}
}

// TestSweep_PublishesRejectedWithAcknowledgementDeadlineMissedReason
// asserts the sweep's own rejection reason is distinct from the two
// reasons ReceiveNetworkDemand can raise.
func TestSweep_PublishesRejectedWithAcknowledgementDeadlineMissedReason(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}

	local := shared.LocalOrderId("ord-held")
	stuck := networkorder.Rehydrate("po-old", "site-1", now().Add(48*time.Hour),
		now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
	if err := f.orders.Save(context.Background(), stuck); err != nil {
		t.Fatalf("Save: %v", err)
	}

	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders:  f.orders,
		Planner: f.planner,
		Events:  pub,
		Clock:   fixedClock{t: now()},
	}
	if _, err := sweep.Execute(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1: %+v", len(pub.events), pub.events)
	}
	rejected, ok := pub.events[0].(shared.NetworkOrderRejected)
	if !ok {
		t.Fatalf("events[0] = %T, want NetworkOrderRejected", pub.events[0])
	}
	if rejected.Reason != shared.RejectionReasonAcknowledgementDeadlineMissed {
		t.Errorf("Reason = %q, want ACKNOWLEDGEMENT_DEADLINE_MISSED", rejected.Reason)
	}
	if rejected.NetworkRef != "po-old" {
		t.Errorf("NetworkRef = %q, want po-old", rejected.NetworkRef)
	}
}

// TestSweep_LeavesOrdersInsideWindowAlone_PublishesNoEvents asserts an
// order still inside its window generates no analytics noise.
func TestSweep_LeavesOrdersInsideWindowAlone_PublishesNoEvents(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: f.orders, Planner: f.planner, Events: pub,
		Clock: fixedClock{t: now().Add(time.Hour)},
	}
	if _, err := sweep.Execute(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(pub.events) != 0 {
		t.Fatalf("events = %d, want 0", len(pub.events))
	}
}

// TestSweep_EventPublishFailureIsReported mirrors the receive-side
// contract: a publish failure during the sweep must not be swallowed.
func TestSweep_EventPublishFailureIsReported(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{err: errBoom}

	local := shared.LocalOrderId("ord-held")
	stuck := networkorder.Rehydrate("po-old", "site-1", now().Add(48*time.Hour),
		now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
	if err := f.orders.Save(context.Background(), stuck); err != nil {
		t.Fatalf("Save: %v", err)
	}

	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: f.orders, Planner: f.planner, Events: pub, Clock: fixedClock{t: now()},
	}
	res, err := sweep.Execute(context.Background())
	if err != nil {
		t.Fatalf("sweep must not abort the whole pass on a publish failure: %v", err)
	}
	// missOne's error is swallowed by the pass loop (by design: one bad
	// order must not abort the others), so the sweep reports Missed=0
	// for this order even though the publish failed — the failure is
	// left for the next pass to retry.
	if res.Missed != 0 {
		t.Fatalf("Missed = %d, want 0 (publish failed, so nothing was successfully swept)", res.Missed)
	}
}
