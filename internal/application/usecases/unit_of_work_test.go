package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// scopeKey marks a context as "inside the unit of work" so the fakes can
// assert every Save/Publish happened within the scope, never outside it.
type scopeKey struct{}

// recordingUnitOfWork is a ports.UnitOfWork fake that (a) tags the ctx it
// hands to fn, (b) counts how many scopes were opened, and (c) reports
// whether the last scope committed (fn returned nil) or rolled back.
type recordingUnitOfWork struct {
	opened     int
	committed  int
	rolledBack int
	beginErr   error
}

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if u.beginErr != nil {
		return u.beginErr
	}
	u.opened++
	err := fn(context.WithValue(ctx, scopeKey{}, true))
	if err != nil {
		u.rolledBack++
		return err
	}
	u.committed++
	return nil
}

func inScope(ctx context.Context) bool {
	v, _ := ctx.Value(scopeKey{}).(bool)
	return v
}

// scopedPublisher records whether each Publish happened inside a scope,
// and lets a test fail the Nth call (rather than every call), so the
// FIRST publish (NetworkOrderReceived) can succeed while the SECOND
// (Acknowledged/Rejected) fails — the case that actually exercises
// rollback in ReceiveNetworkDemand's two-phase flow.
type scopedPublisher struct {
	inScope []bool
	events  []any
	failOn  int // 1-indexed call to fail on, 0 = never
	err     error
}

func (p *scopedPublisher) Publish(ctx context.Context, event any) error {
	p.inScope = append(p.inScope, inScope(ctx))
	call := len(p.inScope)
	if p.failOn != 0 && call == p.failOn {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

func TestReceive_ReceivedPublishRunsInsideOneUnitOfWork(t *testing.T) {
	f := newFixture(true)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := f.receive()
	uc.Events = pub
	uc.UnitOfWork = uow

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Two atomic scopes: one for saveAndPublishReceived, one for
	// acknowledge's own Save+Publish.
	if uow.opened != 2 || uow.committed != 2 || uow.rolledBack != 0 {
		t.Fatalf("expected two committed scopes, got opened=%d committed=%d rolledBack=%d", uow.opened, uow.committed, uow.rolledBack)
	}
	if len(pub.inScope) != 2 || !pub.inScope[0] || !pub.inScope[1] {
		t.Fatalf("expected both publishes to run inside a unit of work scope, got %v", pub.inScope)
	}
}

func TestReceive_SecondPublishFailure_RollsBackOnlyThatScope(t *testing.T) {
	f := newFixture(true)
	pub := &scopedPublisher{failOn: 2, err: errors.New("outbox insert failed")}
	uow := &recordingUnitOfWork{}
	uc := f.receive()
	uc.Events = pub
	uc.UnitOfWork = uow

	_, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1"))
	if err == nil || !errors.Is(err, pub.err) {
		t.Fatalf("expected the publish error to propagate, got %v", err)
	}
	// The first scope (Received) committed; the second (Acknowledged)
	// rolled back.
	if uow.committed != 1 || uow.rolledBack != 1 {
		t.Fatalf("expected one committed and one rolled-back scope, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
	// Release must not have run: it happens after the second scope
	// commits, and that scope rolled back.
	if got := f.planner.calls; !equal(got, []string{"raise"}) {
		t.Fatalf("planner calls = %v, want [raise]", got)
	}
}

func TestReceive_UnitOfWorkBeginFailure_Propagates(t *testing.T) {
	f := newFixture(true)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{beginErr: errors.New("begin failed")}
	uc := f.receive()
	uc.Events = pub
	uc.UnitOfWork = uow

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err == nil || err.Error() != "begin failed" {
		t.Fatalf("expected begin error, got %v", err)
	}
	if len(pub.events) != 0 {
		t.Fatal("expected nothing published when the unit of work cannot begin")
	}
}

func TestReceive_NilUnitOfWork_StillSavesAndPublishes(t *testing.T) {
	f := newFixture(true)
	pub := &scopedPublisher{}
	uc := f.receive()
	uc.Events = pub
	// uc.UnitOfWork left nil deliberately.

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 2 || pub.inScope[0] || pub.inScope[1] {
		t.Fatalf("expected two publishes outside any scope, got events=%d inScope=%v", len(pub.events), pub.inScope)
	}
}

// stuckOrder mirrors TestRejectOverdue_FreesTheHoldOfAnOrderThatWasNeverAnswered's
// setup: an order received a full window ago, held but never answered —
// the real shape of a crash between raising the hold and answering.
func stuckOrder(t *testing.T, local shared.LocalOrderId) *networkorder.NetworkOrder {
	t.Helper()
	return networkorder.Rehydrate("po-old", "site-1", now().Add(48*time.Hour),
		now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
}

func TestRejectOverdue_RejectOneRunsInsideOneUnitOfWork(t *testing.T) {
	f := newFixture(true)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}

	if err := f.orders.Save(context.Background(), stuckOrder(t, "ord-held")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders: f.orders, Planner: f.planner, Events: pub,
		Clock: fixedClock{t: now()}, UnitOfWork: uow,
	}
	res, err := rejectOverdue.Execute(context.Background())
	if err != nil {
		t.Fatalf("rejectOverdue: %v", err)
	}
	if res.Rejected != 1 {
		t.Fatalf("Rejected = %d, want 1", res.Rejected)
	}
	if uow.opened != 1 || uow.committed != 1 || uow.rolledBack != 0 {
		t.Fatalf("expected one committed scope, got opened=%d committed=%d rolledBack=%d", uow.opened, uow.committed, uow.rolledBack)
	}
	if len(pub.inScope) != 1 || !pub.inScope[0] {
		t.Fatalf("expected rejectOne's publish to run inside the unit of work, got %v", pub.inScope)
	}
}

func TestRejectOverdue_PublishFailure_RollsBackTheUnitOfWork(t *testing.T) {
	f := newFixture(true)
	pub := &scopedPublisher{failOn: 1, err: errors.New("outbox insert failed")}
	uow := &recordingUnitOfWork{}

	if err := f.orders.Save(context.Background(), stuckOrder(t, "ord-held")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rejectOverdue := &usecases.RejectOverdueOrders{
		Orders: f.orders, Planner: f.planner, Events: pub,
		Clock: fixedClock{t: now()}, UnitOfWork: uow,
	}
	// rejectOne's error is swallowed by the pass loop by design (one bad
	// order must not abort the others) but the scope must still have
	// rolled back.
	res, err := rejectOverdue.Execute(context.Background())
	if err != nil {
		t.Fatalf("rejectOverdue must not abort the whole pass: %v", err)
	}
	if res.Rejected != 0 {
		t.Fatalf("Rejected = %d, want 0 (the publish failed, so nothing was successfully rejected)", res.Rejected)
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected the scope to roll back, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}
