package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
)

type uowScopeKey struct{}

// recordingUnitOfWork is a fake analytics UnitOfWork: it tags the ctx it
// hands fn, so a test can prove the claim and the apply both ran inside
// the SAME scope, and it records the error fn returned (the one a real
// UnitOfWork would roll back on).
type recordingUnitOfWork struct {
	executions int
	lastErr    error
}

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.executions++
	u.lastErr = fn(context.WithValue(ctx, uowScopeKey{}, u.executions))
	return u.lastErr
}

// scopeProcessed records which UnitOfWork scope MarkProcessed ran in.
type scopeProcessed struct {
	*fakeProcessed
	scopes []any
}

func (p *scopeProcessed) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	p.scopes = append(p.scopes, ctx.Value(uowScopeKey{}))
	return p.fakeProcessed.MarkProcessed(ctx, eventId)
}

// scopeProjection records which UnitOfWork scope each apply ran in and
// can be told to fail.
type scopeProjection struct {
	fakeProjection
	scopes []any
	err    error
}

func (p *scopeProjection) ApplyNetworkOrderReceived(ctx context.Context, eventId string, at time.Time) error {
	p.scopes = append(p.scopes, ctx.Value(uowScopeKey{}))
	if p.err != nil {
		return p.err
	}
	return p.fakeProjection.ApplyNetworkOrderReceived(ctx, eventId, at)
}

// TestAnalyticsConsumer_ClaimAndApplyShareOneUnitOfWork pins ADR 0023: the
// dedupe claim and the projection apply run inside ONE UnitOfWork scope.
func TestAnalyticsConsumer_ClaimAndApplyShareOneUnitOfWork(t *testing.T) {
	uow := &recordingUnitOfWork{}
	processed := &scopeProcessed{fakeProcessed: newFakeProcessed()}
	proj := &scopeProjection{}
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Atomic: uow}

	raw := envelope(t, "evt-uow-1", ceType("NetworkOrderReceived"), time.Now().UTC(), map[string]any{})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if uow.executions != 1 {
		t.Fatalf("UnitOfWork executions = %d, want 1", uow.executions)
	}
	if len(processed.scopes) != 1 || processed.scopes[0] != 1 {
		t.Errorf("MarkProcessed scopes = %v, want [1] (inside the unit of work)", processed.scopes)
	}
	if len(proj.scopes) != 1 || proj.scopes[0] != 1 {
		t.Errorf("apply scopes = %v, want [1] (the SAME unit of work as the claim)", proj.scopes)
	}
}

// TestAnalyticsConsumer_ApplyFailure_FailsTheUnitOfWork proves an apply
// error is returned to the UnitOfWork (so the real one rolls the claim
// back) and surfaces from HandleMessage.
func TestAnalyticsConsumer_ApplyFailure_FailsTheUnitOfWork(t *testing.T) {
	boom := errors.New("projection down")
	uow := &recordingUnitOfWork{}
	proj := &scopeProjection{err: boom}
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: newFakeProcessed(), Atomic: uow}

	raw := envelope(t, "evt-uow-2", ceType("NetworkOrderReceived"), time.Now().UTC(), map[string]any{})
	if err := c.HandleMessage(context.Background(), raw); !errors.Is(err, boom) {
		t.Fatalf("HandleMessage = %v, want the apply error", err)
	}
	if !errors.Is(uow.lastErr, boom) {
		t.Errorf("UnitOfWork saw %v, want the apply error (so it rolls back)", uow.lastErr)
	}
}

// TestAnalyticsConsumer_MarkFailure_SkipsApplyAndFailsTheUnitOfWork proves a
// claim error never reaches the projection and fails the unit of work.
func TestAnalyticsConsumer_MarkFailure_SkipsApplyAndFailsTheUnitOfWork(t *testing.T) {
	uow := &recordingUnitOfWork{}
	proj := &scopeProjection{}
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: failingProcessed{}, Atomic: uow}

	raw := envelope(t, "evt-uow-3", ceType("NetworkOrderReceived"), time.Now().UTC(), map[string]any{})
	if err := c.HandleMessage(context.Background(), raw); err == nil {
		t.Fatal("HandleMessage = nil, want the mark-processed error")
	}
	if uow.lastErr == nil {
		t.Error("UnitOfWork saw no error, want the mark-processed error")
	}
	if len(proj.scopes) != 0 {
		t.Errorf("apply called %d time(s) after a failed claim, want 0", len(proj.scopes))
	}
}

type failingProcessed struct{}

func (failingProcessed) MarkProcessed(context.Context, string) (bool, error) {
	return false, errors.New("dedupe store down")
}
