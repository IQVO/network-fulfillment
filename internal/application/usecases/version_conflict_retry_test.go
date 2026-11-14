package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// rowSimulatingRepo models what the Postgres adapter actually does for the
// race migration 0013 closes: every FindByRef returns a fresh instance of
// the stored row (the memory repo hands back the same pointer, which would
// hide exactly the lost-update this rec proves), and a configurable number
// of saves for one ref are refused with ErrVersionConflict after another
// writer has moved the row's version — the conditional UPDATE finding
// zero matching rows.
type rowSimulatingRepo struct {
	ports.NetworkOrderRepo
	ref       shared.NetworkRef
	conflicts int
	saves     int
}

func copyOrder(o *networkorder.NetworkOrder) *networkorder.NetworkOrder {
	dup := networkorder.Rehydrate(o.NetworkRef(), o.SiteId(),
		o.RequiredShipBy(), o.AcknowledgeBy(), o.ReceivedAt(),
		o.Lines(), o.State(), o.LocalOrderId())
	if outcome := o.HoldOutcome(); outcome != "" {
		dup.RestoreHold(outcome, o.HoldSettledAt())
	}
	if pending, ok := o.PendingShipment(); ok {
		dup.RestorePendingShipment(pending)
	}
	dup.RestoreVersion(o.Version())
	return dup
}

func (r *rowSimulatingRepo) FindByRef(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	o, err := r.NetworkOrderRepo.FindByRef(ctx, ref)
	if err != nil || o == nil || ref != r.ref {
		return o, err
	}
	return copyOrder(o), nil // a fresh instance, like a row read
}

func (r *rowSimulatingRepo) Save(ctx context.Context, o *networkorder.NetworkOrder) error {
	if o.NetworkRef() == r.ref {
		r.saves++
		if r.conflicts > 0 {
			r.conflicts--
			stored, err := r.NetworkOrderRepo.FindByRef(ctx, r.ref)
			if err != nil {
				return err
			}
			stored.AdvanceVersion() // the other writer's save lands first
			return networkorder.ErrVersionConflict
		}
	}
	return r.NetworkOrderRepo.Save(ctx, o)
}

func overdueStuckOrder(t *testing.T) *networkorder.NetworkOrder {
	t.Helper()
	local := shared.LocalOrderId("ord-held")
	return networkorder.Rehydrate("po-race", "site-1",
		now().Add(48*time.Hour), now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, &local)
}

func rejectOverdue(repo ports.NetworkOrderRepo, pub *recordingPublisher) *usecases.RejectOverdueOrders {
	return &usecases.RejectOverdueOrders{
		Orders:  repo,
		Planner: newFixture(true).planner,
		Events:  pub,
		Clock:   fixedClock{t: now()},
	}
}

// TestRejectOverdue_RetriesOnVersionConflict proves the sweep path reloads
// and re-applies its rejection when the poller (or any other writer) won the
// row first: the order still ends REJECTED, one event, after two saves.
func TestRejectOverdue_RetriesOnVersionConflict(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}
	ctx := context.Background()

	if err := f.orders.Save(ctx, overdueStuckOrder(t)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	repo := &rowSimulatingRepo{NetworkOrderRepo: f.orders, ref: "po-race", conflicts: 1}
	res, err := rejectOverdue(repo, pub).Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Rejected != 1 {
		t.Fatalf("Rejected = %d, want 1 (retry must land the rejection)", res.Rejected)
	}
	if repo.saves != 2 {
		t.Fatalf("saves = %d, want 2 (one refused, one landed)", repo.saves)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want exactly one rejection: %+v", len(pub.events), pub.events)
	}
	final, err := f.orders.FindByRef(ctx, "po-race")
	if err != nil || final == nil {
		t.Fatalf("FindByRef: %v %v", final, err)
	}
	if final.State() != networkorder.StateRejected {
		t.Fatalf("final state = %s, want REJECTED", final.State())
	}
}

// TestRejectOverdue_GivesUpAfterAttemptBudget proves a persistently hot row
// is left to the next scheduled pass rather than retried forever: three
// refused saves, no rejection, no event, and the pass itself still succeeds.
func TestRejectOverdue_GivesUpAfterAttemptBudget(t *testing.T) {
	f := newFixture(true)
	pub := &recordingPublisher{}
	ctx := context.Background()

	if err := f.orders.Save(ctx, overdueStuckOrder(t)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	repo := &rowSimulatingRepo{NetworkOrderRepo: f.orders, ref: "po-race", conflicts: 99}
	res, err := rejectOverdue(repo, pub).Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v (the pass must absorb the conflict)", err)
	}
	if res.Rejected != 0 {
		t.Fatalf("Rejected = %d, want 0", res.Rejected)
	}
	if repo.saves != 3 {
		t.Fatalf("saves = %d, want 3 (the attempt budget)", repo.saves)
	}
	if len(pub.events) != 0 {
		t.Fatalf("events = %d, want none", len(pub.events))
	}
}
