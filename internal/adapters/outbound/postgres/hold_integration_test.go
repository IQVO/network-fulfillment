//go:build integration

package postgres_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// rehydratedWithHold builds an order the way a repository would hand it to a
// use case: any state, an optional recorded hold, an optional outcome.
func rehydratedWithHold(t *testing.T, ref shared.NetworkRef, state networkorder.State, id string, outcome networkorder.HoldOutcome, settledAt time.Time) *networkorder.NetworkOrder {
	t.Helper()
	received := time.Now().UTC().Truncate(time.Microsecond).Add(-30 * time.Hour)
	var local *shared.LocalOrderId
	if id != "" {
		l := shared.LocalOrderId(id)
		local = &l
	}
	o := networkorder.Rehydrate(ref, "site-1", received.Add(48*time.Hour), received.Add(networkorder.AcknowledgementWindow), received,
		[]networkorder.Line{mustLine(t, "1", "ASIN-AAA", "sku-aaa", 1)}, state, local)
	if outcome != "" {
		o.RestoreHold(outcome, settledAt)
	}
	return o
}

// TestNetworkOrderRepo_SavesEveryStateTheAggregateCanReach is the sensor for
// a bug class (ADR 0024 finding 4): the aggregate and the schema disagreed on
// which (state, local order id) pairs exist. The old CHECK
// local_order_requires_answer refused NEW and REJECTED with an id, yet
// reconcile.fail produces REJECTED+id and ADR 0024 records the hold on NEW.
// Every pair the use cases can produce must persist and read back.
func TestNetworkOrderRepo_SavesEveryStateTheAggregateCanReach(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()

	states := []networkorder.State{
		networkorder.StateNew, networkorder.StateSubmitted, networkorder.StateAcknowledged,
		networkorder.StateRejected, networkorder.StateConfirmed,
	}
	for _, state := range states {
		for _, id := range []string{"", "ord-" + string(state)} {
			name := string(state) + "/with-hold"
			if id == "" {
				name = string(state) + "/no-hold"
			}
			t.Run(name, func(t *testing.T) {
				ref := uniqueRef("reach-" + string(state))
				o := rehydratedWithHold(t, ref, state, id, "", time.Time{})
				if err := repo.Save(ctx, o); err != nil {
					t.Fatalf("Save(%s, hold=%q): %v", state, id, err)
				}
				got, err := repo.FindByRef(ctx, ref)
				if err != nil || got == nil {
					t.Fatalf("FindByRef = %v, %v", got, err)
				}
				if got.State() != state {
					t.Fatalf("state = %s, want %s", got.State(), state)
				}
				switch {
				case id == "" && got.LocalOrderId() != nil:
					t.Fatalf("localOrderId = %v, want none", *got.LocalOrderId())
				case id != "" && (got.LocalOrderId() == nil || string(*got.LocalOrderId()) != id):
					t.Fatalf("localOrderId = %v, want %q", got.LocalOrderId(), id)
				}
			})
		}
	}
}

func TestNetworkOrderRepo_HoldOutcomeRoundTrips(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()

	settled := time.Now().UTC().Truncate(time.Microsecond)
	for _, outcome := range []networkorder.HoldOutcome{networkorder.HoldCancelled, networkorder.HoldAlreadyReleased} {
		ref := uniqueRef("outcome-" + string(outcome))
		if err := repo.Save(ctx, rehydratedWithHold(t, ref, networkorder.StateRejected, "ord-1", outcome, settled)); err != nil {
			t.Fatalf("Save: %v", err)
		}
		got, err := repo.FindByRef(ctx, ref)
		if err != nil || got == nil {
			t.Fatalf("FindByRef = %v, %v", got, err)
		}
		if got.HoldOutcome() != outcome || !got.HoldSettledAt().Equal(settled) {
			t.Fatalf("outcome/at = %q/%v, want %q/%v", got.HoldOutcome(), got.HoldSettledAt(), outcome, settled)
		}
		if got.HoldOpen() {
			t.Fatal("a settled hold must not read as open after a round trip")
		}
	}
}

// TestNetworkOrderRepo_SettlingAnOpenHoldIsPersisted: the upsert's column list
// must include the outcome, or the sweep would re-cancel the same hold forever.
func TestNetworkOrderRepo_SettlingAnOpenHoldIsPersisted(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()

	ref := uniqueRef("settle")
	o := rehydratedWithHold(t, ref, networkorder.StateNew, "ord-1", "", time.Time{})
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save open: %v", err)
	}
	settled := time.Now().UTC().Truncate(time.Microsecond)
	if err := o.SettleHold(networkorder.HoldCancelled, settled); err != nil {
		t.Fatalf("SettleHold: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save settled: %v", err)
	}
	got, _ := repo.FindByRef(ctx, ref)
	if got == nil || got.HoldOutcome() != networkorder.HoldCancelled {
		t.Fatalf("stored outcome = %v, want CANCELLED persisted by the upsert", got)
	}
}

func TestNetworkOrderRepo_ListOpenHoldsReturnsOnlyOpenHoldsOfDecidedOrOverdueCandidates(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()

	settled := time.Now().UTC().Truncate(time.Microsecond)
	want := map[shared.NetworkRef]bool{}
	seed := func(name string, state networkorder.State, id string, outcome networkorder.HoldOutcome, listed bool) {
		ref := uniqueRef("open-" + name)
		if err := repo.Save(ctx, rehydratedWithHold(t, ref, state, id, outcome, settled)); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		if listed {
			want[ref] = true
		}
	}
	seed("new-open", networkorder.StateNew, "ord-1", "", true)
	seed("rejected-open", networkorder.StateRejected, "ord-2", "", true)
	seed("new-cancelled", networkorder.StateNew, "ord-3", networkorder.HoldCancelled, false)
	seed("rejected-released", networkorder.StateRejected, "ord-4", networkorder.HoldAlreadyReleased, false)
	seed("new-no-hold", networkorder.StateNew, "", "", false)
	seed("submitted", networkorder.StateSubmitted, "ord-5", "", false)
	seed("acknowledged", networkorder.StateAcknowledged, "ord-6", "", false)
	seed("confirmed", networkorder.StateConfirmed, "ord-7", "", false)

	got, err := repo.ListOpenHolds(ctx)
	if err != nil {
		t.Fatalf("ListOpenHolds: %v", err)
	}
	var gotRefs, wantRefs []string
	for _, o := range got {
		gotRefs = append(gotRefs, string(o.NetworkRef()))
	}
	for ref := range want {
		wantRefs = append(wantRefs, string(ref))
	}
	sort.Strings(gotRefs)
	sort.Strings(wantRefs)
	if len(gotRefs) != len(wantRefs) {
		t.Fatalf("ListOpenHolds = %v, want %v", gotRefs, wantRefs)
	}
	for i := range gotRefs {
		if gotRefs[i] != wantRefs[i] {
			t.Fatalf("ListOpenHolds = %v, want %v", gotRefs, wantRefs)
		}
	}
	for _, o := range got {
		if o.LocalOrderId() == nil || !o.HoldOpen() {
			t.Fatalf("listed order %s must carry an open hold", o.NetworkRef())
		}
	}
}
