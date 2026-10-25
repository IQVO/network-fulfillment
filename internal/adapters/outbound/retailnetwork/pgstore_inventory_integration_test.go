//go:build integration

package retailnetwork_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
)

// The inventory feed state and the refused listings (migration 0011, ADR 0027
// part 2) against a real Postgres: what lets a restart skip an unchanged feed and
// keep a refused listing out.

func feedState(part int, subject, fingerprint string, asOf time.Time) retailnetwork.InventoryFeedState {
	return retailnetwork.InventoryFeedState{SiteCode: "site-1", Part: part, Subject: subject, Fingerprint: fingerprint, AsOf: asOf}
}

func TestPGStore_InventoryFeedStateNeverGoesBackAndSurvivesAReopen(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	if err := store.SaveInventoryFeedState(ctx, feedState(1, "site-1#t0", "fp0", t0)); !errors.Is(err, retailnetwork.ErrMappingConflict) {
		t.Fatalf("a site with no node: err = %v, want ErrMappingConflict", err)
	}
	if err := store.SaveNode(ctx, "site-1", "N-1"); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if _, ok, err := store.InventoryFeedState(ctx, "site-1", 1); ok || err != nil {
		t.Fatalf("before any feed: ok=%v err=%v, want none", ok, err)
	}

	steps := []struct {
		name string
		in   retailnetwork.InventoryFeedState
		want [3]any // subject, fingerprint, asOf in force for part 1
	}{
		{"the first attempt is stored", feedState(1, "site-1#t0", "fp0", t0), [3]any{"site-1#t0", "fp0", t0}},
		{"a later pass wins", feedState(1, "site-1#t1", "fp1", t0.Add(time.Minute)), [3]any{"site-1#t1", "fp1", t0.Add(time.Minute)}},
		{"an earlier pass never moves it back", feedState(1, "site-1#t0", "stale", t0), [3]any{"site-1#t1", "fp1", t0.Add(time.Minute)}},
		{"the same pass again is the same attempt, last write wins", feedState(1, "site-1#t1", "fp1b", t0.Add(time.Minute)), [3]any{"site-1#t1", "fp1b", t0.Add(time.Minute)}},
	}
	for _, s := range steps {
		if err := store.SaveInventoryFeedState(ctx, s.in); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		got, ok, err := store.InventoryFeedState(ctx, "site-1", 1)
		if err != nil || !ok {
			t.Fatalf("%s: ok=%v err=%v", s.name, ok, err)
		}
		if got.Subject != s.want[0] || got.Fingerprint != s.want[1] || !got.AsOf.Equal(s.want[2].(time.Time)) {
			t.Fatalf("%s: in force = %+v, want %v", s.name, got, s.want)
		}
	}

	// Parts of one feed are independent.
	if err := store.SaveInventoryFeedState(ctx, feedState(2, "site-1#t1#2", "fp-part-2", t0)); err != nil {
		t.Fatalf("part 2: %v", err)
	}
	if got, ok, err := store.InventoryFeedState(ctx, "site-1", 2); err != nil || !ok || got.Fingerprint != "fp-part-2" {
		t.Fatalf("part 2 = %+v ok=%v err=%v", got, ok, err)
	}
	if got, _, _ := store.InventoryFeedState(ctx, "site-1", 1); got.Fingerprint != "fp1b" {
		t.Fatalf("part 1 changed by a write to part 2: %+v", got)
	}

	// A restart is a new store over the same database: it continues from what is persisted.
	got, ok, err := retailnetwork.NewPGStore(pool).InventoryFeedState(ctx, "site-1", 1)
	if err != nil || !ok || got.Subject != "site-1#t1" || got.Fingerprint != "fp1b" || !got.AsOf.Equal(t0.Add(time.Minute)) {
		t.Fatalf("after a reopen: %+v ok=%v err=%v", got, ok, err)
	}

	for name, bad := range map[string]retailnetwork.InventoryFeedState{
		"part 0":         feedState(0, "s", "f", t0),
		"no subject":     feedState(1, "", "f", t0),
		"no fingerprint": feedState(1, "s", "", t0),
		"no moment":      feedState(1, "s", "f", time.Time{}),
	} {
		if err := store.SaveInventoryFeedState(ctx, bad); err == nil {
			t.Fatalf("%s must be refused", name)
		}
	}
}

func TestPGStore_RefusedListingsAreRememberedPerSiteAndRefreshed(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	if err := store.RejectListing(ctx, "site-1", retailnetwork.RejectedListing{ListingID: "LST-B", RejectedAt: t0}); !errors.Is(err, retailnetwork.ErrMappingConflict) {
		t.Fatalf("a site with no node: err = %v, want ErrMappingConflict", err)
	}
	for site, node := range map[string]string{"site-1": "N-1", "site-2": "N-2"} {
		if err := store.SaveNode(ctx, site, node); err != nil {
			t.Fatalf("SaveNode %s: %v", site, err)
		}
	}
	if got, err := store.RejectedListings(ctx, "site-1"); err != nil || len(got) != 0 {
		t.Fatalf("before any refusal: %v err=%v, want none", got, err)
	}

	for _, l := range []retailnetwork.RejectedListing{
		{ListingID: "LST-C", Reason: "listing LST-C is not on node N-1", RejectedAt: t0},
		{ListingID: "LST-B", Reason: "first", RejectedAt: t0},
	} {
		if err := store.RejectListing(ctx, "site-1", l); err != nil {
			t.Fatalf("RejectListing %s: %v", l.ListingID, err)
		}
	}
	if err := store.RejectListing(ctx, "site-2", retailnetwork.RejectedListing{ListingID: "LST-B", RejectedAt: t0}); err != nil {
		t.Fatalf("RejectListing at site-2: %v", err)
	}
	// The same listing refused again refreshes the record (the 24 hours count from the last refusal).
	if err := store.RejectListing(ctx, "site-1", retailnetwork.RejectedListing{ListingID: "LST-B", Reason: "again", RejectedAt: t0.Add(time.Hour)}); err != nil {
		t.Fatalf("RejectListing again: %v", err)
	}

	got, err := store.RejectedListings(ctx, "site-1")
	if err != nil || len(got) != 2 {
		t.Fatalf("site-1 refusals = %+v err=%v, want two", got, err)
	}
	if got[0].ListingID != "LST-B" || got[0].Reason != "again" || !got[0].RejectedAt.Equal(t0.Add(time.Hour)) || got[1].ListingID != "LST-C" {
		t.Fatalf("site-1 refusals = %+v, want LST-B (refreshed) then LST-C, ordered by listing", got)
	}
	if other, _ := store.RejectedListings(ctx, "site-2"); len(other) != 1 || !other[0].RejectedAt.Equal(t0) {
		t.Fatalf("site-2 refusals = %+v, want its own single, untouched record", other)
	}
	if err := store.RejectListing(ctx, "site-1", retailnetwork.RejectedListing{RejectedAt: t0}); err == nil {
		t.Fatal("a refusal without a listing id must be refused")
	}

	again, err := retailnetwork.NewPGStore(pool).RejectedListings(ctx, "site-1")
	if err != nil || len(again) != 2 {
		t.Fatalf("after a reopen: %+v err=%v, want the same two", again, err)
	}
}

func TestPGStore_TheLedgerAcceptsInventoryRows(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	kind, subject := retailnetwork.KindInventory, "site-1#2026-10-25T12:00:00Z"

	row, err := store.BeginSubmission(ctx, retailnetwork.Submission{Kind: kind, Subject: subject, IdempotencyKey: "nf-inventory-site-1-2026-10-25T12:00:00Z", Body: []byte(`{"fullUpdate":true}`)})
	if err != nil || row.Kind != kind || row.Status != retailnetwork.StatusSending {
		t.Fatalf("BeginSubmission = %+v err=%v, want a SENDING INVENTORY row", row, err)
	}
	if err := store.RecordTransaction(ctx, kind, subject, "tx-1"); err != nil {
		t.Fatalf("RecordTransaction: %v", err)
	}
	if err := store.RecordOutcome(ctx, kind, subject, retailnetwork.StatusFailure, "ErrListingNotOnNode: listing LST-B is not on node N-1"); err != nil {
		t.Fatalf("RecordOutcome: %v", err)
	}
	got, ok, err := store.Submission(ctx, kind, subject)
	if err != nil || !ok || got.Status != retailnetwork.StatusFailure || got.TransactionID != "tx-1" || string(got.Body) != `{"fullUpdate":true}` {
		t.Fatalf("row = %+v ok=%v err=%v, want FAILURE with tx-1 and the body kept", got, ok, err)
	}
}
