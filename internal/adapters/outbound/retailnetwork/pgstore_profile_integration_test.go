//go:build integration

package retailnetwork_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
)

func profileState(site string, revision, attempt int, fingerprint string) retailnetwork.NodeProfileState {
	return retailnetwork.NodeProfileState{
		SiteCode: site, Revision: revision, Attempt: attempt, Fingerprint: fingerprint,
		Content: []byte(fmt.Sprintf(`{"fingerprint":%q}`, fingerprint)),
	}
}

func TestPGStore_NodeProfileRevisionOnlyEverGoesUpAndSurvivesAReopen(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()

	if _, err := store.AdvanceNodeProfile(ctx, profileState("site-1", 1, 1, "a")); !errors.Is(err, retailnetwork.ErrMappingConflict) {
		t.Fatalf("a site with no node: err = %v, want ErrMappingConflict", err)
	}
	if err := store.SaveNode(ctx, "site-1", "N-1"); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if _, ok, err := store.NodeProfileState(ctx, "site-1"); ok || err != nil {
		t.Fatalf("before any declaration: ok=%v err=%v, want none", ok, err)
	}

	steps := []struct {
		name string
		in   retailnetwork.NodeProfileState
		want [3]any // revision, attempt, fingerprint in force
	}{
		{"first revision is stored", profileState("site-1", 1, 1, "a"), [3]any{1, 1, "a"}},
		{"the same pair again changes nothing", profileState("site-1", 1, 1, "other"), [3]any{1, 1, "a"}},
		{"a higher revision wins", profileState("site-1", 3, 1, "c"), [3]any{3, 1, "c"}},
		{"a lower revision never lowers it, whatever its attempt", profileState("site-1", 2, 5, "b"), [3]any{3, 1, "c"}},
		{"a later attempt of the same revision wins", profileState("site-1", 3, 2, "c2"), [3]any{3, 2, "c2"}},
		{"an earlier attempt of it never lowers it", profileState("site-1", 3, 1, "x"), [3]any{3, 2, "c2"}},
	}
	for _, s := range steps {
		got, err := store.AdvanceNodeProfile(ctx, s.in)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if got.Revision != s.want[0] || got.Attempt != s.want[1] || got.Fingerprint != s.want[2] {
			t.Fatalf("%s: in force = %d/%d %q, want %v", s.name, got.Revision, got.Attempt, got.Fingerprint, s.want)
		}
	}

	// A restart is a new store over the same database: it continues from what is persisted.
	reopened := retailnetwork.NewPGStore(pool)
	got, ok, err := reopened.NodeProfileState(ctx, "site-1")
	if err != nil || !ok || got.Revision != 3 || got.Attempt != 2 || got.Fingerprint != "c2" {
		t.Fatalf("after a reopen: %+v ok=%v err=%v, want 3/2 c2", got, ok, err)
	}
	var content struct {
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.Unmarshal(got.Content, &content); err != nil || content.Fingerprint != "c2" {
		t.Fatalf("content = %s (%v), want the JSON that was written", got.Content, err)
	}

	for name, bad := range map[string]retailnetwork.NodeProfileState{
		"revision 0":        profileState("site-1", 0, 1, "a"),
		"attempt 0":         profileState("site-1", 4, 0, "a"),
		"empty fingerprint": profileState("site-1", 4, 1, ""),
		"no content":        {SiteCode: "site-1", Revision: 4, Attempt: 1, Fingerprint: "a"},
	} {
		if _, err := store.AdvanceNodeProfile(ctx, bad); err == nil {
			t.Errorf("%s: want an error, a state like that is never stored", name)
		}
	}
}

func TestPGStore_ConcurrentAdvancesEndAtTheHighestRevision(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if err := store.SaveNode(ctx, "site-1", "N-1"); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}

	const writers = 16
	errs := make([]error, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Revisions 1..16, written in no particular order.
			rev := (i*7)%writers + 1
			_, errs[i] = store.AdvanceNodeProfile(ctx, profileState("site-1", rev, 1, fmt.Sprintf("fp-%d", rev)))
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	got, ok, err := store.NodeProfileState(ctx, "site-1")
	if err != nil || !ok || got.Revision != writers || got.Fingerprint != fmt.Sprintf("fp-%d", writers) {
		t.Fatalf("after %d racing writers: %+v ok=%v err=%v, want revision %d", writers, got, ok, err, writers)
	}
}

func TestPGStore_TheLedgerAcceptsNodeProfileRows(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	kind, subject := retailnetwork.KindNodeProfile, "site-1#1#1"

	row, err := store.BeginSubmission(ctx, retailnetwork.Submission{Kind: kind, Subject: subject, IdempotencyKey: "nf-profile-site-1-1", Body: []byte(`{"profileRevision":1}`)})
	if err != nil || row.Kind != kind || row.Status != retailnetwork.StatusSending {
		t.Fatalf("BeginSubmission = %+v err=%v, want a SENDING NODE_PROFILE row", row, err)
	}
	if err := store.RecordTransaction(ctx, kind, subject, "tx-1"); err != nil {
		t.Fatalf("RecordTransaction: %v", err)
	}
	if err := store.RecordOutcome(ctx, kind, subject, retailnetwork.StatusFailure, "ErrStaleProfileRevision: revision 1 is not newer than 2"); err != nil {
		t.Fatalf("RecordOutcome: %v", err)
	}
	got, ok, err := store.Submission(ctx, kind, subject)
	if err != nil || !ok || got.Status != retailnetwork.StatusFailure || got.TransactionID != "tx-1" {
		t.Fatalf("row = %+v ok=%v err=%v, want FAILURE with tx-1", got, ok, err)
	}
}

// TestMigration0007_UpDownUp proves the migration both ways on a database that
// already holds a node, an acknowledgement and a profile: down drops only the
// profile state and the NODE_PROFILE ledger rows, the rest survives, and a second
// up works again.
func TestMigration0007_UpDownUp(t *testing.T) {
	url := bootDB(t)
	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Migrate(6); err != nil {
		t.Fatalf("Migrate(6): %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	profileTable := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = 'network_node_profiles'`).Scan(&n); err != nil {
			t.Fatalf("information_schema: %v", err)
		}
		return n
	}
	insertLedger := func(kind, subject string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO network_submissions (kind, subject, idempotency_key, request_body, status)
			VALUES ($1, $2, 'k-'||$2, '\x7b7d', 'SENDING')`, kind, subject)
		return err
	}

	if got := profileTable(); got != 0 {
		t.Fatalf("profile table before 0007 = %d, want 0", got)
	}
	if err := insertLedger("NODE_PROFILE", "site-1#1#1"); err == nil {
		t.Fatal("before 0007 the ledger must refuse a NODE_PROFILE row")
	}
	if err := m.Migrate(7); err != nil {
		t.Fatalf("Migrate(7): %v", err)
	}
	if got := profileTable(); got != 1 {
		t.Fatalf("profile table after 0007 = %d, want 1", got)
	}
	for _, row := range [][2]string{{"NODE_PROFILE", "site-1#1#1"}, {"ACKNOWLEDGEMENT", "NR-keep"}} {
		if err := insertLedger(row[0], row[1]); err != nil {
			t.Fatalf("after 0007 the ledger must accept %s: %v", row[0], err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO network_nodes (site_code, node_id) VALUES ('site-1', 'N-1')`); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO network_node_profiles (site_code, revision, fingerprint, content) VALUES ('site-1', 1, 'a', '{}')`); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO network_node_profiles (site_code, revision, fingerprint, content) VALUES ('site-9', 1, 'a', '{}')`); err == nil {
		t.Fatal("a profile for a site with no node must violate the foreign key")
	}

	if err := m.Migrate(6); err != nil {
		t.Fatalf("down to 6: %v", err)
	}
	if got := profileTable(); got != 0 {
		t.Fatalf("profile table after down = %d, want 0", got)
	}
	var profileRows, kept, nodes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'NODE_PROFILE'`).Scan(&profileRows); err != nil || profileRows != 0 {
		t.Fatalf("NODE_PROFILE ledger rows after down = %d err=%v, want 0", profileRows, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'ACKNOWLEDGEMENT'`).Scan(&kept); err != nil || kept != 1 {
		t.Fatalf("acknowledgement rows after down = %d err=%v, want it untouched", kept, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_nodes`).Scan(&nodes); err != nil || nodes != 1 {
		t.Fatalf("node rows after down = %d err=%v, want it untouched", nodes, err)
	}
	if err := insertLedger("NODE_PROFILE", "site-1#2#1"); err == nil {
		t.Fatal("after down the ledger must refuse a NODE_PROFILE row again")
	}
	if err := m.Migrate(7); err != nil {
		t.Fatalf("second up: %v", err)
	}
}
