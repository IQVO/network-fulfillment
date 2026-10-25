//go:build integration

package retailnetwork_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

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

// ledgerProbe is a migrated-or-not database as the migration tests see it.
type ledgerProbe struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	m    *migrate.Migrate
}

func newLedgerProbe(t *testing.T) *ledgerProbe {
	t.Helper()
	url := bootDB(t)
	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &ledgerProbe{t: t, ctx: ctx, pool: pool, m: m}
}

func (p *ledgerProbe) version() uint {
	p.t.Helper()
	v, dirty, err := p.m.Version()
	if err != nil || dirty {
		p.t.Fatalf("Version = %d dirty=%v err=%v, want a clean version", v, dirty, err)
	}
	return v
}

func (p *ledgerProbe) migrateTo(v uint) {
	p.t.Helper()
	if err := p.m.Migrate(v); err != nil {
		p.t.Fatalf("Migrate(%d): %v", v, err)
	}
	if got := p.version(); got != v {
		p.t.Fatalf("version after Migrate(%d) = %d", v, got)
	}
}

func (p *ledgerProbe) step(n int) {
	p.t.Helper()
	if err := p.m.Steps(n); err != nil {
		p.t.Fatalf("Steps(%d): %v", n, err)
	}
}

func (p *ledgerProbe) count(query string, args ...any) int {
	p.t.Helper()
	var n int
	if err := p.pool.QueryRow(p.ctx, query, args...).Scan(&n); err != nil {
		p.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (p *ledgerProbe) profileTables() int {
	p.t.Helper()
	return p.count(`SELECT count(*) FROM information_schema.tables WHERE table_name = 'network_node_profiles'`)
}

func (p *ledgerProbe) inventoryTables() int {
	p.t.Helper()
	return p.count(`SELECT count(*) FROM information_schema.tables WHERE table_name IN ('network_inventory_feeds', 'network_rejected_listings')`)
}

func (p *ledgerProbe) kindRows(kind string) int {
	p.t.Helper()
	return p.count(`SELECT count(*) FROM network_submissions WHERE kind = $1`, kind)
}

func (p *ledgerProbe) insertLedger(kind, subject string) error {
	_, err := p.pool.Exec(p.ctx, `
		INSERT INTO network_submissions (kind, subject, idempotency_key, request_body, status)
		VALUES ($1, $2, 'k-'||$2, '\x7b7d', 'SENDING')`, kind, subject)
	return err
}

// expectLedgerKinds checks that the ledger admits each kind in admit and refuses
// anything else; the probes it inserts are removed again.
func (p *ledgerProbe) expectLedgerKinds(admit []string) {
	p.t.Helper()
	for _, kind := range append(append([]string{}, admit...), "NOT_A_KIND") {
		err := p.insertLedger(kind, "probe-"+kind)
		wanted := kind != "NOT_A_KIND"
		if wanted && err != nil {
			p.t.Fatalf("version %d: the ledger must admit %s: %v", p.version(), kind, err)
		}
		if !wanted && err == nil {
			p.t.Fatalf("version %d: the ledger must refuse a kind it does not know", p.version())
		}
	}
	if _, err := p.pool.Exec(p.ctx, `DELETE FROM network_submissions WHERE subject LIKE 'probe-%'`); err != nil {
		p.t.Fatalf("remove probes: %v", err)
	}
}

// expectLedgerRefuses checks that the ledger does not admit any of the kinds.
func (p *ledgerProbe) expectLedgerRefuses(kinds ...string) {
	p.t.Helper()
	for _, kind := range kinds {
		if err := p.insertLedger(kind, "probe-"+kind); err == nil {
			p.t.Fatalf("version %d: the ledger must not admit %s yet", p.version(), kind)
		}
	}
}

// highestMigration is the number of the newest *.up.sql in migrations/, so the
// test below keeps meaning "all of them" when the next migration is added.
func highestMigration(t *testing.T) uint {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(migrationsDir(t), "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("migration files: %v (%d found)", err, len(files))
	}
	var highest uint64
	for _, f := range files {
		n, err := strconv.ParseUint(strings.SplitN(filepath.Base(f), "_", 2)[0], 10, 32)
		if err != nil {
			t.Fatalf("migration %s has no numeric prefix: %v", f, err)
		}
		highest = max(highest, n)
	}
	return uint(highest)
}

// TestMigrations_FreshDatabaseUpAllDownOneStepUpAgain migrates an empty database
// through EVERY migration, takes the newest one down and brings it back. It is the
// guard against a migration numbered below one that is already on develop: such a
// file is skipped by golang-migrate on any database past its number.
func TestMigrations_FreshDatabaseUpAllDownOneStepUpAgain(t *testing.T) {
	p := newLedgerProbe(t)
	if err := p.m.Up(); err != nil {
		t.Fatalf("Up: %v", err)
	}
	top := highestMigration(t)
	if got := p.version(); got != top {
		t.Fatalf("version after Up = %d, want the newest migration %d", got, top)
	}
	if p.profileTables() != 1 {
		t.Fatal("after Up the node profile table must exist (migration 0010)")
	}
	if p.inventoryTables() != 2 {
		t.Fatal("after Up the inventory feed tables must exist (migration 0011)")
	}
	five := []string{"ACKNOWLEDGEMENT", "SHIPMENT_CONFIRMATION", "NODE_REGISTRATION", "NODE_PROFILE", "INBOUND_RECEIPT"}
	six := append(slices.Clone(five), "INVENTORY")
	p.expectLedgerKinds(six)

	// One step down is 0011: it takes INVENTORY and its tables with it and leaves
	// the five kinds of 0008 admitted.
	p.step(-1)
	if got := p.version(); got != top-1 {
		t.Fatalf("version after one step down = %d, want %d", got, top-1)
	}
	if p.inventoryTables() != 0 {
		t.Fatal("after one step down the inventory feed tables must be gone")
	}
	p.expectLedgerKinds(five)
	p.expectLedgerRefuses("INVENTORY")

	p.step(1)
	if got := p.version(); got != top {
		t.Fatalf("version after the second up = %d, want %d", got, top)
	}
	if p.inventoryTables() != 2 {
		t.Fatal("after the second up the inventory feed tables must exist")
	}
	p.expectLedgerKinds(six)
}

// TestMigration0010_UpDownUp proves the node profile migration both ways on a
// database that already holds everything develop's 0008 and 0009 create: a node, an
// acknowledgement, an inbound receipt in the ledger and a profile. Down drops only
// the profile state and the NODE_PROFILE ledger rows, the rest survives (the
// INBOUND_RECEIPT row above all: a 0010 that rebuilt the ledger's kind check with its
// old four-kind list would fail here), and a second up works again.
func TestMigration0010_UpDownUp(t *testing.T) {
	p := newLedgerProbe(t)
	p.migrateTo(9)

	if got := p.profileTables(); got != 0 {
		t.Fatalf("profile table before 0010 = %d, want 0", got)
	}
	// 0008 already admits both new kinds, so the order of the two ADRs does not matter.
	p.expectLedgerKinds([]string{"ACKNOWLEDGEMENT", "NODE_PROFILE", "INBOUND_RECEIPT"})
	for _, row := range [][2]string{{"ACKNOWLEDGEMENT", "NR-keep"}, {"INBOUND_RECEIPT", "ASN-keep"}} {
		if err := p.insertLedger(row[0], row[1]); err != nil {
			t.Fatalf("seed %s: %v", row[0], err)
		}
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_nodes (site_code, node_id) VALUES ('site-1', 'N-1')`); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	p.migrateTo(10)
	if got := p.profileTables(); got != 1 {
		t.Fatalf("profile table after 0010 = %d, want 1", got)
	}
	if err := p.insertLedger("NODE_PROFILE", "site-1#1#1"); err != nil {
		t.Fatalf("after 0010 the ledger must accept NODE_PROFILE: %v", err)
	}
	if got := p.kindRows("INBOUND_RECEIPT"); got != 1 {
		t.Fatalf("INBOUND_RECEIPT rows after 0010 = %d, want the seeded one", got)
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_node_profiles (site_code, revision, fingerprint, content) VALUES ('site-1', 1, 'a', '{}')`); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_node_profiles (site_code, revision, fingerprint, content) VALUES ('site-9', 1, 'a', '{}')`); err == nil {
		t.Fatal("a profile for a site with no node must violate the foreign key")
	}

	p.step(-1)
	if got := p.version(); got != 9 {
		t.Fatalf("version after down = %d, want 9", got)
	}
	if got := p.profileTables(); got != 0 {
		t.Fatalf("profile table after down = %d, want 0", got)
	}
	if got := p.kindRows("NODE_PROFILE"); got != 0 {
		t.Fatalf("NODE_PROFILE ledger rows after down = %d, want 0", got)
	}
	for kind, want := range map[string]int{"ACKNOWLEDGEMENT": 1, "INBOUND_RECEIPT": 1} {
		if got := p.kindRows(kind); got != want {
			t.Fatalf("%s ledger rows after down = %d, want %d untouched", kind, got, want)
		}
	}
	if got := p.count(`SELECT count(*) FROM network_nodes`); got != 1 {
		t.Fatalf("node rows after down = %d, want it untouched", got)
	}
	p.expectLedgerKinds([]string{"NODE_PROFILE", "INBOUND_RECEIPT"})

	p.step(1)
	if got := p.profileTables(); got != 1 {
		t.Fatalf("profile table after the second up = %d, want 1", got)
	}
}

// TestMigration0011_UpDownUp proves the inventory feed migration both ways on a
// database that holds a ledger row of every kind 0008 admits, a node and a profile.
// The ledger's kind check is rebuilt by 0011, not widened: an up that forgot one of
// the five kinds would fail here on the rows that are already there, and a down
// that kept INVENTORY in the check, or did not remove its rows first, would fail
// on the way back. Everything the migration does not own survives it.
func TestMigration0011_UpDownUp(t *testing.T) {
	p := newLedgerProbe(t)
	p.migrateTo(10)

	if got := p.inventoryTables(); got != 0 {
		t.Fatalf("inventory tables before 0011 = %d, want 0", got)
	}
	five := []string{"ACKNOWLEDGEMENT", "SHIPMENT_CONFIRMATION", "NODE_REGISTRATION", "NODE_PROFILE", "INBOUND_RECEIPT"}
	p.expectLedgerKinds(five)
	p.expectLedgerRefuses("INVENTORY")
	for _, kind := range five {
		if err := p.insertLedger(kind, "keep-"+kind); err != nil {
			t.Fatalf("seed %s: %v", kind, err)
		}
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_nodes (site_code, node_id) VALUES ('site-1', 'N-1')`); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_node_profiles (site_code, revision, fingerprint, content) VALUES ('site-1', 1, 'a', '{}')`); err != nil {
		t.Fatalf("seed profile: %v", err)
	}

	p.migrateTo(11)
	if got := p.inventoryTables(); got != 2 {
		t.Fatalf("inventory tables after 0011 = %d, want 2", got)
	}
	for _, kind := range five {
		if got := p.kindRows(kind); got != 1 {
			t.Fatalf("%s ledger rows after 0011 = %d, want the seeded one (the kind check must stay a superset of 0008's)", kind, got)
		}
	}
	p.expectLedgerKinds(append(slices.Clone(five), "INVENTORY"))
	if err := p.insertLedger("INVENTORY", "site-1#2026-10-25T12:00:00Z"); err != nil {
		t.Fatalf("after 0011 the ledger must accept INVENTORY: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_inventory_feeds (site_code, part, subject, fingerprint, as_of) VALUES ('site-1', 1, 's', 'f', now())`); err != nil {
		t.Fatalf("seed a feed state: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_rejected_listings (site_code, listing_id, reason, rejected_at) VALUES ('site-1', 'LST-B', 'r', now())`); err != nil {
		t.Fatalf("seed a rejected listing: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_inventory_feeds (site_code, part, subject, fingerprint, as_of) VALUES ('site-9', 1, 's', 'f', now())`); err == nil {
		t.Fatal("a feed state for a site with no node must violate the foreign key")
	}
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO network_rejected_listings (site_code, listing_id, rejected_at) VALUES ('site-9', 'LST-B', now())`); err == nil {
		t.Fatal("a rejected listing for a site with no node must violate the foreign key")
	}

	p.step(-1)
	if got := p.version(); got != 10 {
		t.Fatalf("version after down = %d, want 10", got)
	}
	if got := p.inventoryTables(); got != 0 {
		t.Fatalf("inventory tables after down = %d, want 0", got)
	}
	if got := p.kindRows("INVENTORY"); got != 0 {
		t.Fatalf("INVENTORY ledger rows after down = %d, want 0", got)
	}
	for _, kind := range five {
		if got := p.kindRows(kind); got != 1 {
			t.Fatalf("%s ledger rows after down = %d, want 1 untouched", kind, got)
		}
	}
	for table, want := range map[string]int{"network_nodes": 1, "network_node_profiles": 1} {
		if got := p.count(`SELECT count(*) FROM ` + table); got != want {
			t.Fatalf("%s rows after down = %d, want %d untouched", table, got, want)
		}
	}
	p.expectLedgerKinds(five)
	p.expectLedgerRefuses("INVENTORY")

	p.step(1)
	if got := p.inventoryTables(); got != 2 {
		t.Fatalf("inventory tables after the second up = %d, want 2", got)
	}
	p.expectLedgerKinds(append(slices.Clone(five), "INVENTORY"))
}
