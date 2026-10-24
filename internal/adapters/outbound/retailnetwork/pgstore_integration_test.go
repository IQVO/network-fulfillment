//go:build integration

package retailnetwork_test

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}

// bootDB starts a Postgres container with NO migration applied.
func bootDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("networkfulfillment"),
		tcpostgres.WithUsername("networkfulfillment"),
		tcpostgres.WithPassword("networkfulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return url
}

// newStore boots a migrated Postgres and returns the store over its pool.
func newStore(t *testing.T) (*retailnetwork.PGStore, *pgxpool.Pool) {
	t.Helper()
	url := bootDB(t)
	if err := postgres.RunMigrations(url, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return retailnetwork.NewPGStore(pool), pool
}

func samplePO(po string, lines ...retailnetwork.PurchaseOrderLine) retailnetwork.PurchaseOrder {
	if len(lines) == 0 {
		lines = []retailnetwork.PurchaseOrderLine{
			{LineNo: 1, NetworkLineRef: "1", ListingID: "L-1", NetworkProductID: "L-1", Quantity: 2},
			{LineNo: 2, NetworkLineRef: "2", ListingID: "L-2", NetworkProductID: "L-2", Quantity: 5},
		}
	}
	at := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	return retailnetwork.PurchaseOrder{
		PoNumber: po, VendorID: "V-WS", NodeID: "N-1",
		RequiredShipBy: at.Add(48 * time.Hour), AckBy: at.Add(3 * time.Hour), Lines: lines,
	}
}

func TestPGStore_NodeMapIsPersistedBothWaysAndNeverOverwritten(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if _, ok, err := store.NodeIDForSite(ctx, "site-1"); err != nil || ok {
		t.Fatalf("unmapped site: ok=%v err=%v, want not found", ok, err)
	}
	if err := store.SaveNode(ctx, "site-1", "N-1"); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SaveNode(ctx, "site-1", "N-1"); err != nil {
		t.Fatalf("SaveNode again must be a no-op: %v", err)
	}
	if node, ok, _ := store.NodeIDForSite(ctx, "site-1"); !ok || node != "N-1" {
		t.Fatalf("NodeIDForSite = %q %v, want N-1", node, ok)
	}
	if site, ok, _ := store.SiteForNode(ctx, "N-1"); !ok || site != "site-1" {
		t.Fatalf("SiteForNode = %q %v, want site-1", site, ok)
	}
	if err := store.SaveNode(ctx, "site-1", "N-2"); !errors.Is(err, retailnetwork.ErrMappingConflict) {
		t.Fatalf("remapping a site: err = %v, want ErrMappingConflict", err)
	}
	if err := store.SaveNode(ctx, "site-2", "N-1"); err == nil {
		t.Fatal("mapping a second site onto a mapped node must fail")
	}
}

func TestPGStore_APurchaseOrderKeepsOneMintedRefAcrossRedeliveries(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()

	ref1, err := store.SavePurchaseOrder(ctx, samplePO("PO-7f3c"))
	if err != nil {
		t.Fatalf("SavePurchaseOrder: %v", err)
	}
	if ref1 == "" || strings.Contains(string(ref1), "7f3c") {
		t.Fatalf("ref = %q, want an opaque id that does not contain the purchase order number", ref1)
	}
	ref2, err := store.SavePurchaseOrder(ctx, samplePO("PO-7f3c"))
	if err != nil || ref2 != ref1 {
		t.Fatalf("redelivery: ref=%q err=%v, want the same ref %q", ref2, err, ref1)
	}
	other, err := store.SavePurchaseOrder(ctx, samplePO("PO-9a00"))
	if err != nil || other == ref1 {
		t.Fatalf("another purchase order: ref=%q err=%v, want a different ref", other, err)
	}

	var lines int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_purchase_order_lines WHERE network_ref = $1`, string(ref1)).Scan(&lines); err != nil {
		t.Fatalf("count lines: %v", err)
	}
	if lines != 2 {
		t.Fatalf("lines after a redelivery = %d, want 2 (no duplicates)", lines)
	}

	got, ok, err := store.PurchaseOrder(ctx, ref1)
	if err != nil || !ok {
		t.Fatalf("PurchaseOrder: ok=%v err=%v", ok, err)
	}
	want := samplePO("PO-7f3c")
	if got.PoNumber != want.PoNumber || got.VendorID != "V-WS" || got.NodeID != "N-1" ||
		!got.AckBy.Equal(want.AckBy) || !got.RequiredShipBy.Equal(want.RequiredShipBy) {
		t.Fatalf("PurchaseOrder = %+v, want %+v", got, want)
	}
	if len(got.Lines) != 2 || got.Lines[0].LineNo != 1 || got.Lines[1].LineNo != 2 ||
		got.Lines[1].Quantity != 5 || got.Lines[0].ListingID != "L-1" || got.Lines[0].NetworkProductID != "L-1" {
		t.Fatalf("lines = %+v, want line 1 then line 2 with quantities and listings", got.Lines)
	}
	if _, ok, _ := store.PurchaseOrder(ctx, "NR-unknown"); ok {
		t.Fatal("an unknown ref must not be found")
	}
}

func TestPGStore_ConcurrentPollsOfOnePurchaseOrderMintOneRef(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()

	const writers = 16
	refs := make([]shared.NetworkRef, writers)
	errs := make([]error, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			refs[i], errs[i] = store.SavePurchaseOrder(ctx, samplePO("PO-race"))
		}()
	}
	close(start)
	wg.Wait()

	for i := range writers {
		if errs[i] != nil {
			t.Fatalf("writer %d: %v", i, errs[i])
		}
		if refs[i] != refs[0] {
			t.Fatalf("writer %d got ref %q, writer 0 got %q: one purchase order must have one ref", i, refs[i], refs[0])
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_purchase_orders WHERE po_number = 'PO-race'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows = %d err = %v, want exactly 1", n, err)
	}
}

func TestPGStore_LedgerKeepsTheFirstBodyAndKeyByteForByte(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	body := []byte("{\"vendorId\": \"V-WS\",  \"poNumber\":\"PO-1\"}\n")

	first, err := store.BeginSubmission(ctx, retailnetwork.Submission{
		Kind: retailnetwork.KindAcknowledgement, Subject: "NR-1", IdempotencyKey: "nf-ack-NR-1", Body: body,
	})
	if err != nil {
		t.Fatalf("BeginSubmission: %v", err)
	}
	if string(first.Body) != string(body) {
		t.Fatalf("body = %q, want it back byte for byte %q", first.Body, body)
	}
	if first.Status != retailnetwork.StatusSending || first.TransactionID != "" {
		t.Fatalf("new row = %+v, want SENDING with no transaction", first)
	}

	again, err := store.BeginSubmission(ctx, retailnetwork.Submission{
		Kind: retailnetwork.KindAcknowledgement, Subject: "NR-1", IdempotencyKey: "different-key", Body: []byte(`{"other":true}`),
	})
	if err != nil {
		t.Fatalf("second BeginSubmission: %v", err)
	}
	if again.IdempotencyKey != "nf-ack-NR-1" || string(again.Body) != string(body) {
		t.Fatalf("second Begin = key %q body %q: the first key and body must win", again.IdempotencyKey, again.Body)
	}

	if _, err := store.BeginSubmission(ctx, retailnetwork.Submission{
		Kind: retailnetwork.KindAcknowledgement, Subject: "NR-2", IdempotencyKey: strings.Repeat("k", 129), Body: body,
	}); err == nil {
		t.Fatal("a key over 128 characters must be refused before it reaches the network")
	}
}

func TestPGStore_LedgerTransactionAndOutcomeRules(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	kind, subject := retailnetwork.KindShipmentConfirmation, "NR-1"
	if _, err := store.BeginSubmission(ctx, retailnetwork.Submission{Kind: kind, Subject: subject, IdempotencyKey: "nf-ship-NR-1", Body: []byte("{}")}); err != nil {
		t.Fatalf("BeginSubmission: %v", err)
	}

	if err := store.RecordTransaction(ctx, kind, subject, "tx-1"); err != nil {
		t.Fatalf("RecordTransaction: %v", err)
	}
	if err := store.RecordTransaction(ctx, kind, subject, "tx-OTHER"); err != nil {
		t.Fatalf("RecordTransaction again: %v", err)
	}
	row, _, _ := store.Submission(ctx, kind, subject)
	if row.TransactionID != "tx-1" || row.Status != retailnetwork.StatusProcessing {
		t.Fatalf("row = %+v, want tx-1 PROCESSING: the first transaction id is kept", row)
	}

	if err := store.RecordOutcome(ctx, kind, subject, retailnetwork.StatusSuccess, ""); err != nil {
		t.Fatalf("RecordOutcome: %v", err)
	}
	if err := store.RecordOutcome(ctx, kind, subject, retailnetwork.StatusFailure, "late news"); err != nil {
		t.Fatalf("RecordOutcome after terminal: %v", err)
	}
	row, _, _ = store.Submission(ctx, kind, subject)
	if row.Status != retailnetwork.StatusSuccess || row.Errors != "" {
		t.Fatalf("row = %+v, want SUCCESS untouched by a later outcome", row)
	}
	if _, ok, err := store.Submission(ctx, kind, "NR-nope"); ok || err != nil {
		t.Fatalf("missing row: ok=%v err=%v", ok, err)
	}
}

func TestPGStore_ARowWithoutATransactionCanOnlyBeSendingOrFailed(t *testing.T) {
	_, pool := newStore(t)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO network_submissions (kind, subject, idempotency_key, request_body, status)
		VALUES ('ACKNOWLEDGEMENT', 'NR-1', 'k', '\x7b7d', 'PROCESSING')`)
	if err == nil {
		t.Fatal("a PROCESSING row with no transaction id must violate submission_transaction_matches_status")
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO network_submissions (kind, subject, idempotency_key, request_body, status)
		VALUES ('ACKNOWLEDGEMENT', 'NR-2', 'k2', '\x7b7d', 'FAILURE')`); err != nil {
		t.Fatalf("a refused write (FAILURE, no transaction) must be storable: %v", err)
	}
}

func TestPGStore_LabelsAreIdempotentOrderedAndTrackingNumbersAreUnique(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ref1, _ := store.SavePurchaseOrder(ctx, samplePO("PO-1"))
	ref2, _ := store.SavePurchaseOrder(ctx, samplePO("PO-2"))

	labels := []retailnetwork.Label{
		{PackageNo: 2, LabelRef: "LB-2", TrackingNumber: "TRK-2", Carrier: "CARRIER"},
		{PackageNo: 1, LabelRef: "LB-1", TrackingNumber: "TRK-1", Carrier: "CARRIER"},
	}
	if err := store.SaveLabels(ctx, ref1, labels); err != nil {
		t.Fatalf("SaveLabels: %v", err)
	}
	if err := store.SaveLabels(ctx, ref1, labels); err != nil {
		t.Fatalf("SaveLabels again must be a no-op: %v", err)
	}
	got, err := store.Labels(ctx, ref1)
	if err != nil || len(got) != 2 || got[0].PackageNo != 1 || got[1].TrackingNumber != "TRK-2" {
		t.Fatalf("Labels = %+v err=%v, want two labels ordered by package", got, err)
	}
	clash := []retailnetwork.Label{{PackageNo: 1, LabelRef: "LB-X", TrackingNumber: "TRK-1", Carrier: "CARRIER"}}
	if err := store.SaveLabels(ctx, ref2, clash); !errors.Is(err, retailnetwork.ErrMappingConflict) {
		t.Fatalf("a tracking number on two orders: err = %v, want ErrMappingConflict", err)
	}
	if none, err := store.Labels(ctx, ref2); err != nil || len(none) != 0 {
		t.Fatalf("Labels of an unlabelled order = %+v err=%v, want none", none, err)
	}
}

// TestMigration0006_UpDownUp proves the migration both ways on a database that
// already holds a network order: down drops only this adapter's tables and the
// aggregate's rows survive; a second up works again.
func TestMigration0006_UpDownUp(t *testing.T) {
	url := bootDB(t)
	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Migrate(5); err != nil {
		t.Fatalf("Migrate(5): %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		INSERT INTO network_orders (network_ref, site_id, required_ship_by, acknowledge_by, state, received_at)
		VALUES ('NR-keep', 'site-1', now(), now(), 'NEW', now())`); err != nil {
		t.Fatalf("seed order: %v", err)
	}

	tables := func() int {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM information_schema.tables
			WHERE table_name IN ('network_nodes','network_purchase_orders','network_purchase_order_lines','network_submissions','network_labels')`).Scan(&n); err != nil {
			t.Fatalf("information_schema: %v", err)
		}
		return n
	}
	if got := tables(); got != 0 {
		t.Fatalf("tables before 0006 = %d, want 0", got)
	}
	if err := m.Migrate(6); err != nil {
		t.Fatalf("Migrate(6): %v", err)
	}
	if got := tables(); got != 5 {
		t.Fatalf("tables after 0006 = %d, want 5", got)
	}
	if err := m.Migrate(5); err != nil {
		t.Fatalf("down to 5: %v", err)
	}
	if got := tables(); got != 0 {
		t.Fatalf("tables after down = %d, want 0", got)
	}
	var kept int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM network_orders WHERE network_ref = 'NR-keep'`).Scan(&kept); err != nil || kept != 1 {
		t.Fatalf("network order after down: count=%d err=%v, want it untouched", kept, err)
	}
	if err := m.Migrate(6); err != nil {
		t.Fatalf("second up: %v", err)
	}
}
