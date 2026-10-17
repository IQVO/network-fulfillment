//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
)

// bootBareDB starts a Postgres and returns its URL with NO migration applied,
// so a test can drive golang-migrate step by step.
func bootBareDB(t *testing.T) string {
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

func insertOrder(t *testing.T, pool *pgxpool.Pool, ref, state, localOrderID string) error {
	t.Helper()
	var id *string
	if localOrderID != "" {
		id = &localOrderID
	}
	_, err := pool.Exec(context.Background(), `
		INSERT INTO network_orders (network_ref, site_id, required_ship_by, acknowledge_by, state, local_order_id, received_at)
		VALUES ($1, 'site-1', now(), now(), $2, $3, now())`, ref, state, id)
	return err
}

func columnExists(t *testing.T, pool *pgxpool.Pool, column string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'network_orders' AND column_name = $1`, column).Scan(&n); err != nil {
		t.Fatalf("information_schema: %v", err)
	}
	return n == 1
}

// TestMigration0005_UpDownUp proves migration 0005 (ADR 0024 §8) both ways:
// up adds the hold outcome columns and lets NEW and REJECTED carry an id; down
// clears the ids the old CHECK refuses, drops the columns and restores the
// CHECK, and a second up works again. SUBMITTED data survives the round trip.
func TestMigration0005_UpDownUp(t *testing.T) {
	url := bootBareDB(t)
	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })

	// Up to 0004, the schema before this ADR: NEW with an id is refused.
	if err := m.Migrate(4); err != nil {
		t.Fatalf("Migrate(4): %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if columnExists(t, pool, "hold_outcome") {
		t.Fatal("hold_outcome must not exist before migration 0005")
	}
	if err := insertOrder(t, pool, "po-pre-new", "NEW", "ord-1"); err == nil {
		t.Fatal("before 0005 the CHECK must refuse NEW with an id")
	}
	if err := insertOrder(t, pool, "po-pre-submitted", "SUBMITTED", "ord-keep"); err != nil {
		t.Fatalf("insert SUBMITTED+id: %v", err)
	}

	// Up: 0005.
	if err := m.Migrate(5); err != nil {
		t.Fatalf("Migrate(5) up: %v", err)
	}
	for _, c := range []string{"hold_outcome", "hold_settled_at"} {
		if !columnExists(t, pool, c) {
			t.Fatalf("column %s missing after 0005", c)
		}
	}
	for _, state := range []string{"NEW", "REJECTED"} {
		if err := insertOrder(t, pool, "po-"+state, state, "ord-"+state); err != nil {
			t.Fatalf("after 0005 %s must carry an id: %v", state, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE network_orders SET hold_outcome = 'BOGUS', hold_settled_at = now() WHERE network_ref = 'po-NEW'`); err == nil {
		t.Fatal("hold_outcome outside CANCELLED/ALREADY_RELEASED must be refused")
	}
	if _, err := pool.Exec(ctx, `UPDATE network_orders SET hold_outcome = 'CANCELLED' WHERE network_ref = 'po-NEW'`); err == nil {
		t.Fatal("an outcome without its settle instant must be refused")
	}
	if err := insertOrder(t, pool, "po-noid", "NEW", ""); err != nil {
		t.Fatalf("insert NEW without hold: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE network_orders SET hold_outcome = 'CANCELLED', hold_settled_at = now() WHERE network_ref = 'po-noid'`); err == nil {
		t.Fatal("an outcome on an order with no recorded hold must be refused")
	}

	// Down: 0005 only.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down: %v", err)
	}
	for _, c := range []string{"hold_outcome", "hold_settled_at"} {
		if columnExists(t, pool, c) {
			t.Fatalf("column %s still present after the down migration", c)
		}
	}
	var keep, cleared *string
	if err := pool.QueryRow(ctx, `SELECT local_order_id FROM network_orders WHERE network_ref = 'po-pre-submitted'`).Scan(&keep); err != nil || keep == nil || *keep != "ord-keep" {
		t.Fatalf("a SUBMITTED hold id must survive the down migration, got %v (%v)", keep, err)
	}
	if err := pool.QueryRow(ctx, `SELECT local_order_id FROM network_orders WHERE network_ref = 'po-NEW'`).Scan(&cleared); err != nil || cleared != nil {
		t.Fatalf("a NEW row's id must be cleared by the down migration, got %v (%v)", cleared, err)
	}
	if err := insertOrder(t, pool, "po-post-down", "REJECTED", "ord-9"); err == nil {
		t.Fatal("after the down migration the old CHECK must refuse REJECTED with an id again")
	}

	// And up again.
	if err := m.Steps(1); err != nil {
		t.Fatalf("second up: %v", err)
	}
	if !columnExists(t, pool, "hold_outcome") {
		t.Fatal("hold_outcome missing after the second up")
	}
}
