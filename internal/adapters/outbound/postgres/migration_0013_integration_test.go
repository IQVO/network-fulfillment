//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// TestMigration0013_ConditionalUpdateRefusesStaleWriter proves the
// aggregate_version column (migration 0013) actually closes the lost-update
// window end to end, through the repository: a save carrying the version the
// writer loaded is applied and bumps the row; a save from an instance whose
// version the row has moved past affects zero rows and surfaces
// ErrVersionConflict for the poller/sweep retry (or the REST 409).
func TestMigration0013_ConditionalUpdateRefusesStaleWriter(t *testing.T) {
	url := bootBareDB(t)
	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := insertOrder(t, pool, "po-0013", "SUBMITTED", "ord-1"); err != nil {
		t.Fatalf("insert SUBMITTED order: %v", err)
	}
	// insertOrder predates the column: the backfill default is version 0.
	assertRowVersion(t, pool, "po-0013", 0)

	repo := postgres.NewNetworkOrderRepo(pool)
	stale, err := repo.FindByRef(ctx, "po-0013")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if stale.Version() != 0 {
		t.Fatalf("loaded Version = %d, want 0", stale.Version())
	}

	// Another writer wins the row while `stale` is in flight.
	if _, err := pool.Exec(ctx,
		`UPDATE network_orders SET aggregate_version = aggregate_version + 1 WHERE network_ref = $1`,
		"po-0013"); err != nil {
		t.Fatalf("competing update: %v", err)
	}

	// The stale writer's conditional UPDATE matches zero rows.
	if err := stale.ConfirmAcknowledgement(); err != nil {
		t.Fatalf("ConfirmAcknowledgement: %v", err)
	}
	if err := repo.Save(ctx, stale); !errors.Is(err, networkorder.ErrVersionConflict) {
		t.Fatalf("stale Save err = %v, want ErrVersionConflict", err)
	}
	assertRowVersion(t, pool, "po-0013", 1) // refused, row untouched

	// A fresh load sees the winner's version and lands.
	fresh, err := repo.FindByRef(ctx, "po-0013")
	if err != nil {
		t.Fatalf("FindByRef (fresh): %v", err)
	}
	if err := fresh.ConfirmAcknowledgement(); err != nil {
		t.Fatalf("ConfirmAcknowledgement (fresh): %v", err)
	}
	if err := repo.Save(ctx, fresh); err != nil {
		t.Fatalf("fresh Save: %v", err)
	}
	if fresh.Version() != 2 {
		t.Fatalf("fresh.Version() after save = %d, want 2", fresh.Version())
	}
	assertRowVersion(t, pool, "po-0013", 2)

	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM network_orders WHERE network_ref = $1`, "po-0013").Scan(&state); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if state != "ACKNOWLEDGED" {
		t.Fatalf("state = %q, want ACKNOWLEDGED", state)
	}
}

func assertRowVersion(t *testing.T, pool *pgxpool.Pool, ref string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(),
		`SELECT aggregate_version FROM network_orders WHERE network_ref = $1`, ref).Scan(&got); err != nil {
		t.Fatalf("read aggregate_version: %v", err)
	}
	if got != want {
		t.Fatalf("aggregate_version = %d, want %d", got, want)
	}
}
