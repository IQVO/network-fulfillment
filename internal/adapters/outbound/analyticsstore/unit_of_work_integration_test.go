//go:build integration

package analyticsstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
)

// TestUnitOfWork_ClaimAndProjectionRollBackTogether pins ADR 0023 at the
// store level: inside one analytics UnitOfWork the consumer claim
// (analytics_consumed_events) and the projection write (its own claim in
// analytics_processed_events plus the acknowledgement_rollup upsert) share
// ONE transaction, so an error after both writes leaves none of them.
func TestUnitOfWork_ClaimAndProjectionRollBackTogether(t *testing.T) {
	pool := newAnalyticsPool(t)
	ctx := context.Background()
	id := "uow-rollback-" + time.Now().Format("150405.000000000")
	day := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	boom := errors.New("simulated failure after both writes")

	err := analyticsstore.NewUnitOfWork(pool).Execute(ctx, func(ctx context.Context) error {
		if isNew, err := analyticsstore.NewConsumedEventsRepo(pool).MarkProcessed(ctx, id); err != nil || !isNew {
			t.Fatalf("MarkProcessed = (%v, %v), want (true, nil)", isNew, err)
		}
		if err := analyticsstore.NewPostgresProjection(pool).ApplyNetworkOrderReceived(ctx, id, day); err != nil {
			t.Fatalf("ApplyNetworkOrderReceived: %v", err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Execute = %v, want the fn error", err)
	}
	for _, table := range []string{"analytics_consumed_events", "analytics_processed_events"} {
		if n := eventRows(t, pool, table, id); n != 0 {
			t.Errorf("%s rows for %s after rollback = %d, want 0", table, id, n)
		}
	}
	if n := rollupRows(t, pool, day); n != 0 {
		t.Errorf("acknowledgement_rollup rows for %s after rollback = %d, want 0", day.Format("2006-01-02"), n)
	}
}

// TestUnitOfWork_CommitsClaimAndProjectionAndJoinsNestedScope proves the
// success path commits every write, and that a nested Execute joins the
// outer transaction rather than opening a second one.
func TestUnitOfWork_CommitsClaimAndProjectionAndJoinsNestedScope(t *testing.T) {
	pool := newAnalyticsPool(t)
	ctx := context.Background()
	id := "uow-commit-" + time.Now().Format("150405.000000000")
	day := time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)
	uow := analyticsstore.NewUnitOfWork(pool)

	err := uow.Execute(ctx, func(ctx context.Context) error {
		if _, err := analyticsstore.NewConsumedEventsRepo(pool).MarkProcessed(ctx, id); err != nil {
			return err
		}
		return uow.Execute(ctx, func(ctx context.Context) error {
			return analyticsstore.NewPostgresProjection(pool).ApplyNetworkOrderReceived(ctx, id, day)
		})
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, table := range []string{"analytics_consumed_events", "analytics_processed_events"} {
		if n := eventRows(t, pool, table, id); n != 1 {
			t.Errorf("%s rows for %s = %d, want 1", table, id, n)
		}
	}
	if n := rollupRows(t, pool, day); n != 1 {
		t.Errorf("acknowledgement_rollup rows for %s = %d, want 1", day.Format("2006-01-02"), n)
	}
}

func newAnalyticsPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := analyticsstore.NewPool(context.Background(), newAnalyticsURL(t))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func eventRows(t *testing.T, pool *pgxpool.Pool, table, id string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table+" WHERE event_id = $1", id).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func rollupRows(t *testing.T, pool *pgxpool.Pool, day time.Time) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM acknowledgement_rollup WHERE day_bucket = $1",
		day.UTC().Truncate(24*time.Hour)).Scan(&n); err != nil {
		t.Fatalf("count acknowledgement_rollup: %v", err)
	}
	return n
}
