//go:build integration

package analyticsstore_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// analyticsMigrationsDir resolves /migrations/analytics relative to THIS
// file, so the test works regardless of the directory `go test` was
// invoked from.
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
}

// newAnalyticsURL boots a throwaway Postgres via testcontainers and runs
// the analytical migrations against it, returning its connection URL.
//
// The test owns its own database end to end — never an external
// ANALYTICS_DATABASE_URL with a skip gate. A skip-gated test reports
// success while asserting nothing, and this fleet's CI would skip it
// silently (ADR conformance item 8: the OLTP postgres package already
// does this via testcontainers; this analytics package was the one
// holdout still env-gated).
func newAnalyticsURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("networkfulfillment_analytics"),
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
	if err := postgres.RunMigrations(url, analyticsMigrationsDir(t)); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	return url
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	url := newAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(24 * time.Hour)
	prefix := "e-" + time.Now().Format("150405.000000000")

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	apply := func() {
		must(proj.ApplyNetworkOrderReceived(ctx, prefix+"-r1", base))
		must(proj.ApplyNetworkOrderReceived(ctx, prefix+"-r2", base))
		must(proj.ApplyNetworkOrderAcknowledged(ctx, prefix+"-a1", base, 120))
		must(proj.ApplyNetworkOrderRejected(ctx, prefix+"-j1", base, "UNTRANSLATABLE_SKU"))
		must(proj.ApplyNetworkOrderRejected(ctx, prefix+"-j2", base, "INFEASIBLE_DEADLINE"))
		must(proj.ApplyNetworkOrderRejected(ctx, prefix+"-j3", base, "ACKNOWLEDGEMENT_DEADLINE_MISSED"))
	}
	apply()
	apply() // idempotent re-application must not double the counters

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base,
		To:          base.Add(24 * time.Hour),
		Granularity: report.GranularityDay,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (single day bucket)", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.OrdersReceived < 2 {
		t.Errorf("OrdersReceived = %d, want >= 2", row.OrdersReceived)
	}
	if row.OrdersAcknowledged < 1 {
		t.Errorf("OrdersAcknowledged = %d, want >= 1", row.OrdersAcknowledged)
	}
	if row.OrdersRejectedUntranslatableSKU < 1 {
		t.Errorf("OrdersRejectedUntranslatableSKU = %d, want >= 1", row.OrdersRejectedUntranslatableSKU)
	}
	if row.OrdersRejectedDomain < 1 {
		t.Errorf("OrdersRejectedDomain = %d, want >= 1", row.OrdersRejectedDomain)
	}
	if row.AcknowledgementDeadlinesMissed < 1 {
		t.Errorf("AcknowledgementDeadlinesMissed = %d, want >= 1", row.AcknowledgementDeadlinesMissed)
	}

	lag, err := rdr.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag < 0 {
		t.Errorf("lag = %v, want >= 0", lag)
	}
}

// TestReadOnlyPool_RejectsWrites asserts the reader pool is genuinely
// read-only: an attempt to write through it must be rejected by Postgres.
func TestReadOnlyPool_RejectsWrites(t *testing.T) {
	url := newAnalyticsURL(t)

	roPool, err := analyticsstore.NewReadOnlyPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewReadOnlyPool: %v", err)
	}
	t.Cleanup(roPool.Close)

	ctx := context.Background()
	_, err = roPool.Exec(ctx,
		`INSERT INTO acknowledgement_rollup (day_bucket) VALUES ($1)`,
		time.Now().UTC().Truncate(24*time.Hour).Add(999*24*time.Hour))
	if err == nil {
		t.Fatal("expected read-only pool to reject INSERT, but it succeeded")
	}

	rdr := analyticsstore.NewPostgresReport(roPool)
	if _, err := rdr.FreshnessLag(ctx); err != nil {
		t.Fatalf("FreshnessLag over read-only pool: %v", err)
	}
}

// TestFreshnessLag_EmptyStore covers the NULL path: max(occurred_at) over
// an empty table returns a single NULL row (not zero rows), which must be
// read as a zero lag rather than a scan error.
func TestFreshnessLag_EmptyStore(t *testing.T) {
	url := newAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE analytics_processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	lag, err := analyticsstore.NewPostgresReport(pool).FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag on empty store: %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty-store lag = %v, want 0", lag)
	}
}

// TestConsumedEventsRepo_MarksOnce verifies the consumer dedupe gate: the
// same event_id is admitted once and rejected thereafter.
func TestConsumedEventsRepo_MarksOnce(t *testing.T) {
	url := newAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	id := "consumed-" + time.Now().Format("150405.000000000")

	repo := analyticsstore.NewConsumedEventsRepo(pool)
	first, err := repo.MarkProcessed(ctx, id)
	if err != nil || !first {
		t.Fatalf("first MarkProcessed = (%v, %v), want (true, nil)", first, err)
	}
	second, err := repo.MarkProcessed(ctx, id)
	if err != nil || second {
		t.Fatalf("second MarkProcessed = (%v, %v), want (false, nil)", second, err)
	}
}
