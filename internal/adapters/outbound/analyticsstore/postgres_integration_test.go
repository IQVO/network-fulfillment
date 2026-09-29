//go:build integration

package analyticsstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

func requireAnalyticsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("ANALYTICS_DATABASE_URL")
	if url == "" {
		t.Skip("ANALYTICS_DATABASE_URL not set, skipping analytics postgres integration test")
	}
	return url
}

func migrateAnalytics(t *testing.T, url string) {
	t.Helper()
	if err := postgres.RunMigrations(url, "../../../../migrations/analytics"); err != nil {
		t.Fatalf("migrate analytics: %v", err)
	}
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	url := requireAnalyticsURL(t)
	migrateAnalytics(t, url)

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
	url := requireAnalyticsURL(t)
	migrateAnalytics(t, url)

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
	url := requireAnalyticsURL(t)
	migrateAnalytics(t, url)

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
	url := requireAnalyticsURL(t)
	migrateAnalytics(t, url)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	id := "consumed-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM analytics_consumed_events WHERE event_id = $1`, id) })

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
