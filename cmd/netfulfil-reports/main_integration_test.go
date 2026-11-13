//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	// pgtest boots this package's Postgres through testcontainers-go; the
	// blank import keeps that obligation visible in the package that relies
	// on it (internal/architecture's
	// TestPostgresIntegrationTestsUseTestcontainers checks exactly this).
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// analyticsMigrationsDir resolves the repository's analytics migrations
// directory relative to THIS file, so the test works no matter which
// directory go test was invoked from.
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve the test source file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}

// stopWithSIGTERM drives run()'s documented stop contract (run() takes no
// context; SIGTERM is its only stop signal) by signalling the test process
// itself, retrying until run() returns.
//
// The test's own handler was registered BEFORE run() started and is NEVER
// unregistered (no signal.Stop): run()'s internal signal.Notify channel
// stays registered for the rest of the process too, so a retry still in
// flight after run() returned can never fall back to Go's default SIGTERM
// disposition and kill the test binary — the failure mode the fleet's
// product-master PR #98 first CI run hit.
func stopWithSIGTERM(t *testing.T, done <-chan error) error {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			return err
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("run() did not return after SIGTERM")
	return nil
}

// TestReportsServeTheProjectionAndStopCleanly proves the READER composition
// root of the "Network Order Acknowledgement & Translation" data product
// (ADR 0002 section "A separate analytics topic, projector, and reports
// binary": "cmd/netfulfil-reports — the read-only reader. Opens the
// analytical database and serves GET /reports/acknowledgement"):
//
//   - the analytics schema is migrated onto an EMPTY database
//     (pgtest.EmptyURL) with the writer's own migrations — the reader owns
//     no schema (cmd/netfulfil-reports/main.go: "the writer ... is a
//     separate deployable and owns the schema").
//   - facts are seeded through the repo's OWN analytics writer
//     (analyticsstore.NewPostgresProjection, the projection the projector
//     applies per event, atomically with its dedupe claim — ADR 0023): one
//     order received 25 minutes ago, acknowledged 5 minutes later.
//   - run() boots for real; /healthz answers 200 (runbook: liveness and
//     readiness GET /healthz for cmd/netfulfil-reports).
//   - the documented report answers over the real HTTP surface
//     (docs/docs/api-reference/rest/get-acknowledgement-report.api.mdx):
//     one row for the seeded day, 1 order received, 1 acknowledged, the
//     300s average acknowledgement latency, and no rejection counter
//     moved; a query without the required RFC3339 from/to is an RFC 7807
//     400.
//   - /reports/acknowledgement/freshness answers (its api.mdx: "Seconds
//     between now and the occurredAt of the most recently projected domain
//     event"): the lag matches the seeded 20-minute-old acknowledgement.
//   - SIGTERM drains cleanly: run() returns nil.
func TestReportsServeTheProjectionAndStopCleanly(t *testing.T) {
	ctx := context.Background()

	url := pgtest.EmptyURL(t)
	if err := postgres.RunMigrations(url, analyticsMigrationsDir(t)); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}

	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	projection := analyticsstore.NewPostgresProjection(pool)

	// Seed the facts with the repo's own analytics writer: received 25
	// minutes ago, acknowledged 5 minutes later (so 20 minutes ago). Now-
	// relative timestamps keep the freshness assertion meaningful; unique
	// event ids keep a rerun against the same database honest.
	stamp := time.Now().UTC().UnixNano()
	received := time.Now().UTC().Add(-25 * time.Minute)
	acknowledged := received.Add(5 * time.Minute)
	if err := projection.ApplyNetworkOrderReceived(ctx,
		fmt.Sprintf("nf-reports-it-r-%d", stamp), received); err != nil {
		t.Fatalf("seed NetworkOrderReceived: %v", err)
	}
	if err := projection.ApplyNetworkOrderAcknowledged(ctx,
		fmt.Sprintf("nf-reports-it-a-%d", stamp), acknowledged, 300); err != nil {
		t.Fatalf("seed NetworkOrderAcknowledged: %v", err)
	}

	// run()'s documented environment, exactly the names it reads.
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("HTTP_ADDR", freeAddr(t))
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a
	// dial to localhost:4317 at shutdown (the same trio cmd/netfulfil's
	// e2e test sets).
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	base := "http://" + os.Getenv("HTTP_ADDR")

	// The test's SIGTERM handler is registered BEFORE run() starts and is
	// never unregistered — see stopWithSIGTERM.
	term := make(chan os.Signal, 16)
	signal.Notify(term, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run() }()

	waitUntil(t, "GET /healthz = 200", func() bool {
		code, body := getJSON(t, base+"/healthz")
		return code == http.StatusOK && body["status"] == "ok"
	})

	// The documented report (GET /reports/acknowledgement): from and to
	// are required RFC3339; the row is bucketed to the seeded day.
	bucket := received.UTC().Truncate(24 * time.Hour)
	window := "from=" + bucket.Format(time.RFC3339) +
		"&to=" + bucket.Add(24*time.Hour).Format(time.RFC3339) + "&granularity=day"
	code, body := getJSON(t, base+"/reports/acknowledgement?"+window)
	if code != http.StatusOK {
		t.Fatalf("GET /reports/acknowledgement = %d %v", code, body)
	}
	rows, _ := body["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows = %v, want exactly the seeded day", rows)
	}
	row := rows[0].(map[string]any)
	if row["dayBucket"] != bucket.Format(time.RFC3339) {
		t.Fatalf("dayBucket = %v, want %s", row["dayBucket"], bucket.Format(time.RFC3339))
	}
	if row["ordersReceived"] != 1.0 || row["ordersAcknowledged"] != 1.0 {
		t.Fatalf("row = %v: want 1 received and 1 acknowledged", row)
	}
	if row["avgAcknowledgementLatencySeconds"] != 300.0 {
		t.Fatalf("avgAcknowledgementLatencySeconds = %v, want 300", row["avgAcknowledgementLatencySeconds"])
	}
	if row["ordersRejectedUntranslatableSku"] != 0.0 || row["ordersRejectedDomain"] != 0.0 ||
		row["acknowledgementDeadlinesMissed"] != 0.0 || row["ordersRejectedSubmissionFailed"] != 0.0 {
		t.Fatalf("row = %v: no rejection counter may move for the seeded lifecycle", row)
	}

	// A malformed query is a 400, not an empty report: from/to are
	// required (the api.mdx request schema; reports_handler.go writes an
	// RFC 7807 problem).
	if code, body := getJSON(t, base+"/reports/acknowledgement"); code != http.StatusBadRequest {
		t.Fatalf("missing from/to = %d %v, want 400", code, body)
	}

	// The documented freshness probe: seconds between now and the
	// occurredAt of the most recently projected event — the seeded
	// acknowledgement, 20 minutes old at seed time.
	code, body = getJSON(t, base+"/reports/acknowledgement/freshness")
	if code != http.StatusOK {
		t.Fatalf("GET /reports/acknowledgement/freshness = %d %v", code, body)
	}
	lag, ok := body["lagSeconds"].(float64)
	if !ok || lag < 600 || lag > 1800 {
		t.Fatalf("lagSeconds = %v, want the ~20-minute lag of the seeded acknowledgement", body["lagSeconds"])
	}

	if err := stopWithSIGTERM(t, done); err != nil {
		t.Fatalf("a SIGTERM'd reports server must drain cleanly, got %v", err)
	}
}
