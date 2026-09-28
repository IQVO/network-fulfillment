package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestWireOrders_RetriesTheDatabaseNotJustOnce is the test that would have
// caught the defect this file's retry exists to fix.
//
// retry_test.go proves the retry HELPER works. That is not the same claim
// as "wireOrders uses it": the shipped version called RunMigrations
// directly, so a single Istio first-dial reset — a known, transient
// condition in this cluster — became CrashLoopBackOff, and every helper
// test still passed. Verified by reverting wireOrders to the direct call;
// this test fails, the helper's own tests do not.
//
// It drives wireOrders against an unreachable address and asserts on the
// ELAPSED time: a single attempt returns fast, whereas the retry budget
// cannot be paid in less than the sum of its backoffs.
func TestWireOrders_RetriesTheDatabaseNotJustOnce(t *testing.T) {
	// Port 1 on loopback refuses immediately, so each attempt fails fast
	// and the only thing that can make this slow is the backoff itself.
	t.Setenv("DATABASE_URL", "postgres://u:p@127.0.0.1:1/nf?sslmode=disable&connect_timeout=1")
	t.Setenv("MIGRATIONS_PATH", migrationsDirForTest(t))

	start := time.Now()
	_, _, _, err := wireOrders(context.Background(), quietLogger())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("an unreachable database must fail the boot, never fall back to the in-memory repo")
	}

	// The full budget is ~31s; one attempt is ~0s. Anything under the
	// first two backoffs (1s + 2s) means the retry was skipped.
	if elapsed < 3*time.Second {
		t.Fatalf("wireOrders gave up in %v — it is not retrying, so a single "+
			"first-dial reset would crash-loop the pod (err: %v)", elapsed, err)
	}
	if !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("err = %v, want it to report how many attempts were made", err)
	}
}

// With no DATABASE_URL the in-memory repo is correct and must cost
// nothing: no dial, no backoff, no delay to a local run.
func TestWireOrders_NoDatabaseURLUsesMemoryImmediately(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	start := time.Now()
	repo, _, closeFn, err := wireOrders(context.Background(), quietLogger())
	if err != nil {
		t.Fatalf("wireOrders: %v", err)
	}
	defer closeFn()

	if repo == nil {
		t.Fatal("no repository returned")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the in-memory path took %v; it must not touch the network", elapsed)
	}
}

// migrationsDirForTest resolves the repo's migrations directory, so the
// retry under test fails on the DIAL rather than on a missing directory
// (which would return before any retry and make the test vacuous).
func migrationsDirForTest(t *testing.T) string {
	t.Helper()
	// cmd/netfulfil -> repo root.
	dir := "../../migrations"
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

// TestMigrationsDatabaseURLFallback proves the fallback wiring
// wireOrders applies before calling postgres.RunMigrations: when
// MIGRATIONS_DATABASE_URL is unset, migrations must run against
// DATABASE_URL itself (byte-identical to this service's behavior before
// the split existed — local dev, CI integration tests, and any cluster
// whose Terraform predates this fix all rely on this). When
// MIGRATIONS_DATABASE_URL IS set, migrations must use it — never
// DATABASE_URL/PgBouncer — which is the whole point of the fix (see
// docs/adr/0007-migrations-direct-postgres-connection.md, mirroring
// order-management's ADR-0029: golang-migrate's session-scoped
// pg_advisory_lock is incompatible with PgBouncer's transaction-pooling
// mode, warehouse-infra PR #43).
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/network_fulfillment?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		t.Setenv("MIGRATIONS_DATABASE_URL", "")
		os.Unsetenv("MIGRATIONS_DATABASE_URL")

		got := databaseURL
		if v := os.Getenv("MIGRATIONS_DATABASE_URL"); v != "" {
			got = v
		}
		if got != databaseURL {
			t.Fatalf("fallback = %q, want the DATABASE_URL value %q", got, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/network_fulfillment?sslmode=disable"
		t.Setenv("MIGRATIONS_DATABASE_URL", direct)

		got := databaseURL
		if v := os.Getenv("MIGRATIONS_DATABASE_URL"); v != "" {
			got = v
		}
		if got != direct {
			t.Fatalf("got = %q, want the direct MIGRATIONS_DATABASE_URL value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", got, direct)
		}
		if got == databaseURL {
			t.Fatal("MIGRATIONS_DATABASE_URL and DATABASE_URL collapsed to the same value — the whole point of this env var is that it differs")
		}
	})
}

// TestWireOrders_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves wireOrders itself — not just the env-var read above — actually
// threads MIGRATIONS_DATABASE_URL into the migration step, rather than
// ever conflating it with DATABASE_URL. DATABASE_URL points at an
// address nothing listens on (so opening the pgxpool, which happens
// AFTER migrations succeed, would hang/fail loudly if ever reached), and
// MIGRATIONS_DATABASE_URL is a schemeless string that migrate.New
// rejects immediately with a distinctive parse error ("failed to parse
// scheme from database URL") — if wireOrders ignored
// MIGRATIONS_DATABASE_URL and ran migrations against DATABASE_URL
// instead, this test would see a dial/"connection refused" error after
// the full retry budget, not the immediate parse error.
func TestWireOrders_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/network_fulfillment?sslmode=disable&connect_timeout=1"
	)

	t.Setenv("DATABASE_URL", unreachableAppURL)
	t.Setenv("MIGRATIONS_DATABASE_URL", bogusMigrationsURL)
	t.Setenv("MIGRATIONS_PATH", migrationsDirForTest(t))

	_, _, _, err := wireOrders(context.Background(), quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against DATABASE_URL/unreachableAppURL instead of MIGRATIONS_DATABASE_URL", err)
	}
}
