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
