package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestMigrationsDatabaseURLFallback proves the env lookup run() applies before
// calling buildOrders: getenv("MIGRATIONS_DATABASE_URL", databaseURL) returns
// the variable's own value when set and DATABASE_URL otherwise (ADR 0007, the
// PgBouncer / pg_advisory_lock split every migrating binary here honours).
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/network_fulfillment?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		t.Setenv("MIGRATIONS_DATABASE_URL", "")
		os.Unsetenv("MIGRATIONS_DATABASE_URL")
		if got := getenv("MIGRATIONS_DATABASE_URL", databaseURL); got != databaseURL {
			t.Fatalf("fallback = %q, want the DATABASE_URL value %q", got, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/network_fulfillment?sslmode=disable"
		t.Setenv("MIGRATIONS_DATABASE_URL", direct)
		if got := getenv("MIGRATIONS_DATABASE_URL", databaseURL); got != direct {
			t.Fatalf("got %q, want the direct MIGRATIONS_DATABASE_URL value %q", got, direct)
		}
	})
}

// TestBuildOrders_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations proves
// buildOrders itself threads migrationsDatabaseURL into the migration step and
// databaseURL into the pool. databaseURL points at an address nothing listens
// on (opening the pool, which happens AFTER migrations succeed, would fail
// differently) and migrationsDatabaseURL is a schemeless string migrate rejects
// at once with a distinctive parse error: ignoring migrationsDatabaseURL would
// surface a dial error instead.
func TestBuildOrders_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/network_fulfillment?sslmode=disable&connect_timeout=1"
	)
	_, _, _, err := buildOrders(context.Background(), unreachableAppURL, bogusMigrationsURL, "../../migrations", quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v: want the bogus-URL parse error from migrate.New; a dial error would mean migrations ran against databaseURL", err)
	}
}
