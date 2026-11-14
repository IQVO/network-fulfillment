// Package pgtest boots ONE real Postgres through testcontainers per test
// binary and hands every test its own database, cloned from a template that
// already has this repository's migrations applied. It never reads
// DATABASE_URL and never skips: a missing Docker is a test failure, the fleet
// rule internal/architecture's TestPostgresIntegrationTestsUseTestcontainers
// encodes for every Postgres-touching integration test.
//
// It is this repo's counterpart of customer-ordering's internal/testing/pgtest
// — the fleet's shared shape; internal/adapters/inbound/mcp's
// streamable_http_integration_test.go runs the same template-database scheme
// inline per package — adapted to a repo whose migrations are .sql files
// under /migrations applied by postgres.RunMigrations(url, path) rather than
// embedded ones.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
)

const (
	user         = "network_fulfillment_test"
	password     = "network_fulfillment_test"
	adminDB      = "postgres"
	templateName = "network_fulfillment_template"
)

var (
	once      sync.Once
	container *tcpostgres.PostgresContainer
	errStart  error
	seq       atomic.Int64
)

// start boots the shared container and builds the migrated template database
// every test database is cloned from.
func start() {
	ctx := context.Background()
	container, errStart = tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(adminDB),
		tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password),
		tcpostgres.BasicWaitStrategies(),
	)
	if errStart != nil {
		return
	}
	errStart = buildTemplate(ctx)
}

// buildTemplate creates the template database and applies the migrations to
// it twice: the binaries under test run them again on every boot, and that
// second run must be a no-op (the same idempotency proof customer-ordering's
// pgtest makes).
func buildTemplate(ctx context.Context) error {
	if err := adminExec(ctx, `CREATE DATABASE `+templateName); err != nil {
		return err
	}
	dsn, err := databaseURL(ctx, templateName)
	if err != nil {
		return err
	}
	dir, err := migrationsDir()
	if err != nil {
		return err
	}
	for range 2 {
		if err := postgres.RunMigrations(dsn, dir); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	return nil
}

// migrationsDir resolves the repository's migrations directory relative to
// THIS file, so the helper works regardless of the directory go test was
// invoked from (the same technique the postgres package's own
// migrationsDir helper uses).
func migrationsDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to resolve the pgtest source file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations"), nil
}

// databaseURL rewrites the container's connection string to the named
// database, keeping the credentials and parameters the container advertises.
func databaseURL(ctx context.Context, db string) (string, error) {
	base, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", err
	}
	return withDB(base, db), nil
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// adminExec runs one statement against the container's admin database, the
// channel every CREATE DATABASE goes through.
func adminExec(ctx context.Context, sql string) error {
	dsn, err := databaseURL(ctx, adminDB)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return err
}

// URL returns the DSN of a FRESH database (migrations already applied) on the
// shared container. Any failure aborts the test: this helper never hands back
// a database that was not migrated, and never skips.
func URL(t *testing.T) string {
	return namedFromTemplate(t, "t")
}

// EmptyURL returns the DSN of a FRESH, EMPTY database on the shared
// container — no migrations applied, not even the operational template's.
// It is for the binaries that own their schema themselves (the analytics
// projector runs the analytics migrations on start): handing those a
// pre-migrated database would hide the migration step the binary must
// prove it performs. The database is created WITHOUT a TEMPLATE clause,
// so it inherits the container's pristine template1 and nothing else.
func EmptyURL(t *testing.T) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("t_empty_%d", seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn, err := databaseURL(ctx, name)
	if err != nil {
		t.Fatalf("database url: %v", err)
	}
	return dsn
}

// namedFromTemplate clones the migrated template database under a fresh
// unique name and returns its DSN.
func namedFromTemplate(t *testing.T, prefix string) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("%s_%d", prefix, seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name+` TEMPLATE `+templateName); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn, err := databaseURL(ctx, name)
	if err != nil {
		t.Fatalf("database url: %v", err)
	}
	return dsn
}

// Shutdown terminates the shared container; call it from TestMain once every
// test has run.
func Shutdown() {
	if container != nil {
		_ = testcontainers.TerminateContainer(container)
	}
}
