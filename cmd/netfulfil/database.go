package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// wireOrders chooses the NetworkOrderRepo implementation.
//
// With no DATABASE_URL the service runs on the in-memory repo, which is
// what keeps a local run and the unit suite free of infrastructure (ADR
// 0001 §4). With one set, it MUST reach Postgres: returning an error
// rather than falling back is the whole point, because the fallback is
// silent and its cost is the acknowledgement deadlines this context owes
// the network.
//
// Migrations run here, at startup, matching every sibling service in this
// fleet — the alternative is a separate job that can be forgotten, and a
// schema that lags the binary is how a context starts answering wrongly
// rather than not at all.
//
// The returned pool is nil exactly when the repo is the in-memory one;
// main uses that alone to decide whether a UnitOfWork/outbox is wired,
// so the two can never disagree about which mode is active.
func wireOrders(ctx context.Context, logger *slog.Logger) (ports.NetworkOrderRepo, *pgxpool.Pool, func(), error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Info("order repository wired", "backend", "memory")
		return memory.NewNetworkOrderRepo(), nil, func() {}, nil
	}

	// Retried, because in this fleet EVERY injected pod's first outbound
	// TCP dial is reset ~10s after the app starts (Istio native sidecars;
	// `holdApplicationUntilProxyStarts` is a no-op for them). A single
	// attempt turns that known, transient condition into CrashLoopBackOff:
	// observed live — migrations failed with "read: connection reset by
	// peer", the process exited, and the pod never got far enough to serve
	// its own health probe.
	//
	// The retry is NOT a weakening of the fail-closed rule. After the
	// budget is exhausted this still refuses to boot; it just stops
	// treating a sidecar warm-up as a permanent failure.
	migrationsDatabaseURL := migrationsURL(databaseURL)
	if err := retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath())
	}); err != nil {
		return nil, nil, nil, err
	}

	pool, err := openPool(ctx, logger, databaseURL)
	if err != nil {
		return nil, nil, nil, err
	}

	logger.Info("order repository wired", "backend", "postgres")
	return postgres.NewNetworkOrderRepo(pool), pool, pool.Close, nil
}

// migrationsURL picks the connection string used ONLY for the
// golang-migrate step in wireOrders.
//
// MIGRATIONS_DATABASE_URL, when set, is a DIRECT (non-pooled,
// session-mode) Postgres connection string used ONLY for the
// golang-migrate step — the pgxpool opened right after it
// (postgres.NewPool(ctx, databaseURL), used for every
// request this process serves) always uses databaseURL/DATABASE_URL,
// unchanged. golang-migrate's postgres driver takes a session-scoped
// `SELECT pg_advisory_lock($1)` to serialize concurrent migration
// runs across replicas starting at the same time, which is
// incompatible with PgBouncer's transaction-pooling mode (this
// fleet's pool_mode for every OLTP DATABASE_URL, warehouse-infra PR
// #43): each statement in one logical client session can land on a
// different physical backend connection under transaction pooling,
// so the advisory lock never behaves as a real mutex. Two or more
// replicas starting concurrently (an HPA scale-out, or an ordinary
// rolling deploy with replicas>1) then crash-loop with `pq: unnamed
// prepared statement does not exist` / `pq: canceling statement due
// to statement timeout` for ~1-2 minutes until one wins the race.
// See docs/adr/0007-migrations-direct-postgres-connection.md (mirrors
// order-management's ADR-0029) for the full incident and fix.
//
// Falls back to databaseURL when MIGRATIONS_DATABASE_URL is unset,
// which is every environment that doesn't provision the split (local
// dev, CI integration tests, a cluster whose Terraform predates this
// fix) — byte-identical to wireOrders' behavior before this change in
// that case.
func migrationsURL(databaseURL string) string {
	if v := os.Getenv("MIGRATIONS_DATABASE_URL"); v != "" {
		return v
	}
	return databaseURL
}

// openPool opens the request-serving pgxpool and verifies it with a
// retried ping.
func openPool(ctx context.Context, logger *slog.Logger, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first real failure would surface inside a
	// request rather than at boot — turning a misconfigured deployment
	// into an intermittent 500 instead of a refusal to start.
	if err := retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
