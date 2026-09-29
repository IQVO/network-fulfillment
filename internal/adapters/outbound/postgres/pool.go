// Package postgres provides pgxpool-backed implementations of the outbound
// ports, plus a golang-migrate runner for the SQL migrations in
// /migrations.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the OLTP pool's per-process connection ceiling, shared by
// both cmd/netfulfil (the api Deployment, HPA-scalable up to
// charts/network-fulfillment values.yaml's autoscaling.maxReplicas, 4)
// and cmd/mcp (the mcp Deployment — deliberately NOT HPA-scalable, fixed
// at a manually-set replicaCount; see values.yaml's autoscaling: block
// comment for why).
//
// Sized against this fleet's shared Postgres instance's REAL
// max_connections (100, the Bitnami chart's own unmodified default —
// verified live in order-management ADR 0026, the reference this PR
// ports: warehouse-infra's terraform/postgres.tf does not override it,
// and `\l` against the deployed release shows every one of the fleet's
// ~10 backend services' OLTP and analytics databases living in that ONE
// Postgres instance, not one per service). At the OLTP Deployment's HPA
// ceiling of 4 replicas, 4 * 10 = 40 connections — the same 10 chosen by
// order-management's identically-shaped api pool, for the same reason:
// a conservative slice of the shared ceiling that leaves headroom for
// every other fleet service (and this service's own mcp/projector/
// reports processes) drawing from the same 100-connection budget. See
// docs/adr/0006-horizontal-autoscaling-and-pgxpool-tuning.md for the
// full connection-budget accounting.
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection
// on the OLTP database before Postgres cancels it. network-fulfillment's
// OLTP queries (NetworkOrderRepo's Save/FindByRef/ListUnanswered/ListAll,
// the outbox writer and relay) are all single-aggregate or small
// bounded-scan operations keyed by network_ref or the partial
// idx_network_orders_unanswered index, normally completing in low
// milliseconds. 5s mirrors order-management's OLTP pool exactly: generous
// headroom for real transient contention (a lock wait behind a
// concurrent writer) without letting one runaway or blocked query hold a
// pool slot — and therefore a bulkhead slot the HPA's replica math is
// sizing capacity around — indefinitely.
const StatementTimeout = "5s"

// NewPool opens a connection pool against databaseURL, with MaxConns and
// StatementTimeout applied to every connection.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return NewPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout)
}

// NewPoolWithLimits is NewPool's shared implementation, taking maxConns
// and statementTimeout explicitly so an integration test can drive a
// much shorter timeout directly -- proving the AfterConnect hook really
// applies the setting to every new connection, by triggering an actual
// cancellation -- without waiting out the real production value.
// Production callers should use NewPool.
func NewPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, config)
}
