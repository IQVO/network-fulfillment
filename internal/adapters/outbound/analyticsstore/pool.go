package analyticsstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the analytics writer's (cmd/netfulfil-projector) per-process
// connection ceiling. The projector is fixed at ONE replica — its Kafka
// consumer group id is minted fresh per process instance
// (inboundkafka.NewUniqueConsumerGroup(AnalyticsConsumerGroupPrefix)),
// the deliberate exception to the fleet's stable-shared-group pattern:
// running more than one projector at a time would let a second instance
// join with its own brand-new group and independently replay the WHOLE
// topic from the earliest offset, double-projecting every historical
// event rather than sharing partitions the way a stable shared group
// would. A small, flat pool is enough for its workload: single-row
// ON CONFLICT upserts into the acknowledgement-report projection, one
// event at a time.
const MaxConns = 5

// ReportsMaxConns is the analytics reader's (cmd/netfulfil-reports)
// per-process connection ceiling. Unlike the projector, reports IS
// HPA-scalable (stateless REST reads, chart's autoscaling.reports block,
// min 1 / max 3): at that HPA ceiling, 3 * 5 = 15 connections against
// the analytical database — comfortably inside the shared Postgres
// instance's ceiling alongside the OLTP path's 40. See
// docs/adr/0006-horizontal-autoscaling-and-pgxpool-tuning.md for the
// full connection-budget accounting across this service's four
// processes on the ONE shared Postgres instance.
const ReportsMaxConns = 5

// StatementTimeout bounds the analytics WRITER's (projector) queries.
// Slightly more generous than the OLTP side's 5s: a Kafka consumer
// replaying the analytics topic from the earliest offset after a
// redeploy issues its upserts in a tight loop, and a transient lock wait
// here should not need to be as tight as an interactive OLTP request —
// but it must still not be unbounded, or one poisoned/oversized batch
// could wedge the single projector instance's only connection pool
// indefinitely (with no replica to fail over to, that would stall the
// ENTIRE analytics pipeline, not just one of several api pods).
const StatementTimeout = "10s"

// ReportsStatementTimeout bounds the analytics READER's (reports)
// queries. Its acknowledgement report aggregates counts across the
// analytical database's projection tables — wider-shaped than the OLTP
// side's always-single-aggregate-by-ref reads — so it gets more
// headroom than StatementTimeout, but still a hard ceiling: a caller
// request against a growing analytical table must not be able to hold a
// reports connection forever.
const ReportsStatementTimeout = "15s"

// NewPool builds a pgxpool over the analytical database at databaseURL,
// mirroring the OLTP postgres.NewPool: MaxConns and StatementTimeout are
// applied to every connection. It is used by the writer
// (cmd/netfulfil-projector).
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout, false)
}

// NewReadOnlyPool builds a pgxpool over the analytical database in which
// every connection is pinned to a read-only transaction default
// (default_transaction_read_only=on), with ReportsMaxConns and
// ReportsStatementTimeout applied. The reader process
// (cmd/netfulfil-reports) uses this so a bug there cannot mutate the
// read model even if the database role itself is not read-only —
// defence in depth on top of the read-only ANALYTICS_DATABASE_URL role.
func NewReadOnlyPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPoolWithLimits(ctx, databaseURL, ReportsMaxConns, ReportsStatementTimeout, true)
}

// newPoolWithLimits is the shared implementation behind NewPool/
// NewReadOnlyPool, parameterised so a test can drive a much shorter
// statementTimeout directly (proving the AfterConnect hook actually
// applies the setting to every new connection, by triggering a real
// cancellation) without waiting out the production value.
func newPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string, readOnly bool) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	if readOnly {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
