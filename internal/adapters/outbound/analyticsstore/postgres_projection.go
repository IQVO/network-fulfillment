package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// PostgresProjection is the WRITER implementation of report.ProjectionStore,
// backed by a pgxpool over the analytical database. Every Apply* runs in a
// transaction that first claims the event id in analytics_processed_events
// (ON CONFLICT DO NOTHING); it only mutates the rollup when the claim is
// new, making each apply idempotent per eventId under Kafka's
// at-least-once delivery. It is the only writer of the analytical
// database.
type PostgresProjection struct {
	pool *pgxpool.Pool
}

// NewPostgresProjection constructs a PostgresProjection over pool.
func NewPostgresProjection(pool *pgxpool.Pool) *PostgresProjection {
	return &PostgresProjection{pool: pool}
}

// claim inserts eventId into analytics_processed_events, returning true iff
// this call newly recorded it (so the caller should apply the effect). It
// runs inside tx so the claim and the effect commit atomically.
func claim(ctx context.Context, tx pgx.Tx, eventId string, occurredAt time.Time) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO analytics_processed_events (event_id, occurred_at)
		 VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`,
		eventId, occurredAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// inTx runs fn in a transaction, committing on success and rolling back on
// error.
func (p *PostgresProjection) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// rollupDelta is the set of counter increments a single event contributes
// to a day-bucket row.
type rollupDelta struct {
	ordersReceived                   int
	ordersAcknowledged               int
	sumAcknowledgementLatencySeconds float64
	acknowledgementLatencyCount      int
	ordersRejectedUntranslatableSKU  int
	ordersRejectedDomain             int
	acknowledgementDeadlinesMissed   int
}

// apply claims eventId and, when the claim is new, upserts delta into the
// day_bucket row. It is the shared body of every Apply* method.
func (p *PostgresProjection) apply(ctx context.Context, eventId string, at time.Time, delta rollupDelta) error {
	return p.inTx(ctx, func(tx pgx.Tx) error {
		isNew, err := claim(ctx, tx, eventId, at)
		if err != nil {
			return fmt.Errorf("analyticsstore: claim event: %w", err)
		}
		if !isNew {
			return nil
		}
		return upsertRollup(ctx, tx, at, delta)
	})
}

// ApplyNetworkOrderReceived records an order intake. Idempotent on eventId.
func (p *PostgresProjection) ApplyNetworkOrderReceived(ctx context.Context, eventId string, at time.Time) error {
	return p.apply(ctx, eventId, at, rollupDelta{ordersReceived: 1})
}

// ApplyNetworkOrderAcknowledged records a commitment. Idempotent on
// eventId.
func (p *PostgresProjection) ApplyNetworkOrderAcknowledged(ctx context.Context, eventId string, at time.Time, latencySeconds float64) error {
	return p.apply(ctx, eventId, at, rollupDelta{
		ordersAcknowledged:               1,
		sumAcknowledgementLatencySeconds: latencySeconds,
		acknowledgementLatencyCount:      1,
	})
}

// ApplyNetworkOrderRejected records a refusal bucketed by reason.
// Idempotent on eventId. A reason outside the known set claims the event
// (so it is never retried forever) but contributes no counter.
func (p *PostgresProjection) ApplyNetworkOrderRejected(ctx context.Context, eventId string, at time.Time, reason string) error {
	delta := rollupDelta{}
	switch reason {
	case "UNTRANSLATABLE_SKU":
		delta.ordersRejectedUntranslatableSKU = 1
	case "INFEASIBLE_DEADLINE":
		delta.ordersRejectedDomain = 1
	case "ACKNOWLEDGEMENT_DEADLINE_MISSED":
		delta.acknowledgementDeadlinesMissed = 1
	}
	return p.apply(ctx, eventId, at, delta)
}

// upsertRollup adds delta into the day_bucket row, inserting it if absent.
// day_bucket is derived by truncating at to the UTC day.
func upsertRollup(ctx context.Context, tx pgx.Tx, at time.Time, delta rollupDelta) error {
	bucket := at.UTC().Truncate(24 * time.Hour)
	_, err := tx.Exec(ctx,
		`INSERT INTO acknowledgement_rollup (
			day_bucket, orders_received, orders_acknowledged,
			sum_acknowledgement_latency_seconds, acknowledgement_latency_count,
			orders_rejected_untranslatable_sku, orders_rejected_domain,
			acknowledgement_deadlines_missed)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (day_bucket) DO UPDATE SET
			orders_received                     = acknowledgement_rollup.orders_received + EXCLUDED.orders_received,
			orders_acknowledged                 = acknowledgement_rollup.orders_acknowledged + EXCLUDED.orders_acknowledged,
			sum_acknowledgement_latency_seconds  = acknowledgement_rollup.sum_acknowledgement_latency_seconds + EXCLUDED.sum_acknowledgement_latency_seconds,
			acknowledgement_latency_count        = acknowledgement_rollup.acknowledgement_latency_count + EXCLUDED.acknowledgement_latency_count,
			orders_rejected_untranslatable_sku   = acknowledgement_rollup.orders_rejected_untranslatable_sku + EXCLUDED.orders_rejected_untranslatable_sku,
			orders_rejected_domain               = acknowledgement_rollup.orders_rejected_domain + EXCLUDED.orders_rejected_domain,
			acknowledgement_deadlines_missed     = acknowledgement_rollup.acknowledgement_deadlines_missed + EXCLUDED.acknowledgement_deadlines_missed`,
		bucket, delta.ordersReceived, delta.ordersAcknowledged,
		delta.sumAcknowledgementLatencySeconds, delta.acknowledgementLatencyCount,
		delta.ordersRejectedUntranslatableSKU, delta.ordersRejectedDomain,
		delta.acknowledgementDeadlinesMissed)
	if err != nil {
		return fmt.Errorf("analyticsstore: upsert rollup: %w", err)
	}
	return nil
}

// Compile-time assertion that PostgresProjection satisfies the write port.
var _ report.ProjectionStore = (*PostgresProjection)(nil)
