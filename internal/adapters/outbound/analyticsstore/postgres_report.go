package analyticsstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// PostgresReport is the READER implementation of report.ReportStore,
// backed by a pgxpool over the analytical database. The pool it is given
// is expected to be pinned to a read-only role /
// default_transaction_read_only=on, so a bug in the reader cannot mutate
// the read model. The reader never issues writes.
type PostgresReport struct {
	pool *pgxpool.Pool
}

// NewPostgresReport constructs a PostgresReport over pool.
func NewPostgresReport(pool *pgxpool.Pool) *PostgresReport {
	return &PostgresReport{pool: pool}
}

// Query returns the report rows matching q. From is inclusive, To is
// exclusive.
func (r *PostgresReport) Query(ctx context.Context, q report.ReportQuery) (report.AcknowledgementReport, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT day_bucket, orders_received, orders_acknowledged,
			sum_acknowledgement_latency_seconds, acknowledgement_latency_count,
			orders_rejected_untranslatable_sku, orders_rejected_domain,
			acknowledgement_deadlines_missed
		 FROM acknowledgement_rollup
		 WHERE day_bucket >= $1 AND day_bucket < $2
		 ORDER BY day_bucket`,
		q.From, q.To)
	if err != nil {
		return report.AcknowledgementReport{}, fmt.Errorf("analyticsstore: query rollup: %w", err)
	}
	defer rows.Close()

	var out report.AcknowledgementReport
	for rows.Next() {
		var (
			row    report.Row
			bucket time.Time
		)
		if err := rows.Scan(
			&bucket, &row.OrdersReceived, &row.OrdersAcknowledged,
			&row.SumAcknowledgementLatencySeconds, &row.AcknowledgementLatencyCount,
			&row.OrdersRejectedUntranslatableSKU, &row.OrdersRejectedDomain,
			&row.AcknowledgementDeadlinesMissed,
		); err != nil {
			return report.AcknowledgementReport{}, fmt.Errorf("analyticsstore: scan row: %w", err)
		}
		row.Key.DayBucket = bucket.UTC()
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return report.AcknowledgementReport{}, fmt.Errorf("analyticsstore: iterate rows: %w", err)
	}
	return out, nil
}

// FreshnessLag returns now minus the most recent event's occurred_at, i.e.
// how far the read model trails real time. Zero when the read model is
// empty or (defensively) when the latest event is future-dated.
func (r *PostgresReport) FreshnessLag(ctx context.Context) (time.Duration, error) {
	// max() over an empty table returns a single NULL row (not zero
	// rows), so scan into a nullable *time.Time and treat NULL as "read
	// model empty".
	var latest *time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT max(occurred_at) FROM analytics_processed_events`).Scan(&latest)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("analyticsstore: freshness query: %w", err)
	}
	if latest == nil || latest.IsZero() {
		return 0, nil
	}
	lag := time.Since(*latest)
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

// Compile-time assertion that PostgresReport satisfies the read port.
var _ report.ReportStore = (*PostgresReport)(nil)
