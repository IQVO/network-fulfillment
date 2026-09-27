package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
)

// Sink is where OutboxRelay forwards drained events — in production the
// Kafka Publisher's/AnalyticsPublisher's Send; in tests a recorder.
type Sink interface {
	Send(ctx context.Context, msgs ...outboundkafka.Encoded) error
}

// OutboxRelay drains unpublished outbox_events rows onto a Sink, oldest
// first, marking each row published as it goes.
//
// Delivery is at-least-once: a crash between a successful Send and the
// row's UPDATE republishes that event on the next pass. Ordering per
// aggregate is preserved because rows are drained in id order within one
// relay pass and FOR UPDATE SKIP LOCKED keeps two relays (a rolling
// deploy's overlapping old and new pod) from claiming the same row.
type OutboxRelay struct {
	pool      *pgxpool.Pool
	sink      Sink
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

// RelayOption customises an OutboxRelay.
type RelayOption func(*OutboxRelay)

// WithInterval sets how long the relay sleeps between passes when the
// last pass found nothing to publish. Default 1s.
func WithInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize caps how many rows one pass claims. Default 100.
func WithBatchSize(n int) RelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// NewOutboxRelay constructs a relay draining pool into sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, logger *slog.Logger, opts ...RelayOption) *OutboxRelay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &OutboxRelay{pool: pool, sink: sink, logger: logger, interval: time.Second, batchSize: 100}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox until ctx is cancelled. A pass that publishes a
// full batch is followed immediately by another pass (there is probably
// more waiting); an empty pass sleeps for the configured interval. A
// failing pass is logged and retried after the interval — the rows stay
// unpublished, so nothing is lost.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// pendingRow is one claimed outbox_events row awaiting relay.
type pendingRow struct {
	id      int64
	encoded outboundkafka.Encoded
}

// RelayOnce performs a single pass: claim up to batchSize unpublished rows
// under a row lock, send each in order, and mark it published. It returns
// how many rows were published. On the first Send failure the pass stops
// (preserving per-aggregate ordering — a later event must not overtake a
// failed earlier one), records the error on that row, and returns it;
// rows already sent in this pass stay marked published.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin relay pass: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	var batch []pendingRow
	for rows.Next() {
		var (
			p            pendingRow
			headersBytes []byte
		)
		if err := rows.Scan(&p.id, &p.encoded.Topic, &p.encoded.EventType, &p.encoded.Key, &p.encoded.Value, &headersBytes); err != nil {
			rows.Close()
			return 0, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		headers, err := unmarshalHeaders(headersBytes)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("postgres: unmarshal outbox row %d headers: %w", p.id, err)
		}
		p.encoded.Headers = headers
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: read outbox rows: %w", err)
	}

	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.encoded); err != nil {
			if _, uerr := tx.Exec(ctx, `
				UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1
			`, p.id, err.Error()); uerr != nil {
				err = errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr))
			}
			if cerr := tx.Commit(ctx); cerr != nil {
				err = errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr))
			}
			return published, fmt.Errorf("outbox relay: send %s (row %d, topic %s): %w", p.encoded.EventType, p.id, p.encoded.Topic, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1
		`, p.id); err != nil {
			return published, fmt.Errorf("postgres: mark outbox row %d published: %w", p.id, err)
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return published, fmt.Errorf("postgres: commit relay pass: %w", err)
	}
	if published > 0 {
		r.logger.DebugContext(ctx, "outbox relay published events", "count", published)
	}
	return published, nil
}

// unmarshalHeaders is the inverse of marshalHeaders.
func unmarshalHeaders(raw []byte) ([]kafkago.Header, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var decoded []outboxHeader
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	out := make([]kafkago.Header, len(decoded))
	for i, h := range decoded {
		out[i] = kafkago.Header{Key: h.Key, Value: h.Value}
	}
	return out, nil
}
