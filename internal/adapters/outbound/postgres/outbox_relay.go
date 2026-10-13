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
//
// With WithMaxAttempts(n), n > 0 (ADR 0021; cmd/netfulfil passes
// OUTBOX_MAX_ATTEMPTS, default 20), a row that has failed n times is
// excluded from claims and logged at ERROR: later rows, including rows with
// the same key, then overtake it. That trade is deliberate (one poison row
// must not stop the whole relay forever) and the excluded row stays in
// outbox_events (attempts, last_error) until an operator re-arms it.
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        Sink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// DefaultOutboxMaxAttempts is the attempt cap a composition root uses when
// OUTBOX_MAX_ATTEMPTS is unset (fleet convention, ADR 0021). The relay's own
// zero value stays 0 (unlimited) so the option is always an explicit choice.
const DefaultOutboxMaxAttempts = 20

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

// WithMaxAttempts overrides the default of 0 (unlimited retries): a row
// that has failed this many times is excluded from future claims (still
// visible in outbox_events for manual inspection) instead of retried
// indefinitely, and the failure that reaches the cap is logged at ERROR
// with the row's id, topic, type and key (ADR 0021). 0 keeps retrying
// without bound. Every cmd/* that builds a relay must pass it (architecture
// fitness test TestOutboxRelaysPassAttemptCap).
func WithMaxAttempts(n int) RelayOption {
	return func(r *OutboxRelay) { r.maxAttempts = n }
}

// MaxAttempts reports the configured attempt cap (0 = unlimited).
func (r *OutboxRelay) MaxAttempts() int { return r.maxAttempts }

// sendFailure is the error RelayOnce returns when the sink rejected a row
// and the attempt was recorded. The relay has already logged it (WARN, or
// ERROR when the row reached the attempt cap), so Run does not log it again.
type sendFailure struct{ err error }

func (e *sendFailure) Error() string { return e.err.Error() }
func (e *sendFailure) Unwrap() error { return e.err }

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
// unpublished, so nothing is lost. A send failure was already logged by
// the pass itself (WARN below the attempt cap, ERROR when the row reaches
// it), so only other failures (database errors) are logged here.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		var sf *sendFailure
		if err != nil && !errors.Is(err, context.Canceled) && !errors.As(err, &sf) {
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

	batch, err := r.claimOutboxRows(ctx, tx)
	if err != nil {
		return 0, err
	}
	return r.publishBatch(ctx, tx, batch)
}

// claimOutboxRows selects up to batchSize unpublished rows under a row
// lock, in id order, and decodes them for sending. With an attempt cap,
// rows that have already failed maxAttempts times are skipped (ADR 0021).
func (r *OutboxRelay) claimOutboxRows(ctx context.Context, tx pgx.Tx) ([]pendingRow, error) {
	var attemptsFilter string
	args := []any{r.batchSize}
	if r.maxAttempts > 0 {
		attemptsFilter = "AND attempts < $2"
		args = append(args, r.maxAttempts)
	}
	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL `+attemptsFilter+`
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	var batch []pendingRow
	for rows.Next() {
		var (
			p            pendingRow
			headersBytes []byte
		)
		if err := rows.Scan(&p.id, &p.encoded.Topic, &p.encoded.EventType, &p.encoded.Key, &p.encoded.Value, &headersBytes); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		headers, err := unmarshalHeaders(headersBytes)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: unmarshal outbox row %d headers: %w", p.id, err)
		}
		p.encoded.Headers = headers
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read outbox rows: %w", err)
	}
	return batch, nil
}

// publishBatch sends each claimed row in order and marks it published. On
// the first Send failure the pass stops (preserving per-aggregate
// ordering — a later event must not overtake a failed earlier one),
// records the error on that row, and returns it; rows already sent in
// this pass stay marked published.
func (r *OutboxRelay) publishBatch(ctx context.Context, tx pgx.Tx, batch []pendingRow) (int, error) {
	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.encoded); err != nil {
			return published, r.recordSendFailure(ctx, tx, p, err)
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

// recordSendFailure records the send error on the failed row and commits
// the pass so earlier rows keep their published marks, wrapping every
// bookkeeping failure around the original send error. Only once the attempt
// is committed is the failure logged, so an ERROR saying a row reached the
// attempt cap is never emitted for an attempt that was rolled back.
func (r *OutboxRelay) recordSendFailure(ctx context.Context, tx pgx.Tx, p pendingRow, err error) error {
	wrap := func(e error) error {
		return fmt.Errorf("outbox relay: send %s (row %d, topic %s): %w", p.encoded.EventType, p.id, p.encoded.Topic, e)
	}
	var attempts int
	if uerr := tx.QueryRow(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1
		RETURNING attempts
	`, p.id, err.Error()).Scan(&attempts); uerr != nil {
		return wrap(errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr)))
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		return wrap(errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr)))
	}
	r.logSendFailure(ctx, p, attempts, err)
	return &sendFailure{err: wrap(err)}
}

// logSendFailure logs a failed send at WARN while the row will be retried,
// and at ERROR once the row has reached the attempt cap (ADR 0021): from
// then on claimOutboxRows excludes it, later rows overtake it (including
// rows with the same key) and it stays in outbox_events until an operator
// re-arms it (`UPDATE outbox_events SET attempts = 0 WHERE id = ...`).
func (r *OutboxRelay) logSendFailure(ctx context.Context, p pendingRow, attempts int, err error) {
	if r.maxAttempts > 0 && attempts >= r.maxAttempts {
		r.logger.ErrorContext(ctx, "outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause",
			"outbox_id", p.id, "topic", p.encoded.Topic, "event_type", p.encoded.EventType,
			"key", string(p.encoded.Key), "attempts", attempts, "max_attempts", r.maxAttempts, "error", err)
		return
	}
	r.logger.WarnContext(ctx, "outbox relay: send failed, stopping this pass at the failed row",
		"outbox_id", p.id, "topic", p.encoded.Topic, "event_type", p.encoded.EventType, "attempts", attempts, "error", err)
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
