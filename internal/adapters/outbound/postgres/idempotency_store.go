package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// IdempotencyStore implements ports.IdempotencyStore over the
// idempotency_keys table (migration 0012). The key alone is the primary key,
// so concurrent requests with one key are serialized by the unique index:
// exactly one INSERT wins, the others see the claim and wait for its answer.
type IdempotencyStore struct {
	pool *pgxpool.Pool
}

// NewIdempotencyStore constructs an IdempotencyStore over pool.
func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool}
}

var _ ports.IdempotencyStore = (*IdempotencyStore)(nil)

// Claim reserves key for fingerprint, or reports the existing record. A
// record older than 24 hours is expired and the key starts afresh.
func (s *IdempotencyStore) Claim(ctx context.Context, key, fingerprint string) (contract.IdempotencyClaim, error) {
	q := querierFrom(ctx, s.pool)
	if _, err := q.Exec(ctx, `DELETE FROM idempotency_keys WHERE idem_key = $1 AND created_at < now() - interval '24 hours'`, key); err != nil {
		return contract.IdempotencyClaim{}, fmt.Errorf("postgres: expire idempotency key: %w", err)
	}
	tag, err := q.Exec(ctx, `INSERT INTO idempotency_keys (idem_key, fingerprint) VALUES ($1, $2) ON CONFLICT (idem_key) DO NOTHING`, key, fingerprint)
	if err != nil {
		return contract.IdempotencyClaim{}, fmt.Errorf("postgres: claim idempotency key: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return contract.IdempotencyClaim{Owned: true}, nil
	}

	var (
		storedFP         string
		status           *int
		contentType, loc string
		body             []byte
	)
	err = q.QueryRow(ctx, `SELECT fingerprint, status, content_type, location, body FROM idempotency_keys WHERE idem_key = $1`, key).
		Scan(&storedFP, &status, &contentType, &loc, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		// Released between our INSERT and SELECT: the caller claims again.
		return contract.IdempotencyClaim{Pending: true}, nil
	}
	if err != nil {
		return contract.IdempotencyClaim{}, fmt.Errorf("postgres: read idempotency key: %w", err)
	}
	if storedFP != fingerprint {
		return contract.IdempotencyClaim{}, contract.ErrIdempotencyKeyReused
	}
	if status == nil {
		return contract.IdempotencyClaim{Pending: true}, nil
	}
	return contract.IdempotencyClaim{Replay: &contract.StoredResponse{Status: *status, ContentType: contentType, Location: loc, Body: body}}, nil
}

// Complete stores the deterministic answer for an owned key.
func (s *IdempotencyStore) Complete(ctx context.Context, key string, resp contract.StoredResponse) error {
	_, err := querierFrom(ctx, s.pool).Exec(ctx,
		`UPDATE idempotency_keys SET status = $2, content_type = $3, location = $4, body = $5, completed_at = now() WHERE idem_key = $1`,
		key, resp.Status, resp.ContentType, resp.Location, resp.Body)
	if err != nil {
		return fmt.Errorf("postgres: complete idempotency key: %w", err)
	}
	return nil
}

// Release forgets an owned key whose execution failed (5xx or panic), so a
// retry re-executes instead of replaying a failure.
func (s *IdempotencyStore) Release(ctx context.Context, key string) error {
	_, err := querierFrom(ctx, s.pool).Exec(ctx, `DELETE FROM idempotency_keys WHERE idem_key = $1 AND status IS NULL`, key)
	if err != nil {
		return fmt.Errorf("postgres: release idempotency key: %w", err)
	}
	return nil
}
