//go:build integration

package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
)

// These tests pin the invariant the savepoint fix guards in sibling repos
// (slotting-optimization 45e3714): a mapped 4xx produced after a statement
// error is still recorded and returned (never a 500 from 25P02), and a 2xx
// over a failed write is never returned or cached.
//
// This repo's middleware has NO shared transaction: Claim/Complete/Release run
// on the pool and auto-commit, and every use case opens its own UnitOfWork
// transaction. A handler-aborted transaction therefore cannot poison the
// outcome write, so no SAVEPOINT is needed; the tests keep it that way.

// abortOwnTx mimics a use case that runs a statement which fails on the
// server (a unique violation, as on a lost race) inside its own transaction.
func abortOwnTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (idem_key, fingerprint) VALUES ('poison-dup', 'f')`); err != nil {
			return // the second insert aborts the transaction (23505)
		}
	}
	t.Fatal("the duplicate insert did not abort the transaction")
}

func abortedKeyed(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/network-orders/po-x/shipment-confirmation", nil)
	req.Header.Set("Idempotency-Key", key)
	return req
}

func TestIdempotency_A409AfterAnAbortedTransactionIsRecordedAndReplayed(t *testing.T) {
	api := newRESTAPI(t, true)
	var calls atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		abortOwnTx(t, r.Context(), api.pool)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":409}`))
	})
	mw := inboundhttp.Idempotent(postgres.NewIdempotencyStore(api.pool))(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, abortedKeyed("k-aborted-409"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}
	var status int
	if err := api.pool.QueryRow(context.Background(), `SELECT status FROM idempotency_keys WHERE idem_key = 'k-aborted-409'`).Scan(&status); err != nil || status != http.StatusConflict {
		t.Fatalf("409 outcome not committed: status=%d err=%v", status, err)
	}
	replay := httptest.NewRecorder()
	mw.ServeHTTP(replay, abortedKeyed("k-aborted-409"))
	if replay.Code != http.StatusConflict || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay = %d replayed=%q", replay.Code, replay.Header().Get("Idempotency-Replayed"))
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1 (replay must not re-run it)", calls.Load())
	}
}

func TestIdempotency_AHandlerFailureAfterAnAbortedTransactionIsNotCachedAndRetries(t *testing.T) {
	api := newRESTAPI(t, true)
	var calls atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			abortOwnTx(t, r.Context(), api.pool)
			w.WriteHeader(http.StatusInternalServerError) // the use case surfaces the failed write
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mw := inboundhttp.Idempotent(postgres.NewIdempotencyStore(api.pool))(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, abortedKeyed("k-aborted-5xx"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d, want 500", first.Code)
	}
	var n int
	if err := api.pool.QueryRow(context.Background(), `SELECT count(*) FROM idempotency_keys WHERE idem_key = 'k-aborted-5xx'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("failed attempt left %d rows (err=%v): the key could not retry", n, err)
	}
	second := httptest.NewRecorder()
	mw.ServeHTTP(second, abortedKeyed("k-aborted-5xx"))
	if second.Code != http.StatusNoContent || calls.Load() != 2 {
		t.Fatalf("retry = %d after %d calls, want 204 after 2", second.Code, calls.Load())
	}
}
