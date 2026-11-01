package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// Idempotency (fleet REST guideline section 2): every POST and PATCH route
// sits behind Idempotent. The key alone identifies the record; the
// fingerprint is method + path + sha256(body). Same key and fingerprint
// replays the stored answer with Idempotency-Replayed: true; a different
// fingerprint is 409 idempotency-key-reused; 2xx and 4xx are stored, a 5xx
// or panic releases the key so the retry re-executes.
const (
	idempotencyKeyHeader      = "Idempotency-Key"
	idempotencyReplayedHeader = "Idempotency-Replayed"
	maxIdempotencyKeyLen      = 128
	maxIdempotentBody         = 1 << 20
	pendingPoll               = 20 * time.Millisecond
	pendingWait               = 30 * time.Second
)

var (
	problemKeyRequired = problemInfo{"idempotency-key-required", "Idempotency-Key header is required"}
	problemKeyInvalid  = problemInfo{"idempotency-key-invalid", "Idempotency-Key must be 1-128 printable ASCII characters"}
	problemKeyReused   = problemInfo{"idempotency-key-reused", "Idempotency-Key was already used with a different request"}
	problemKeyPending  = problemInfo{"idempotency-key-in-progress", "A request with this Idempotency-Key is still being processed"}
)

// Idempotent wraps one route with the Idempotency-Key middleware.
func Idempotent(store ports.IdempotencyStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := checkIdempotencyKey(w, r)
			if !ok {
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, maxIdempotentBody))
			if err != nil {
				writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-request-body", "Request body could not be read"}, err.Error(), r.URL.Path)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			sum := sha256.Sum256(body)
			fp := r.Method + " " + r.URL.EscapedPath() + " " + hex.EncodeToString(sum[:])

			claim, err := claimKey(r.Context(), store, key, fp)
			switch {
			case errors.Is(err, contract.ErrIdempotencyKeyReused):
				writeProblem(w, http.StatusConflict, problemKeyReused, err.Error(), r.URL.Path)
				return
			case errors.Is(err, context.DeadlineExceeded):
				writeProblem(w, http.StatusConflict, problemKeyPending, "retry later with the same key", r.URL.Path)
				return
			case err != nil:
				writeError(w, r, err)
				return
			case claim.Replay != nil:
				w.Header().Set(idempotencyReplayedHeader, "true")
				writeStored(w, *claim.Replay)
				return
			}
			executeOwned(w, r, next, store, key)
		})
	}
}

func checkIdempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	vals := r.Header.Values(idempotencyKeyHeader)
	if len(vals) == 0 || vals[0] == "" {
		writeProblem(w, http.StatusBadRequest, problemKeyRequired, "the Idempotency-Key header is required on this operation", r.URL.Path)
		return "", false
	}
	key := vals[0]
	if len(vals) > 1 || !validIdempotencyKey(key) {
		writeProblem(w, http.StatusBadRequest, problemKeyInvalid, "the Idempotency-Key header must be 1-128 printable ASCII characters", r.URL.Path)
		return "", false
	}
	return key, true
}

func validIdempotencyKey(key string) bool {
	if len(key) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

// claimKey claims the key, waiting while a concurrent request with the same
// fingerprint still holds it, so the loser replays the winner's answer.
func claimKey(ctx context.Context, store ports.IdempotencyStore, key, fp string) (contract.IdempotencyClaim, error) {
	deadline := time.Now().Add(pendingWait)
	for {
		claim, err := store.Claim(ctx, key, fp)
		if err != nil || !claim.Pending {
			return claim, err
		}
		if time.Now().After(deadline) {
			return contract.IdempotencyClaim{}, context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return contract.IdempotencyClaim{}, ctx.Err()
		case <-time.After(pendingPoll):
		}
	}
}

func executeOwned(w http.ResponseWriter, r *http.Request, next http.Handler, store ports.IdempotencyStore, key string) {
	rec := &bufferedResponse{header: http.Header{}, status: http.StatusOK}
	defer func() {
		if p := recover(); p != nil {
			_ = store.Release(context.WithoutCancel(r.Context()), key)
			panic(p)
		}
	}()
	next.ServeHTTP(rec, r)

	stored := contract.StoredResponse{
		Status:      rec.status,
		ContentType: rec.header.Get("Content-Type"),
		Location:    rec.header.Get("Location"),
		Body:        rec.body.Bytes(),
	}
	ctx := context.WithoutCancel(r.Context())
	if rec.status >= http.StatusInternalServerError {
		_ = store.Release(ctx, key)
	} else if err := store.Complete(ctx, key, stored); err != nil {
		_ = store.Release(ctx, key)
		writeError(w, r, err)
		return
	}
	for k, v := range rec.header {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.status)
	_, _ = w.Write(stored.Body)
}

func writeStored(w http.ResponseWriter, s contract.StoredResponse) {
	if s.ContentType != "" {
		w.Header().Set("Content-Type", s.ContentType)
	}
	if s.Location != "" {
		w.Header().Set("Location", s.Location)
	}
	w.WriteHeader(s.Status)
	_, _ = w.Write(s.Body)
}

// bufferedResponse captures the handler's answer so it can be stored before
// it is sent.
type bufferedResponse struct {
	header      http.Header
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(status int) {
	if !b.wroteHeader {
		b.status, b.wroteHeader = status, true
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	b.WriteHeader(http.StatusOK)
	return b.body.Write(p)
}
