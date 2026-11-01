package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
)

// countingHandler answers status with a body naming the call number.
func countingHandler(calls *atomic.Int64, status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"call":` + string(rune('0'+n)) + `}`))
	})
}

func idemPost(t *testing.T, h http.Handler, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set(idempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestIdempotent_FreshKeyThenByteIdenticalReplay(t *testing.T) {
	var calls atomic.Int64
	h := Idempotent(memory.NewIdempotencyStore())(countingHandler(&calls, http.StatusCreated))
	first := idemPost(t, h, "/x", "k1", `{"a":1}`)
	if first.Code != http.StatusCreated || first.Header().Get(idempotencyReplayedHeader) != "" {
		t.Fatalf("first = %d replayed=%q", first.Code, first.Header().Get(idempotencyReplayedHeader))
	}
	again := idemPost(t, h, "/x", "k1", `{"a":1}`)
	if again.Code != http.StatusCreated || again.Header().Get(idempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q", again.Code, again.Header().Get(idempotencyReplayedHeader))
	}
	if again.Body.String() != first.Body.String() || calls.Load() != 1 {
		t.Fatalf("replay body %q vs %q, calls=%d", again.Body.String(), first.Body.String(), calls.Load())
	}
}

func TestIdempotent_SameKeyDifferentRequestIs409(t *testing.T) {
	var calls atomic.Int64
	h := Idempotent(memory.NewIdempotencyStore())(countingHandler(&calls, http.StatusNoContent))
	_ = idemPost(t, h, "/x", "k1", `{"a":1}`)
	for name, tc := range map[string][2]string{"body": {"/x", `{"a":2}`}, "path": {"/y", `{"a":1}`}} {
		rec := idemPost(t, h, tc[0], "k1", tc[1])
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "/idempotency-key-reused") {
			t.Errorf("%s: %d %s, want 409 idempotency-key-reused", name, rec.Code, rec.Body.String())
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestIdempotent_MissingOrInvalidKeyIs400(t *testing.T) {
	var calls atomic.Int64
	h := Idempotent(memory.NewIdempotencyStore())(countingHandler(&calls, http.StatusNoContent))
	cases := map[string]string{"": "idempotency-key-required", strings.Repeat("k", 129): "idempotency-key-invalid", "a b": "idempotency-key-invalid"}
	for key, want := range cases {
		rec := idemPost(t, h, "/x", key, "")
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "/"+want) {
			t.Errorf("key %q: %d %s, want 400 %s", key, rec.Code, rec.Body.String(), want)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times for rejected keys", calls.Load())
	}
}

func TestIdempotent_4xxIsCachedAnd5xxIsNot(t *testing.T) {
	var calls4, calls5 atomic.Int64
	store := memory.NewIdempotencyStore()
	h4 := Idempotent(store)(countingHandler(&calls4, http.StatusConflict))
	_ = idemPost(t, h4, "/x", "k4", "")
	if rec := idemPost(t, h4, "/x", "k4", ""); rec.Code != http.StatusConflict || rec.Header().Get(idempotencyReplayedHeader) != "true" || calls4.Load() != 1 {
		t.Fatalf("4xx replay = %d replayed=%q calls=%d", rec.Code, rec.Header().Get(idempotencyReplayedHeader), calls4.Load())
	}
	h5 := Idempotent(store)(countingHandler(&calls5, http.StatusInternalServerError))
	_ = idemPost(t, h5, "/x", "k5", "")
	if rec := idemPost(t, h5, "/x", "k5", ""); rec.Header().Get(idempotencyReplayedHeader) != "" || calls5.Load() != 2 {
		t.Fatalf("5xx retry replayed=%q calls=%d, want re-executed", rec.Header().Get(idempotencyReplayedHeader), calls5.Load())
	}
}

func TestIdempotent_ConcurrentSameKeyExecutesOnce(t *testing.T) {
	var calls atomic.Int64
	h := Idempotent(memory.NewIdempotencyStore())(countingHandler(&calls, http.StatusNoContent))
	var wg sync.WaitGroup
	codes := make([]int, 16)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = idemPost(t, h, "/x", "same", `{}`).Code
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusNoContent {
			t.Fatalf("request %d answered %d", i, c)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want exactly 1", calls.Load())
	}
}
