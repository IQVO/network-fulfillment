package retailnetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorder is the Prometheus gauge stand-in: dependency -> last state.
type recorder struct {
	mu     sync.Mutex
	states map[string]int64
}

func (r *recorder) SetState(dependency string, state int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = map[string]int64{}
	}
	r.states[dependency] = state
}

func (r *recorder) state(dependency string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[dependency]
}

// sleeper records the waits the client asked for instead of waiting.
type sleeper struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (s *sleeper) sleep(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	s.waits = append(s.waits, d)
	s.mu.Unlock()
	return ctx.Err()
}

func (s *sleeper) recorded() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.waits...)
}

type seen struct {
	Method, Path, RawQuery, IdemKey, ContentType string
	Body                                         string
}

// serverFunc answers call number n (1-based) and records every request.
type fake struct {
	srv   *httptest.Server
	calls atomic.Int32
	mu    sync.Mutex
	seen  []seen
}

func newFake(t *testing.T, h func(n int, w http.ResponseWriter, r *http.Request)) *fake {
	t.Helper()
	f := &fake{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(f.calls.Add(1))
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body)) // a handler may read the body too
		f.mu.Lock()
		f.seen = append(f.seen, seen{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Idempotency-Key"), r.Header.Get("Content-Type"), string(body)})
		f.mu.Unlock()
		h(n, w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fake) requests() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.seen...)
}

func testClient(t *testing.T, base string, mutate func(*clientConfig)) (*apiClient, *sleeper, *recorder) {
	t.Helper()
	sl, rec := &sleeper{}, &recorder{}
	cfg := clientConfig{
		baseURL: base, vendorID: "V-WS", logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		recorder: rec, sleep: sl.sleep, cooldown: 40 * time.Millisecond,
		now: func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := newAPIClient(cfg)
	if err != nil {
		t.Fatalf("newAPIClient: %v", err)
	}
	return c, sl, rec
}

func problem(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": slug, "title": slug, "status": status, "detail": "detail of " + slug})
}

func ok(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func TestNewAPIClient_RefusesAnythingButAnAbsoluteURL(t *testing.T) {
	for _, base := range []string{"", "vendor-connectivity:8080", "/just/a/path", "http://"} {
		if _, err := newAPIClient(clientConfig{baseURL: base, vendorID: "V-WS"}); err == nil {
			t.Errorf("base %q was accepted", base)
		}
	}
}

func TestClient_ReadsCarryTheVendorAndWritesCarryTheKey(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			ok(w, map[string]string{"transactionId": "tx-1"})
			return
		}
		ok(w, map[string]any{"purchaseOrders": []any{}})
	})
	c, _, _ := testClient(t, f.srv.URL+"/api/retail-network/vendor-connectivity", nil)

	if _, err := c.pollPage(context.Background(), time.Date(2026, 10, 24, 11, 0, 0, 0, time.UTC), ""); err != nil {
		t.Fatalf("pollPage: %v", err)
	}
	tx, err := c.postAsync(context.Background(), "submitAcknowledgement", []string{"vendor", "acknowledgements"}, "nf-ack-NR-1", []byte(`{"a":1}`))
	if err != nil || tx != "tx-1" {
		t.Fatalf("postAsync = %q, %v", tx, err)
	}

	got := f.requests()
	if len(got) != 2 {
		t.Fatalf("requests = %d, want 2", len(got))
	}
	read, write := got[0], got[1]
	if read.Path != "/api/retail-network/vendor-connectivity/vendor/purchase-orders" {
		t.Errorf("read path = %q: the base URL's own path prefix must be kept", read.Path)
	}
	for _, want := range []string{"vendorId=V-WS", "limit=200", "createdAfter=2026-10-24T11%3A00%3A00Z"} {
		if !strings.Contains(read.RawQuery, want) {
			t.Errorf("read query %q lacks %q", read.RawQuery, want)
		}
	}
	if read.IdemKey != "" || read.ContentType != "" {
		t.Errorf("a read carried Idempotency-Key %q / Content-Type %q", read.IdemKey, read.ContentType)
	}
	if write.Method != http.MethodPost || write.IdemKey != "nf-ack-NR-1" || write.ContentType != "application/json" || write.Body != `{"a":1}` {
		t.Errorf("write = %+v, want POST with the key, JSON content type and the body untouched", write)
	}
}

func TestClient_PageTokenReplacesTheFilterAndAPathElementIsEscaped(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) { ok(w, map[string]any{}) })
	c, _, _ := testClient(t, f.srv.URL, nil)

	if _, err := c.pollPage(context.Background(), time.Date(2026, 10, 24, 11, 0, 0, 0, time.UTC), "tok-2"); err != nil {
		t.Fatalf("pollPage: %v", err)
	}
	if _, err := c.getPurchaseOrder(context.Background(), "PO/1 2?x"); err != nil {
		t.Fatalf("getPurchaseOrder: %v", err)
	}
	got := f.requests()
	if q := got[0].RawQuery; !strings.Contains(q, "nextToken=tok-2") || strings.Contains(q, "createdAfter") {
		t.Errorf("page 2 query = %q, want the token and no createdAfter (the network ignores other filters)", q)
	}
	if got[1].Path != "/vendor/purchase-orders/PO/1 2?x" || !strings.Contains(got[1].RawQuery, "vendorId=V-WS") {
		t.Errorf("escaped request = %+v, want the decoded path PO/1 2?x and the vendorId query", got[1])
	}
}

func TestClient_PollAllFollowsNextTokenUntilItIsAbsent(t *testing.T) {
	pages := map[string]purchaseOrderPageDTO{
		"":    {PurchaseOrders: []purchaseOrderDTO{{PoNumber: "PO-1"}, {PoNumber: "PO-2"}}, NextToken: "t1"},
		"t1":  {PurchaseOrders: []purchaseOrderDTO{{PoNumber: "PO-3"}}, NextToken: "t2"},
		"t2":  {PurchaseOrders: []purchaseOrderDTO{{PoNumber: "PO-4"}}},
		"bad": {},
	}
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		ok(w, pages[r.URL.Query().Get("nextToken")])
	})
	c, _, _ := testClient(t, f.srv.URL, nil)

	all, err := c.pollAll(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("pollAll: %v", err)
	}
	var got []string
	for _, po := range all {
		got = append(got, po.PoNumber)
	}
	if strings.Join(got, ",") != "PO-1,PO-2,PO-3,PO-4" {
		t.Fatalf("purchase orders = %v, want all four in page order", got)
	}
	if q := f.requests()[0].RawQuery; strings.Contains(q, "createdAfter") {
		t.Errorf("a zero watermark must send no createdAfter, got %q", q)
	}
}

func TestClient_PollAllRefusesARepeatedPageToken(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		ok(w, purchaseOrderPageDTO{NextToken: "same"})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	if _, err := c.pollAll(context.Background(), time.Time{}); err == nil || !strings.Contains(err.Error(), "repeated page token") {
		t.Fatalf("err = %v, want a repeated-token error rather than an endless loop", err)
	}
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("requests = %d, want 2 (the repeat is caught on the second page)", n)
	}
}

func TestClient_429WaitsForRetryAfterThenSucceeds(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.Header().Set("Retry-After", "7")
			problem(w, http.StatusTooManyRequests, "rate-limited")
			return
		}
		ok(w, map[string]any{"nodes": []any{}})
	})
	c, sl, _ := testClient(t, f.srv.URL, nil)

	if _, err := c.listNodes(context.Background()); err != nil {
		t.Fatalf("listNodes: %v", err)
	}
	if got := sl.recorded(); len(got) != 1 || got[0] != 7*time.Second {
		t.Fatalf("waits = %v, want exactly the 7s the network asked for", got)
	}
	if f.calls.Load() != 2 {
		t.Fatalf("requests = %d, want 2", f.calls.Load())
	}
}

func TestClient_429WithAnImpossibleWaitIsReportedNotSlept(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "300")
		problem(w, http.StatusTooManyRequests, "rate-limited")
	})
	c, sl, _ := testClient(t, f.srv.URL, nil)

	_, err := c.listNodes(context.Background())
	var rl *RateLimitedError
	if !errors.As(err, &rl) || rl.RetryAfter != 300*time.Second {
		t.Fatalf("err = %v, want RateLimitedError{RetryAfter: 300s}", err)
	}
	if len(sl.recorded()) != 0 || f.calls.Load() != 1 {
		t.Fatalf("slept %v after %d calls, want neither: the caller's ticker is the retry", sl.recorded(), f.calls.Load())
	}
}

func TestClient_429LongerThanTheCallersDeadlineIsReportedNotSlept(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "20")
		problem(w, http.StatusTooManyRequests, "rate-limited")
	})
	c, sl, _ := testClient(t, f.srv.URL, func(cfg *clientConfig) {
		// The client's clock is 2026-10-24T12:00Z; the caller gives it 5 seconds.
		cfg.now = time.Now
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.listNodes(ctx)
	var rl *RateLimitedError
	if !errors.As(err, &rl) {
		t.Fatalf("err = %v, want RateLimitedError: waiting 20s would outlive a 5s deadline", err)
	}
	if len(sl.recorded()) != 0 {
		t.Fatalf("waits = %v, want none", sl.recorded())
	}
}

func TestClient_429WithoutRetryAfterBacksOffAndExhaustsTheAttempts(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		problem(w, http.StatusTooManyRequests, "rate-limited")
	})
	c, sl, _ := testClient(t, f.srv.URL, nil)

	_, err := c.listNodes(context.Background())
	var rl *RateLimitedError
	if !errors.As(err, &rl) || !strings.Contains(err.Error(), "gave up after 5 attempts") {
		t.Fatalf("err = %v, want it to give up after 5 attempts wrapping RateLimitedError", err)
	}
	if f.calls.Load() != 5 || len(sl.recorded()) != 4 {
		t.Fatalf("calls=%d waits=%v, want 5 calls and 4 waits", f.calls.Load(), sl.recorded())
	}
	for i, w := range sl.recorded() {
		if w <= 0 || w > 4*time.Second {
			t.Errorf("wait %d = %v, want a positive backoff within the 2s cap (plus jitter)", i, w)
		}
	}
}

func TestClient_5xxIsRetriedWithTheSameKeyAndBody(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n <= 2 {
			problem(w, http.StatusServiceUnavailable, "upstream-unavailable")
			return
		}
		w.WriteHeader(http.StatusAccepted)
		ok(w, map[string]string{"transactionId": "tx-9"})
	})
	c, sl, _ := testClient(t, f.srv.URL, nil)

	tx, err := c.postAsync(context.Background(), "submitShipmentConfirmation", []string{"vendor", "shipment-confirmations"}, "nf-ship-NR-1", []byte(`{"shippedAt":"x"}`))
	if err != nil || tx != "tx-9" {
		t.Fatalf("postAsync = %q, %v", tx, err)
	}
	reqs := f.requests()
	if len(reqs) != 3 || len(sl.recorded()) != 2 {
		t.Fatalf("requests=%d waits=%v, want 3 requests and 2 waits", len(reqs), sl.recorded())
	}
	for i, r := range reqs {
		if r.IdemKey != "nf-ship-NR-1" || r.Body != `{"shippedAt":"x"}` {
			t.Errorf("attempt %d = %+v: a retry must resend the identical key and body", i+1, r)
		}
	}
}

func TestClient_AllAttemptsFailingReportsTheLastError(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		problem(w, http.StatusBadGateway, "upstream-refused")
	})
	c, _, _ := testClient(t, f.srv.URL, func(cfg *clientConfig) { cfg.maxAttempts = 3 })
	_, err := c.listNodes(context.Background())
	var se *serverError
	if !errors.As(err, &se) || se.status != 502 || !strings.Contains(err.Error(), "gave up after 3 attempts") {
		t.Fatalf("err = %v, want a serverError 502 after 3 attempts", err)
	}
	if f.calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", f.calls.Load())
	}
}

func TestClient_ATimeoutIsRetried(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			time.Sleep(300 * time.Millisecond) // longer than the 50ms call timeout
		}
		ok(w, map[string]any{"nodes": []any{}})
	})
	c, sl, _ := testClient(t, f.srv.URL, func(cfg *clientConfig) { cfg.callTimeout = 50 * time.Millisecond })

	if _, err := c.listNodes(context.Background()); err != nil {
		t.Fatalf("listNodes: %v", err)
	}
	if len(sl.recorded()) != 1 {
		t.Fatalf("waits = %v, want one backoff after the timed-out attempt", sl.recorded())
	}
}

func TestClient_A4xxIsNeverRetriedAndKeepsItsProblem(t *testing.T) {
	cases := []struct {
		status int
		slug   string
	}{
		{http.StatusUnprocessableEntity, "validation-failed"},
		{http.StatusNotFound, "not-found"},
		{http.StatusConflict, "ack-window-closed"},
		{http.StatusConflict, "concurrent-update"},
	}
	for _, tc := range cases {
		t.Run(tc.slug, func(t *testing.T) {
			f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) { problem(w, tc.status, tc.slug) })
			c, sl, _ := testClient(t, f.srv.URL, nil)

			_, err := c.postAsync(context.Background(), "submitAcknowledgement", []string{"vendor", "acknowledgements"}, "k", []byte("{}"))
			var api *APIError
			if !errors.As(err, &api) || api.Status != tc.status || api.Type != tc.slug || api.Detail != "detail of "+tc.slug {
				t.Fatalf("err = %#v, want the problem document", err)
			}
			if Problem(err) != tc.slug {
				t.Fatalf("Problem = %q", Problem(err))
			}
			if f.calls.Load() != 1 || len(sl.recorded()) != 0 {
				t.Fatalf("calls=%d waits=%v: a 4xx is an answer, and the network caches it per key", f.calls.Load(), sl.recorded())
			}
		})
	}
}

func TestClient_ANonProblemErrorBodyStillNamesTheStatus(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<html>nope</html>"))
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	_, err := c.listNodes(context.Background())
	if Problem(err) != "http-403" {
		t.Fatalf("Problem = %q, want http-403", Problem(err))
	}
}

func TestClient_ADecodableStatusWithAnUndecodableBodyIsAnError(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("not json")) })
	c, _, _ := testClient(t, f.srv.URL, nil)
	if _, err := c.listNodes(context.Background()); err == nil || !strings.Contains(err.Error(), "undecodable") {
		t.Fatalf("err = %v, want an undecodable-answer error", err)
	}
}

func TestClient_AcceptedWithoutATransactionIdIsAnError(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		ok(w, map[string]string{})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	if _, err := c.postAsync(context.Background(), "submitAcknowledgement", []string{"vendor", "acknowledgements"}, "k", []byte("{}")); err == nil {
		t.Fatal("a 202 without a transactionId cannot be reconciled and must not be treated as sent")
	}
}

func TestClient_TheBreakerOpensOnDependencyFailuresRejectsWithoutCallingAndRecovers(t *testing.T) {
	var healthy atomic.Bool
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			ok(w, map[string]any{"nodes": []any{}})
			return
		}
		problem(w, http.StatusInternalServerError, "internal-error")
	})
	c, _, rec := testClient(t, f.srv.URL, func(cfg *clientConfig) { cfg.maxAttempts = 1 })

	for range 5 { // five consecutive failures trip it (resilience.ReadyToTrip)
		if _, err := c.listNodes(context.Background()); err == nil {
			t.Fatal("a 500 must be an error")
		}
	}
	before := f.calls.Load()
	if _, err := c.listNodes(context.Background()); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	if f.calls.Load() != before {
		t.Fatal("an open breaker must not reach the network")
	}
	if got := rec.state(DependencyName); got != 2 {
		t.Fatalf("circuit_breaker_state{dependency=%q} = %d, want 2 (open)", DependencyName, got)
	}

	time.Sleep(80 * time.Millisecond) // past the 40ms cooldown
	healthy.Store(true)
	if _, err := c.listNodes(context.Background()); err != nil {
		t.Fatalf("the half-open probe against a healthy network: %v", err)
	}
	if got := rec.state(DependencyName); got != 0 {
		t.Fatalf("state after a successful probe = %d, want 0 (closed)", got)
	}
}

func TestClient_AnswersThatAreNotDependencyFailuresNeverTripTheBreaker(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n%2 == 0 {
			problem(w, http.StatusConflict, "already-acknowledged")
			return
		}
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "rate-limited")
	})
	c, _, rec := testClient(t, f.srv.URL, func(cfg *clientConfig) { cfg.maxAttempts = 1 })

	for range 12 {
		_, err := c.listNodes(context.Background())
		if errors.Is(err, ErrCircuitOpen) {
			t.Fatal("429 and 4xx answers show the dependency working; they must not open the breaker")
		}
	}
	if got := rec.state(DependencyName); got != 0 {
		t.Fatalf("state = %d, want closed", got)
	}
}

func TestClient_ACallerThatGivesUpIsNotRetriedAndNotCountedAgainstTheDependency(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		ok(w, map[string]any{})
	})
	c, sl, rec := testClient(t, f.srv.URL, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()

	_, err := c.listNodes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(sl.recorded()) != 0 {
		t.Fatalf("waits = %v: a cancelled caller is not retried", sl.recorded())
	}
	if rec.state(DependencyName) != 0 {
		t.Fatal("a cancelled call must not count as a dependency failure")
	}
}

func TestClient_GetTransactionAndGetPurchaseOrderDecode(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "transactions") {
			ok(w, map[string]any{"transactionId": "tx-1", "vendorId": "V-WS", "kind": "ACKNOWLEDGEMENT", "status": "FAILURE",
				"receivedAt": "2026-10-24T12:00:00Z", "errors": []map[string]string{{"code": "ErrSimulatedFailure", "message": "simulated"}}})
			return
		}
		ok(w, map[string]any{"poNumber": "PO-1", "vendorId": "V-WS", "nodeId": "N-1", "state": "ACCEPTED",
			"lines":           []map[string]any{{"lineNo": 1, "listingId": "L-1", "quantity": 2}},
			"acknowledgement": map[string]string{"decision": "ACCEPT", "transactionId": "tx-5"}})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)

	tx, err := c.getTransaction(context.Background(), "tx-1")
	if err != nil || tx.Status != "FAILURE" || len(tx.Errors) != 1 || tx.Errors[0].Code != "ErrSimulatedFailure" {
		t.Fatalf("getTransaction = %+v, %v", tx, err)
	}
	po, err := c.getPurchaseOrder(context.Background(), "PO-1")
	if err != nil || po.State != "ACCEPTED" || po.Acknowledgement == nil || po.Acknowledgement.TransactionID != "tx-5" || po.Lines[0].ListingID != "L-1" {
		t.Fatalf("getPurchaseOrder = %+v, %v", po, err)
	}
}
