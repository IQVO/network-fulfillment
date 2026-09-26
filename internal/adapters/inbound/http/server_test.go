package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var errBoom = errors.New("boom")

func now() time.Time { return time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC) }

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type fakeStats struct{ s poller.Stats }

func (f fakeStats) Stats() poller.Stats { return f.s }

// failingRepo makes the 500 path reachable, so the error mapping is
// exercised rather than assumed.
type failingRepo struct{ err error }

func (r failingRepo) Save(context.Context, *networkorder.NetworkOrder) error { return r.err }
func (r failingRepo) FindByRef(context.Context, shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	return nil, r.err
}
func (r failingRepo) ListUnanswered(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, r.err
}
func (r failingRepo) ListAll(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, r.err
}

type testEnv struct {
	orders  *memory.NetworkOrderRepo
	handler http.Handler
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	orders := memory.NewNetworkOrderRepo()
	srv := &inboundhttp.Server{
		Orders:      orders,
		Poller:      fakeStats{},
		Clock:       fixedClock{t: now()},
		NetworkMode: "stub",
	}
	return &testEnv{orders: orders, handler: srv.Routes()}
}

func (e *testEnv) do(t *testing.T, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func mustLine(t *testing.T, ref, product, sku string, qty int) networkorder.Line {
	t.Helper()
	l, err := networkorder.NewLine(shared.NetworkLineRef(ref), shared.NetworkProductId(product), shared.SKU(sku), qty)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return l
}

// seed stores an order received `receivedAgo` before the fixed clock.
func (e *testEnv) seed(t *testing.T, ref string, receivedAgo time.Duration, lines ...networkorder.Line) *networkorder.NetworkOrder {
	t.Helper()
	receivedAt := now().Add(-receivedAgo)
	if len(lines) == 0 {
		lines = []networkorder.Line{mustLine(t, "1", "ASIN-1", "sku-1", 2)}
	}
	o, err := networkorder.Receive(shared.NetworkRef(ref), "site-1", now().Add(48*time.Hour), lines, receivedAt)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := e.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return o
}

type problemBody struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

func assertProblem(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) problemBody {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, wantStatus, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var body problemBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec.Body.String())
	}
	if !strings.HasPrefix(body.Type, "https://errors.network-fulfillment.warehouse-systems.dev/") {
		t.Fatalf("problem type %q is outside this service's namespace", body.Type)
	}
	if body.Status != wantStatus {
		t.Fatalf("problem.status = %d, want %d", body.Status, wantStatus)
	}
	return body
}

func TestHealthz(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodGet, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestGetNetworkOrder_ReturnsBothVocabularies(t *testing.T) {
	e := newTestEnv(t)
	e.seed(t, "po-1", time.Hour,
		mustLine(t, "1", "ASIN-AAA", "sku-aaa", 3),
		mustLine(t, "2", "ASIN-BBB", "sku-bbb", 1),
	)

	rec := e.do(t, http.MethodGet, "/network-orders/po-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	var got struct {
		NetworkRef             string  `json:"networkRef"`
		State                  string  `json:"state"`
		LocalOrderId           *string `json:"localOrderId"`
		AcknowledgementOverdue bool    `json:"acknowledgementOverdue"`
		AcknowledgeBy          string  `json:"acknowledgeBy"`
		Lines                  []struct {
			NetworkLineRef   string `json:"networkLineRef"`
			NetworkProductId string `json:"networkProductId"`
			SKU              string `json:"sku"`
			Quantity         int    `json:"quantity"`
		} `json:"lines"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.NetworkRef != "po-1" || got.State != "NEW" {
		t.Fatalf("ref/state = %q/%q, want po-1/NEW", got.NetworkRef, got.State)
	}
	if got.LocalOrderId != nil {
		t.Fatalf("localOrderId = %v, want absent before acknowledgement", *got.LocalOrderId)
	}
	if len(got.Lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(got.Lines))
	}
	// Both vocabularies, or the ACL's translation decision is invisible
	// to whoever has to reconcile a disputed order.
	if got.Lines[0].NetworkProductId != "ASIN-AAA" || got.Lines[0].SKU != "sku-aaa" {
		t.Fatalf("line 0 = %+v, want both ASIN-AAA and sku-aaa", got.Lines[0])
	}
	if got.Lines[0].NetworkLineRef != "1" {
		t.Fatalf("networkLineRef = %q, want 1", got.Lines[0].NetworkLineRef)
	}
}

// The response must never carry customer PII (ADR 0001 hard rule 2). The
// aggregate models none today, so this is a guard against the field being
// added to the DTO later rather than a check of current behaviour.
func TestGetNetworkOrder_CarriesNoCustomerPII(t *testing.T) {
	e := newTestEnv(t)
	e.seed(t, "po-pii", time.Hour)

	rec := e.do(t, http.MethodGet, "/network-orders/po-pii")

	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	forbidden := []string{
		"shipTo", "shipToName", "shipToAddress", "address", "addressLine1",
		"name", "recipient", "phone", "phoneNumber", "email", "postalCode",
		"city", "customer", "buyer",
	}
	body := strings.ToLower(rec.Body.String())
	for _, f := range forbidden {
		if _, ok := raw[f]; ok {
			t.Fatalf("response exposes %q; customer PII stops in this context", f)
		}
		if strings.Contains(body, strings.ToLower(`"`+f+`"`)) {
			t.Fatalf("response body contains a %q key; customer PII stops in this context", f)
		}
	}
}

// Overdue is DERIVED via the domain predicate, so the surface cannot
// disagree with the sweep about what overdue means.
func TestGetNetworkOrder_ReportsOverdueUsingTheDomainPredicate(t *testing.T) {
	e := newTestEnv(t)
	// Received 30h before the fixed clock: past the 24h window.
	e.seed(t, "po-late", 30*time.Hour)
	// Received 1h before: well inside it.
	e.seed(t, "po-fresh", time.Hour)

	for ref, wantOverdue := range map[string]bool{"po-late": true, "po-fresh": false} {
		rec := e.do(t, http.MethodGet, "/network-orders/"+ref)
		var got struct {
			AcknowledgementOverdue bool `json:"acknowledgementOverdue"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode %s: %v", ref, err)
		}
		if got.AcknowledgementOverdue != wantOverdue {
			t.Fatalf("%s acknowledgementOverdue = %v, want %v", ref, got.AcknowledgementOverdue, wantOverdue)
		}
	}
}

func TestGetNetworkOrder_ExposesTheLocalOrderOnceLinked(t *testing.T) {
	e := newTestEnv(t)
	o := e.seed(t, "po-linked", time.Hour)
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := o.LinkLocalOrder("ord-xyz"); err != nil {
		t.Fatalf("LinkLocalOrder: %v", err)
	}
	if err := e.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rec := e.do(t, http.MethodGet, "/network-orders/po-linked")
	var got struct {
		State        string  `json:"state"`
		LocalOrderId *string `json:"localOrderId"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.State != "ACKNOWLEDGED" {
		t.Fatalf("state = %q, want ACKNOWLEDGED", got.State)
	}
	// The correlation this fleet already got wrong once (OM ADR 0018):
	// it must be the stored mapping, present and exact.
	if got.LocalOrderId == nil || *got.LocalOrderId != "ord-xyz" {
		t.Fatalf("localOrderId = %v, want ord-xyz", got.LocalOrderId)
	}
}

func TestGetNetworkOrder_UnknownRefIs404WithItsOwnProblemType(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodGet, "/network-orders/po-absent")
	body := assertProblem(t, rec, http.StatusNotFound)
	if !strings.HasSuffix(body.Type, "/network-order-not-found") {
		t.Fatalf("problem.type = %q, want the network-order-not-found slug", body.Type)
	}
}

func TestListUnanswered_ReturnsOnlyUnansweredOrders(t *testing.T) {
	e := newTestEnv(t)
	e.seed(t, "po-new", time.Hour)
	answered := e.seed(t, "po-acked", time.Hour)
	if err := answered.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := e.orders.Save(context.Background(), answered); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rec := e.do(t, http.MethodGet, "/network-orders")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got struct {
		NetworkOrders []struct {
			NetworkRef string `json:"networkRef"`
			State      string `json:"state"`
		} `json:"networkOrders"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.NetworkOrders) != 1 || got.NetworkOrders[0].NetworkRef != "po-new" {
		t.Fatalf("unanswered = %+v, want only po-new", got.NetworkOrders)
	}
}

// An empty list must be [] and not null: a client iterating the field
// should not have to special-case JSON null.
func TestListUnanswered_EmptyIsAnEmptyArrayNotNull(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodGet, "/network-orders")
	if !strings.Contains(rec.Body.String(), `"networkOrders":[]`) {
		t.Fatalf("body = %s, want an empty array", rec.Body.String())
	}
}

func TestInboundStatus_ReportsModeCountsAndOverdue(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	srv := &inboundhttp.Server{
		Orders: orders,
		Poller: fakeStats{s: poller.Stats{
			Polls: 7, Received: 4, Failed: 1,
			Since: time.Date(2026, 9, 23, 7, 55, 0, 0, time.UTC),
		}},
		Clock:       fixedClock{t: now()},
		NetworkMode: "sandbox",
	}
	e := &testEnv{orders: orders, handler: srv.Routes()}
	e.seed(t, "po-late", 30*time.Hour)
	e.seed(t, "po-fresh", time.Hour)

	rec := e.do(t, http.MethodGet, "/inbound-status")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got struct {
		NetworkMode string  `json:"networkMode"`
		Polls       int     `json:"polls"`
		Received    int     `json:"received"`
		Failed      int     `json:"failed"`
		Since       *string `json:"since"`
		Unanswered  int     `json:"unanswered"`
		Overdue     int     `json:"overdue"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The running mode must be verifiable, not assumed.
	if got.NetworkMode != "sandbox" {
		t.Fatalf("networkMode = %q, want sandbox", got.NetworkMode)
	}
	if got.Polls != 7 || got.Received != 4 || got.Failed != 1 {
		t.Fatalf("counters = %+v, want 7/4/1", got)
	}
	if got.Since == nil || *got.Since != "2026-09-23T07:55:00Z" {
		t.Fatalf("since = %v, want the poller's watermark", got.Since)
	}
	if got.Unanswered != 2 || got.Overdue != 1 {
		t.Fatalf("unanswered/overdue = %d/%d, want 2/1", got.Unanswered, got.Overdue)
	}
}

// A cold start that has never completed a clean pass must be
// distinguishable from one that has: zero here after real uptime means no
// poll has ever succeeded, which is the silent failure worth alerting on.
func TestInboundStatus_ColdStartOmitsTheWatermark(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodGet, "/inbound-status")
	if strings.Contains(rec.Body.String(), `"since"`) {
		t.Fatalf("body = %s, want no since field before a clean pass", rec.Body.String())
	}
}

func TestRepositoryFailureIs500(t *testing.T) {
	srv := &inboundhttp.Server{
		Orders:      failingRepo{err: errBoom},
		Poller:      fakeStats{},
		Clock:       fixedClock{t: now()},
		NetworkMode: "stub",
	}
	e := &testEnv{handler: srv.Routes()}

	for _, path := range []string{"/network-orders/po-1", "/network-orders", "/inbound-status"} {
		rec := e.do(t, http.MethodGet, path)
		assertProblem(t, rec, http.StatusInternalServerError)
	}
}

// The general guard. An error mapped to a 4xx by statusFor but missing
// from problemFor falls through to the internal-error slug, so the caller
// is told it hit a server bug when it hit a business rule. order-
// management shipped exactly that defect: the status was right, the body
// was not, and a status-only assertion passed.
func TestNo4xxProblemFallsBackToInternalError(t *testing.T) {
	e := newTestEnv(t)

	cases := []struct {
		name, path string
		want       int
	}{
		{"unknown network order", "/network-orders/po-nope", http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := assertProblem(t, e.do(t, http.MethodGet, tc.path), tc.want)
			if strings.HasSuffix(body.Type, "/internal-error") {
				t.Fatalf("a %d response fell back to the internal-error problem type: %+v", tc.want, body)
			}
			if strings.Contains(body.Title, "unexpected internal error") {
				t.Fatalf("a deliberate business rule must not be titled as an internal error: %q", body.Title)
			}
		})
	}
}

// Writes must not be reachable: demand enters by polling only, and an
// HTTP intake would be a fictional second inbound path.
func TestSurfaceIsReadOnly(t *testing.T) {
	e := newTestEnv(t)
	e.seed(t, "po-1", time.Hour)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		for _, path := range []string{"/network-orders", "/network-orders/po-1", "/inbound-status"} {
			rec := e.do(t, method, path)
			if rec.Code != http.StatusMethodNotAllowed && rec.Code != http.StatusNotFound {
				t.Fatalf("%s %s = %d, want 404/405 — this surface must be read-only", method, path, rec.Code)
			}
		}
	}
}
