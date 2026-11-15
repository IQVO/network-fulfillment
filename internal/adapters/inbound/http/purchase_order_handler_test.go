package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// GET /purchase-orders/{poNumber} (ADR 0036): a keyed read of the same
// NetworkOrder as GET /network-orders/{networkRef}, found by the network's
// purchase-order number through the persisted poNumber -> networkRef map.

// poEnv is a testEnv whose server also has the purchase-order index.
type poEnv struct {
	*testEnv
	index *memory.PurchaseOrderIndex
}

func newPOEnv(t *testing.T) *poEnv {
	t.Helper()
	orders := memory.NewNetworkOrderRepo()
	index := memory.NewPurchaseOrderIndex()
	srv := &inboundhttp.Server{
		Orders:         orders,
		PurchaseOrders: index,
		Poller:         fakeStats{},
		Clock:          fixedClock{t: now()},
		NetworkMode:    "stub",
	}
	return &poEnv{testEnv: &testEnv{orders: orders, handler: srv.Routes()}, index: index}
}

// seedFiled stores an order under ref and records that the purchase order with
// this number was filed under it.
func (e *poEnv) seedFiled(t *testing.T, poNumber, ref string) *networkorder.NetworkOrder {
	t.Helper()
	o := e.seed(t, ref, time.Hour)
	e.index.Register(poNumber, shared.NetworkRef(ref))
	return o
}

func (e *poEnv) save(t *testing.T, o *networkorder.NetworkOrder) {
	t.Helper()
	if err := e.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save %s: %v", o.NetworkRef(), err)
	}
}

func decodeMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v (body: %s)", err, rec.Body.String())
	}
	return raw
}

func TestGetByPurchaseOrder_AcknowledgedOrderCarriesItsLocalOrderId(t *testing.T) {
	e := newPOEnv(t)
	o := e.seedFiled(t, "PO-7f3c", "NR-acked")
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := o.LinkLocalOrder("ord-xyz"); err != nil {
		t.Fatalf("LinkLocalOrder: %v", err)
	}
	e.save(t, o)

	rec := e.do(t, http.MethodGet, "/purchase-orders/PO-7f3c")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	got := decodeMap(t, rec)
	if got["state"] != "ACKNOWLEDGED" || got["networkRef"] != "NR-acked" || got["localOrderId"] != "ord-xyz" {
		t.Fatalf("order = %v, want ACKNOWLEDGED, networkRef NR-acked, localOrderId ord-xyz", got)
	}
	for _, key := range []string{"siteId", "requiredShipBy", "acknowledgeBy", "receivedAt", "acknowledgementOverdue", "lines"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("order = %v, want the documented property %q", got, key)
		}
	}
}

// The two routes read one stored order through one function: the body must be
// byte for byte the sibling's, whatever the order's state.
func TestGetByPurchaseOrder_IsTheSameRepresentationAsGetByNetworkRef(t *testing.T) {
	e := newPOEnv(t)
	o := e.seedFiled(t, "PO-same", "NR-same")
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := o.LinkLocalOrder("ord-same"); err != nil {
		t.Fatalf("LinkLocalOrder: %v", err)
	}
	e.save(t, o)

	byNumber := e.do(t, http.MethodGet, "/purchase-orders/PO-same")
	byRef := e.do(t, http.MethodGet, "/network-orders/NR-same")
	if byNumber.Code != http.StatusOK || byRef.Code != http.StatusOK {
		t.Fatalf("statuses = %d and %d, want 200 and 200", byNumber.Code, byRef.Code)
	}
	if byNumber.Body.String() != byRef.Body.String() {
		t.Fatalf("by number: %s\nby ref:    %s", byNumber.Body.String(), byRef.Body.String())
	}
}

// ADR 0024 records the hold on a NEW order and a REJECTED order keeps the id of
// the hold it freed; the contract says localOrderId is absent until
// acknowledged, so the keyed read must not expose it either.
func TestGetByPurchaseOrder_NewAndRejectedOrdersHaveNoLocalOrderId(t *testing.T) {
	e := newPOEnv(t)
	pending := e.seedFiled(t, "PO-new", "NR-new")
	if err := pending.RecordHold("ord-new"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	rejected := e.seedFiled(t, "PO-rejected", "NR-rejected")
	if err := rejected.RecordHold("ord-rejected"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	if err := rejected.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	e.save(t, pending)
	e.save(t, rejected)

	for po, state := range map[string]string{"PO-new": "NEW", "PO-rejected": "REJECTED"} {
		rec := e.do(t, http.MethodGet, "/purchase-orders/"+po)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200 (body: %s)", po, rec.Code, rec.Body.String())
		}
		got := decodeMap(t, rec)
		if got["state"] != state {
			t.Fatalf("%s: state = %v, want %s", po, got["state"], state)
		}
		if v, exposed := got["localOrderId"]; exposed {
			t.Fatalf("%s: localOrderId = %v exposed; the contract says absent until acknowledged", po, v)
		}
	}
}

func TestGetByPurchaseOrder_UnknownNumberIs404WithItsOwnProblemType(t *testing.T) {
	e := newPOEnv(t)
	e.seedFiled(t, "PO-known", "NR-known")

	rec := e.do(t, http.MethodGet, "/purchase-orders/PO-absent")
	body := assertProblem(t, rec, http.StatusNotFound)
	if !strings.HasSuffix(body.Type, "/purchase-order-not-found") {
		t.Fatalf("problem.type = %q, want the purchase-order-not-found slug (not the sibling's network-order-not-found)", body.Type)
	}
	if body.Title != "Purchase order not found" || body.Instance != "/purchase-orders/PO-absent" {
		t.Fatalf("problem = %+v, want its own title and the request path as instance", body)
	}
}

// The adapter records the map row when it polls and the use case saves the
// order afterwards: in between the number is known and there is no order yet.
func TestGetByPurchaseOrder_ANumberWhoseOrderIsNotStoredIsTheSame404(t *testing.T) {
	e := newPOEnv(t)
	e.index.Register("PO-early", "NR-not-saved-yet")

	body := assertProblem(t, e.do(t, http.MethodGet, "/purchase-orders/PO-early"), http.StatusNotFound)
	if !strings.HasSuffix(body.Type, "/purchase-order-not-found") {
		t.Fatalf("problem.type = %q, want purchase-order-not-found: the caller named a purchase order, not a ref", body.Type)
	}
}

func TestGetByPurchaseOrder_MalformedNumberIs400(t *testing.T) {
	e := newPOEnv(t)
	for name, target := range map[string]string{
		"a space":                  "/purchase-orders/PO%207f3c",
		"an escaped slash":         "/purchase-orders/PO%2F7f3c",
		"a non-canonical escape":   "/purchase-orders/%41",
		"a colon":                  "/purchase-orders/PO:7f3c",
		"a plus":                   "/purchase-orders/PO+7f3c",
		"a non-ASCII letter":       "/purchase-orders/PO-%C3%A9",
		"a control character":      "/purchase-orders/PO%01",
		"129 characters":           "/purchase-orders/" + strings.Repeat("a", 129),
		"a semicolon (matrix)":     "/purchase-orders/PO;x=1",
		"an at sign":               "/purchase-orders/PO@x",
		"a left square bracket":    "/purchase-orders/PO[1",
		"a backtick":               "/purchase-orders/PO`1",
		"a left curly bracket":     "/purchase-orders/PO%7B1",
		"a right parenthesis":      "/purchase-orders/PO)1",
		"an exclamation mark":      "/purchase-orders/PO!1",
		"a tab":                    "/purchase-orders/PO%091",
		"a dollar sign":            "/purchase-orders/PO$1",
		"an asterisk":              "/purchase-orders/PO*1",
		"a comma":                  "/purchase-orders/PO,1",
		"an equals sign":           "/purchase-orders/PO=1",
		"an ampersand":             "/purchase-orders/PO&1",
		"an apostrophe":            "/purchase-orders/PO'1",
		"a double-escaped percent": "/purchase-orders/PO%251",
	} {
		t.Run(name, func(t *testing.T) {
			rec := e.do(t, http.MethodGet, target)
			body := assertProblem(t, rec, http.StatusBadRequest)
			if !strings.HasSuffix(body.Type, "/invalid-purchase-order-number") {
				t.Fatalf("problem.type = %q, want invalid-purchase-order-number", body.Type)
			}
			if body.Title != "Invalid purchase order number" || !strings.Contains(body.Detail, "1 to 128 characters") {
				t.Fatalf("problem = %+v, want its title and the rule in the detail", body)
			}
		})
	}
}

// The check is a shape, not a format: whatever the network issued that fits a
// URL path segment reaches the lookup (here, it is unknown: 404, not 400).
func TestGetByPurchaseOrder_AnyUrlSafeNumberReachesTheLookup(t *testing.T) {
	e := newPOEnv(t)
	for _, po := range []string{
		"p", "PO-7f3c9a1e5b2d4086", "po_1", "a.b", "a~b", "0", "ZZZ-999",
		strings.Repeat("a", 128), "..", "-",
	} {
		rec := e.do(t, http.MethodGet, "/purchase-orders/"+po)
		body := assertProblem(t, rec, http.StatusNotFound)
		if !strings.HasSuffix(body.Type, "/purchase-order-not-found") {
			t.Fatalf("%q: problem.type = %q, want purchase-order-not-found (the number is well-formed)", po, body.Type)
		}
	}
}

func TestGetByPurchaseOrder_AQueryParameterIs400(t *testing.T) {
	e := newPOEnv(t)
	e.seedFiled(t, "PO-1", "NR-1")

	for target, param := range map[string]string{
		"/purchase-orders/PO-1?limit=10":       "limit",
		"/purchase-orders/PO-1?cursor=abc":     "cursor",
		"/purchase-orders/PO-1?state=NEW":      "state",
		"/purchase-orders/PO-1?zeta=1&alpha=2": "alpha",
		"/purchase-orders/PO-1?flag":           "flag",
	} {
		rec := e.do(t, http.MethodGet, target)
		body := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(body.Type, "/unknown-query-parameter") {
			t.Fatalf("%s: problem.type = %q, want unknown-query-parameter", target, body.Type)
		}
		if !strings.Contains(body.Detail, "query parameter "+param+" is not declared by this route") {
			t.Fatalf("%s: detail = %q, want the first parameter (%s) named", target, body.Detail, param)
		}
	}
	// An empty query string is no parameter.
	if rec := e.do(t, http.MethodGet, "/purchase-orders/PO-1?"); rec.Code != http.StatusOK {
		t.Fatalf("empty query: status = %d, want 200", rec.Code)
	}
}

// It is a keyed read, never a listing, and never a write.
func TestPurchaseOrders_IsNotAListingAndNotAWrite(t *testing.T) {
	e := newPOEnv(t)
	e.seedFiled(t, "PO-1", "NR-1")

	if rec := e.do(t, http.MethodGet, "/purchase-orders"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /purchase-orders = %d, want 404: there is no listing", rec.Code)
	}
	if rec := e.do(t, http.MethodGet, "/purchase-orders/"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /purchase-orders/ = %d, want 404: there is no listing", rec.Code)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		if rec := e.do(t, method, "/purchase-orders/PO-1"); rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s /purchase-orders/PO-1 = %d, want 405: the route is read-only", method, rec.Code)
		}
	}
}

func TestGetByPurchaseOrder_NotRegisteredWhenTheIndexIsNil(t *testing.T) {
	e := newTestEnv(t) // PurchaseOrders is unset (nil) in the base fixture
	if rec := e.do(t, http.MethodGet, "/purchase-orders/PO-1"); rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when PurchaseOrders is nil (route not registered)", rec.Code)
	}
}

// failingIndex makes the index's error path reachable.
type failingIndex struct{ err error }

func (f failingIndex) FindNetworkRefByPONumber(context.Context, string) (shared.NetworkRef, bool, error) {
	return "", false, f.err
}

func TestGetByPurchaseOrder_StoreFailuresAre500(t *testing.T) {
	healthy := memory.NewNetworkOrderRepo()
	known := memory.NewPurchaseOrderIndex()
	known.Register("PO-1", "NR-1")

	for name, srv := range map[string]*inboundhttp.Server{
		"the index fails":      {Orders: healthy, PurchaseOrders: failingIndex{err: errBoom}},
		"the order read fails": {Orders: failingRepo{err: errBoom}, PurchaseOrders: known},
	} {
		t.Run(name, func(t *testing.T) {
			srv.Poller, srv.Clock, srv.NetworkMode = fakeStats{}, fixedClock{t: now()}, "stub"
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/purchase-orders/PO-1", nil))
			body := assertProblem(t, rec, http.StatusInternalServerError)
			if !strings.HasSuffix(body.Type, "/internal-error") {
				t.Fatalf("problem.type = %q, want internal-error", body.Type)
			}
		})
	}
}

// A typed not-found from the index is the route's own 404, whatever wraps it.
func TestGetByPurchaseOrder_ATypedNotFoundFromTheIndexIs404(t *testing.T) {
	srv := &inboundhttp.Server{
		Orders:         memory.NewNetworkOrderRepo(),
		PurchaseOrders: failingIndex{err: usecases.ErrPurchaseOrderNotFound},
		Poller:         fakeStats{}, Clock: fixedClock{t: now()}, NetworkMode: "stub",
	}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/purchase-orders/PO-1", nil))
	body := assertProblem(t, rec, http.StatusNotFound)
	if !strings.HasSuffix(body.Type, "/purchase-order-not-found") {
		t.Fatalf("problem.type = %q, want purchase-order-not-found", body.Type)
	}
}

// A NUL byte is never a resource (rejectNUL runs before the route).
func TestGetByPurchaseOrder_ANulByteIsAnyOtherEndpointsNul(t *testing.T) {
	e := newPOEnv(t)
	body := assertProblem(t, e.do(t, http.MethodGet, "/purchase-orders/PO-4%0004"), http.StatusNotFound)
	if !strings.HasSuffix(body.Type, "/resource-not-found") {
		t.Fatalf("path NUL: problem.type = %q, want resource-not-found", body.Type)
	}
	body = assertProblem(t, e.do(t, http.MethodGet, "/purchase-orders/PO-1?x=%00"), http.StatusBadRequest)
	if !strings.HasSuffix(body.Type, "/invalid-request") {
		t.Fatalf("query NUL: problem.type = %q, want invalid-request", body.Type)
	}
}
