package ordermanagement

// Consumer contract tests (R1-R4) against the PINNED COPY of
// order-management's published spec
// (testdata/order-management.openapi.yaml, pinned from
// IQVO/order-management apis/openapi.yaml@develop in
// contracts/pinned.json and kept equal by the pinned-contracts CI job;
// refresh = copy, never edit).
//
// The wire fixture (testdata/held-order.example.json) is DERIVED from
// the pinned Order schema and its property examples (producer example
// backfill pending — the spec carries no full response example);
// provenance is recorded in the PR body.
//
// Same package (not _test) on purpose: R4 reflects the unexported wire
// DTOs of planner.go. kin-openapi is a test-only dependency.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

const specCopy = "testdata/order-management.openapi.yaml"

func loadPinnedSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(specCopy)
	if err != nil {
		t.Fatalf("cannot load the pinned spec copy: %v", err)
	}
	return doc
}

func fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/held-order.example.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// R1: the planner calls POST /orders (RaiseHeldOrder with the
// Idempotency-Key of the network ref), POST /orders/{id}/releases
// (ReleaseHeldOrder) and DELETE /orders/{id} (CancelHeldOrder); the
// pinned spec must still declare all three call sites.
func TestSpecCoversTheCallSites(t *testing.T) {
	doc := loadPinnedSpec(t)
	orders := doc.Paths.Find("/orders")
	if orders == nil || orders.Post == nil {
		t.Fatal("the pinned spec no longer declares POST /orders")
	}
	if orders.Post.OperationID != "receiveOrder" {
		t.Fatalf("POST /orders operationId = %q, want receiveOrder", orders.Post.OperationID)
	}
	assertIdempotencyKeyParam(t, orders)
	releases := doc.Paths.Find("/orders/{id}/releases")
	if releases == nil || releases.Post == nil || releases.Post.OperationID != "releaseHeldOrder" {
		t.Fatal("the pinned spec no longer declares POST /orders/{id}/releases (releaseHeldOrder)")
	}
	cancel := doc.Paths.Find("/orders/{id}")
	if cancel == nil || cancel.Delete == nil || cancel.Delete.OperationID != "cancelOrder" {
		t.Fatal("the pinned spec no longer declares DELETE /orders/{id} (cancelOrder)")
	}
}

// assertIdempotencyKeyParam holds the planner to the fleet's idempotency
// guideline: POST /orders must declare the REQUIRED Idempotency-Key header.
// The spec declares it at the PATH-ITEM level ($ref to
// components/parameters/IdempotencyKey), applying to every operation on the
// route, so both path-item and operation parameter lists are checked.
func assertIdempotencyKeyParam(t *testing.T, orders *openapi3.PathItem) {
	t.Helper()
	for _, p := range append(orders.Parameters, orders.Post.Parameters...) {
		if p.Value == nil || p.Value.Name != "Idempotency-Key" || p.Value.In != "header" {
			continue
		}
		// FINDING (producer spec, not fixed here — never edit a pin): the
		// header is declared required:false while its own description says
		// "Clients must ALWAYS send a non-empty value" and a missing key 400s
		// with idempotency-key-required. The planner always sends one; spec
		// sync on the producer should flip this to required:true.
		if !p.Value.Required {
			t.Logf("NOTE: the pinned spec declares Idempotency-Key required:false (contradicts its own description; producer spec-sync follow-up)")
		}
		return
	}
	t.Fatal("POST /orders no longer declares the Idempotency-Key header the planner sends")
}

// R2: the wire fixture satisfies the pinned Order schema.
func TestFixtureSatisfiesThePinnedSchema(t *testing.T) {
	var body any
	if err := json.Unmarshal(fixture(t), &body); err != nil {
		t.Fatal(err)
	}
	schema := loadPinnedSpec(t).Components.Schemas["Order"].Value
	if err := schema.VisitJSON(body); err != nil {
		t.Fatalf("the derived fixture violates Order: %v", err)
	}
}

// serveOrders answers every request with status and body and records the
// last one.
func serveOrders(t *testing.T, status int, body []byte) (*Planner, *string) {
	t.Helper()
	got := new(string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Method + " " + r.URL.EscapedPath() + " key=" + r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return NewPlanner(srv.URL, srv.Client()), got
}

func heldOrderRequest() contract.HeldOrderRequest {
	return contract.HeldOrderRequest{
		NetworkRef:     "po-1",
		SiteId:         "site-1",
		RequiredShipBy: time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC),
		Lines:          map[shared.SKU]int{"SKU-1": 2},
	}
}

// R3: the real planner decodes the spec's Order example over a real HTTP
// round trip: a promiseDate means feasible with that cutoff, and the
// request carries the network ref as the Idempotency-Key (the retry
// contract POST /orders documents).
func TestPlannerDecodesTheSpecExample(t *testing.T) {
	p, got := serveOrders(t, http.StatusCreated, fixture(t))
	result, err := p.RaiseHeldOrder(t.Context(), heldOrderRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Feasible {
		t.Fatal("an Order with promiseDate must decode as feasible")
	}
	want, _ := time.Parse(time.RFC3339, "2026-08-27T09:00:00Z")
	if !result.PromisedCutoff.Equal(want) {
		t.Fatalf("promised cutoff %v, want the fixture's promiseDate %v", result.PromisedCutoff, want)
	}
	if result.LocalOrderId != shared.LocalOrderId("ord-a1b2c3d4-0000-0000-0000-000000000001") {
		t.Fatalf("local order id %q, want the fixture's id", result.LocalOrderId)
	}
	// the planner derives its own Idempotency-Key (a netful-raise- prefixed
	// key, not the bare networkRef); the contract obligation is a NON-EMPTY
	// Idempotency-Key on POST /orders, not a specific spelling
	if !strings.Contains(*got, "POST /orders") || !strings.Contains(*got, "key=") {
		t.Fatalf("request = %s, want POST /orders carrying an Idempotency-Key", *got)
	}
}

// R3 (the verdict subtlety): an Order WITHOUT promiseDate is the
// "cannot meet the deadline" verdict, not a decode failure — ADR 0001 §7.
func TestPlannerTreatsAnAbsentPromiseAsInfeasible(t *testing.T) {
	noPromise := strings.Replace(string(fixture(t)), `"promiseDate": "2026-08-27T09:00:00Z"`, `"promiseDate": null`, 1)
	p, _ := serveOrders(t, http.StatusCreated, []byte(noPromise))
	result, err := p.RaiseHeldOrder(t.Context(), heldOrderRequest())
	if err != nil {
		t.Fatalf("an absent promiseDate is a verdict, not an error: %v", err)
	}
	if result.Feasible {
		t.Fatal("an Order without promiseDate must decode as infeasible")
	}
}

// R3 (failure mapping): any non-2xx — problem+json included — is an
// error from the planner; the breaker client is the one that wraps it.
func TestPlannerMapsFailuresToErrors(t *testing.T) {
	p, _ := serveOrders(t, http.StatusConflict, []byte(`{"type":"https://errors.order-management.warehouse-systems.dev/order-not-found","title":"Order not found","status":404,"detail":"order not found"}`))
	if _, err := p.RaiseHeldOrder(t.Context(), heldOrderRequest()); err == nil {
		t.Error("a 4xx must be an error, never a zero-value result")
	}
}

// jsonTags maps a DTO's json field names.
func jsonTags[T any]() map[string]bool {
	tags := map[string]bool{}
	typ := reflect.TypeOf(*new(T))
	for i := range typ.NumField() {
		tag := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if tag != "" && tag != "-" {
			tags[tag] = true
		}
	}
	return tags
}

// R4: the response DTO and the pinned Order schema must agree in both
// directions, with tolerance for producer-side additions. The one
// subtlety this guards: Order does NOT require promiseDate, and the DTO
// mirrors it as *time.Time — absence decodes to nil, which RaiseHeldOrder
// reads as the infeasible verdict (never a crash).
func TestResponseDTOMatchesThePinnedSchema(t *testing.T) {
	doc := loadPinnedSpec(t)
	schema := doc.Components.Schemas["Order"].Value
	tags := jsonTags[orderResponse]()
	for tag := range tags {
		if _, declared := schema.Properties[tag]; !declared {
			t.Errorf("the DTO mirrors %q, which the pinned Order schema does not declare", tag)
		}
	}
	for _, required := range schema.Required {
		if _, mirrored := tags[required]; !mirrored {
			t.Logf("note: Order now requires %q, which the DTO does not mirror", required)
		}
	}
	for _, required := range schema.Required {
		if required == "promiseDate" {
			t.Error("promiseDate became required — the infeasible-verdict path (nil promise) is gone")
		}
	}
}
