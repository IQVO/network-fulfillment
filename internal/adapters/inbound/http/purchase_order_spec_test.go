package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

// The contract test of GET /purchase-orders/{poNumber}: apis/openapi.yaml and
// the handler must say the same thing. The spec is the OpenAPI-first source of
// the route (ADR 0036); this test reads it, not a copy of it.

const specPath = "../../../../apis/openapi.yaml"

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load %s: %v", specPath, err)
	}
	return doc
}

func lookupOperation(t *testing.T, doc *openapi3.T) *openapi3.Operation {
	t.Helper()
	item := doc.Paths.Find("/purchase-orders/{poNumber}")
	if item == nil || item.Get == nil {
		t.Fatal("apis/openapi.yaml no longer declares GET /purchase-orders/{poNumber}")
	}
	if item.Post != nil || item.Put != nil || item.Patch != nil || item.Delete != nil {
		t.Fatal("/purchase-orders/{poNumber} must declare GET only: it is a keyed read")
	}
	return item.Get
}

func TestSpec_TheLookupIsAKeyedReadOfTheSameNetworkOrder(t *testing.T) {
	doc := loadSpec(t)
	op := lookupOperation(t, doc)

	if op.OperationID != "getNetworkOrderByPurchaseOrder" || len(op.Tags) != 1 || op.Tags[0] != "network-orders" {
		t.Fatalf("operation = %q %v, want getNetworkOrderByPurchaseOrder tagged network-orders", op.OperationID, op.Tags)
	}
	if len(op.Parameters) != 1 || op.Parameters[0].Value.Name != "poNumber" || op.Parameters[0].Value.In != "path" || !op.Parameters[0].Value.Required {
		t.Fatal("the route must declare exactly one parameter, the required path parameter poNumber: no query parameter, no paging")
	}

	sibling := doc.Paths.Find("/network-orders/{networkRef}").Get.Responses.Value("200").Value.Content["application/json"].Schema
	ok := op.Responses.Value("200").Value.Content["application/json"].Schema
	if ok.Ref != sibling.Ref || ok.Ref != "#/components/schemas/NetworkOrder" {
		t.Fatalf("200 schema = %q, want the sibling's %q: one representation", ok.Ref, sibling.Ref)
	}
	for code, slug := range map[string]string{"400": "invalid-purchase-order-number", "404": "purchase-order-not-found"} {
		resp := op.Responses.Value(code)
		if resp == nil || resp.Value.Content["application/problem+json"] == nil {
			t.Fatalf("the route must declare a %s application/problem+json response", code)
		}
		if !strings.Contains(*resp.Value.Description, slug) {
			t.Fatalf("the %s description must name `%s`: %s", code, slug, *resp.Value.Description)
		}
	}
	if op.Responses.Value("500") == nil {
		t.Fatal("the route must declare its 500 problem")
	}
	for _, want := range []string{"keyed read", "not a listing", "unknown-query-parameter", "purchase-order-not-found"} {
		if !strings.Contains(strings.ToLower(op.Description), want) {
			t.Errorf("the description must say %q so a caller can tell what this route is", want)
		}
	}
}

// The listing's wording is reconciled with the new route, not contradicted.
func TestSpec_TheListingStaysTheOnlyListAndPointsAtTheKeyedReads(t *testing.T) {
	doc := loadSpec(t)
	desc := doc.Paths.Find("/network-orders").Get.Description
	if strings.Contains(desc, "Only this list is exposed") {
		t.Fatal("GET /network-orders still says \"Only this list is exposed\", which the keyed read contradicts")
	}
	for _, want := range []string{"only LISTING", "/purchase-orders/{poNumber}", "/network-orders/{networkRef}"} {
		if !strings.Contains(desc, want) {
			t.Errorf("GET /network-orders must say %q", want)
		}
	}
	for _, p := range doc.Paths.Find("/network-orders").Get.Parameters {
		if p.Ref != "#/components/parameters/Limit" && p.Ref != "#/components/parameters/Cursor" {
			t.Errorf("GET /network-orders gained the parameter %q: the list takes limit and cursor only", p.Value.Name)
		}
	}
}

// The spec's poNumber schema and the handler agree on which numbers are
// well-formed: whatever the schema accepts reaches the lookup, whatever it
// rejects is a 400.
func TestSpec_PoNumberSchemaAndHandlerAgree(t *testing.T) {
	doc := loadSpec(t)
	schema := lookupOperation(t, doc).Parameters[0].Value.Schema.Value
	e := newPOEnv(t)

	for _, po := range []string{
		"PO-7f3c", "a", "A", "0", "-", ".", "_", "~", "a-._~Z9", strings.Repeat("z", 128),
		strings.Repeat("z", 129), "PO 1", "PO:1", "PO+1", "PO@1", "PO[1", "PO{1", "PO`1", "PO/1",
		"PO%1", "PO\u00e91", "", "PO\t1",
	} {
		wantValid := schema.VisitJSON(po) == nil
		if got := handlerAccepts(t, e, po); got != wantValid {
			t.Errorf("%q: the spec's schema says well-formed = %v, the handler says %v", po, wantValid, got)
		}
	}
}

// handlerAccepts reports whether the route takes po as a well-formed number,
// by calling the unexported validation through the real handler: a 400 is a
// rejection and anything else reached the lookup. Characters that cannot be
// written into a request target as they are go through the router escaped.
func handlerAccepts(t *testing.T, e *poEnv, po string) bool {
	t.Helper()
	if po == "" || strings.ContainsAny(po, "/%\t\u00e9") {
		// Not expressible as a raw path segment: the handler sees the escaped
		// form, which contains a '%' and is rejected, or no segment at all.
		return false
	}
	rec := e.do(t, http.MethodGet, "/purchase-orders/"+strings.ReplaceAll(po, " ", "%20"))
	return rec.Code != http.StatusBadRequest
}

// The 200 body of every state the route answers is valid against the
// NetworkOrder schema, and the problems against the Problem schema.
func TestSpec_ResponsesValidateAgainstTheDeclaredSchemas(t *testing.T) {
	doc := loadSpec(t)
	op := lookupOperation(t, doc)
	order := op.Responses.Value("200").Value.Content["application/json"].Schema.Value
	problem := op.Responses.Value("404").Value.Content["application/problem+json"].Schema.Value

	e := newPOEnv(t)
	acked := e.seedFiled(t, "PO-acked", "NR-acked")
	if err := acked.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := acked.LinkLocalOrder("ord-1"); err != nil {
		t.Fatalf("LinkLocalOrder: %v", err)
	}
	e.save(t, acked)
	e.seedFiled(t, "PO-new", "NR-new")

	for _, po := range []string{"PO-acked", "PO-new"} {
		rec := e.do(t, http.MethodGet, "/purchase-orders/"+po)
		if err := order.VisitJSON(decodeAny(t, rec.Body.Bytes())); err != nil {
			t.Fatalf("%s: the 200 body does not satisfy the NetworkOrder schema: %v\n%s", po, err, rec.Body.String())
		}
	}
	for _, target := range []string{"/purchase-orders/PO-absent", "/purchase-orders/PO%201", "/purchase-orders/PO-new?limit=1"} {
		rec := e.do(t, http.MethodGet, target)
		if err := problem.VisitJSON(decodeAny(t, rec.Body.Bytes())); err != nil {
			t.Fatalf("%s: the problem body does not satisfy the Problem schema: %v\n%s", target, err, rec.Body.String())
		}
	}
}

func decodeAny(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode: %v (%s)", err, raw)
	}
	return v
}
