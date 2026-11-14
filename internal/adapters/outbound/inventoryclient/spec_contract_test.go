package inventoryclient

// Consumer contract tests (R1-R4) against the PINNED COPY of
// inventory-storage's published spec
// (testdata/inventory-storage.openapi.yaml, pinned from
// IQVO/inventory-storage apis/openapi.yaml@develop in
// contracts/pinned.json and kept equal by the pinned-contracts CI job;
// refresh = copy, never edit).
//
// The wire fixture (testdata/usable-inventory.example.json) is the pinned
// UsableInventory schema's own example values — never hand-tuned, so the
// test cannot drift from what the producer documents.
//
// Same package (not _test) on purpose: R4 reflects the unexported
// response DTO usableInventoryResponse. kin-openapi is a test-only
// dependency.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

const specCopy = "testdata/inventory-storage.openapi.yaml"

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
	raw, err := os.ReadFile("testdata/usable-inventory.example.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// R1: the client calls GET /inventory/{sku}/usable (client.go,
// UsableQuantity); the pinned spec must still declare exactly that call
// site, with the path parameter it escapes into the URL and the 200
// schema the client decodes.
func TestSpecCoversTheCallSite(t *testing.T) {
	doc := loadPinnedSpec(t)
	path := doc.Paths.Find("/inventory/{sku}/usable")
	if path == nil || path.Get == nil {
		t.Fatal("the pinned spec no longer declares GET /inventory/{sku}/usable")
	}
	if path.Get.OperationID != "getUsableInventory" {
		t.Fatalf("operationId = %q, want getUsableInventory", path.Get.OperationID)
	}
	var param *openapi3.Parameter
	for _, p := range path.Get.Parameters {
		if p.Value != nil && p.Value.Name == "sku" && p.Value.In == "path" {
			param = p.Value
		}
	}
	if param == nil || !param.Required {
		t.Fatal("the route no longer declares the required path parameter 'sku'")
	}
	resp := path.Get.Responses.Value("200")
	if resp == nil || resp.Value.Content["application/json"] == nil {
		t.Fatal("the route's 200 response no longer serves application/json")
	}
	if got := resp.Value.Content["application/json"].Schema.Value; got != doc.Components.Schemas["UsableInventory"].Value {
		t.Fatal("the route's 200 schema is no longer UsableInventory")
	}
}

// R2: the wire fixture satisfies the pinned UsableInventory schema.
func TestFixtureSatisfiesThePinnedSchema(t *testing.T) {
	var body any
	if err := json.Unmarshal(fixture(t), &body); err != nil {
		t.Fatal(err)
	}
	schema := loadPinnedSpec(t).Components.Schemas["UsableInventory"].Value
	if err := schema.VisitJSON(body); err != nil {
		t.Fatalf("the spec's own example violates UsableInventory: %v", err)
	}
}

// serveUsable answers every request with status and body and records the
// last one.
func serveUsable(t *testing.T, status int, body []byte) (*Client, *string) {
	t.Helper()
	got := new(string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Method + " " + r.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, srv.Client()), got
}

// R3: the real client decodes the spec's example over a real HTTP round
// trip, and any failure status is an error, never a silent zero.
func TestClientDecodesTheSpecExample(t *testing.T) {
	c, got := serveUsable(t, http.StatusOK, fixture(t))
	usable, err := c.UsableQuantity(t.Context(), shared.SKU("SKU-1"))
	if err != nil {
		t.Fatal(err)
	}
	if usable != 7 {
		t.Fatalf("usable = %d, want 7 (the spec example's value)", usable)
	}
	if *got != "GET /inventory/SKU-1/usable" {
		t.Fatalf("request = %s, want GET /inventory/SKU-1/usable", *got)
	}

	c2, _ := serveUsable(t, http.StatusInternalServerError, []byte(`{}`))
	if _, err := c2.UsableQuantity(t.Context(), shared.SKU("SKU-1")); err == nil {
		t.Error("a 5xx must be an error, never a silent zero")
	}
}

// jsonTags maps the DTO's json field names.
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

// R4: the response DTO and the pinned schema must agree in both
// directions, with tolerance for producer-side additions. UsableInventory
// declares NOTHING required, and the one field the caller reads
// (usable) is a plain int: absence decodes to 0, which the endpoint's
// own contract defines as "we hold none" — the documented zero-value
// default, not a decode gap.
func TestResponseDTOMatchesThePinnedSchema(t *testing.T) {
	schema := loadPinnedSpec(t).Components.Schemas["UsableInventory"].Value
	tags := jsonTags[usableInventoryResponse]()
	for tag := range tags {
		if _, declared := schema.Properties[tag]; !declared {
			t.Errorf("the DTO mirrors %q, which the pinned spec does not declare", tag)
		}
	}
	for _, required := range schema.Required {
		if _, mirrored := tags[required]; !mirrored {
			t.Errorf("the pinned spec requires %q, which the DTO does not mirror", required)
		}
	}
	if len(schema.Required) != 0 {
		t.Logf("note: UsableInventory now requires %v — re-check the zero-value tolerance above", schema.Required)
	}
}
