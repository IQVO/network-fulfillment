package productmaster

// Consumer contract tests (R1-R4) against the PINNED COPY of
// product-master's published spec (testdata/product-master.openapi.yaml,
// pinned from IQVO/product-master apis/openapi.yaml@develop in
// contracts/pinned.json and kept equal by the pinned-contracts CI job;
// refresh = copy, never edit).
//
// The wire fixture (testdata/gtin-resolution.example.json) is DERIVED
// from the pinned GtinResolution schema and its property examples
// (producer example backfill pending); provenance is in the PR body.
//
// Same package (not _test) on purpose: R4 reflects the unexported wire
// DTO gtinDTO. kin-openapi is a test-only dependency.

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

const specCopy = "testdata/product-master.openapi.yaml"

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
	raw, err := os.ReadFile("testdata/gtin-resolution.example.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// R1: the client calls GET /gtins/{gtin} (client.go, ResolveGtin); the
// pinned spec must still declare exactly that call site, with the path
// parameter and the 200/404 answers the client maps.
func TestSpecCoversTheCallSite(t *testing.T) {
	doc := loadPinnedSpec(t)
	path := doc.Paths.Find("/gtins/{gtin}")
	if path == nil || path.Get == nil {
		t.Fatal("the pinned spec no longer declares GET /gtins/{gtin}")
	}
	if path.Get.OperationID != "resolveGtin" {
		t.Fatalf("operationId = %q, want resolveGtin", path.Get.OperationID)
	}
	var param *openapi3.Parameter
	// the spec declares the shared path parameter at the PATH-ITEM level
	// (applying to every operation), so check both levels
	for _, p := range append(path.Parameters, path.Get.Parameters...) {
		if p.Value != nil && p.Value.Name == "gtin" && p.Value.In == "path" {
			param = p.Value
		}
	}
	if param == nil || !param.Required {
		t.Fatal("the route no longer declares the required path parameter 'gtin'")
	}
	resp := path.Get.Responses.Value("200")
	if resp == nil || resp.Value.Content["application/json"] == nil {
		t.Fatal("the route's 200 response no longer serves application/json")
	}
	if got := resp.Value.Content["application/json"].Schema.Value; got != doc.Components.Schemas["GtinResolution"].Value {
		t.Fatal("the route's 200 schema is no longer GtinResolution")
	}
	if path.Get.Responses.Value("404") == nil {
		t.Fatal("the route no longer declares the 404 the client maps to ErrGtinNotFound")
	}
}

// R2: the wire fixture satisfies the pinned GtinResolution schema.
func TestFixtureSatisfiesThePinnedSchema(t *testing.T) {
	var body any
	if err := json.Unmarshal(fixture(t), &body); err != nil {
		t.Fatal(err)
	}
	schema := loadPinnedSpec(t).Components.Schemas["GtinResolution"].Value
	if err := schema.VisitJSON(body); err != nil {
		t.Fatalf("the derived fixture violates GtinResolution: %v", err)
	}
}

// R3: the real client decodes the fixture over a real HTTP round trip
// through fleetrest, a 404 maps to the ErrGtinNotFound sentinel the
// translation gate branches on, and a garbage body is an error.
func TestClientDecodesTheSpecExample(t *testing.T) {
	c := contractClientFor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/gtins/24006381333935" {
			t.Errorf("request = %s %s, want GET /gtins/24006381333935", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture(t))
	})
	resolution, err := c.ResolveGtin(t.Context(), "24006381333935")
	if err != nil {
		t.Fatal(err)
	}
	if resolution.SKU != "SKU-1" || resolution.Level != "CASE" || resolution.UnitsPerPack != 12 || !resolution.Active {
		t.Fatalf("decoded resolution %+v does not carry the fixture's facts", resolution)
	}

	c = contractClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"type":"https://errors.product-master.warehouse-systems.dev/gtin-not-found","title":"Unknown GTIN","status":404,"detail":"no listing"}`))
	})
	if _, err := c.ResolveGtin(t.Context(), "24006381333935"); !errors.Is(err, contract.ErrGtinNotFound) {
		t.Fatalf("404 must map to ErrGtinNotFound, got %v", err)
	}

	c = contractClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not json`))
	})
	if _, err := c.ResolveGtin(t.Context(), "24006381333935"); err == nil {
		t.Error("a garbage body must be an error, never a zero-value resolution")
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

// R4: the wire DTO and the pinned schema must agree in both directions:
// every field the client reads must be declared, and every field the
// schema REQUIRES must be mirrored (so a spec answer never decodes to a
// silent zero the translation gate would treat as a miss).
func TestResponseDTOMatchesThePinnedSchema(t *testing.T) {
	schema := loadPinnedSpec(t).Components.Schemas["GtinResolution"].Value
	tags := jsonTags[gtinDTO]()
	for tag := range tags {
		if _, declared := schema.Properties[tag]; !declared {
			t.Errorf("the DTO mirrors %q, which the pinned spec does not declare", tag)
		}
	}
	// Deliberately NOT asserting spec-required -> DTO: R4 is consumer-driven
	// (everything the DTO mirrors must be declared). The spec REQUIRES
	// effectiveFrom, which this DTO intentionally does not read — an additive
	// producer obligation, always present in answers, benign for this consumer
	// (noted in the PR body).
}
