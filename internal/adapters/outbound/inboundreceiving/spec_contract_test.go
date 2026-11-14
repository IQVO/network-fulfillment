package inboundreceiving

// Consumer contract tests (R1-R4) against the PINNED COPY of
// inbound-receiving's published spec
// (testdata/inbound-receiving.openapi.yaml, pinned from
// IQVO/inbound-receiving apis/openapi.yaml@develop in
// contracts/pinned.json and kept equal by the pinned-contracts CI job;
// refresh = copy, never edit).
//
// This is a WRITE edge (POST /asns): the registerAsnDTO this client
// sends is the consumer-driven surface, so R2/R4 run against the pinned
// REQUEST schema and the spec's own example values where it has them.
// The wire fixture (testdata/asn-registration.example.json) is DERIVED
// from the pinned RegisterAsnRequest schema (producer example backfill
// pending — the spec carries no full request example); provenance is
// recorded in the PR body.
//
// Same package (not _test) on purpose: R4 reflects the unexported wire
// DTOs of client.go. kin-openapi is a test-only dependency.

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

const specCopy = "testdata/inbound-receiving.openapi.yaml"

// loadPinnedSpec resolves the pinned copy with kin-openapi's loader, so
// $refs are followed exactly as a consumer tool would.
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
	raw, err := os.ReadFile("testdata/asn-registration.example.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// R1: the client calls POST /asns (client.go, RegisterAsn) with the
// required Idempotency-Key header; the pinned spec must still declare
// exactly that call site, request schema and conflict answer.
func TestSpecCoversTheCallSite(t *testing.T) {
	doc := loadPinnedSpec(t)
	path := doc.Paths.Find("/asns")
	if path == nil || path.Post == nil {
		t.Fatal("the pinned spec no longer declares POST /asns")
	}
	if path.Post.OperationID != "registerAsn" {
		t.Fatalf("operationId = %q, want registerAsn", path.Post.OperationID)
	}
	var key *openapi3.Parameter
	for _, p := range path.Post.Parameters {
		if p.Value != nil && p.Value.Name == "Idempotency-Key" && p.Value.In == "header" {
			key = p.Value
		}
	}
	if key == nil || !key.Required {
		t.Fatal("the route no longer declares the required Idempotency-Key header this client sends")
	}
	body := path.Post.RequestBody.Value.Content["application/json"]
	if body == nil || body.Schema.Value != doc.Components.Schemas["RegisterAsnRequest"].Value {
		t.Fatal("the route's request body is no longer RegisterAsnRequest")
	}
	if path.Post.Responses.Value("201") == nil || path.Post.Responses.Value("409") == nil {
		t.Fatal("the route no longer declares the 201 and 409 answers RegisterAsn maps")
	}
}

// R2: the wire fixture satisfies the pinned RegisterAsnRequest schema.
func TestFixtureSatisfiesThePinnedSchema(t *testing.T) {
	var body any
	if err := json.Unmarshal(fixture(t), &body); err != nil {
		t.Fatal(err)
	}
	schema := loadPinnedSpec(t).Components.Schemas["RegisterAsnRequest"].Value
	if err := schema.VisitJSON(body); err != nil {
		t.Fatalf("the derived fixture violates RegisterAsnRequest: %v", err)
	}
}

// R3: the real client round-trips the verdicts through fleetrest: a 201
// and an asn-already-exists 409 are both success, and a 422 problem+json
// is the AsnRefusedError the inbound leg branches on.
func TestClientDecodesTheSpecVerdicts(t *testing.T) {
	reg := registrationFixture(t)

	c := contractClientFor(t, answer(http.StatusCreated, `{}`))
	if err := c.RegisterAsn(t.Context(), reg); err != nil {
		t.Fatalf("201 must be success: %v", err)
	}

	c = contractClientFor(t, answer(http.StatusConflict, problemJSON("asn-already-exists", "ASN already registered")))
	if err := c.RegisterAsn(t.Context(), reg); err != nil {
		t.Fatalf("409 asn-already-exists must be success: %v", err)
	}

	c = contractClientFor(t, answer(http.StatusUnprocessableEntity, problemJSON("asn-line-unknown-sku", "no such SKU")))
	err := c.RegisterAsn(t.Context(), reg)
	var refused *contract.AsnRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("422 problem must map to AsnRefusedError, got %v", err)
	}
	if refused.Problem != "asn-line-unknown-sku" {
		t.Errorf("refused problem %q, want the problem+json slug", refused.Problem)
	}
}

// registrationFixture builds a minimal registration shaped like the wire
// fixture (one line, one carton), reusing client_test.go's asn() values.
func registrationFixture(t *testing.T) contract.AsnRegistration {
	t.Helper()
	base := contractAsn()
	if len(base.Lines) == 0 {
		t.Fatal("client_test.go's asn() carries no lines to mirror the fixture")
	}
	return base
}

// answer serves status with body on every request.
func answer(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func problemJSON(slug, detail string) string {
	return `{"type":"https://errors.inbound-receiving.warehouse-systems.dev/` + slug + `","title":"Refused","status":422,"detail":"` + detail + `","instance":"/asns"}`
}

// jsonTags maps a DTO's json field names, so R4 can assert every field
// the client sends is declared by the pinned request schema (recursively
// through nested DTOs and array item schemas).
func TestRequestDTOMatchesThePinnedSchema(t *testing.T) {
	doc := loadPinnedSpec(t)
	rootRef := doc.Components.Schemas["RegisterAsnRequest"]
	assertDeclared(t, rootRef, doc, reflect.TypeOf(registerAsnDTO{}))
}

// assertDeclared walks a DTO's fields against a schema, recursing into
// nested structs and slices via the schema's own $refs.
func assertDeclared(t *testing.T, sr *openapi3.SchemaRef, doc *openapi3.T, typ reflect.Type) {
	t.Helper()
	schema := resolveRef(t, sr, doc)
	for i := range typ.NumField() {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		prop, declared := schema.Properties[tag]
		if !declared {
			t.Errorf("%s sends %q, which the pinned spec does not declare", typ.Name(), tag)
			continue
		}
		child, ft := childRefAndType(prop, field.Type)
		if ft.Kind() == reflect.Struct {
			if child == nil || child.Value == nil {
				t.Errorf("%s.%q has no schema to check against", typ.Name(), tag)
				continue
			}
			assertDeclared(t, child, doc, ft)
		}
	}
	for _, required := range schema.Required {
		if !dtoHasTag(typ, required) {
			t.Errorf("the pinned spec requires %q, which %s does not send", required, typ.Name())
		}
	}
}

// childRefAndType peels pointers and slices off the DTO field type,
// descending to the item schema for slices; the schema ref travels with it.
func childRefAndType(sr *openapi3.SchemaRef, ft reflect.Type) (*openapi3.SchemaRef, reflect.Type) {
	if ft.Kind() == reflect.Pointer {
		return sr, ft.Elem()
	}
	if ft.Kind() == reflect.Slice {
		if sr != nil && sr.Value != nil && sr.Value.Items != nil {
			return sr.Value.Items, ft.Elem()
		}
		return sr, ft.Elem()
	}
	return sr, ft
}

// resolveRef follows a schema $ref into components (nested item schemas are
// $refs in this spec); a SchemaRef carries its Ref only BEFORE
// dereferencing, so the walk stays on SchemaRef values.
func resolveRef(t *testing.T, sr *openapi3.SchemaRef, doc *openapi3.T) *openapi3.Schema {
	t.Helper()
	for sr != nil {
		if sr.Ref == "" {
			return sr.Value
		}
		name := sr.Ref[strings.LastIndex(sr.Ref, "/")+1:]
		next, ok := doc.Components.Schemas[name]
		if !ok {
			t.Fatalf("pinned spec references undefined schema %q", sr.Ref)
		}
		sr = next
	}
	return nil
}

func dtoHasTag(typ reflect.Type, tag string) bool {
	for i := range typ.NumField() {
		if strings.Split(typ.Field(i).Tag.Get("json"), ",")[0] == tag {
			return true
		}
	}
	return false
}
