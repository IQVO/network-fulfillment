package mcp

// Consumer contract tests (R1/R3-lite/R4) for the INTRA-REPO reports
// edge: cmd/mcp's ReportsRESTClient calls the netfulfil-reports binary
// (REPORTS_BASE_URL) at GET /reports/acknowledgement and GET
// /reports/acknowledgement/freshness. The contract validates against the
// PINNED COPY of THIS repo's own apis/openapi.yaml
// (testdata/network-fulfillment.openapi.yaml) — a self-pin in
// contracts/pinned.json: producer and consumer live in the same module,
// so the pin guards against editing the reports section of the spec
// without updating the MCP reports client. (The fleet registry names
// apis/openapi-reports.yaml for this edge; this repo documents the
// reports surface inside apis/openapi.yaml under the 'reports' tag, so
// the self-pin registers that file — see the PR body.)
//
// The wire fixtures (testdata/acknowledgement-report.example.json,
// testdata/freshness.example.json) are DERIVED from the pinned
// AcknowledgementReport/Freshness schemas; provenance is in the PR body.
//
// Same package (not _test) on purpose: R4 reflects this package's view
// DTOs. kin-openapi is a test-only dependency.

import (
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

const specCopy = "testdata/network-fulfillment.openapi.yaml"

func loadPinnedSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(specCopy)
	if err != nil {
		t.Fatalf("cannot load the pinned spec copy: %v", err)
	}
	return doc
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// R1: the reports client calls GET /reports/acknowledgement (with the
// required from/to query parameters and the optional granularity) and
// GET /reports/acknowledgement/freshness; the pinned self-spec must
// still declare exactly those call sites and schemas.
func TestSpecCoversTheCallSites(t *testing.T) {
	doc := loadPinnedSpec(t)
	assertAcknowledgementRoute(t, doc)
	assertFreshnessRoute(t, doc)
}

func assertAcknowledgementRoute(t *testing.T, doc *openapi3.T) {
	t.Helper()
	report := doc.Paths.Find("/reports/acknowledgement")
	if report == nil || report.Get == nil {
		t.Fatal("the pinned self-spec no longer declares GET /reports/acknowledgement")
	}
	if report.Get.OperationID != "getAcknowledgementReport" {
		t.Fatalf("operationId = %q, want getAcknowledgementReport", report.Get.OperationID)
	}
	byName := map[string]*openapi3.Parameter{}
	for _, p := range report.Get.Parameters {
		if p.Value != nil {
			byName[p.Value.Name] = p.Value
		}
	}
	for _, name := range []string{"from", "to"} {
		if p := byName[name]; p == nil || p.In != "query" || !p.Required {
			t.Fatalf("GET /reports/acknowledgement no longer declares the required query parameter %q", name)
		}
	}
	if p := byName["granularity"]; p == nil || p.Required {
		t.Fatal("granularity must stay an OPTIONAL query parameter (the client omits it)")
	}
	if report.Get.Responses.Value("200").Value.Content["application/json"].Schema.Value != doc.Components.Schemas["AcknowledgementReport"].Value {
		t.Fatal("the report route's 200 schema is no longer AcknowledgementReport")
	}
}

func assertFreshnessRoute(t *testing.T, doc *openapi3.T) {
	t.Helper()
	freshness := doc.Paths.Find("/reports/acknowledgement/freshness")
	if freshness == nil || freshness.Get == nil || freshness.Get.OperationID != "getAcknowledgementReportFreshness" {
		t.Fatal("the pinned self-spec no longer declares GET /reports/acknowledgement/freshness")
	}
	if freshness.Get.Responses.Value("200").Value.Content["application/json"].Schema.Value != doc.Components.Schemas["Freshness"].Value {
		t.Fatal("the freshness route's 200 schema is no longer Freshness")
	}
}

// serveReports answers every request with status and body and records the
// last request line.
func serveReports(t *testing.T, status int, body []byte) (*ReportsRESTClient, *string) {
	t.Helper()
	got := new(string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Method + " " + r.URL.Path + "?" + r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return NewReportsRESTClient(srv.URL, srv.Client()), got
}

// R3-lite: the real client decodes the derived fixtures over a real HTTP
// round trip, sending the required from/to query parameters; any
// non-2xx is an error the tool surfaces, never a silent empty report.
func TestReportsClientDecodesTheSpecExamples(t *testing.T) {
	c, got := serveReports(t, http.StatusOK, fixture(t, "acknowledgement-report.example.json"))
	view, err := c.GetAcknowledgementReport(t.Context(), AcknowledgementReportQuery{From: "2026-09-23T00:00:00Z", To: "2026-09-24T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Rows) != 1 || view.Rows[0].DayBucket != "2026-09-23" || view.Rows[0].OrdersReceived != 10 {
		t.Fatalf("decoded report %+v does not carry the fixture's facts", view)
	}
	if !strings.Contains(*got, "from=2026-09-23T00%3A00%3A00Z") || !strings.Contains(*got, "to=2026-09-24T00%3A00%3A00Z") {
		t.Fatalf("request = %s, want the required from/to query parameters", *got)
	}

	c, got = serveReports(t, http.StatusOK, fixture(t, "freshness.example.json"))
	freshness, err := c.GetFreshness(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if freshness.LagSeconds != 12.5 {
		t.Fatalf("lagSeconds = %v, want 12.5 (the fixture's value)", freshness.LagSeconds)
	}
	if *got != "GET /reports/acknowledgement/freshness?" {
		t.Fatalf("request = %s, want GET /reports/acknowledgement/freshness", *got)
	}

	c, _ = serveReports(t, http.StatusInternalServerError, []byte(`{}`))
	if _, err := c.GetFreshness(t.Context()); err == nil {
		t.Error("a 5xx must be an error, never a silent zero freshness")
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

// R4: the view DTOs and the pinned self-spec must agree in both
// directions — every field the tool reads is declared, and every field
// the schema requires is mirrored — so the spec's own reports surface
// can never drift from the client that renders it.
func TestViewDTOsMatchThePinnedSchema(t *testing.T) {
	doc := loadPinnedSpec(t)

	rowSchema := doc.Components.Schemas["AcknowledgementReportRow"].Value
	rowTags := jsonTags[AcknowledgementRowView]()
	for tag := range rowTags {
		if _, declared := rowSchema.Properties[tag]; !declared {
			t.Errorf("AcknowledgementRowView mirrors %q, which the pinned spec does not declare", tag)
		}
	}
	for _, required := range rowSchema.Required {
		if _, mirrored := rowTags[required]; !mirrored {
			t.Errorf("the pinned spec requires %q, which AcknowledgementRowView does not mirror", required)
		}
	}

	freshnessSchema := doc.Components.Schemas["Freshness"].Value
	freshnessTags := jsonTags[FreshnessView]()
	for tag := range freshnessTags {
		if _, declared := freshnessSchema.Properties[tag]; !declared {
			t.Errorf("FreshnessView mirrors %q, which the pinned spec does not declare", tag)
		}
	}
	for _, required := range freshnessSchema.Required {
		if _, mirrored := freshnessTags[required]; !mirrored {
			t.Errorf("the pinned spec requires %q, which FreshnessView does not mirror", required)
		}
	}
}
