package retailnetwork

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"gopkg.in/yaml.v3"
)

// The contract test (ADR 0026 Decision 9). testdata/vendor-connectivity-openapi.yaml
// is a BYTE COPY of vendor-connectivity's apis/openapi.yaml (blob
// 7393aeb57374eb7e38d98272a6851a5ee4ba36a1, last changed by commit
// 26c54625e72343d5e1e7ede651dcfe03ed149706, copied from its develop at
// 802cfe28096ac25288522063db3ab92aba60de06 on 2026-10-24). The repository has a
// JSON Schema validator already in go.sum (google/jsonschema-go, via the MCP
// SDK), so this test validates for real instead of comparing field lists:
//
//   - every request body the adapter builds validates against its component
//     schema and carries no property the schema does not list (the server decodes
//     with DisallowUnknownFields);
//   - every response shape the adapter reads is a sample the schema accepts, and
//     decodes through the adapter's own DTOs;
//   - the DTOs name only properties that exist, and request DTOs send every
//     required one;
//   - every enum value the adapter sends or branches on exists in the spec;
//   - every route the client calls exists, with the Idempotency-Key header on
//     writes and the vendorId query on reads.
//
// Drift: the copy is not refreshed automatically. When vendor-connectivity
// changes its contract, copy the new file over this one, update the commit above
// and in ADR 0026, and fix what fails.

const specPath = "testdata/vendor-connectivity-openapi.yaml"

type apiSpec struct {
	root     map[string]any
	schemas  map[string]any
	resolved map[string]*jsonschema.Resolved
}

func loadSpec(t *testing.T) *apiSpec {
	t.Helper()
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse %s: %v", specPath, err)
	}
	components, _ := root["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	if len(schemas) == 0 {
		t.Fatalf("%s has no components.schemas", specPath)
	}
	return &apiSpec{root: root, schemas: schemas, resolved: map[string]*jsonschema.Resolved{}}
}

// schema resolves component `name` as a JSON Schema whose $refs point at the
// other components.
func (s *apiSpec) schema(t *testing.T, name string) *jsonschema.Resolved {
	t.Helper()
	if r, ok := s.resolved[name]; ok {
		return r
	}
	defs, err := json.Marshal(s.schemas)
	if err != nil {
		t.Fatalf("marshal components: %v", err)
	}
	doc := `{"$ref":"#/$defs/` + name + `","$defs":` +
		strings.ReplaceAll(string(defs), "#/components/schemas/", "#/$defs/") + `}`
	var sc jsonschema.Schema
	if err := json.Unmarshal([]byte(doc), &sc); err != nil {
		t.Fatalf("schema %s: %v", name, err)
	}
	r, err := sc.Resolve(nil)
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	s.resolved[name] = r
	return r
}

func (s *apiSpec) validate(t *testing.T, name string, body []byte) {
	t.Helper()
	var instance any
	if err := json.Unmarshal(body, &instance); err != nil {
		t.Fatalf("%s: not JSON: %v\n%s", name, err, body)
	}
	if err := s.schema(t, name).Validate(instance); err != nil {
		t.Errorf("%s: %v\n%s", name, err, body)
	}
}

func (s *apiSpec) properties(t *testing.T, name string) []string {
	t.Helper()
	def, ok := s.schemas[name].(map[string]any)
	if !ok {
		t.Fatalf("component %q not found", name)
	}
	props, _ := def["properties"].(map[string]any)
	out := make([]string, 0, len(props))
	for k := range props {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func (s *apiSpec) required(t *testing.T, name string) []string {
	t.Helper()
	def, _ := s.schemas[name].(map[string]any)
	var out []string
	if req, ok := def["required"].([]any); ok {
		for _, r := range req {
			out = append(out, r.(string))
		}
	}
	slices.Sort(out)
	return out
}

// enum returns the enum at components.schemas.<name>[.properties.<prop>].
func (s *apiSpec) enum(t *testing.T, name string, prop string) []string {
	t.Helper()
	def, _ := s.schemas[name].(map[string]any)
	if prop != "" {
		props, _ := def["properties"].(map[string]any)
		def, _ = props[prop].(map[string]any)
	}
	vals, ok := def["enum"].([]any)
	if !ok {
		t.Fatalf("no enum at %s.%s", name, prop)
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = v.(string)
	}
	return out
}

func jsonFields(typ reflect.Type) []string {
	var out []string
	for i := range typ.NumField() {
		tag := typ.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		out = append(out, strings.Split(tag, ",")[0])
	}
	slices.Sort(out)
	return out
}

func TestContract_RequestBodiesTheAdapterBuildsValidateAgainstTheSpec(t *testing.T) {
	spec := loadSpec(t)
	po := PurchaseOrder{PoNumber: "PO-7f3c", Lines: []PurchaseOrderLine{{LineNo: 1, Quantity: 2}, {LineNo: 2, Quantity: 5}}}
	lat, lon := -23.55, -46.63

	accept, _ := buildAcknowledgementBody("V-WS", po, wireDecision{Decision: decisionAccept})
	reject, _ := buildAcknowledgementBody("V-WS", po, wireDecision{Decision: decisionReject, ReasonCode: reasonUnknownListing})
	label, _ := buildLabelBody("V-WS", "PO-7f3c", 2)
	ship, _ := buildShipmentConfirmationBody("V-WS", "PO-7f3c", time.Date(2026, 10, 24, 12, 30, 0, 0, time.UTC), []string{"TRK-1", "TRK-2"})
	node, _ := buildNodeRegistrationBody("V-WS", NodeDefinition{SiteCode: "site-1", NodeType: "FULFILLMENT_CENTER", Subtype: "SORTABLE",
		RegionID: "R-SP", Timezone: "America/Sao_Paulo", PostalCode: "01310-100", CountryCode: "BR", Lat: &lat, Lon: &lon})
	nodeMinimal, _ := buildNodeRegistrationBody("V-WS", NodeDefinition{SiteCode: "site-1", NodeType: "FULFILLMENT_CENTER",
		RegionID: "R-SP", Timezone: "America/Sao_Paulo", PostalCode: "01310-100", CountryCode: "BR"})

	cases := []struct {
		name, schema string
		body         []byte
	}{
		{"accept", "AcknowledgementRequest", accept},
		{"reject", "AcknowledgementRequest", reject},
		{"label", "LabelRequest", label},
		{"shipment confirmation", "ShipmentConfirmationRequest", ship},
		{"node registration", "NodeRegistrationRequest", node},
		{"node registration, required fields only", "NodeRegistrationRequest", nodeMinimal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec.validate(t, tc.schema, tc.body)

			var sent map[string]json.RawMessage
			if err := json.Unmarshal(tc.body, &sent); err != nil {
				t.Fatalf("body is not an object: %v", err)
			}
			known := spec.properties(t, tc.schema)
			for key := range sent {
				if !slices.Contains(known, key) {
					t.Errorf("the body sends %q, which %s does not list: the server refuses unknown fields", key, tc.schema)
				}
			}
		})
	}
}

func TestContract_TheSchemaActuallyRejectsABadBody(t *testing.T) {
	// A validator that accepts everything would make the test above vacuous.
	spec := loadSpec(t)
	bad := map[string]string{
		"AcknowledgementRequest":      `{"vendorId":"V-WS","poNumber":"PO-1","decision":"MAYBE","lines":[{"lineNo":1,"quantity":1}]}`,
		"LabelRequest":                `{"vendorId":"V-WS","poNumber":"PO-1","packages":[]}`,
		"ShipmentConfirmationRequest": `{"vendorId":"V-WS","poNumber":"PO-1","shippedAt":"2026-10-24T12:00:00Z","trackingNumbers":[]}`,
		"NodeRegistrationRequest":     `{"vendorId":"V-WS","vendorNodeCode":"s","nodeType":"PARCEL_SORTER","regionId":"r","timezone":"t","postalCode":"p","countryCode":"BR"}`,
	}
	for name, body := range bad {
		var instance any
		if err := json.Unmarshal([]byte(body), &instance); err != nil {
			t.Fatal(err)
		}
		if err := spec.schema(t, name).Validate(instance); err == nil {
			t.Errorf("%s accepted %s", name, body)
		}
	}
}

func TestContract_DTOsNameOnlyRealPropertiesAndRequestsSendEveryRequiredOne(t *testing.T) {
	spec := loadSpec(t)
	responses := map[string]reflect.Type{
		"PurchaseOrder":       reflect.TypeOf(purchaseOrderDTO{}),
		"Acknowledgement":     reflect.TypeOf(acknowledgementDTO{}),
		"PoLine":              reflect.TypeOf(poLineDTO{}),
		"PurchaseOrderPage":   reflect.TypeOf(purchaseOrderPageDTO{}),
		"SubmissionError":     reflect.TypeOf(submissionErrorDTO{}),
		"Transaction":         reflect.TypeOf(transactionDTO{}),
		"TransactionAccepted": reflect.TypeOf(transactionAcceptedDTO{}),
		"VendorNode":          reflect.TypeOf(vendorNodeDTO{}),
		"VendorNodeList":      reflect.TypeOf(vendorNodeListDTO{}),
		"Label":               reflect.TypeOf(labelDTO{}),
		"LabelResponse":       reflect.TypeOf(labelResponseDTO{}),
	}
	requests := map[string]reflect.Type{
		"AcknowledgementRequest":      reflect.TypeOf(acknowledgementRequestDTO{}),
		"AckLine":                     reflect.TypeOf(ackLineDTO{}),
		"LabelPackage":                reflect.TypeOf(labelPackageDTO{}),
		"LabelRequest":                reflect.TypeOf(labelRequestDTO{}),
		"ShipmentConfirmationRequest": reflect.TypeOf(shipmentConfirmationRequestDTO{}),
		"NodeRegistrationRequest":     reflect.TypeOf(nodeRegistrationRequestDTO{}),
	}
	for name, typ := range responses {
		for _, f := range jsonFields(typ) {
			if !slices.Contains(spec.properties(t, name), f) {
				t.Errorf("%s reads %q, which %s does not define", typ.Name(), f, name)
			}
		}
	}
	for name, typ := range requests {
		fields := jsonFields(typ)
		for _, f := range fields {
			if !slices.Contains(spec.properties(t, name), f) {
				t.Errorf("%s sends %q, which %s does not define", typ.Name(), f, name)
			}
		}
		for _, r := range spec.required(t, name) {
			if !slices.Contains(fields, r) {
				t.Errorf("%s does not send %q, which %s requires", typ.Name(), r, name)
			}
		}
	}
}

func TestContract_EveryValueTheAdapterSendsOrBranchesOnExistsInTheSpec(t *testing.T) {
	spec := loadSpec(t)
	checks := []struct {
		what, schema, prop string
		values             []string
	}{
		{"decision", "AcknowledgementRequest", "decision", []string{decisionAccept, decisionReject}},
		{"reject reason", "RejectReason", "", []string{reasonCannotMeetShipBy, reasonUnknownListing}},
		{"purchase order state", "PurchaseOrderState", "", []string{stateIssued}},
		{"transaction status", "SubmissionStatus", "", []string{txProcessing, txSuccess, txFailure}},
		{"submission kind", "SubmissionKind", "", []string{string(KindAcknowledgement), string(KindShipmentConfirmation), string(KindNodeRegistration)}},
		{"node type", "NodeType", "", []string{"FULFILLMENT_CENTER", "INBOUND_CROSS_DOCK", "RETURNS_CENTER"}},
	}
	for _, c := range checks {
		enum := spec.enum(t, c.schema, c.prop)
		for _, v := range c.values {
			if !slices.Contains(enum, v) {
				t.Errorf("%s %q is not in %s %v", c.what, v, c.schema, enum)
			}
		}
	}
	// The network has no expiry reason: that is why ACKNOWLEDGEMENT_DEADLINE_MISSED
	// is not written (ADR 0026 Decision 3). If one appears, the decision needs a revisit.
	for _, r := range spec.enum(t, "RejectReason", "") {
		if strings.Contains(r, "EXPIR") || strings.Contains(r, "DEADLINE") {
			t.Errorf("RejectReason now has %q: revisit the mapping of ACKNOWLEDGEMENT_DEADLINE_MISSED", r)
		}
	}
}

func TestContract_ARecordedPurchaseOrderPageIsValidAndDecodes(t *testing.T) {
	spec := loadSpec(t)
	page := `{"purchaseOrders":[{"poNumber":"PO-7f3c","vendorId":"V-WS","nodeId":"N-1","state":"ISSUED",
		"issuedAt":"2026-10-24T12:00:00Z","requiredShipBy":"2026-10-26T18:00:00Z","ackBy":"2026-10-24T15:00:00Z",
		"lines":[{"lineNo":1,"listingId":"L-1","quantity":2}],"labels":[],"trackingNumbers":[]}],"nextToken":"abc"}`
	spec.validate(t, "PurchaseOrderPage", []byte(page))

	var got purchaseOrderPageDTO
	if err := json.Unmarshal([]byte(page), &got); err != nil {
		t.Fatal(err)
	}
	po := got.PurchaseOrders[0]
	if po.PoNumber != "PO-7f3c" || po.NodeID != "N-1" || po.State != stateIssued || po.Lines[0].ListingID != "L-1" || got.NextToken != "abc" {
		t.Errorf("decoded page = %+v", got)
	}
	if want := time.Date(2026, 10, 24, 15, 0, 0, 0, time.UTC); !po.AckBy.Equal(want) {
		t.Errorf("ackBy = %v, want %v", po.AckBy, want)
	}
	if want := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC); !po.RequiredShipBy.Equal(want) {
		t.Errorf("requiredShipBy = %v, want %v", po.RequiredShipBy, want)
	}
}

func TestContract_RecordedTransactionsAreValidAndDecode(t *testing.T) {
	spec := loadSpec(t)
	cases := []struct {
		name, body string
		check      func(transactionDTO) bool
	}{
		{"node registration carries the node id", `{"transactionId":"tx-1","vendorId":"V-WS","kind":"NODE_REGISTRATION","status":"SUCCESS",
			"receivedAt":"2026-10-24T12:00:00Z","processedAt":"2026-10-24T12:00:05Z","errors":[],"nodeId":"N-9"}`,
			func(tx transactionDTO) bool { return tx.NodeID == "N-9" && tx.Status == txSuccess }},
		{"a failure carries its errors and the order", `{"transactionId":"tx-2","vendorId":"V-WS","kind":"ACKNOWLEDGEMENT","status":"FAILURE",
			"receivedAt":"2026-10-24T12:00:00Z","processedAt":"2026-10-24T12:00:05Z",
			"errors":[{"code":"ErrSimulatedFailure","message":"simulated","path":"$.decision"}],"poNumber":"PO-7f3c"}`,
			func(tx transactionDTO) bool {
				return tx.Status == txFailure && tx.Errors[0].Code == "ErrSimulatedFailure" && tx.PoNumber == "PO-7f3c"
			}},
		{"still processing", `{"transactionId":"tx-3","vendorId":"V-WS","kind":"SHIPMENT_CONFIRMATION","status":"PROCESSING",
			"receivedAt":"2026-10-24T12:00:00Z"}`,
			func(tx transactionDTO) bool { return tx.Status == txProcessing && tx.ProcessedAt == nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec.validate(t, "Transaction", []byte(tc.body))
			var tx transactionDTO
			if err := json.Unmarshal([]byte(tc.body), &tx); err != nil || !tc.check(tx) {
				t.Errorf("decoded = %+v, %v", tx, err)
			}
		})
	}
	spec.validate(t, "TransactionAccepted", []byte(`{"transactionId":"tx-3"}`))
}

func TestContract_RecordedNodesLabelsAndProblemsAreValidAndDecode(t *testing.T) {
	spec := loadSpec(t)

	nodes := `{"nodes":[{"nodeId":"N-1","vendorId":"V-WS","vendorNodeCode":"site-1","nodeType":"FULFILLMENT_CENTER","status":"ACTIVE"}]}`
	spec.validate(t, "VendorNodeList", []byte(nodes))
	var list vendorNodeListDTO
	if err := json.Unmarshal([]byte(nodes), &list); err != nil || list.Nodes[0].VendorNodeCode != "site-1" || list.Nodes[0].NodeID != "N-1" {
		t.Errorf("decoded nodes = %+v, %v", list, err)
	}

	labels := `{"poNumber":"PO-7f3c","labels":[{"packageNo":1,"labelRef":"LB-1","trackingNumber":"TRK-1","carrier":"CARRIER"}]}`
	spec.validate(t, "LabelResponse", []byte(labels))
	var lr labelResponseDTO
	if err := json.Unmarshal([]byte(labels), &lr); err != nil || lr.Labels[0].TrackingNumber != "TRK-1" || lr.Labels[0].PackageNo != 1 {
		t.Errorf("decoded labels = %+v, %v", lr, err)
	}

	spec.validate(t, "Problem", []byte(`{"type":"ack-window-closed","title":"The acknowledgement window is closed","status":409,"detail":"too late","instance":"/vendor/acknowledgements"}`))
}

// TestContract_EveryRouteTheClientCallsExistsWithItsRequiredParameters drives
// each client operation against a recording server and looks the route up in the
// spec's paths.
func TestContract_EveryRouteTheClientCallsExistsWithItsRequiredParameters(t *testing.T) {
	spec := loadSpec(t)
	paths, _ := spec.root["paths"].(map[string]any)

	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "shipping-labels"):
			ok(w, map[string]any{"poNumber": "PO-1", "labels": []any{}})
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusAccepted)
			ok(w, map[string]string{"transactionId": "tx-1"})
		default:
			ok(w, map[string]any{})
		}
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	ctx := context.Background()
	_, _ = c.pollPage(ctx, time.Time{}, "")
	_, _ = c.getPurchaseOrder(ctx, "PO-1")
	_, _ = c.getTransaction(ctx, "tx-1")
	_, _ = c.listNodes(ctx)
	_, _ = c.postAsync(ctx, "submitAcknowledgement", []string{"vendor", "acknowledgements"}, "k1", []byte("{}"))
	_, _ = c.postAsync(ctx, "submitShipmentConfirmation", []string{"vendor", "shipment-confirmations"}, "k2", []byte("{}"))
	_, _ = c.postAsync(ctx, "registerVendorNode", []string{"vendor", "nodes"}, "k3", []byte("{}"))
	_, _ = c.requestLabels(ctx, "k4", []byte("{}"))

	reqs := f.requests()
	if len(reqs) != 8 {
		t.Fatalf("requests = %d, want 8", len(reqs))
	}
	for _, r := range reqs {
		item := lookupPath(paths, r.Path)
		if item == nil {
			t.Errorf("%s %s is not a path of the spec", r.Method, r.Path)
			continue
		}
		op, _ := item[strings.ToLower(r.Method)].(map[string]any)
		if op == nil {
			t.Errorf("%s %s: the spec has no such operation", r.Method, r.Path)
			continue
		}
		refs := parameterRefs(op)
		if r.Method == http.MethodPost {
			if !slices.Contains(refs, "#/components/parameters/IdempotencyKey") || r.IdemKey == "" {
				t.Errorf("POST %s: the spec requires Idempotency-Key (listed=%v) and the client sent %q", r.Path, refs, r.IdemKey)
			}
			continue
		}
		if !slices.Contains(refs, "#/components/parameters/VendorId") || !strings.Contains(r.RawQuery, "vendorId=V-WS") {
			t.Errorf("GET %s: the spec requires vendorId (listed=%v) and the client sent %q", r.Path, refs, r.RawQuery)
		}
	}
}

func parameterRefs(op map[string]any) []string {
	var refs []string
	params, _ := op["parameters"].([]any)
	for _, p := range params {
		if m, ok := p.(map[string]any); ok {
			if ref, ok := m["$ref"].(string); ok {
				refs = append(refs, ref)
			}
		}
	}
	return refs
}

// lookupPath finds the spec path item a concrete request path matches.
func lookupPath(paths map[string]any, concrete string) map[string]any {
	for tmpl, item := range paths {
		segs := strings.Split(tmpl, "/")
		for i, seg := range segs {
			if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
				segs[i] = "[^/]+"
			} else {
				segs[i] = regexp.QuoteMeta(seg)
			}
		}
		if regexp.MustCompile("^" + strings.Join(segs, "/") + "$").MatchString(concrete) {
			m, _ := item.(map[string]any)
			return m
		}
	}
	return nil
}
