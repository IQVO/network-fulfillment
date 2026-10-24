package retailnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// The inbound half of the Vendor API (ADR 0028): the poll, the tree flattening
// and the exactly-once receipt, against the stateful fake of the demand loop.

const (
	ssccPallet = "006141410000000036"
	ssccA      = "006141410000000012"
	ssccB      = "006141410000000029"
	gtinEach   = "00614141000036"
)

func inboundTree(id string) inboundShipmentDTO {
	return inboundShipmentDTO{
		ShipmentID: id, SellerID: "S-100", NodeID: "N-1", ShippedAt: t0, Status: stateShipped,
		Pallets: []shipmentPalletDTO{{SSCC: ssccPallet, Cartons: []shipmentCartonDTO{{
			SSCC: ssccA, Kind: "HOMOGENEOUS_CASE", GrossWeightG: 4200, DimsMm: &dimsDTO{L: 400, W: 300, H: 200},
			Contents: []shipmentItemDTO{{GTIN: gtinEach, QtyEaches: 12, Lot: "L-77", Expiry: "2027-03-01"}},
		}}}},
		Cartons: []shipmentCartonDTO{{SSCC: ssccB, Kind: "MIXED", Contents: []shipmentItemDTO{{GTIN: gtinEach, QtyEaches: 6}}}},
	}
}

// serveInbound answers GET /vendor/inbound-shipments with pages keyed by nextToken.
func serveInbound(f *liveFixture, pages map[string]inboundShipmentPageDTO) {
	f.sim.inject = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || r.URL.Path != "/vendor/inbound-shipments" {
			return false
		}
		ok(w, pages[r.URL.Query().Get("nextToken")])
		return true
	}
}

func TestInbound_PollFlattensPalletsAndCartonsAndPages(t *testing.T) {
	f := readyFixture(t, nil)
	_, _ = f.g.PollDemand(context.Background(), t0) // converge the node: FC-1 <-> N-1
	closed := inboundTree("SHP-closed")
	closed.Status = "CLOSED"
	serveInbound(f, map[string]inboundShipmentPageDTO{
		"":   {InboundShipments: []inboundShipmentDTO{inboundTree("SHP-1"), closed}, NextToken: "p2"},
		"p2": {InboundShipments: []inboundShipmentDTO{inboundTree("SHP-1"), inboundTree("SHP-2")}},
	})

	got, err := f.g.PollInboundShipments(context.Background(), "FC-1", t0)
	if err != nil {
		t.Fatalf("PollInboundShipments: %v", err)
	}
	if len(got) != 2 || got[0].Ref != "SHP-1" || got[1].Ref != "SHP-2" {
		t.Fatalf("shipments = %+v, want SHP-1 once and SHP-2 (a repeated and a non-SHIPPED one dropped)", got)
	}
	expectFlattenedShipment(t, got[0])
	expectPagedQueries(t, f.requestsTo(http.MethodGet, "/vendor/inbound-shipments"))
}

func expectFlattenedShipment(t *testing.T, s contract.InboundShipment) {
	t.Helper()
	if s.OwnerRef != "S-100" || s.SiteId != "FC-1" || !s.ShippedAt.Equal(t0) || len(s.Cartons) != 2 {
		t.Fatalf("shipment = %+v", s)
	}
	expectPalletCarton(t, s.Cartons[0])
	if c := s.Cartons[1]; c.SSCC != ssccB || c.ParentSSCC != "" || c.Kind != contract.CartonMixed || c.Dims != nil {
		t.Errorf("top-level carton = %+v", c)
	}
}

func expectPalletCarton(t *testing.T, c contract.InboundCarton) {
	t.Helper()
	if c.SSCC != ssccA || c.ParentSSCC != ssccPallet || c.Kind != contract.CartonHomogeneousCase || c.GrossWeightG != 4200 {
		t.Errorf("pallet carton = %+v", c)
	}
	if c.Dims == nil || c.Dims.LengthMm != 400 || c.Dims.HeightMm != 200 {
		t.Errorf("pallet carton dims = %+v", c.Dims)
	}
	if c.Contents[0] != (contract.InboundContent{GTIN: gtinEach, QtyEaches: 12, Lot: "L-77", Expiry: "2027-03-01"}) {
		t.Errorf("pallet carton contents = %+v", c.Contents)
	}
}

func expectPagedQueries(t *testing.T, reqs []seen) {
	t.Helper()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want 2 pages", len(reqs))
	}
	for _, want := range []string{"vendorId=V-WS", "nodeId=N-1", "createdAfter=2026-10-24T09%3A58%3A00Z"} {
		if !strings.Contains(reqs[0].RawQuery, want) {
			t.Errorf("first page query %q misses %q (the cursor minus the 2 minute overlap)", reqs[0].RawQuery, want)
		}
	}
	if strings.Contains(reqs[1].RawQuery, "createdAfter") || !strings.Contains(reqs[1].RawQuery, "nextToken=p2") {
		t.Errorf("next page query %q: a token replaces the filters", reqs[1].RawQuery)
	}
}

func TestInbound_PollOfASiteWithoutANodeIsEmptyAndMakesNoCall(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-9"}})
	got, err := f.g.PollInboundShipments(context.Background(), "FC-9", time.Time{})
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Errorf("%d requests, want none", n)
	}
}

func TestInbound_PollRefusesARepeatedPageToken(t *testing.T) {
	f := readyFixture(t, nil)
	_, _ = f.g.PollDemand(context.Background(), t0)
	serveInbound(f, map[string]inboundShipmentPageDTO{
		"":     {InboundShipments: []inboundShipmentDTO{inboundTree("SHP-1")}, NextToken: "loop"},
		"loop": {InboundShipments: []inboundShipmentDTO{inboundTree("SHP-2")}, NextToken: "loop"},
	})
	if _, err := f.g.PollInboundShipments(context.Background(), "FC-1", t0); err == nil || !strings.Contains(err.Error(), "repeated page token") {
		t.Fatalf("err = %v, want the repeated token refused", err)
	}
}

func receiptFor(ref string) contract.InboundReceipt {
	return contract.InboundReceipt{
		ShipmentRef: ref, SiteId: "FC-1", ReceivedAt: t0.Add(24 * time.Hour),
		Cartons: []contract.ReceiptCarton{
			{SSCC: ssccA, Status: contract.ReceiptReceived, Items: []contract.ReceiptItem{{GTIN: gtinEach, QtyEaches: 12}}},
			{SSCC: ssccB, Status: contract.ReceiptMissing},
		},
	}
}

func TestInbound_ReceiptIsSentOnceWithADeterministicKeyAndTheExactBody(t *testing.T) {
	f := readyFixture(t, nil)
	_, _ = f.g.PollDemand(context.Background(), t0)
	for range 3 {
		if err := f.g.SubmitInboundReceipt(context.Background(), receiptFor("SHP-1")); err != nil {
			t.Fatalf("SubmitInboundReceipt: %v", err)
		}
	}
	posts := f.requestsTo(http.MethodPost, "/vendor/inbound-receipts")
	if len(posts) != 1 {
		t.Fatalf("POSTs = %d, want exactly 1 for three calls", len(posts))
	}
	if posts[0].IdemKey != "nf-rcpt-SHP-1" {
		t.Errorf("Idempotency-Key = %q", posts[0].IdemKey)
	}
	expectReceiptBody(t, posts[0].Body)
	row, found := f.store.row(KindInboundReceipt, "SHP-1")
	if !found || row.TransactionID == "" || row.Status != StatusSuccess {
		t.Errorf("ledger row = %+v, %v", row, found)
	}
}

func expectReceiptBody(t *testing.T, body string) {
	t.Helper()
	var sent inboundReceiptRequestDTO
	if err := json.Unmarshal([]byte(body), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.VendorID != "V-WS" || sent.NodeID != "N-1" || sent.ShipmentID != "SHP-1" || len(sent.Cartons) != 2 ||
		sent.Cartons[0].Items[0].QtyEaches != 12 || sent.Cartons[1].Status != "MISSING" ||
		sent.Cartons[1].Items == nil || sent.Cartons[1].ComplianceCodes == nil {
		t.Errorf("body = %s", body)
	}
}

func TestInbound_ReceiptRetriesATransientFailureWithTheSameKey(t *testing.T) {
	f := readyFixture(t, nil)
	_, _ = f.g.PollDemand(context.Background(), t0)
	f.sim.inject = failFirst(http.MethodPost, "/vendor/inbound-receipts", 2, http.StatusServiceUnavailable, nil)
	if err := f.g.SubmitInboundReceipt(context.Background(), receiptFor("SHP-1")); err != nil {
		t.Fatalf("SubmitInboundReceipt: %v", err)
	}
	posts := f.requestsTo(http.MethodPost, "/vendor/inbound-receipts")
	if len(posts) != 3 {
		t.Fatalf("attempts = %d, want 3 (two 503s then 202)", len(posts))
	}
	for _, p := range posts {
		if p.IdemKey != "nf-rcpt-SHP-1" || p.Body != posts[0].Body {
			t.Errorf("a retry changed the key or the body: %+v", p)
		}
	}
}

func TestInbound_ReceiptTheNetworkFailsIsReportedAsAnError(t *testing.T) {
	f := readyFixture(t, nil)
	_, _ = f.g.PollDemand(context.Background(), t0)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: "ErrCartonNotOnManifest", Message: "no such carton"})
	err := f.g.SubmitInboundReceipt(context.Background(), receiptFor("SHP-1"))
	if !errors.Is(err, ErrWriteRefused) || !strings.Contains(err.Error(), "no such carton") {
		t.Fatalf("err = %v, want ErrWriteRefused carrying the network's errors", err)
	}
}

func TestInbound_ADefinitiveRefusalIsRecordedOnceAndNeverResent(t *testing.T) {
	f := readyFixture(t, nil)
	_, _ = f.g.PollDemand(context.Background(), t0)
	f.sim.inject = refuseFirst(http.MethodPost, "/vendor/inbound-receipts", 1, http.StatusUnprocessableEntity, "validation-failed")
	for range 2 {
		if err := f.g.SubmitInboundReceipt(context.Background(), receiptFor("SHP-1")); !errors.Is(err, ErrWriteRefused) {
			t.Fatalf("err = %v, want ErrWriteRefused", err)
		}
	}
	if n := len(f.requestsTo(http.MethodPost, "/vendor/inbound-receipts")); n != 1 {
		t.Errorf("POSTs = %d, want 1: the refusal is stored and the second call sends nothing", n)
	}
}

func TestInbound_ReceiptForASiteWithoutANodeIsRefusedBeforeAnyIO(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-9"}})
	r := receiptFor("SHP-1")
	r.SiteId = "FC-9"
	if err := f.g.SubmitInboundReceipt(context.Background(), r); !errors.Is(err, ErrUnknownSite) {
		t.Fatalf("err = %v, want ErrUnknownSite", err)
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Errorf("%d requests, want none", n)
	}
}

// ---- the contract (ADR 0028 "Contract test"): the vendored spec is the judge ----

func TestInboundContract_TheReceiptBodyValidatesAndSendsOnlyListedProperties(t *testing.T) {
	spec := loadSpec(t)
	body, err := buildInboundReceiptBody("V-WS", "N-1", receiptFor("SHP-1"))
	if err != nil {
		t.Fatal(err)
	}
	spec.validate(t, "InboundReceiptRequest", body)
	var sent map[string]json.RawMessage
	_ = json.Unmarshal(body, &sent)
	for key := range sent {
		if !slices.Contains(spec.properties(t, "InboundReceiptRequest"), key) {
			t.Errorf("the body sends %q, which InboundReceiptRequest does not list", key)
		}
	}
	for _, name := range []string{"InboundReceiptRequest", "InboundReceiptCarton", "InboundReceiptItem"} {
		for _, req := range spec.required(t, name) {
			if !slices.Contains(jsonFieldsFor(name), req) {
				t.Errorf("%s: the adapter never sends required property %q", name, req)
			}
		}
	}
	for _, status := range []contract.ReceiptStatus{contract.ReceiptReceived, contract.ReceiptMissing, contract.ReceiptUnexpected, contract.ReceiptDamaged} {
		if !slices.Contains(spec.enum(t, "InboundReceiptCarton", "status"), string(status)) {
			t.Errorf("status %s is not in the spec", status)
		}
	}
}

func jsonFieldsFor(name string) []string {
	switch name {
	case "InboundReceiptRequest":
		return jsonFields(reflect.TypeOf(inboundReceiptRequestDTO{}))
	case "InboundReceiptCarton":
		return jsonFields(reflect.TypeOf(receiptCartonDTO{}))
	default:
		return jsonFields(reflect.TypeOf(receiptItemDTO{}))
	}
}

func TestInboundContract_ARecordedShipmentPageIsValidAndDecodes(t *testing.T) {
	spec := loadSpec(t)
	page := `{"inboundShipments":[{"shipmentId":"SHP-1001","sellerId":"S-100","nodeId":"n-1","shippedAt":"2026-10-24T13:00:00Z","status":"SHIPPED",
		"pallets":[{"sscc":"006141410000000036","cartons":[{"sscc":"006141410000000012","parentSscc":"006141410000000036","kind":"HOMOGENEOUS_CASE",
		"contents":[{"gtin":"00614141000036","qtyEaches":12,"lot":"L-77","expiry":"2027-03-01"}],"grossWeightG":4200,"dimsMm":{"l":400,"w":300,"h":200}}]}],
		"cartons":[{"sscc":"006141410000000029","kind":"MIXED","contents":[{"gtin":"00614141000036","qtyEaches":6}],"grossWeightG":2100}]}],"nextToken":"abc"}`
	spec.validate(t, "InboundShipmentPage", []byte(page))
	var dto inboundShipmentPageDTO
	if err := json.Unmarshal([]byte(page), &dto); err != nil {
		t.Fatal(err)
	}
	s := shipmentFromDTO(dto.InboundShipments[0], "FC-1")
	if s.Ref != "SHP-1001" || s.OwnerRef != "S-100" || len(s.Cartons) != 2 || s.Cartons[0].ParentSSCC != ssccPallet || s.Cartons[1].GrossWeightG != 2100 {
		t.Errorf("decoded = %+v", s)
	}
}

func TestInboundContract_EveryRouteTheInboundClientCallsExistsWithItsParameters(t *testing.T) {
	spec := loadSpec(t)
	paths, _ := spec.root["paths"].(map[string]any)
	f := newFake(t, func(_ int, w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			ok(w, map[string]string{"transactionId": "tx-1"})
			return
		}
		ok(w, map[string]any{"inboundShipments": []any{}})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	_, _ = c.inboundPage(context.Background(), "N-1", t0, "")
	_, _ = c.postAsync(context.Background(), "submitInboundReceipt", pathInboundReceipts, "k", []byte("{}"))
	for _, r := range f.requests() {
		item := lookupPath(paths, r.Path)
		if item == nil {
			t.Fatalf("%s is not a path of the spec", r.Path)
		}
		op, _ := item[strings.ToLower(r.Method)].(map[string]any)
		if op == nil {
			t.Fatalf("%s %s: no such operation", r.Method, r.Path)
		}
		refs := parameterRefs(op)
		if r.Method == http.MethodPost && (!slices.Contains(refs, "#/components/parameters/IdempotencyKey") || r.IdemKey == "") {
			t.Errorf("POST %s: Idempotency-Key listed=%v sent=%q", r.Path, refs, r.IdemKey)
		}
		if r.Method == http.MethodGet && (!slices.Contains(refs, "#/components/parameters/VendorId") || !strings.Contains(r.RawQuery, "nodeId=N-1")) {
			t.Errorf("GET %s: vendorId listed=%v query=%q", r.Path, refs, r.RawQuery)
		}
	}
}
