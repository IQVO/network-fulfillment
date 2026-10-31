//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// The three fake services the inbound loop (ADR 0028) talks to besides the
// Vendor API's demand half: the inbound half of the Vendor API (wrapped around
// the demand fake), inbound-receiving and product-master. All are standard
// httptest servers; Postgres and Kafka stay real.

type fakeShipment struct {
	ID        string
	Seller    string
	NodeCode  string
	ShippedAt time.Time
	Status    string
	Pallets   []map[string]any
	Cartons   []map[string]any
}

// inboundNetwork serves the inbound routes of the Vendor API and hands every
// other path to the demand fake.
type inboundNetwork struct {
	srv    *httptest.Server
	demand *vendorFake

	mu        sync.Mutex
	shipments []fakeShipment
	polls     []string // raw queries of GET /vendor/inbound-shipments
	receipts  map[string]string
	keyTx     map[string]string
	bodies    map[string]string // idempotency key -> body
	posts     int
	txs       map[string]string // transaction id -> shipment
	status    string            // status a new receipt transaction ends with
	errors    []map[string]string
}

func newInboundNetwork(t *testing.T, demand *vendorFake) *inboundNetwork {
	t.Helper()
	n := &inboundNetwork{demand: demand, receipts: map[string]string{}, keyTx: map[string]string{}, bodies: map[string]string{}, txs: map[string]string{}, status: "SUCCESS"}
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *inboundNetwork) ship(s fakeShipment) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s.Status == "" {
		s.Status = "SHIPPED"
	}
	n.shipments = append(n.shipments, s)
}

func (n *inboundNetwork) pollQueries() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.polls...)
}

func (n *inboundNetwork) receiptBody(shipment string) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.receipts[shipment]
}

func (n *inboundNetwork) receiptPosts() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.posts
}

func (n *inboundNetwork) failReceipts(errs ...map[string]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status, n.errors = "FAILURE", errs
}

func (n *inboundNetwork) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/vendor/inbound-shipments":
		n.poll(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/vendor/inbound-receipts":
		n.receipt(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/vendor/transactions/") && n.knowsTx(strings.TrimPrefix(r.URL.Path, "/vendor/transactions/")):
		n.transaction(w, strings.TrimPrefix(r.URL.Path, "/vendor/transactions/"))
	default:
		n.demand.serve(w, r)
	}
}

func (n *inboundNetwork) knowsTx(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.txs[id]
	return ok
}

func (n *inboundNetwork) poll(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.polls = append(n.polls, r.URL.RawQuery)
	nodeID := r.URL.Query().Get("nodeId")
	out := []map[string]any{}
	for _, s := range n.shipments {
		if s.Status != "SHIPPED" || n.demand.nodeIDOf(s.NodeCode) != nodeID {
			continue
		}
		out = append(out, map[string]any{
			"shipmentId": s.ID, "sellerId": s.Seller, "nodeId": nodeID, "shippedAt": s.ShippedAt.UTC().Format(time.RFC3339Nano),
			"status": s.Status, "pallets": orEmpty(s.Pallets), "cartons": orEmpty(s.Cartons),
		})
	}
	fakeOK(w, http.StatusOK, map[string]any{"inboundShipments": out})
}

func orEmpty(v []map[string]any) []map[string]any {
	if v == nil {
		return []map[string]any{}
	}
	return v
}

func (n *inboundNetwork) receipt(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.posts++
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		fakeProblem(w, http.StatusBadRequest, "idempotency-key-required")
		return
	}
	if prev, seen := n.bodies[key]; seen && prev != string(raw) {
		fakeProblem(w, http.StatusUnprocessableEntity, "idempotency-key-reused")
		return
	}
	id, replay := n.keyTx[key]
	if !replay {
		var body struct {
			ShipmentID string `json:"shipmentId"`
		}
		_ = json.Unmarshal(raw, &body)
		id = fmt.Sprintf("tx-rcpt-%d", len(n.keyTx)+1)
		n.keyTx[key], n.bodies[key], n.txs[id] = id, string(raw), body.ShipmentID
		n.receipts[body.ShipmentID] = string(raw)
	}
	fakeOK(w, http.StatusAccepted, map[string]string{"transactionId": id})
}

func (n *inboundNetwork) transaction(w http.ResponseWriter, id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]any{"transactionId": id, "vendorId": n.demand.vendorID, "kind": "INBOUND_RECEIPT", "status": n.status, "receivedAt": time.Now().UTC()}
	if len(n.errors) > 0 {
		out["errors"] = n.errors
	}
	fakeOK(w, http.StatusOK, out)
}

// fakeInboundReceiving is inbound-receiving's POST /asns (and nothing else).
type fakeInboundReceiving struct {
	srv *httptest.Server
	mu  sync.Mutex
	// asns is every distinct ASN number registered, with the first body.
	asns     map[string]string
	keys     map[string]string
	requests int
	refuse   map[string]string // asn number -> problem slug to answer with
}

func newFakeInboundReceiving(t *testing.T) *fakeInboundReceiving {
	t.Helper()
	f := &fakeInboundReceiving{asns: map[string]string{}, keys: map[string]string{}, refuse: map[string]string{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeInboundReceiving) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method != http.MethodPost || r.URL.Path != "/asns" {
		fakeProblem(w, http.StatusNotFound, "not-found")
		return
	}
	f.requests++
	var body struct {
		AsnNumber string `json:"asnNumber"`
	}
	_ = json.Unmarshal(raw, &body)
	if slug := f.refuse[body.AsnNumber]; slug != "" {
		fakeProblem(w, http.StatusUnprocessableEntity, slug)
		return
	}
	if _, exists := f.asns[body.AsnNumber]; exists {
		fakeProblem(w, http.StatusConflict, "asn-already-exists")
		return
	}
	f.asns[body.AsnNumber], f.keys[body.AsnNumber] = string(raw), r.Header.Get("Idempotency-Key")
	fakeOK(w, http.StatusCreated, map[string]any{"asnNumber": body.AsnNumber, "state": "Registered"})
}

func (f *fakeInboundReceiving) asnCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.asns)
}

func (f *fakeInboundReceiving) body(asn string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.asns[asn]
}

func (f *fakeInboundReceiving) key(asn string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keys[asn]
}

func (f *fakeInboundReceiving) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

// fakeProductMaster is product-master's GET /gtins/{gtin}.
type fakeProductMaster struct {
	srv   *httptest.Server
	mu    sync.Mutex
	gtins map[string]map[string]any
	asked map[string]int
}

func newFakeProductMaster(t *testing.T) *fakeProductMaster {
	t.Helper()
	f := &fakeProductMaster{gtins: map[string]map[string]any{}, asked: map[string]int{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeProductMaster) declare(gtin, sku, level string, unitsPerPack int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gtins[gtin] = map[string]any{"gtin": gtin, "sku": sku, "level": level, "unitsPerPack": unitsPerPack, "active": true, "effectiveFrom": "2026-10-06T20:00:00Z"}
}

func (f *fakeProductMaster) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gtin := strings.TrimPrefix(r.URL.Path, "/gtins/")
	f.asked[gtin]++
	if g, ok := f.gtins[gtin]; ok {
		fakeOK(w, http.StatusOK, g)
		return
	}
	fakeProblem(w, http.StatusNotFound, "gtin-not-found")
}

func (f *fakeProductMaster) timesAsked(gtin string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.asked[gtin]
}

// nodeIDOf is the id of the node registered under code, "" when none is.
func (v *vendorFake) nodeIDOf(code string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.nodeID(code)
}
