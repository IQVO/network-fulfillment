//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// vendorFake is a stateful stand-in for the retail network's Vendor API, just
// enough of it for the live demand loop (ADR 0026) to run through the REAL
// entry point: nodes (list, get, register), the node profile (ADR 0027),
// purchase orders (poll, get), acknowledgements, transactions, labels and
// shipment confirmations.
//
// It keeps the network's rules that the adapter's crash safety rests on:
//   - every write needs an Idempotency-Key; the same key and body replays the
//     first answer (the same transactionId), the same key with another body is
//     422 idempotency-key-reused;
//   - a purchase order is acknowledged once: a second acknowledgement under a
//     new key is 409 already-acknowledged;
//   - an acknowledgement takes effect only when its transaction is SUCCESS
//     (the order stays ISSUED, and so is delivered again by a poll, until then);
//   - a label needs an acknowledged order, and a shipment confirmation needs
//     exactly the labelled tracking numbers;
//   - vendorId must be the one the fake serves;
//   - a node profile revision that is not above the one the node holds fails
//     its transaction with ErrStaleProfileRevision (not an HTTP error), as
//     node-topology does.
//
// It is a copy of the minimum of the adapter package's own test fake, which is
// test-only there and written against its unexported DTOs.
type vendorFake struct {
	srv      *httptest.Server
	vendorID string

	mu       sync.Mutex
	nodes    []fakeNode
	orders   []*fakeOrder
	txs      map[string]*fakeTx
	writes   map[string]fakeWrite // "METHOD path key" -> first body and answer
	requests []fakeRequest
	seq      int

	// profileKeys are the write slots of the node profile PUTs, in arrival order.
	profileKeys []string

	// inventoryKeys are the write slots of the inventory feeds (POST
	// /vendor/inventory), in arrival order; listings are the listing ids each node
	// carries (a node with no entry accepts any); stock is what the network holds
	// per node and listing after the feeds it applied.
	inventoryKeys []string
	listings      map[string]map[string]bool
	stock         map[string]map[string]int

	// holdAcks keeps acknowledgement transactions PROCESSING until
	// releaseAcks; otherwise one read answers PROCESSING and the next SUCCESS.
	holdAcks bool
}

type fakeNode struct {
	id, code        string
	profileRevision int
}

type fakeOrder struct {
	poNumber, nodeCode, state string
	listingID                 string
	quantity                  int
	issuedAt, shipBy, ackBy   time.Time
	ackTx                     string
	labels                    []fakeLabel
}

type fakeLabel struct {
	PackageNo      int    `json:"packageNo"`
	LabelRef       string `json:"labelRef"`
	TrackingNumber string `json:"trackingNumber"`
	Carrier        string `json:"carrier"`
}

type fakeTx struct {
	id, kind, status, poNumber, nodeCode, nodeID string
	reads                                        int
	revision                                     int                 // a NODE_PROFILE's profileRevision
	body                                         string              // an INVENTORY feed's request body
	errors                                       []map[string]string // why a FAILURE failed
}

type fakeWrite struct {
	body   string
	txID   string
	key    string
	labels []fakeLabel
}

type fakeRequest struct{ Method, Path, Key, Body string }

func newVendorFake(t *testing.T, vendorID string) *vendorFake {
	t.Helper()
	v := &vendorFake{vendorID: vendorID, txs: map[string]*fakeTx{}, writes: map[string]fakeWrite{}}
	v.srv = httptest.NewServer(http.HandlerFunc(v.serve))
	t.Cleanup(v.srv.Close)
	return v
}

// ---- test-facing controls and observations ----

// addNode puts an already registered node on the network (a site to adopt).
func (v *vendorFake) addNode(code string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.addNodeLocked(code)
}

func (v *vendorFake) addNodeLocked(code string) string {
	id := fmt.Sprintf("N-%d", len(v.nodes)+1)
	v.nodes = append(v.nodes, fakeNode{id: id, code: code})
	return id
}

// issue makes the network issue a one-line purchase order to nodeCode.
func (v *vendorFake) issue(poNumber, nodeCode, listingID string, quantity int, now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.orders = append(v.orders, &fakeOrder{
		poNumber: poNumber, nodeCode: nodeCode, state: "ISSUED", listingID: listingID, quantity: quantity,
		issuedAt: now, shipBy: now.Add(48 * time.Hour), ackBy: now.Add(6 * time.Hour),
	})
}

// holdAckTransactions keeps acknowledgement transactions PROCESSING until
// releaseAcks.
func (v *vendorFake) holdAckTransactions() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.holdAcks = true
}

// releaseAcks lets held acknowledgement transactions settle on their next read.
func (v *vendorFake) releaseAcks() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.holdAcks = false
}

// requestCount counts every HTTP request of method on path, replays included.
func (v *vendorFake) requestCount(method, path string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	n := 0
	for _, r := range v.requests {
		if r.Method == method && r.Path == path {
			n++
		}
	}
	return n
}

// writeCount counts the distinct writes created on path (one per idempotency key).
func (v *vendorFake) writeCount(path string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	n := 0
	for slot := range v.writes {
		if strings.HasPrefix(slot, http.MethodPost+" "+path+" ") {
			n++
		}
	}
	return n
}

func (v *vendorFake) orderState(poNumber string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	if o := v.order(poNumber); o != nil {
		return o.state
	}
	return ""
}

// ackByOf is the acknowledgement deadline the network gave the order.
func (v *vendorFake) ackByOf(poNumber string) time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	if o := v.order(poNumber); o != nil {
		return o.ackBy
	}
	return time.Time{}
}

func (v *vendorFake) nodeCodes() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []string
	for _, n := range v.nodes {
		out = append(out, n.code)
	}
	return out
}

// fakeProfileWrite is one distinct node profile write (one per idempotency key).
type fakeProfileWrite struct {
	Key      string
	Revision int
	Body     string
}

// profileWrites are the distinct profile writes the network took, in order.
func (v *vendorFake) profileWrites() []fakeProfileWrite {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []fakeProfileWrite
	for _, key := range v.profileKeys {
		w := v.writes[key]
		out = append(out, fakeProfileWrite{Key: w.key, Revision: v.txs[w.txID].revision, Body: w.body})
	}
	return out
}

// profileRevisionOf is the profile revision the network holds for the node.
func (v *vendorFake) profileRevisionOf(nodeID string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	if n := v.nodeByID(nodeID); n != nil {
		return n.profileRevision
	}
	return 0
}

// bodyOf returns the first body written on path under any key.
func (v *vendorFake) bodyOf(path string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, r := range v.requests {
		if r.Method == http.MethodPost && r.Path == path {
			return r.Body
		}
	}
	return ""
}

// ---- the API ----

func (v *vendorFake) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	v.mu.Lock()
	defer v.mu.Unlock()
	v.requests = append(v.requests, fakeRequest{Method: r.Method, Path: r.URL.Path, Key: r.Header.Get("Idempotency-Key"), Body: string(raw)})

	switch {
	case r.Method == http.MethodGet:
		v.read(w, r)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/vendor/nodes/") && strings.HasSuffix(r.URL.Path, "/profile"):
		v.writeProfile(w, r, raw)
	case r.Method == http.MethodPost:
		v.write(w, r, raw)
	default:
		fakeProblem(w, http.StatusNotFound, "not-found")
	}
}

// read routes the GETs; every one needs the vendorId of the account.
func (v *vendorFake) read(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("vendorId") != v.vendorID {
		fakeProblem(w, http.StatusNotFound, "vendor-not-found")
		return
	}
	switch path := r.URL.Path; {
	case path == "/vendor/nodes":
		v.listNodes(w)
	case strings.HasPrefix(path, "/vendor/nodes/"):
		v.getNode(w, strings.TrimPrefix(path, "/vendor/nodes/"))
	case path == "/vendor/purchase-orders":
		v.pollOrders(w)
	case strings.HasPrefix(path, "/vendor/purchase-orders/"):
		v.getOrder(w, strings.TrimPrefix(path, "/vendor/purchase-orders/"))
	case strings.HasPrefix(path, "/vendor/transactions/"):
		v.getTx(w, strings.TrimPrefix(path, "/vendor/transactions/"))
	default:
		fakeProblem(w, http.StatusNotFound, "not-found")
	}
}

func fakeProblem(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": slug, "title": slug, "status": status})
}

func fakeOK(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (v *vendorFake) nodeID(code string) string {
	for _, n := range v.nodes {
		if n.code == code {
			return n.id
		}
	}
	return ""
}

func (v *vendorFake) order(poNumber string) *fakeOrder {
	for _, o := range v.orders {
		if o.poNumber == poNumber {
			return o
		}
	}
	return nil
}

func (v *vendorFake) nodeByID(id string) *fakeNode {
	for i := range v.nodes {
		if v.nodes[i].id == id {
			return &v.nodes[i]
		}
	}
	return nil
}

func (v *vendorFake) nodeJSON(n fakeNode) map[string]any {
	out := map[string]any{"nodeId": n.id, "vendorId": v.vendorID, "vendorNodeCode": n.code, "nodeType": "FULFILLMENT_CENTER", "status": "ACTIVE"}
	if n.profileRevision > 0 {
		out["profileRevision"] = n.profileRevision
	}
	return out
}

func (v *vendorFake) listNodes(w http.ResponseWriter) {
	nodes := make([]map[string]any, 0, len(v.nodes))
	for _, n := range v.nodes {
		nodes = append(nodes, v.nodeJSON(n))
	}
	fakeOK(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func (v *vendorFake) getNode(w http.ResponseWriter, id string) {
	n := v.nodeByID(id)
	if n == nil {
		fakeProblem(w, http.StatusNotFound, "not-found")
		return
	}
	fakeOK(w, http.StatusOK, v.nodeJSON(*n))
}

func (v *vendorFake) orderJSON(o *fakeOrder) map[string]any {
	out := map[string]any{
		"poNumber": o.poNumber, "vendorId": v.vendorID, "nodeId": v.nodeID(o.nodeCode), "state": o.state,
		"issuedAt": o.issuedAt, "requiredShipBy": o.shipBy, "ackBy": o.ackBy,
		"lines": []map[string]any{{"lineNo": 1, "listingId": o.listingID, "quantity": o.quantity}},
	}
	if o.ackTx != "" {
		out["acknowledgement"] = map[string]any{"decision": "ACCEPT", "transactionId": o.ackTx}
	}
	return out
}

// pollOrders lists every order whose node exists, whatever its state: the
// adapter keeps only ISSUED ones, and an issued one is delivered again on
// every poll until its acknowledgement succeeded.
func (v *vendorFake) pollOrders(w http.ResponseWriter) {
	page := make([]map[string]any, 0, len(v.orders))
	for _, o := range v.orders {
		if v.nodeID(o.nodeCode) != "" {
			page = append(page, v.orderJSON(o))
		}
	}
	fakeOK(w, http.StatusOK, map[string]any{"purchaseOrders": page})
}

func (v *vendorFake) getOrder(w http.ResponseWriter, poNumber string) {
	o := v.order(poNumber)
	if o == nil {
		fakeProblem(w, http.StatusNotFound, "not-found")
		return
	}
	fakeOK(w, http.StatusOK, v.orderJSON(o))
}

// getTx reads a transaction. A node registration and a shipment confirmation
// succeed at once; an acknowledgement answers PROCESSING on its first read (and
// for as long as holdAcks), then SUCCESS.
func (v *vendorFake) getTx(w http.ResponseWriter, id string) {
	tx := v.txs[id]
	if tx == nil {
		fakeProblem(w, http.StatusNotFound, "not-found")
		return
	}
	tx.reads++
	if tx.status == "PROCESSING" && (tx.kind != "ACKNOWLEDGEMENT" || (!v.holdAcks && tx.reads > 1)) {
		v.settle(tx)
	}
	out := map[string]any{"transactionId": tx.id, "vendorId": v.vendorID, "kind": tx.kind, "status": tx.status, "receivedAt": time.Now().UTC()}
	if tx.nodeID != "" {
		out["nodeId"] = tx.nodeID
	}
	if tx.poNumber != "" {
		out["poNumber"] = tx.poNumber
	}
	if len(tx.errors) > 0 {
		out["errors"] = tx.errors
	}
	fakeOK(w, http.StatusOK, out)
}

func (v *vendorFake) settle(tx *fakeTx) {
	tx.status = "SUCCESS"
	switch tx.kind {
	case "NODE_PROFILE":
		n := v.nodeByID(tx.nodeID)
		if n == nil || tx.revision <= n.profileRevision {
			held := 0
			if n != nil {
				held = n.profileRevision
			}
			tx.status = "FAILURE"
			tx.errors = []map[string]string{{"code": "ErrStaleProfileRevision", "message": fmt.Sprintf("revision %d is not above the %d the node holds", tx.revision, held), "path": "$.profileRevision"}}
			return
		}
		n.profileRevision = tx.revision
	case "NODE_REGISTRATION":
		tx.nodeID = v.addNodeLocked(tx.nodeCode)
	case "ACKNOWLEDGEMENT":
		v.order(tx.poNumber).state = "ACKNOWLEDGED"
	case "SHIPMENT_CONFIRMATION":
		v.order(tx.poNumber).state = "SHIPPED"
	case "INVENTORY":
		v.settleInventory(tx)
	}
}

// write handles every POST with the network's idempotency rules.
func (v *vendorFake) write(w http.ResponseWriter, r *http.Request, raw []byte) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		fakeProblem(w, http.StatusBadRequest, "idempotency-key-required")
		return
	}
	var req struct {
		VendorID       string `json:"vendorId"`
		PoNumber       string `json:"poNumber"`
		VendorNodeCode string `json:"vendorNodeCode"`
		Decision       string `json:"decision"`
		Lines          []struct {
			LineNo   int `json:"lineNo"`
			Quantity int `json:"quantity"`
		} `json:"lines"`
		Packages []struct {
			PackageNo int `json:"packageNo"`
		} `json:"packages"`
		TrackingNumbers []string `json:"trackingNumbers"`
	}
	if err := json.Unmarshal(raw, &req); err != nil || req.VendorID != v.vendorID {
		fakeProblem(w, http.StatusBadRequest, "validation-failed")
		return
	}
	slot := r.Method + " " + r.URL.Path + " " + key
	if prev, seen := v.writes[slot]; seen {
		if prev.body != string(raw) {
			fakeProblem(w, http.StatusUnprocessableEntity, "idempotency-key-reused")
			return
		}
		if prev.labels != nil {
			fakeOK(w, http.StatusOK, map[string]any{"poNumber": req.PoNumber, "labels": prev.labels})
			return
		}
		fakeOK(w, http.StatusAccepted, map[string]any{"transactionId": prev.txID})
		return
	}
	switch r.URL.Path {
	case "/vendor/nodes":
		v.accept(w, slot, raw, &fakeTx{kind: "NODE_REGISTRATION", nodeCode: req.VendorNodeCode})
	case "/vendor/acknowledgements":
		v.acknowledge(w, slot, raw, req.PoNumber, req.Decision, len(req.Lines), func(i int) int { return req.Lines[i].Quantity })
	case "/vendor/shipping-labels":
		v.label(w, slot, raw, req.PoNumber, len(req.Packages))
	case "/vendor/shipment-confirmations":
		v.confirmShipment(w, slot, raw, req.PoNumber, req.TrackingNumbers)
	case "/vendor/inventory":
		v.writeInventory(w, slot, raw)
	default:
		fakeProblem(w, http.StatusNotFound, "not-found")
	}
}

// writeProfile is PUT /vendor/nodes/{nodeId}/profile: asynchronous, idempotent
// per key, and refused up front (422) when the body is not a profile the
// network could take. Whether the revision is above the node's is judged when
// the transaction settles.
func (v *vendorFake) writeProfile(w http.ResponseWriter, r *http.Request, raw []byte) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		fakeProblem(w, http.StatusBadRequest, "idempotency-key-required")
		return
	}
	nodeID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/vendor/nodes/"), "/profile")
	var req struct {
		VendorID            string `json:"vendorId"`
		ProfileRevision     int    `json:"profileRevision"`
		Timezone            string `json:"timezone"`
		HandlingTimeMinutes int    `json:"handlingTimeMinutes"`
		Cutoffs             []struct {
			LocalTime string `json:"localTime"`
		} `json:"cutoffs"`
		ThroughputCap struct {
			Value int `json:"value"`
		} `json:"throughputCap"`
	}
	if err := json.Unmarshal(raw, &req); err != nil || req.VendorID != v.vendorID || v.nodeByID(nodeID) == nil {
		fakeProblem(w, http.StatusUnprocessableEntity, "validation-failed")
		return
	}
	if req.ProfileRevision < 1 || req.Timezone == "" || req.HandlingTimeMinutes < 1 || len(req.Cutoffs) == 0 || req.ThroughputCap.Value < 1 {
		fakeProblem(w, http.StatusUnprocessableEntity, "validation-failed")
		return
	}
	slot := r.Method + " " + r.URL.Path + " " + key
	if prev, seen := v.writes[slot]; seen {
		if prev.body != string(raw) {
			fakeProblem(w, http.StatusUnprocessableEntity, "idempotency-key-reused")
			return
		}
		fakeOK(w, http.StatusAccepted, map[string]any{"transactionId": prev.txID})
		return
	}
	v.seq++
	tx := &fakeTx{id: fmt.Sprintf("tx-%d", v.seq), kind: "NODE_PROFILE", status: "PROCESSING", nodeID: nodeID, revision: req.ProfileRevision}
	v.txs[tx.id] = tx
	v.writes[slot] = fakeWrite{body: string(raw), txID: tx.id, key: key}
	v.profileKeys = append(v.profileKeys, slot)
	fakeOK(w, http.StatusAccepted, map[string]any{"transactionId": tx.id})
}

func (v *vendorFake) accept(w http.ResponseWriter, slot string, raw []byte, tx *fakeTx) {
	v.seq++
	tx.id, tx.status = fmt.Sprintf("tx-%d", v.seq), "PROCESSING"
	v.txs[tx.id] = tx
	v.writes[slot] = fakeWrite{body: string(raw), txID: tx.id}
	fakeOK(w, http.StatusAccepted, map[string]any{"transactionId": tx.id})
}

func (v *vendorFake) acknowledge(w http.ResponseWriter, slot string, raw []byte, poNumber, decision string, lines int, qty func(int) int) {
	o := v.order(poNumber)
	switch {
	case o == nil:
		fakeProblem(w, http.StatusNotFound, "not-found")
	case o.state != "ISSUED" || o.ackTx != "":
		fakeProblem(w, http.StatusConflict, "already-acknowledged")
	case decision != "ACCEPT" || lines != 1 || qty(0) != o.quantity:
		// The whole order, every line with its ordered quantity.
		fakeProblem(w, http.StatusBadRequest, "validation-failed")
	default:
		tx := &fakeTx{kind: "ACKNOWLEDGEMENT", poNumber: poNumber}
		v.accept(w, slot, raw, tx)
		o.ackTx = tx.id
	}
}

func (v *vendorFake) label(w http.ResponseWriter, slot string, raw []byte, poNumber string, packages int) {
	o := v.order(poNumber)
	switch {
	case o == nil:
		fakeProblem(w, http.StatusNotFound, "not-found")
		return
	case o.state != "ACKNOWLEDGED":
		fakeProblem(w, http.StatusConflict, "label-before-acceptance")
		return
	case packages < 1:
		fakeProblem(w, http.StatusBadRequest, "validation-failed")
		return
	}
	labels := make([]fakeLabel, packages)
	for i := range labels {
		labels[i] = fakeLabel{PackageNo: i + 1, LabelRef: fmt.Sprintf("LBL-%s-%d", poNumber, i+1), TrackingNumber: fmt.Sprintf("TRK-%s-%d", poNumber, i+1), Carrier: "CARRIER-X"}
	}
	o.labels = labels
	v.writes[slot] = fakeWrite{body: string(raw), labels: labels}
	fakeOK(w, http.StatusOK, map[string]any{"poNumber": poNumber, "labels": labels})
}

func (v *vendorFake) confirmShipment(w http.ResponseWriter, slot string, raw []byte, poNumber string, tracking []string) {
	o := v.order(poNumber)
	switch {
	case o == nil:
		fakeProblem(w, http.StatusNotFound, "not-found")
		return
	case len(o.labels) == 0:
		fakeProblem(w, http.StatusConflict, "confirm-without-label")
		return
	}
	var want []string
	for _, l := range o.labels {
		want = append(want, l.TrackingNumber)
	}
	if !slices.Equal(want, tracking) {
		fakeProblem(w, http.StatusBadRequest, "validation-failed")
		return
	}
	v.accept(w, slot, raw, &fakeTx{kind: "SHIPMENT_CONFIRMATION", poNumber: poNumber})
}

// ---- order-management ----

// planFake stands in for order-management's two endpoints this flow uses
// (the e2e test does the same with a fixed order id): POST /orders holds an
// order, POST /orders/{id}/release releases it. It counts raw requests, so a
// second raise of the same demand is visible even though the Idempotency-Key
// would make a real order-management replay it.
type planFake struct {
	srv *httptest.Server

	mu       sync.Mutex
	raises   int
	keys     map[string]string // Idempotency-Key -> order id
	skus     []string
	releases []string
	cancels  int
}

func newPlanFake(t *testing.T) *planFake {
	t.Helper()
	p := &planFake{keys: map[string]string{}}
	p.srv = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *planFake) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/orders":
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			http.Error(w, `{"type":"idempotency-key-required"}`, http.StatusBadRequest)
			return
		}
		p.raises++
		id, replay := p.keys[key]
		if !replay {
			id = fmt.Sprintf("om-live-%d", len(p.keys)+1)
			p.keys[key] = id
		}
		var body struct {
			Lines []struct {
				SKU      string `json:"sku"`
				Quantity int    `json:"quantity"`
			} `json:"lines"`
		}
		_ = json.Unmarshal(raw, &body)
		for _, l := range body.Lines {
			p.skus = append(p.skus, fmt.Sprintf("%s x%d", l.SKU, l.Quantity))
		}
		fakeOK(w, http.StatusCreated, map[string]any{
			"id": id, "promiseDate": time.Now().Add(12 * time.Hour).UTC(), "lines": []map[string]any{{"status": "HELD"}},
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/orders/") && strings.HasSuffix(r.URL.Path, "/release"):
		p.releases = append(p.releases, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/orders/"), "/release"))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/orders/"):
		p.cancels++
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (p *planFake) snapshot() (raises, orders int, releases []string, cancels int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raises, len(p.keys), slices.Clone(p.releases), p.cancels
}

// heldLines is every "<sku> x<quantity>" the service asked to hold.
func (p *planFake) heldLines() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.skus)
}
