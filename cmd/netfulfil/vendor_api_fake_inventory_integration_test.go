//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// The inventory half of vendorFake (ADR 0027 part 2): POST /vendor/inventory as
// the Vendor API describes it, asynchronous and idempotent per key like every
// other write, with the one rule the adapter's failure handling rests on: a feed
// that names a listing the node does not carry fails its whole transaction with
// ErrListingNotOnNode, at the line the JSON path points to, and applies nothing.
// A full feed zeroes the listings it omits, which is why the service must never
// send one that is not complete.

type fakeInventoryLine struct {
	ListingID    string `json:"listingId"`
	AvailableQty int    `json:"availableQty"`
	Basis        string `json:"basis"`
}

type fakeInventoryRequest struct {
	VendorID   string              `json:"vendorId"`
	NodeID     string              `json:"nodeId"`
	FullUpdate bool                `json:"fullUpdate"`
	AsOf       time.Time           `json:"asOf"`
	Lines      []fakeInventoryLine `json:"lines"`
}

// writeInventory is POST /vendor/inventory. A replay of a key is answered by
// write() before it gets here. The feed is refused up front (422) when it is not
// one the network could take; whether each listing is on the node is judged when
// the transaction settles.
func (v *vendorFake) writeInventory(w http.ResponseWriter, slot string, raw []byte) {
	var req fakeInventoryRequest
	if err := json.Unmarshal(raw, &req); err != nil || req.VendorID != v.vendorID || v.nodeByID(req.NodeID) == nil ||
		req.AsOf.IsZero() || len(req.Lines) < 1 || len(req.Lines) > 5000 {
		fakeProblem(w, http.StatusUnprocessableEntity, "validation-failed")
		return
	}
	for _, l := range req.Lines {
		if l.ListingID == "" || l.AvailableQty < 0 || (l.Basis != "PHYSICAL" && l.Basis != "THROUGHPUT_CONSTRAINED" && l.Basis != "UNKNOWN") {
			fakeProblem(w, http.StatusUnprocessableEntity, "validation-failed")
			return
		}
	}
	v.seq++
	tx := &fakeTx{id: fmt.Sprintf("tx-%d", v.seq), kind: "INVENTORY", status: "PROCESSING", nodeID: req.NodeID, body: string(raw)}
	v.txs[tx.id] = tx
	v.writes[slot] = fakeWrite{body: string(raw), txID: tx.id, key: strings.TrimPrefix(slot, http.MethodPost+" /vendor/inventory ")}
	v.inventoryKeys = append(v.inventoryKeys, slot)
	fakeOK(w, http.StatusAccepted, map[string]any{"transactionId": tx.id})
}

// settleInventory judges a feed: every listing must be on the node, else the
// whole transaction fails and nothing is applied; otherwise the node's stock is
// replaced line by line, and a full feed zeroes every listing it leaves out.
func (v *vendorFake) settleInventory(tx *fakeTx) {
	var req fakeInventoryRequest
	_ = json.Unmarshal([]byte(tx.body), &req)
	carried := v.listings[tx.nodeID]
	var errs []map[string]string
	for i, l := range req.Lines {
		if carried != nil && !carried[l.ListingID] {
			errs = append(errs, map[string]string{
				"code": "ErrListingNotOnNode", "message": fmt.Sprintf("listing %s is not on node %s", l.ListingID, tx.nodeID),
				"path": fmt.Sprintf("$.lines[%d].listingId", i),
			})
		}
	}
	if len(errs) > 0 {
		tx.status, tx.errors = "FAILURE", errs
		return
	}
	if v.stock == nil {
		v.stock = map[string]map[string]int{}
	}
	held := v.stock[tx.nodeID]
	if held == nil {
		held = map[string]int{}
		v.stock[tx.nodeID] = held
	}
	if req.FullUpdate {
		for id := range held {
			held[id] = 0
		}
	}
	for _, l := range req.Lines {
		held[l.ListingID] = l.AvailableQty
	}
}

// setNodeListings declares which listings the node carries; a feed naming any
// other fails. Without a call a node accepts every listing.
func (v *vendorFake) setNodeListings(nodeID string, ids ...string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.listings == nil {
		v.listings = map[string]map[string]bool{}
	}
	carried := map[string]bool{}
	for _, id := range ids {
		carried[id] = true
	}
	v.listings[nodeID] = carried
}

// fakeInventoryFeed is one distinct inventory write (one per idempotency key)
// and how its transaction ended.
type fakeInventoryFeed struct {
	Key, NodeID, Status string
	Full                bool
	AsOf                time.Time
	Lines               []fakeInventoryLine
	Errors              []map[string]string
}

// inventoryFeeds are the distinct feeds the network took, in order.
func (v *vendorFake) inventoryFeeds() []fakeInventoryFeed {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []fakeInventoryFeed
	for _, slot := range v.inventoryKeys {
		w := v.writes[slot]
		var req fakeInventoryRequest
		_ = json.Unmarshal([]byte(w.body), &req)
		tx := v.txs[w.txID]
		out = append(out, fakeInventoryFeed{
			Key: w.key, NodeID: req.NodeID, Status: tx.status, Full: req.FullUpdate, AsOf: req.AsOf, Lines: req.Lines, Errors: tx.errors,
		})
	}
	return out
}

// stockOf is the quantity the network holds for a listing of a node; ok is false
// when no applied feed ever named it.
func (v *vendorFake) stockOf(nodeID, listingID string) (qty int, ok bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	qty, ok = v.stock[nodeID][listingID]
	return qty, ok
}

// usableStock stands in for inventory-storage's usable-inventory read model,
// GET /inventory/{sku}/usable: what the capability offers are computed from.
type usableStock struct {
	srv *httptest.Server

	mu     sync.Mutex
	usable map[string]int
}

func newUsableStock(t *testing.T, usable map[string]int) *usableStock {
	t.Helper()
	s := &usableStock{usable: usable}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sku := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/inventory/"), "/usable")
		s.mu.Lock()
		n := s.usable[sku] // an unknown SKU is 0, by that endpoint's own contract
		s.mu.Unlock()
		fakeOK(w, http.StatusOK, map[string]any{"sku": sku, "usable": n})
	}))
	t.Cleanup(s.srv.Close)
	return s
}
