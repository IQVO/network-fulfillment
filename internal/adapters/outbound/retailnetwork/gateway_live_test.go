package retailnetwork

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// failFirst answers the first n requests for method+path with status (and the
// given headers) instead of letting the fake Vendor API see them.
func failFirst(method, path string, n, status int, header map[string]string) func(http.ResponseWriter, *http.Request) bool {
	var count atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != method || r.URL.Path != path || int(count.Add(1)) > n {
			return false
		}
		for k, v := range header {
			w.Header().Set(k, v)
		}
		problem(w, status, "injected")
		return true
	}
}

// slowFirst makes the first n requests for a GET path outlive the call timeout.
func slowFirst(path string, n int, d time.Duration) func(http.ResponseWriter, *http.Request) bool {
	var count atomic.Int32
	return func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == path && int(count.Add(1)) <= n {
			time.Sleep(d)
		}
		return false
	}
}

func (f *liveFixture) requestsTo(method, path string) []seen {
	var out []seen
	for _, r := range f.fake.requests() {
		if r.Method == method && r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *liveFixture) poll(t *testing.T) []string {
	t.Helper()
	got, err := f.g.PollDemand(context.Background(), t0)
	if err != nil {
		t.Fatalf("PollDemand: %v", err)
	}
	refs := make([]string, len(got))
	for i, d := range got {
		refs[i] = string(d.NetworkRef)
	}
	return refs
}

// ---- node registration ----

func TestLive_RegistersAMissingNodeOnceAndPersistsItsNodeID(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-1"}, nodes: []NodeDefinition{fcNode("FC-1")}})

	f.poll(t)
	f.poll(t)

	posts := f.requestsTo(http.MethodPost, "/vendor/nodes")
	if len(posts) != 1 || f.sim.writes("/vendor/nodes") != 1 {
		t.Fatalf("POST /vendor/nodes x%d (network created %d), want exactly one registration over two polls", len(posts), f.sim.writes("/vendor/nodes"))
	}
	if posts[0].IdemKey != "nf-node-FC-1" {
		t.Errorf("Idempotency-Key = %q, want the deterministic nf-node-FC-1", posts[0].IdemKey)
	}
	for _, want := range []string{`"vendorId":"V-WS"`, `"vendorNodeCode":"FC-1"`, `"nodeType":"FULFILLMENT_CENTER"`, `"timezone":"America/Sao_Paulo"`} {
		if !strings.Contains(posts[0].Body, want) {
			t.Errorf("registration body %s lacks %s", posts[0].Body, want)
		}
	}
	id, ok, _ := f.store.NodeIDForSite(context.Background(), "FC-1")
	if !ok || id == "" {
		t.Fatalf("site FC-1 is not mapped to a node after registration (%q, %v)", id, ok)
	}
	row, _ := f.store.row(KindNodeRegistration, "FC-1")
	if row.Status != StatusSuccess || row.TransactionID == "" {
		t.Errorf("ledger row = %+v, want SUCCESS with the transaction id", row)
	}
}

func TestLive_AdoptsAnExistingNodeByItsVendorNodeCodeWithoutRegistering(t *testing.T) {
	f := readyFixture(t, func(c *fixtureCfg) { c.nodes = []NodeDefinition{fcNode("FC-1")} })

	f.poll(t)

	if id, ok, _ := f.store.NodeIDForSite(context.Background(), "FC-1"); !ok || id != "N-1" {
		t.Fatalf("FC-1 -> %q (%v), want the network's N-1", id, ok)
	}
	if site, ok, _ := f.store.SiteForNode(context.Background(), "N-1"); !ok || site != "FC-1" {
		t.Fatalf("N-1 -> %q (%v), want FC-1", site, ok)
	}
	if n := len(f.requestsTo(http.MethodPost, "/vendor/nodes")); n != 0 {
		t.Fatalf("registered %d times; a node the network already lists is adopted", n)
	}
}

func TestLive_ARegistrationStaysPendingUntilTheTransactionSettles(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-1"}, nodes: []NodeDefinition{fcNode("FC-1")}})
	f.sim.setOutcome(txProcessing)

	f.poll(t)
	if _, ok, _ := f.store.NodeIDForSite(context.Background(), "FC-1"); ok {
		t.Fatal("the node was mapped while its registration is still PROCESSING")
	}
	row, _ := f.store.row(KindNodeRegistration, "FC-1")
	if row.Status != StatusProcessing || row.TransactionID != "tx-1" {
		t.Fatalf("ledger row = %+v, want PROCESSING with tx-1", row)
	}

	f.sim.finish("tx-1", txSuccess)
	f.poll(t)

	if id, ok, _ := f.store.NodeIDForSite(context.Background(), "FC-1"); !ok || id == "" {
		t.Fatal("the node was not mapped after the transaction succeeded")
	}
	if n := f.sim.writes("/vendor/nodes"); n != 1 {
		t.Fatalf("network created %d registrations, want 1: the later tick reconciles, it does not re-register", n)
	}
}

func TestLive_ACrashAfterTheLedgerRowResendsTheStoredBodyAndKey(t *testing.T) {
	changed := fcNode("FC-1")
	changed.Timezone = "UTC" // the configuration moved on after the row was written
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-1"}, nodes: []NodeDefinition{changed}})
	stored, err := buildNodeRegistrationBody("V-WS", fcNode("FC-1"))
	if err != nil {
		t.Fatalf("buildNodeRegistrationBody: %v", err)
	}
	if _, err := f.store.BeginSubmission(context.Background(), Submission{
		Kind: KindNodeRegistration, Subject: "FC-1", IdempotencyKey: "nf-node-FC-1", Body: stored,
	}); err != nil {
		t.Fatalf("BeginSubmission: %v", err)
	}

	f.poll(t)

	posts := f.requestsTo(http.MethodPost, "/vendor/nodes")
	if len(posts) != 1 || posts[0].Body != string(stored) || posts[0].IdemKey != "nf-node-FC-1" {
		t.Fatalf("posts = %+v, want one POST with the stored body byte for byte (a rebuilt body is idempotency-key-reused)", posts)
	}
}

func TestLive_ARefusedRegistrationIsRecordedNeverRetriedAndNeverBlocksThePoll(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-1"}, nodes: []NodeDefinition{fcNode("FC-1")}})
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: "ErrVendorNodeCodeTaken", Message: "taken"})

	f.poll(t)
	f.poll(t)

	row, _ := f.store.row(KindNodeRegistration, "FC-1")
	if row.Status != StatusFailure || !strings.Contains(row.Errors, "ErrVendorNodeCodeTaken") {
		t.Fatalf("ledger row = %+v, want FAILURE carrying the network's error", row)
	}
	if n := len(f.requestsTo(http.MethodPost, "/vendor/nodes")); n != 1 {
		t.Fatalf("POST x%d, want 1: a terminal failure is not re-sent every tick", n)
	}
	if !strings.Contains(f.logs.String(), "ErrVendorNodeCodeTaken") {
		t.Errorf("the refusal was not logged for the operator:\n%s", f.logs.String())
	}
}

func TestLive_ASiteWithNoNodeAndNoDefinitionIsLoggedNotFatal(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-9"}})

	f.poll(t)

	if n := len(f.requestsTo(http.MethodPost, "/vendor/nodes")); n != 0 {
		t.Fatalf("registered %d nodes without a definition", n)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "FC-9") {
		t.Errorf("want a warning naming FC-9, got:\n%s", logs)
	}
}

func TestLive_ARegistrationRetriedAfter5xxResendsTheSameKeyAndBody(t *testing.T) {
	f := newLiveFixture(t, fixtureCfg{sites: []string{"FC-1"}, nodes: []NodeDefinition{fcNode("FC-1")}})
	f.sim.inject = failFirst(http.MethodPost, "/vendor/nodes", 2, http.StatusServiceUnavailable, nil)

	f.poll(t)

	posts := f.requestsTo(http.MethodPost, "/vendor/nodes")
	if len(posts) != 3 || f.sim.writes("/vendor/nodes") != 1 {
		t.Fatalf("POST x%d, network created %d; want 3 attempts and ONE registration", len(posts), f.sim.writes("/vendor/nodes"))
	}
	for i, p := range posts {
		if p.IdemKey != posts[0].IdemKey || p.Body != posts[0].Body {
			t.Errorf("attempt %d changed the key or body: %+v", i+1, p)
		}
	}
}

// ---- PollDemand ----

func TestLive_PollDemandFollowsNextTokenAndMapsNodesAndListings(t *testing.T) {
	f := readyFixture(t, nil)
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 2, 1), issuedPO("PO-2", "N-1", 5)}, NextToken: "t1"})
	f.sim.setPage("t1", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-3", "N-1", 1)}})

	got, err := f.g.PollDemand(context.Background(), t0)
	if err != nil || len(got) != 3 {
		t.Fatalf("PollDemand = %d demands, %v; want 3 across two pages", len(got), err)
	}
	d := got[0]
	if d.SiteId != "FC-1" || !d.RequiredShipBy.Equal(shipBy) || !d.AcknowledgeBy.Equal(ackByTime) {
		t.Errorf("demand = %+v, want site FC-1, the required ship-by and the network's ackBy as AcknowledgeBy", d)
	}
	if len(d.Lines) != 2 || d.Lines[0].NetworkProductId != "L-PO-1-1" || d.Lines[0].Quantity != 2 || d.Lines[1].NetworkProductId != "L-PO-1-2" || d.Lines[1].Quantity != 1 {
		t.Errorf("lines = %+v, want the listing ids as the opaque product ids with their quantities", d.Lines)
	}
	if d.Lines[0].NetworkLineRef == "" || d.Lines[0].NetworkLineRef == d.Lines[1].NetworkLineRef {
		t.Errorf("line refs %q / %q must be set and distinct", d.Lines[0].NetworkLineRef, d.Lines[1].NetworkLineRef)
	}
	if strings.Contains(string(d.NetworkRef), "PO-1") {
		t.Errorf("NetworkRef %q is derived from the purchase order number; it must be minted", d.NetworkRef)
	}
	po, ok, _ := f.store.PurchaseOrder(context.Background(), d.NetworkRef)
	if !ok || po.PoNumber != "PO-1" || po.NodeID != "N-1" || len(po.Lines) != 2 || po.Lines[1].ListingID != "L-PO-1-2" || po.Lines[1].Quantity != 1 {
		t.Errorf("persisted order = %+v (%v), want the map back to PO-1 / N-1 and its lines", po, ok)
	}

	pages := f.requestsTo(http.MethodGet, "/vendor/purchase-orders")
	if len(pages) != 2 {
		t.Fatalf("page requests = %d, want 2", len(pages))
	}
	if !strings.Contains(pages[0].RawQuery, "createdAfter=2026-10-24T09%3A58%3A00Z") {
		t.Errorf("first page query %q: createdAfter must be since minus the 2 minute overlap", pages[0].RawQuery)
	}
	if !strings.Contains(pages[1].RawQuery, "nextToken=t1") || strings.Contains(pages[1].RawQuery, "createdAfter") {
		t.Errorf("second page query %q, want the token alone", pages[1].RawQuery)
	}
}

func TestLive_AZeroWatermarkAsksForEverything(t *testing.T) {
	f := readyFixture(t, nil)
	if _, err := f.g.PollDemand(context.Background(), time.Time{}); err != nil {
		t.Fatalf("PollDemand: %v", err)
	}
	if q := f.requestsTo(http.MethodGet, "/vendor/purchase-orders")[0].RawQuery; strings.Contains(q, "createdAfter") {
		t.Errorf("query %q: a zero watermark has no overlap to subtract", q)
	}
}

func TestLive_ARedeliveredPurchaseOrderYieldsTheSameNetworkRef(t *testing.T) {
	f := readyFixture(t, nil)
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 2), issuedPO("PO-2", "N-1", 1)}})

	first := f.poll(t)
	second := f.poll(t)

	if len(first) != 2 || len(second) != 2 || first[0] != second[0] || first[1] != second[1] || first[0] == first[1] {
		t.Fatalf("refs %v then %v, want the same two distinct refs on re-delivery", first, second)
	}
}

func TestLive_APurchaseOrderRepeatedInsideOnePollIsReturnedOnce(t *testing.T) {
	f := readyFixture(t, nil)
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 2)}, NextToken: "t1"})
	f.sim.setPage("t1", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 2), issuedPO("PO-2", "N-1", 1)}})

	if refs := f.poll(t); len(refs) != 2 {
		t.Fatalf("demands = %v, want PO-1 once and PO-2", refs)
	}
}

func TestLive_OnlyIssuedPurchaseOrdersAreDemand(t *testing.T) {
	f := readyFixture(t, nil)
	var others []purchaseOrderDTO
	for i, state := range []string{"ACCEPTED", "EXPIRED", "CANCELLED"} {
		po := issuedPO("PO-X"+string(rune('1'+i)), "N-1", 1)
		po.State = state
		others = append(others, po)
	}
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: append([]purchaseOrderDTO{issuedPO("PO-1", "N-1", 1)}, others...)})

	if refs := f.poll(t); len(refs) != 1 {
		t.Fatalf("demands = %v, want only the ISSUED one: the rest were answered here or ended by the network", refs)
	}
	if len(f.store.byPo) != 1 {
		t.Errorf("the store holds %d purchase orders; an order that is not demand must not be recorded", len(f.store.byPo))
	}
}

func TestLive_APurchaseOrderOnAnUnknownNodeIsSkippedWithAWarningAndTheRestStillFlow(t *testing.T) {
	f := readyFixture(t, nil)
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{
		issuedPO("PO-1", "N-1", 1), issuedPO("PO-GHOST", "N-404", 1), issuedPO("PO-2", "N-1", 1),
	}})

	refs := f.poll(t)

	if len(refs) != 2 {
		t.Fatalf("demands = %v, want the two on the known node", refs)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "N-404") {
		t.Errorf("want a warning naming the unknown node N-404, got:\n%s", logs)
	}
	if _, seenGhost := f.store.byPo["PO-GHOST"]; seenGhost {
		t.Error("a purchase order on an unknown node was persisted")
	}
	if n := len(f.requestsTo(http.MethodGet, "/vendor/nodes")); n != 1 {
		t.Errorf("GET /vendor/nodes x%d, want one refresh per poll (it is what would have taught the adapter the node)", n)
	}
}

// ---- failure injection on the poll ----

func TestLive_PollDemandWaitsOutA429AndSucceeds(t *testing.T) {
	f := readyFixture(t, nil)
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 1)}})
	f.sim.inject = failFirst(http.MethodGet, "/vendor/purchase-orders", 1, http.StatusTooManyRequests, map[string]string{"Retry-After": "3"})

	if refs := f.poll(t); len(refs) != 1 {
		t.Fatalf("demands = %v, want 1 after the 429", refs)
	}
	if w := f.sl.recorded(); len(w) != 1 || w[0] != 3*time.Second {
		t.Fatalf("waits = %v, want exactly the 3s Retry-After", w)
	}
}

func TestLive_PollDemandRetriesA5xxAndATimeout(t *testing.T) {
	f := readyFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.callTimeout = 50 * time.Millisecond } })
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 1)}})
	fail5xx := failFirst(http.MethodGet, "/vendor/purchase-orders", 1, http.StatusServiceUnavailable, nil)
	slow := slowFirst("/vendor/nodes", 1, 300*time.Millisecond)
	f.sim.inject = func(w http.ResponseWriter, r *http.Request) bool { return slow(w, r) || fail5xx(w, r) }

	if refs := f.poll(t); len(refs) != 1 {
		t.Fatalf("demands = %v, want 1 after a timeout on the node listing and a 503 on the page", refs)
	}
	if w := f.sl.recorded(); len(w) != 2 {
		t.Fatalf("backoffs = %v, want one per failed attempt", w)
	}
}

func TestLive_PollDemandReportsAnOutageAndPersistsNothing(t *testing.T) {
	f := readyFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 2 } })
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 1)}})
	f.sim.inject = failFirst(http.MethodGet, "/vendor/purchase-orders", 99, http.StatusInternalServerError, nil)

	got, err := f.g.PollDemand(context.Background(), t0)

	var se *serverError
	if !errors.As(err, &se) || got != nil {
		t.Fatalf("PollDemand = %v, %v; want no demand and the 5xx as the error so the poller retries the same window", got, err)
	}
	if len(f.store.byPo) != 0 {
		t.Error("a failed poll persisted a purchase order")
	}
}
