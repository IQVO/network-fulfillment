package retailnetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ---- an in-memory Store with the semantics of PGStore ----

type memStore struct {
	mu       sync.Mutex
	nodes    map[string]string
	orders   map[shared.NetworkRef]PurchaseOrder
	byPo     map[string]shared.NetworkRef
	subs     map[string]Submission
	labels   map[shared.NetworkRef][]Label
	profiles map[string]NodeProfileState
	feeds    map[string]InventoryFeedState
	rejected map[string][]RejectedListing
	minted   int
}

var _ Store = (*memStore)(nil)

func newMemStore() *memStore {
	return &memStore{
		nodes: map[string]string{}, orders: map[shared.NetworkRef]PurchaseOrder{}, byPo: map[string]shared.NetworkRef{},
		subs: map[string]Submission{}, labels: map[shared.NetworkRef][]Label{}, profiles: map[string]NodeProfileState{},
		feeds: map[string]InventoryFeedState{}, rejected: map[string][]RejectedListing{},
	}
}

func subKey(kind SubmissionKind, subject string) string { return string(kind) + "|" + subject }

func (s *memStore) NodeIDForSite(_ context.Context, site string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.nodes[site]
	return id, ok, nil
}

func (s *memStore) SiteForNode(_ context.Context, nodeID string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for site, id := range s.nodes {
		if id == nodeID {
			return site, true, nil
		}
	}
	return "", false, nil
}

func (s *memStore) SaveNode(_ context.Context, site, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if got, ok := s.nodes[site]; ok && got != nodeID {
		return fmt.Errorf("%w: site %q", ErrMappingConflict, site)
	}
	s.nodes[site] = nodeID
	return nil
}

func (s *memStore) SavePurchaseOrder(_ context.Context, po PurchaseOrder) (shared.NetworkRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ref, ok := s.byPo[po.PoNumber]; ok {
		return ref, nil
	}
	s.minted++
	po.Ref = shared.NetworkRef(fmt.Sprintf("NR-test-%d", s.minted))
	s.byPo[po.PoNumber] = po.Ref
	s.orders[po.Ref] = po
	return po.Ref, nil
}

func (s *memStore) PurchaseOrder(_ context.Context, ref shared.NetworkRef) (PurchaseOrder, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	po, ok := s.orders[ref]
	return po, ok, nil
}

func (s *memStore) Submission(_ context.Context, kind SubmissionKind, subject string) (Submission, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subs[subKey(kind, subject)]
	return sub, ok, nil
}

func (s *memStore) BeginSubmission(_ context.Context, sub Submission) (Submission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if have, ok := s.subs[subKey(sub.Kind, sub.Subject)]; ok {
		return have, nil
	}
	sub.Status, sub.TransactionID, sub.Errors = StatusSending, "", ""
	s.subs[subKey(sub.Kind, sub.Subject)] = sub
	return sub, nil
}

func (s *memStore) RecordTransaction(_ context.Context, kind SubmissionKind, subject, transactionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.subs[subKey(kind, subject)]
	if sub.TransactionID == "" {
		sub.TransactionID = transactionID
		if sub.Status == StatusSending {
			sub.Status = StatusProcessing
		}
		s.subs[subKey(kind, subject)] = sub
	}
	return nil
}

func (s *memStore) RecordOutcome(_ context.Context, kind SubmissionKind, subject string, status SubmissionStatus, errs string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.subs[subKey(kind, subject)]
	if !sub.Status.Terminal() {
		sub.Status, sub.Errors = status, errs
		s.subs[subKey(kind, subject)] = sub
	}
	return nil
}

func (s *memStore) SaveLabels(_ context.Context, ref shared.NetworkRef, labels []Label) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	have := s.labels[ref]
	for _, l := range labels {
		known := false
		for _, h := range have {
			known = known || h.PackageNo == l.PackageNo
		}
		if !known {
			have = append(have, l)
		}
	}
	s.labels[ref] = have
	return nil
}

func (s *memStore) Labels(_ context.Context, ref shared.NetworkRef) ([]Label, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]Label(nil), s.labels[ref]...)
	sort.Slice(out, func(i, j int) bool { return out[i].PackageNo < out[j].PackageNo })
	return out, nil
}

// row reads a ledger row for an assertion.
func (s *memStore) row(kind SubmissionKind, subject string) (Submission, bool) {
	sub, ok, _ := s.Submission(context.Background(), kind, subject)
	return sub, ok
}

func (s *memStore) NodeProfileState(_ context.Context, site string) (NodeProfileState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.profiles[site]
	return st, ok, nil
}

// AdvanceNodeProfile has PGStore's guard: the pair only ever grows, a site with
// no node is refused.
func (s *memStore) AdvanceNodeProfile(_ context.Context, st NodeProfileState) (NodeProfileState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[st.SiteCode]; !ok {
		return NodeProfileState{}, fmt.Errorf("%w: site %q has no node", ErrMappingConflict, st.SiteCode)
	}
	cur, ok := s.profiles[st.SiteCode]
	if !ok || st.Revision > cur.Revision || (st.Revision == cur.Revision && st.Attempt > cur.Attempt) {
		s.profiles[st.SiteCode] = st
		return st, nil
	}
	return cur, nil
}

func feedKeyOf(site string, part int) string { return fmt.Sprintf("%s#%d", site, part) }

func (s *memStore) InventoryFeedState(_ context.Context, site string, part int) (InventoryFeedState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.feeds[feedKeyOf(site, part)]
	return st, ok, nil
}

// SaveInventoryFeedState has PGStore's guard: a site with no node is refused and
// a part never goes back to an earlier asOf.
func (s *memStore) SaveInventoryFeedState(_ context.Context, st InventoryFeedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[st.SiteCode]; !ok {
		return fmt.Errorf("%w: site %q has no node", ErrMappingConflict, st.SiteCode)
	}
	if cur, ok := s.feeds[feedKeyOf(st.SiteCode, st.Part)]; ok && cur.AsOf.After(st.AsOf) {
		return nil
	}
	s.feeds[feedKeyOf(st.SiteCode, st.Part)] = st
	return nil
}

func (s *memStore) RejectedListings(_ context.Context, site string) ([]RejectedListing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]RejectedListing(nil), s.rejected[site]...)
	sort.Slice(out, func(i, j int) bool { return out[i].ListingID < out[j].ListingID })
	return out, nil
}

func (s *memStore) RejectListing(_ context.Context, site string, l RejectedListing) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[site]; !ok {
		return fmt.Errorf("%w: site %q has no node", ErrMappingConflict, site)
	}
	list := s.rejected[site]
	for i := range list {
		if list[i].ListingID == l.ListingID {
			list[i] = l
			return nil
		}
	}
	s.rejected[site] = append(list, l)
	return nil
}

// ---- a stateful fake Vendor API on top of client_test.go's newFake ----

type vendorSim struct {
	mu      sync.Mutex
	nodes   []vendorNodeDTO
	pages   map[string]purchaseOrderPageDTO
	orders  map[string]purchaseOrderDTO
	txs     map[string]*transactionDTO
	bodies  map[string]string // "METHOD path key" -> first body
	txByKey map[string]string
	codes   map[string]string // transaction id -> vendorNodeCode of a registration
	answers map[string]labelResponseDTO
	created map[string]int // path -> distinct writes
	outcome string         // status a new transaction ends with
	outErrs []submissionErrorDTO
	seq     int

	// The node profile, as node-topology treats it (ADR 0027): a revision that is
	// not above the one the network holds fails the transaction with
	// ErrStaleProfileRevision.
	profileCalls map[string]profileCall // transaction id -> the declaration it carries
	networkRev   map[string]int         // node id -> the revision the network holds
	hideRevision bool                   // the node view shows no profileRevision

	// inject runs before routing; true means it answered. Called without the lock.
	inject func(w http.ResponseWriter, r *http.Request) bool
}

type profileCall struct {
	node     string
	revision int
}

func newVendorSim() *vendorSim {
	return &vendorSim{
		pages: map[string]purchaseOrderPageDTO{}, orders: map[string]purchaseOrderDTO{}, txs: map[string]*transactionDTO{},
		bodies: map[string]string{}, txByKey: map[string]string{}, codes: map[string]string{}, answers: map[string]labelResponseDTO{},
		created: map[string]int{}, outcome: txSuccess,
		profileCalls: map[string]profileCall{}, networkRev: map[string]int{},
	}
}

func (v *vendorSim) handle(_ int, w http.ResponseWriter, r *http.Request) {
	if v.inject != nil && v.inject(w, r) {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		v.write(w, r)
	case path == "/vendor/nodes":
		ok(w, vendorNodeListDTO{Nodes: v.visibleNodesLocked()})
	case strings.HasPrefix(path, "/vendor/nodes/"):
		for _, n := range v.visibleNodesLocked() {
			if n.NodeID == strings.TrimPrefix(path, "/vendor/nodes/") {
				ok(w, n)
				return
			}
		}
		problem(w, http.StatusNotFound, "not-found")
	case path == "/vendor/purchase-orders":
		ok(w, v.pages[r.URL.Query().Get("nextToken")])
	case strings.HasPrefix(path, "/vendor/purchase-orders/"):
		po, found := v.orders[strings.TrimPrefix(path, "/vendor/purchase-orders/")]
		if !found {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		ok(w, po)
	case strings.HasPrefix(path, "/vendor/transactions/"):
		tx, found := v.txs[strings.TrimPrefix(path, "/vendor/transactions/")]
		if !found {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		ok(w, tx)
	default:
		problem(w, http.StatusNotFound, "not-found")
	}
}

func txKind(path string) string {
	switch {
	case path == "/vendor/nodes":
		return "NODE_REGISTRATION"
	case strings.HasPrefix(path, "/vendor/nodes/") && strings.HasSuffix(path, "/profile"):
		return "NODE_PROFILE"
	case path == "/vendor/acknowledgements":
		return "ACKNOWLEDGEMENT"
	case path == "/vendor/inventory":
		return "INVENTORY"
	default:
		return "SHIPMENT_CONFIRMATION"
	}
}

// nodeOfProfilePath is the node id of /vendor/nodes/<id>/profile.
func nodeOfProfilePath(path string) string {
	return strings.TrimSuffix(strings.TrimPrefix(path, "/vendor/nodes/"), "/profile")
}

// write handles a POST with the network's idempotency rules: the same key and
// body replays the first answer, the same key with another body is a 422.
func (v *vendorSim) write(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	slot := r.Method + " " + r.URL.Path + " " + r.Header.Get("Idempotency-Key")
	if prev, seen := v.bodies[slot]; seen && prev != string(body) {
		problem(w, http.StatusUnprocessableEntity, "idempotency-key-reused")
		return
	}
	var req struct {
		PoNumber        string `json:"poNumber"`
		ProfileRevision int    `json:"profileRevision"`
		Packages        []struct {
			PackageNo int `json:"packageNo"`
		} `json:"packages"`
	}
	_ = json.Unmarshal(body, &req)
	if r.URL.Path == "/vendor/shipping-labels" {
		v.writeLabels(w, slot, string(body), req.PoNumber, len(req.Packages))
		return
	}
	id, replay := v.txByKey[slot]
	if !replay {
		v.seq++
		id = fmt.Sprintf("tx-%d", v.seq)
		v.txByKey[slot], v.bodies[slot] = id, string(body)
		v.created[r.URL.Path]++
		tx := &transactionDTO{TransactionID: id, VendorID: "V-WS", Kind: txKind(r.URL.Path), Status: txProcessing,
			ReceivedAt: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC), PoNumber: req.PoNumber}
		var node struct {
			VendorNodeCode string `json:"vendorNodeCode"`
		}
		_ = json.Unmarshal(body, &node)
		v.codes[id] = node.VendorNodeCode
		if tx.Kind == "NODE_PROFILE" {
			v.profileCalls[id] = profileCall{node: nodeOfProfilePath(r.URL.Path), revision: req.ProfileRevision}
		}
		v.txs[id] = tx
		v.settleLocked(tx, v.outcome, v.outErrs)
	}
	w.WriteHeader(http.StatusAccepted)
	ok(w, transactionAcceptedDTO{TransactionID: id})
}

func (v *vendorSim) writeLabels(w http.ResponseWriter, slot, body, po string, packages int) {
	if _, seen := v.bodies[slot]; !seen {
		v.bodies[slot] = body
		v.created["/vendor/shipping-labels"]++
		resp := labelResponseDTO{PoNumber: po}
		for n := 1; n <= packages; n++ {
			resp.Labels = append(resp.Labels, labelDTO{PackageNo: n, LabelRef: fmt.Sprintf("LBL-%s-%d", po, n),
				TrackingNumber: fmt.Sprintf("TRK-%s-%d", po, n), Carrier: "CARRIER-X"})
		}
		v.answers[slot] = resp
	}
	ok(w, v.answers[slot])
}

// settleLocked moves a transaction to status. A node registration that succeeds
// gets its nodeId, and the node appears in the vendor's listing. A profile
// declaration that would succeed is judged the way node-topology judges it: a
// revision not above the one the network holds fails with ErrStaleProfileRevision.
func (v *vendorSim) settleLocked(tx *transactionDTO, status string, errs []submissionErrorDTO) {
	tx.Status, tx.Errors = status, errs
	if tx.Kind == "NODE_REGISTRATION" && status == txSuccess {
		tx.NodeID = fmt.Sprintf("N-%d", 100+len(v.nodes))
		v.nodes = append(v.nodes, vendorNodeDTO{NodeID: tx.NodeID, VendorID: "V-WS", VendorNodeCode: v.codes[tx.TransactionID], NodeType: "FULFILLMENT_CENTER", Status: "ACTIVE"})
	}
	if tx.Kind == "NODE_PROFILE" && status == txSuccess {
		call := v.profileCalls[tx.TransactionID]
		if held := v.networkRev[call.node]; call.revision <= held {
			tx.Status = txFailure
			tx.Errors = []submissionErrorDTO{{Code: errStaleProfileRevision, Message: fmt.Sprintf("revision %d is not newer than %d", call.revision, held), Path: "$.profileRevision"}}
			return
		}
		v.networkRev[call.node] = call.revision
	}
}

// visibleNodesLocked is the node listing, each node with the profile revision
// the network holds for it (unless the view is set to show none).
func (v *vendorSim) visibleNodesLocked() []vendorNodeDTO {
	out := append([]vendorNodeDTO(nil), v.nodes...)
	for i := range out {
		if !v.hideRevision {
			out[i].ProfileRevision = v.networkRev[out[i].NodeID]
		}
	}
	return out
}

// holdRevision makes the network already hold revision rev for a node, as after
// a wiped table on our side.
func (v *vendorSim) holdRevision(nodeID string, rev int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.networkRev[nodeID] = rev
}

func (v *vendorSim) heldRevision(nodeID string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.networkRev[nodeID]
}

func (v *vendorSim) hideRevisions(hide bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.hideRevision = hide
}

func (v *vendorSim) setOutcome(status string, errs ...submissionErrorDTO) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.outcome, v.outErrs = status, errs
}

// finish settles a transaction that was left PROCESSING.
func (v *vendorSim) finish(id, status string, errs ...submissionErrorDTO) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.settleLocked(v.txs[id], status, errs)
}

func (v *vendorSim) writes(path string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.created[path]
}

func (v *vendorSim) setPage(token string, page purchaseOrderPageDTO) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.pages[token] = page
}

func (v *vendorSim) addNode(n vendorNodeDTO) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.nodes = append(v.nodes, n)
}

func (v *vendorSim) addOrder(po purchaseOrderDTO) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.orders[po.PoNumber] = po
}

func (v *vendorSim) addTx(tx transactionDTO) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.txs[tx.TransactionID] = &tx
}

// ---- fixtures ----

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fixtureCfg struct {
	sites  []string
	nodes  []NodeDefinition
	client func(*clientConfig)
}

type liveFixture struct {
	g     *LiveGateway
	sim   *vendorSim
	fake  *fake
	store *memStore
	sl    *sleeper
	logs  *syncBuffer
}

func newLiveFixture(t *testing.T, cfg fixtureCfg) *liveFixture {
	t.Helper()
	sim := newVendorSim()
	f := newFake(t, sim.handle)
	c, sl, _ := testClient(t, f.srv.URL, cfg.client)
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c.logger = logger
	st := newMemStore()
	g := newLiveGateway(c, st, LiveConfig{Sites: cfg.sites, Nodes: cfg.nodes, Logger: logger})
	return &liveFixture{g: g, sim: sim, fake: f, store: st, sl: sl, logs: logs}
}

func fcNode(site string) NodeDefinition {
	return NodeDefinition{SiteCode: site, NodeType: "FULFILLMENT_CENTER", RegionID: "SP", Timezone: "America/Sao_Paulo", PostalCode: "01000-000", CountryCode: "BR"}
}

var (
	t0        = time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	shipBy    = time.Date(2026, 10, 25, 18, 0, 0, 0, time.UTC)
	ackByTime = time.Date(2026, 10, 24, 22, 0, 0, 0, time.UTC)
)

// issuedPO is an ISSUED purchase order on nodeID with one line per quantity.
func issuedPO(poNumber, nodeID string, quantities ...int) purchaseOrderDTO {
	po := purchaseOrderDTO{PoNumber: poNumber, VendorID: "V-WS", NodeID: nodeID, State: stateIssued, IssuedAt: t0, RequiredShipBy: shipBy, AckBy: ackByTime}
	for i, q := range quantities {
		po.Lines = append(po.Lines, poLineDTO{LineNo: i + 1, ListingID: fmt.Sprintf("L-%s-%d", poNumber, i+1), Quantity: q})
	}
	return po
}

// ready returns a fixture whose site FC-1 is already mapped to node N-1.
func readyFixture(t *testing.T, mutate func(*fixtureCfg)) *liveFixture {
	t.Helper()
	cfg := fixtureCfg{sites: []string{"FC-1"}}
	if mutate != nil {
		mutate(&cfg)
	}
	f := newLiveFixture(t, cfg)
	f.sim.addNode(vendorNodeDTO{NodeID: "N-1", VendorID: "V-WS", VendorNodeCode: "FC-1", NodeType: "FULFILLMENT_CENTER", Status: "ACTIVE"})
	return f
}
