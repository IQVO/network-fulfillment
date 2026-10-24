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
	mu     sync.Mutex
	nodes  map[string]string
	orders map[shared.NetworkRef]PurchaseOrder
	byPo   map[string]shared.NetworkRef
	subs   map[string]Submission
	labels map[shared.NetworkRef][]Label
	minted int
}

var _ Store = (*memStore)(nil)

func newMemStore() *memStore {
	return &memStore{
		nodes: map[string]string{}, orders: map[shared.NetworkRef]PurchaseOrder{}, byPo: map[string]shared.NetworkRef{},
		subs: map[string]Submission{}, labels: map[shared.NetworkRef][]Label{},
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
	// inject runs before routing; true means it answered. Called without the lock.
	inject func(w http.ResponseWriter, r *http.Request) bool
}

func newVendorSim() *vendorSim {
	return &vendorSim{
		pages: map[string]purchaseOrderPageDTO{}, orders: map[string]purchaseOrderDTO{}, txs: map[string]*transactionDTO{},
		bodies: map[string]string{}, txByKey: map[string]string{}, codes: map[string]string{}, answers: map[string]labelResponseDTO{},
		created: map[string]int{}, outcome: txSuccess,
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
	case r.Method == http.MethodPost:
		v.write(w, r)
	case path == "/vendor/nodes":
		ok(w, vendorNodeListDTO{Nodes: v.nodes})
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
	switch path {
	case "/vendor/nodes":
		return "NODE_REGISTRATION"
	case "/vendor/acknowledgements":
		return "ACKNOWLEDGEMENT"
	default:
		return "SHIPMENT_CONFIRMATION"
	}
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
		PoNumber string `json:"poNumber"`
		Packages []struct {
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
// gets its nodeId, and the node appears in the vendor's listing.
func (v *vendorSim) settleLocked(tx *transactionDTO, status string, errs []submissionErrorDTO) {
	tx.Status, tx.Errors = status, errs
	if tx.Kind == "NODE_REGISTRATION" && status == txSuccess {
		tx.NodeID = fmt.Sprintf("N-%d", 100+len(v.nodes))
		v.nodes = append(v.nodes, vendorNodeDTO{NodeID: tx.NodeID, VendorID: "V-WS", VendorNodeCode: v.codes[tx.TransactionID], NodeType: "FULFILLMENT_CENTER", Status: "ACTIVE"})
	}
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
