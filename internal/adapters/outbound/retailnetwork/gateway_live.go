package retailnetwork

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	"github.com/claudioed/network-fulfillment/internal/resilience"
)

// pollOverlap is how far before the poller's watermark a poll looks (ADR 0026
// Decision 5): a purchase order committed on the network just after the previous
// poll began is still found, and the re-delivery that follows is harmless.
const pollOverlap = 2 * time.Minute

// The Vendor API route the gateway registers nodes on.
var pathNodes = []string{"vendor", "nodes"}

// LiveConfig is what NewLiveGateway needs (ADR 0026 Decision 10).
type LiveConfig struct {
	// BaseURL and VendorID are NETWORK_BASE_URL and NETWORK_VENDOR_ID.
	BaseURL  string
	VendorID string
	// Sites are the site codes this service runs; a site with a node on the
	// network is adopted by its vendorNodeCode. Every site of Nodes is a site.
	Sites []string
	// Nodes is what the network needs to register a site that has no node yet
	// (NETWORK_NODES_FILE).
	Nodes []NodeDefinition
	// Store persists the identifier maps and the submission ledger. Required.
	Store      Store
	HTTPClient *http.Client
	Logger     *slog.Logger
	Recorder   resilience.StateRecorder
}

// LiveGateway is the demand loop of ports.NetworkGateway against the retail
// network's Vendor API (ADR 0026): our vocabulary on the outside, the network's
// identifiers mapped and persisted by the Store on the inside.
type LiveGateway struct {
	client *apiClient
	store  Store
	logger *slog.Logger
	sites  []string
	defs   map[string]NodeDefinition
}

// NewLiveGateway builds the gateway and its Vendor API client.
func NewLiveGateway(cfg LiveConfig) (*LiveGateway, error) {
	if cfg.Store == nil {
		return nil, errors.New("retail-network: the live gateway needs a Store")
	}
	if cfg.VendorID == "" {
		return nil, errors.New("retail-network: NETWORK_VENDOR_ID is required")
	}
	c, err := newAPIClient(clientConfig{baseURL: cfg.BaseURL, vendorID: cfg.VendorID, httpClient: cfg.HTTPClient, logger: cfg.Logger, recorder: cfg.Recorder})
	if err != nil {
		return nil, err
	}
	return newLiveGateway(c, cfg.Store, cfg), nil
}

func newLiveGateway(c *apiClient, st Store, cfg LiveConfig) *LiveGateway {
	g := &LiveGateway{client: c, store: st, logger: cfg.Logger, defs: make(map[string]NodeDefinition, len(cfg.Nodes))}
	if g.logger == nil {
		g.logger = c.logger
	}
	known := map[string]bool{}
	for _, s := range cfg.Sites {
		known[s] = true
	}
	for _, n := range cfg.Nodes {
		g.defs[n.SiteCode] = n
		known[n.SiteCode] = true
	}
	for s := range known {
		g.sites = append(g.sites, s)
	}
	sort.Strings(g.sites)
	return g
}

// ---- nodes (Decision 7) ----

// convergeNodes takes every configured site one step closer to having a node.
// It never blocks the poll: a failure is logged and the next tick tries again.
func (g *LiveGateway) convergeNodes(ctx context.Context) {
	if len(g.sites) == 0 {
		return
	}
	nodes, err := g.client.listNodes(ctx)
	if err != nil {
		g.logger.Warn("retail-network: listing the vendor's nodes failed; node convergence skipped this tick", "err", err)
		return
	}
	listed := make(map[string]string, len(nodes))
	for _, n := range nodes {
		listed[n.VendorNodeCode] = n.NodeID
	}
	for _, site := range g.sites {
		if err := g.convergeSite(ctx, site, listed); err != nil {
			g.logger.Warn("retail-network: node convergence incomplete; the next tick retries", "site", site, "err", err)
		}
	}
}

func (g *LiveGateway) convergeSite(ctx context.Context, site string, listed map[string]string) error {
	if nodeID, ok := listed[site]; ok {
		return g.store.SaveNode(ctx, site, nodeID)
	}
	if _, mapped, err := g.store.NodeIDForSite(ctx, site); err != nil || mapped {
		return err
	}
	def, ok := g.defs[site]
	if !ok {
		g.logger.Warn("retail-network: site has no node on the network and no node definition to register one from", "site", site)
		return nil
	}
	return g.registerNode(ctx, def)
}

// registerNode registers def's site as a vendor node. The ledger row, with the
// exact body, is written before the call; the same key and body are what any
// retry sends, so the network never creates the node twice.
func (g *LiveGateway) registerNode(ctx context.Context, def NodeDefinition) error {
	body, err := buildNodeRegistrationBody(g.client.vendorID, def)
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindNodeRegistration, Subject: def.SiteCode, IdempotencyKey: idempotencyKey("node", def.SiteCode), Body: body,
	})
	if err != nil {
		return err
	}
	if row, err = g.send(ctx, row, "registerVendorNode", pathNodes); err != nil {
		return err
	}
	return g.settleRegistration(ctx, row)
}

// settleRegistration reads the registration's transaction and, once it
// succeeded, stores the nodeId it carries. A PROCESSING transaction is left for
// the next tick.
func (g *LiveGateway) settleRegistration(ctx context.Context, row Submission) error {
	if row.TransactionID == "" {
		return nil
	}
	tx, err := g.client.getTransaction(ctx, row.TransactionID)
	if err != nil {
		return err
	}
	if tx.Status == txFailure {
		errs := joinErrors(tx.Errors)
		if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusFailure, errs); err != nil {
			return err
		}
		return fmt.Errorf("retail-network: the network refused the registration of node %s: %s", row.Subject, errs)
	}
	if tx.Status != txSuccess {
		return nil
	}
	if tx.NodeID == "" {
		return fmt.Errorf("retail-network: the registration of node %s succeeded without a nodeId", row.Subject)
	}
	if err := g.store.SaveNode(ctx, row.Subject, tx.NodeID); err != nil {
		return err
	}
	return g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusSuccess, "")
}

func joinErrors(errs []submissionErrorDTO) string {
	parts := make([]string, len(errs))
	for i, e := range errs {
		parts[i] = e.Code + ": " + e.Message
	}
	return strings.Join(parts, "; ")
}

// send makes the one write of a ledger row that has no transaction id yet and
// stores the id the 202 carries. A row that already has a transaction (or is
// settled) is not sent again: the stored body and key are what is sent, so a
// retry after a crash never changes the body and never creates a second write.
func (g *LiveGateway) send(ctx context.Context, row Submission, op string, path []string) (Submission, error) {
	if row.TransactionID != "" || row.Status.Terminal() {
		return row, nil
	}
	txID, err := g.client.postAsync(ctx, op, path, row.IdempotencyKey, row.Body)
	if err != nil {
		return row, err
	}
	if err := g.store.RecordTransaction(ctx, row.Kind, row.Subject, txID); err != nil {
		return row, err
	}
	row.TransactionID, row.Status = txID, StatusProcessing
	return row, nil
}

// ---- PollDemand (Decision 5) ----

// PollDemand returns the network's ISSUED purchase orders as demand in our
// vocabulary. Re-delivery is normal: a purchase order seen again yields the same
// NetworkRef, and one repeated inside this poll is returned once. A purchase
// order on a node this service has no map for is skipped with a warning, never
// a crash; the next poll lists the nodes again.
func (g *LiveGateway) PollDemand(ctx context.Context, since time.Time) ([]contract.InboundDemand, error) {
	g.convergeNodes(ctx)
	pos, err := g.client.pollAll(ctx, pollWindowStart(since))
	if err != nil {
		return nil, err
	}
	var out []contract.InboundDemand
	seen := make(map[string]bool, len(pos))
	for _, po := range pos {
		if po.State != stateIssued || seen[po.PoNumber] {
			continue
		}
		seen[po.PoNumber] = true
		d, ok, err := g.demandFrom(ctx, po)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, d)
		}
	}
	return out, nil
}

func pollWindowStart(since time.Time) time.Time {
	if since.IsZero() {
		return since
	}
	return since.Add(-pollOverlap)
}

func (g *LiveGateway) demandFrom(ctx context.Context, po purchaseOrderDTO) (contract.InboundDemand, bool, error) {
	site, known, err := g.store.SiteForNode(ctx, po.NodeID)
	if err != nil {
		return contract.InboundDemand{}, false, err
	}
	if !known {
		g.logger.Warn("retail-network: purchase order skipped, its node is not mapped to any of our sites",
			"nodeId", po.NodeID, "poNumber", po.PoNumber)
		return contract.InboundDemand{}, false, nil
	}
	stored := toStoredOrder(po, g.client.vendorID)
	ref, err := g.store.SavePurchaseOrder(ctx, stored)
	if err != nil {
		return contract.InboundDemand{}, false, err
	}
	return contract.InboundDemand{
		NetworkRef: ref, SiteId: shared.SiteId(site), RequiredShipBy: po.RequiredShipBy, AcknowledgeBy: po.AckBy,
		Lines: inboundLines(stored.Lines),
	}, true, nil
}

func toStoredOrder(po purchaseOrderDTO, vendorID string) PurchaseOrder {
	if po.VendorID != "" {
		vendorID = po.VendorID
	}
	out := PurchaseOrder{
		PoNumber: po.PoNumber, VendorID: vendorID, NodeID: po.NodeID,
		RequiredShipBy: po.RequiredShipBy, AckBy: po.AckBy, Lines: make([]PurchaseOrderLine, len(po.Lines)),
	}
	for i, l := range po.Lines {
		out.Lines[i] = PurchaseOrderLine{
			LineNo: l.LineNo, NetworkLineRef: shared.NetworkLineRef(fmt.Sprintf("line-%d", l.LineNo)),
			ListingID: l.ListingID, NetworkProductID: shared.NetworkProductId(l.ListingID), Quantity: l.Quantity,
		}
	}
	return out
}

func inboundLines(lines []PurchaseOrderLine) []contract.InboundLine {
	out := make([]contract.InboundLine, len(lines))
	for i, l := range lines {
		out[i] = contract.InboundLine{NetworkLineRef: l.NetworkLineRef, NetworkProductId: l.NetworkProductID, Quantity: l.Quantity}
	}
	return out
}
