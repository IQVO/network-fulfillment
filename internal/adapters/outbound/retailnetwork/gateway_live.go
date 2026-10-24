package retailnetwork

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
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

	capabilityNoted, availabilityNoted atomic.Bool // the supply-loop no-ops log at Info once each
}

var _ ports.NetworkGateway = (*LiveGateway)(nil)

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
// A definitive refusal (definitiveRefusals) is recorded as FAILURE once and
// returned as ErrWriteRefused every time after.
func (g *LiveGateway) send(ctx context.Context, row Submission, op string, path []string) (Submission, error) {
	if row.Status == StatusFailure && row.TransactionID == "" {
		return row, fmt.Errorf("%w earlier: %s", ErrWriteRefused, row.Errors)
	}
	if row.TransactionID != "" || row.Status.Terminal() {
		return row, nil
	}
	txID, err := g.client.postAsync(ctx, op, path, row.IdempotencyKey, row.Body)
	if err != nil {
		return row, g.refuse(ctx, row, err)
	}
	if err := g.store.RecordTransaction(ctx, row.Kind, row.Subject, txID); err != nil {
		return row, err
	}
	row.TransactionID, row.Status = txID, StatusProcessing
	return row, nil
}

// definitiveRefusals are the problem types after which the same write can
// never succeed: the network answered, and caches that answer per key. Anything
// else (a conflict that is only concurrent-update, a 404 from a proxy, a 403)
// is retried on the next attempt rather than turned into a verdict.
var definitiveRefusals = map[string]bool{
	"ack-window-closed":       true,
	"illegal-transition":      true,
	"validation-failed":       true,
	"confirm-without-label":   true,
	"label-before-acceptance": true,
}

const problemAlreadyAcknowledged = "already-acknowledged"

func (g *LiveGateway) refuse(ctx context.Context, row Submission, err error) error {
	if !definitiveRefusals[Problem(err)] {
		return err
	}
	if rerr := g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusFailure, err.Error()); rerr != nil {
		return errors.Join(err, rerr)
	}
	return fmt.Errorf("%w: %w", ErrWriteRefused, err)
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

// ---- SubmitAcknowledgement and SubmissionStatus (Decisions 3, 4, 6) ----

var pathAcknowledgements = []string{"vendor", "acknowledgements"}

func (g *LiveGateway) orderFor(ctx context.Context, ref shared.NetworkRef) (PurchaseOrder, error) {
	po, ok, err := g.store.PurchaseOrder(ctx, ref)
	if err != nil {
		return PurchaseOrder{}, err
	}
	if !ok {
		return PurchaseOrder{}, fmt.Errorf("%w: %s", ErrUnknownRef, ref)
	}
	return po, nil
}

// SubmitAcknowledgement answers the order in full: accept, or reject with the
// reason mapped by ackDecision. The ledger row, with the exact body, is written
// before the call; a second call finds the transaction id and sends nothing. A
// ACKNOWLEDGEMENT_DEADLINE_MISSED answer is not written at all (the network
// expires the order itself).
func (g *LiveGateway) SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, answer contract.Acknowledgement) error {
	decision, send, err := ackDecision(answer)
	if err != nil {
		return err
	}
	if !send {
		g.logger.Info("retail-network: no acknowledgement written, the window is closed and the network expires the order itself", "networkRef", ref)
		return nil
	}
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return err
	}
	body, err := buildAcknowledgementBody(g.client.vendorID, po, decision)
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindAcknowledgement, Subject: string(ref), IdempotencyKey: idempotencyKey("ack", string(ref)), Body: body,
	})
	if err != nil {
		return err
	}
	_, err = g.sendAcknowledgement(ctx, po, row)
	return err
}

// sendAcknowledgement sends the stored answer if it was never sent. A 409
// already-acknowledged (the key outlived its 24 hours) means the network holds
// the answer: the order's own acknowledgement gives the transaction to follow.
func (g *LiveGateway) sendAcknowledgement(ctx context.Context, po PurchaseOrder, row Submission) (Submission, error) {
	row, err := g.send(ctx, row, "submitAcknowledgement", pathAcknowledgements)
	if Problem(err) != problemAlreadyAcknowledged {
		return row, err
	}
	got, err := g.client.getPurchaseOrder(ctx, po.PoNumber)
	if err != nil || got.Acknowledgement == nil || got.Acknowledgement.TransactionID == "" {
		return row, err
	}
	txID := got.Acknowledgement.TransactionID
	if err := g.store.RecordTransaction(ctx, row.Kind, row.Subject, txID); err != nil {
		return row, err
	}
	row.TransactionID, row.Status = txID, StatusProcessing
	return row, nil
}

// SubmissionStatus reconciles the acknowledgement of ref against the network's
// transaction record. No ledger row is NOT_SENT; a row whose call never got
// through is sent again from its stored body and key first.
func (g *LiveGateway) SubmissionStatus(ctx context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	row, found, err := g.store.Submission(ctx, KindAcknowledgement, string(ref))
	if err != nil {
		return "", err
	}
	if !found {
		return contract.SubmissionNotSent, nil
	}
	if row.Status.Terminal() {
		return ledgerToPort(row.Status), nil
	}
	if row.TransactionID == "" {
		if row, err = g.resend(ctx, ref, row); err != nil {
			return refusedAsFailure(err)
		}
		if row.TransactionID == "" {
			return contract.SubmissionPending, nil
		}
	}
	return g.reconcile(ctx, row)
}

func (g *LiveGateway) resend(ctx context.Context, ref shared.NetworkRef, row Submission) (Submission, error) {
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return row, err
	}
	return g.sendAcknowledgement(ctx, po, row)
}

// refusedAsFailure turns a definitive refusal into the FAILURE verdict the
// reconciliation acts on; any other error stays an error and is retried.
func refusedAsFailure(err error) (contract.SubmissionStatusValue, error) {
	if errors.Is(err, ErrWriteRefused) {
		return contract.SubmissionFailure, nil
	}
	return "", err
}

// reconcile asks the network about the row's transaction and stores the verdict.
func (g *LiveGateway) reconcile(ctx context.Context, row Submission) (contract.SubmissionStatusValue, error) {
	tx, err := g.client.getTransaction(ctx, row.TransactionID)
	if err != nil {
		return "", err
	}
	if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, ledgerStatus(tx.Status), joinErrors(tx.Errors)); err != nil {
		return "", err
	}
	return submissionStatus(tx.Status), nil
}

// ledgerToPort maps a settled ledger status to the port's. Anything not
// terminal is PENDING.
func ledgerToPort(status SubmissionStatus) contract.SubmissionStatusValue {
	if status == StatusSuccess {
		return contract.SubmissionSuccess
	}
	if status == StatusFailure {
		return contract.SubmissionFailure
	}
	return contract.SubmissionPending
}

// ---- RequestLabel and SubmitShipmentConfirmation (Decision 4) ----

var pathShipmentConfirmations = []string{"vendor", "shipment-confirmations"}

// RequestLabel asks for the labels of an order that leaves in packageCount
// packages and persists them. It is idempotent per ref: labels already
// persisted are returned as they are (the first count wins, since the same key
// with another body would be idempotency-key-reused), and a retry of an
// interrupted request carries the same key, so the network issues one set.
func (g *LiveGateway) RequestLabel(ctx context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error) {
	if packageCount < 1 {
		return nil, fmt.Errorf("%w (got %d)", ErrBadPackageCount, packageCount)
	}
	have, err := g.store.Labels(ctx, ref)
	if err != nil {
		return nil, err
	}
	if len(have) > 0 {
		return labelResults(have), nil
	}
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return nil, err
	}
	body, err := buildLabelBody(g.client.vendorID, po.PoNumber, packageCount)
	if err != nil {
		return nil, err
	}
	resp, err := g.client.requestLabels(ctx, idempotencyKey("label", string(ref)), body)
	if err != nil {
		return nil, err
	}
	labels, err := labelsFromResponse(resp, packageCount)
	if err != nil {
		return nil, err
	}
	if err := g.store.SaveLabels(ctx, ref, labels); err != nil {
		return nil, err
	}
	return labelResults(labels), nil
}

// labelsFromResponse checks the network's answer against the request: one label
// per package, each with a tracking number (the shipment confirmation needs
// exactly these). A package number the network omits is its position.
func labelsFromResponse(resp labelResponseDTO, packages int) ([]Label, error) {
	if len(resp.Labels) != packages {
		return nil, fmt.Errorf("retail-network: requestLabel: asked for %d package labels, the network returned %d", packages, len(resp.Labels))
	}
	out := make([]Label, packages)
	for i, l := range resp.Labels {
		if l.TrackingNumber == "" {
			return nil, fmt.Errorf("retail-network: requestLabel: the label of package %d has no tracking number", i+1)
		}
		no := l.PackageNo
		if no == 0 {
			no = i + 1
		}
		out[i] = Label{PackageNo: no, LabelRef: l.LabelRef, TrackingNumber: l.TrackingNumber, Carrier: l.Carrier}
	}
	return out, nil
}

func labelResults(labels []Label) []contract.LabelResult {
	out := make([]contract.LabelResult, len(labels))
	for i, l := range labels {
		out[i] = contract.LabelResult{LabelRef: l.LabelRef, TrackingNumber: l.TrackingNumber, Carrier: l.Carrier}
	}
	return out
}

// SubmitShipmentConfirmation tells the network the order shipped at shippedAt,
// with the tracking numbers of the labels persisted by RequestLabel (no label is
// ErrNoLabel). The ledger row keeps the first body, so the first shippedAt wins
// and a second call sends nothing. The transaction's outcome is recorded for
// operators; a network FAILURE is logged, not returned, because no use case
// reconciles a confirmation (ADR 0026, Consequences).
func (g *LiveGateway) SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef, shippedAt time.Time) error {
	labels, err := g.store.Labels(ctx, ref)
	if err != nil {
		return err
	}
	if len(labels) == 0 {
		return fmt.Errorf("%w: %s", ErrNoLabel, ref)
	}
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return err
	}
	body, err := buildShipmentConfirmationBody(g.client.vendorID, po.PoNumber, shippedAt, trackingNumbers(labels))
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindShipmentConfirmation, Subject: string(ref), IdempotencyKey: idempotencyKey("ship", string(ref)), Body: body,
	})
	if err != nil {
		return err
	}
	if row, err = g.send(ctx, row, "submitShipmentConfirmation", pathShipmentConfirmations); err != nil {
		return err
	}
	g.recordConfirmationOutcome(ctx, ref, row)
	return nil
}

func trackingNumbers(labels []Label) []string {
	out := make([]string, len(labels))
	for i, l := range labels {
		out[i] = l.TrackingNumber
	}
	return out
}

// recordConfirmationOutcome stores what the network made of the confirmation,
// best effort: the write is already accepted, so a failing read only means the
// next call tries again.
func (g *LiveGateway) recordConfirmationOutcome(ctx context.Context, ref shared.NetworkRef, row Submission) {
	if row.TransactionID == "" || row.Status.Terminal() {
		return
	}
	status, err := g.reconcile(ctx, row)
	if err != nil {
		g.logger.Warn("retail-network: could not read the outcome of a shipment confirmation", "networkRef", ref, "err", err)
		return
	}
	if status != contract.SubmissionFailure {
		return
	}
	failed, _, _ := g.store.Submission(ctx, row.Kind, row.Subject)
	g.logger.Error("retail-network: the network failed a shipment confirmation; the order stays accepted there until an operator acts",
		"networkRef", ref, "errors", failed.Errors)
}

// ---- the supply loop's seams (Decision 1, ADR 0027) ----

// DeclareCapability is a documented, non-failing no-op: the node profile belongs
// to the supply loop, ADR 0027. It logs once at Info, then at Debug.
func (g *LiveGateway) DeclareCapability(_ context.Context, _ contract.CapabilityDeclaration) error {
	g.noteSupplyNoOp(&g.capabilityNoted, "DeclareCapability")
	return nil
}

// SubmitAvailability is a documented, non-failing no-op: the inventory feed
// belongs to the supply loop, ADR 0027. It logs once at Info, then at Debug.
func (g *LiveGateway) SubmitAvailability(_ context.Context, _ contract.AvailabilityUpdate) error {
	g.noteSupplyNoOp(&g.availabilityNoted, "SubmitAvailability")
	return nil
}

func (g *LiveGateway) noteSupplyNoOp(noted *atomic.Bool, method string) {
	msg := "retail-network: " + method + " is a no-op in the live gateway until the supply loop (ADR 0027) is built"
	if noted.CompareAndSwap(false, true) {
		g.logger.Info(msg, "method", method)
		return
	}
	g.logger.Debug(msg, "method", method)
}
