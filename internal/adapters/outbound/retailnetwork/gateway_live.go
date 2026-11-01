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

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/resilience"
)

// pollOverlap is how far before the poller's watermark a poll looks (ADR 0026
// Decision 5): a purchase order committed on the network just after the previous
// poll began is still found, and the re-delivery that follows is harmless.
const pollOverlap = 2 * time.Minute

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
	Store Store
	// Listings is the product dictionary read for one site (ADR 0027 §10): the
	// listing each SKU is fed under. Without it the gateway cannot feed inventory.
	Listings   ListingResolver
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

	// listings finds the listing a SKU is fed under at a site (the inventory feed).
	listings ListingResolver
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
	g := &LiveGateway{client: c, store: st, logger: cfg.Logger, listings: cfg.Listings, defs: make(map[string]NodeDefinition, len(cfg.Nodes))}
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

// Sites are the sites this gateway converges onto network nodes, in order: the
// ones DeclareNodeProfiles declares a profile for (ADR 0027).
func (g *LiveGateway) Sites() []string { return append([]string(nil), g.sites...) }

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
	return g.sendWith(ctx, row, http.MethodPost, op, path)
}

// sendWith is send for a write the Vendor API takes with another method (the
// node profile is a PUT).
func (g *LiveGateway) sendWith(ctx context.Context, row Submission, method, op string, path []string) (Submission, error) {
	if row.Status == StatusFailure && row.TransactionID == "" {
		return row, fmt.Errorf("%w earlier: %s", ErrWriteRefused, row.Errors)
	}
	if row.TransactionID != "" || row.Status.Terminal() {
		return row, nil
	}
	txID, err := g.client.writeAsync(ctx, method, op, path, row.IdempotencyKey, row.Body)
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
