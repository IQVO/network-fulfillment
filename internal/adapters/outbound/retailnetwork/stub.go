package retailnetwork

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var _ ports.NetworkGateway = (*StubGateway)(nil)

// StubGateway implements the network protocol against nothing at all. It
// records what was submitted so tests and local runs can assert on it,
// and returns whatever demand was seeded into it.
//
// It is a deliberate, first-class part of the design rather than test
// scaffolding: ADR 0001 §4 requires the whole service to be runnable,
// end to end, with no credentials in existence.
type StubGateway struct {
	mu sync.Mutex

	pending  []contract.InboundDemand
	inbound  []contract.InboundShipment
	receipts map[string]contract.InboundReceipt

	acknowledged map[shared.NetworkRef]bool
	reasons      map[shared.NetworkRef]shared.RejectionReason
	confirmed    map[shared.NetworkRef]bool
	profiles     map[shared.SiteId]contract.NodeProfile

	logger *slog.Logger
}

func NewStubGateway(logger *slog.Logger) *StubGateway {
	if logger == nil {
		logger = slog.Default()
	}
	return &StubGateway{
		acknowledged: make(map[shared.NetworkRef]bool),
		reasons:      make(map[shared.NetworkRef]shared.RejectionReason),
		confirmed:    make(map[shared.NetworkRef]bool),
		profiles:     make(map[shared.SiteId]contract.NodeProfile),
		receipts:     make(map[string]contract.InboundReceipt),
		logger:       logger,
	}
}

// Seed adds demand the next PollDemand will return. Used by local runs
// and tests to drive the service without a network.
func (g *StubGateway) Seed(demand ...contract.InboundDemand) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pending = append(g.pending, demand...)
}

// PollDemand returns every unit still pending. It deliberately does NOT
// clear pending on return: that would advance the network's own cursor
// for demand we have not yet finished with, and the poller's contract
// (poller.go's own doc comment on `since`: "a pass where any demand
// failed leaves it where it was, so the next poll re-fetches that
// demand and tries again") requires a unit whose Execute fails to come
// back on the NEXT poll, not vanish. A real network's cursor only
// advances past an order once WE have acknowledged it; SubmitAcknowledgement
// is this stub's equivalent of that, so pending is trimmed there instead
// — see its own comment.
func (g *StubGateway) PollDemand(_ context.Context, _ time.Time) ([]contract.InboundDemand, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]contract.InboundDemand, len(g.pending))
	copy(out, g.pending)
	return out, nil
}

// SubmitAcknowledgement records the network's answer AND retires the
// demand from pending. This is the point ReceiveNetworkDemand has fully
// answered the order (see its reject/acknowledge, both of which call
// this before anything else that could still fail) — so it is the right
// place to stop re-delivering it, mirroring a real network's own cursor
// only advancing once we have told it something. A unit whose use case
// failed BEFORE reaching here (e.g. the order-management call errored)
// is deliberately left in pending, so the next PollDemand hands it back
// for a retry rather than losing it.
func (g *StubGateway) SubmitAcknowledgement(_ context.Context, ref shared.NetworkRef, answer contract.Acknowledgement) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.acknowledged[ref] = answer.Accepted
	g.reasons[ref] = answer.Reason
	g.pending = removeByRef(g.pending, ref)
	g.logger.Info("stub network acknowledgement", "networkRef", ref, "accepted", answer.Accepted, "reason", answer.Reason)
	return nil
}

// removeByRef returns pending with every demand matching ref dropped,
// preserving order and without mutating the input slice's backing array
// (PollDemand may be holding a copy taken from it concurrently).
func removeByRef(pending []contract.InboundDemand, ref shared.NetworkRef) []contract.InboundDemand {
	out := make([]contract.InboundDemand, 0, len(pending))
	for _, d := range pending {
		if d.NetworkRef != ref {
			out = append(out, d)
		}
	}
	return out
}

func (g *StubGateway) SubmitShipmentConfirmation(_ context.Context, ref shared.NetworkRef, shippedAt time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.confirmed[ref] = true
	g.logger.Info("stub network shipment confirmation", "networkRef", ref, "shippedAt", shippedAt)
	return nil
}

// SubmitAvailability records what we claim we can ship, for tests and
// local runs to assert on; the stub makes no external call.
func (g *StubGateway) SubmitAvailability(_ context.Context, update contract.AvailabilityUpdate) error {
	g.logger.Info("stub network availability update", "siteId", update.SiteId, "sku", update.SKU, "quantity", update.Quantity)
	return nil
}

// DeclareNodeProfile records the profile (for tests and local runs to assert on)
// and sends nothing: the stub makes no external call, so a declaration never
// leaves the process and has no revision (ADR 0027).
func (g *StubGateway) DeclareNodeProfile(_ context.Context, profile contract.NodeProfile) (contract.ProfileResult, error) {
	g.mu.Lock()
	g.profiles[profile.SiteId] = profile
	g.mu.Unlock()
	g.logger.Info("stub network node profile declaration (nothing sent)", "siteId", profile.SiteId,
		"cutoffs", len(profile.Cutoffs), "handlingTime", profile.HandlingTime, "throughputPerDay", profile.ThroughputPerDay)
	return contract.ProfileResult{Outcome: contract.ProfileUnchanged}, nil
}

// DeclaredProfile reports the profile last passed to DeclareNodeProfile for a
// site, and whether there was one.
func (g *StubGateway) DeclaredProfile(site shared.SiteId) (contract.NodeProfile, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	p, ok := g.profiles[site]
	return p, ok
}

// RequestLabel returns deterministic fake labels, one per package — never a
// ship-to name, address or phone number, same as a real adapter would be
// restricted to (ADR 0009 §3). Package 1 keeps the plain "<ref>" suffix; the
// others add "-<n>".
func (g *StubGateway) RequestLabel(_ context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error) {
	labels := make([]contract.LabelResult, 0, max(packageCount, 1))
	for n := 1; n <= max(packageCount, 1); n++ {
		suffix := string(ref)
		if n > 1 {
			suffix = fmt.Sprintf("%s-%d", ref, n)
		}
		labels = append(labels, contract.LabelResult{
			LabelRef:       "stub-label-" + suffix,
			TrackingNumber: "stub-tracking-" + suffix,
			Carrier:        "STUB_CARRIER",
		})
	}
	return labels, nil
}

// SubmissionStatus reconciles instantly and deterministically: the stub
// never leaves anything genuinely pending. A ref that was acknowledged or
// confirmed reconciles to SUCCESS; any other ref (not yet submitted, or
// unknown) reconciles to PENDING rather than FAILURE, so a caller never
// mistakes "nothing has happened yet" for a real network refusal.
func (g *StubGateway) SubmissionStatus(_ context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.acknowledged[ref]; ok {
		return contract.SubmissionSuccess, nil
	}
	if g.confirmed[ref] {
		return contract.SubmissionSuccess, nil
	}
	return contract.SubmissionPending, nil
}

// RejectionReason reports the reason submitted with a rejection of ref (empty
// for an acceptance or when nothing was submitted).
func (g *StubGateway) RejectionReason(ref shared.NetworkRef) shared.RejectionReason {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reasons[ref]
}

// Acknowledgement reports what was submitted for a ref, and whether
// anything was.
func (g *StubGateway) Acknowledgement(ref shared.NetworkRef) (accepted bool, submitted bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.acknowledged[ref]
	return v, ok
}

// ---- the inbound loop (ADR 0028) ----

var _ ports.InboundGateway = (*StubGateway)(nil)

// SeedInbound adds shipments the next PollInboundShipments returns, for local
// runs and tests.
func (g *StubGateway) SeedInbound(shipments ...contract.InboundShipment) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inbound = append(g.inbound, shipments...)
}

// PollInboundShipments returns the seeded shipments of site shipped after since.
func (g *StubGateway) PollInboundShipments(_ context.Context, site shared.SiteId, since time.Time) ([]contract.InboundShipment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []contract.InboundShipment
	for _, s := range g.inbound {
		if s.SiteId == site && s.ShippedAt.After(since) {
			out = append(out, s)
		}
	}
	return out, nil
}

// SubmitInboundReceipt records the receipt, once per shipment.
func (g *StubGateway) SubmitInboundReceipt(_ context.Context, r contract.InboundReceipt) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, done := g.receipts[r.ShipmentRef]; !done {
		g.receipts[r.ShipmentRef] = r
	}
	g.logger.Info("stub network inbound receipt", "shipment", r.ShipmentRef, "cartons", len(r.Cartons))
	return nil
}

// InboundReceipt reports the receipt submitted for a shipment, if any.
func (g *StubGateway) InboundReceipt(shipmentRef string) (contract.InboundReceipt, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.receipts[shipmentRef]
	return r, ok
}
