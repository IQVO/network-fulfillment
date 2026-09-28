// Package network holds the outbound adapter for the external retail
// network, and the NETWORK_MODE switch that decides which one is wired.
//
// This package is the ONLY place in the codebase that may know the
// network's own vocabulary. Nothing above it — no use case, no domain
// type, no other adapter — names a purchase order, an ASIN, a selling
// party or an acknowledgement code (ADR 0001 §2, §4).
package network

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Mode selects which gateway implementation is wired at boot.
type Mode string

const (
	// ModeStub talks to nobody. It is the DEFAULT, deliberately: the
	// kind cluster and the e2e suite must never need credentials, and a
	// live network call must be impossible to make by accident (ADR 0001
	// §4).
	ModeStub Mode = "stub"

	// ModeSandbox targets the network's sandbox. Requires credentials;
	// not yet implemented.
	ModeSandbox Mode = "sandbox"

	// ModeLive targets the real network. Requires credentials; not yet
	// implemented.
	ModeLive Mode = "live"
)

// ParseMode reads NETWORK_MODE. Anything unrecognised — including the
// empty string — is stub. Failing CLOSED matters more than failing
// loudly here: a typo in a deployment manifest must not be the reason
// this service starts submitting real acknowledgements to a real
// retailer.
func ParseMode(s string) Mode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case string(ModeSandbox):
		return ModeSandbox
	case string(ModeLive):
		return ModeLive
	default:
		return ModeStub
	}
}

// NewGateway wires the gateway for a mode.
//
// sandbox and live return an explicit error rather than silently
// degrading to the stub. A deployment that ASKED for a real network and
// got a fake one would look healthy while quietly answering nobody —
// the worst possible failure for this context.
func NewGateway(mode Mode, logger *slog.Logger) (ports.NetworkGateway, error) {
	switch mode {
	case ModeStub:
		return NewStubGateway(logger), nil
	case ModeSandbox, ModeLive:
		return nil, fmt.Errorf("NETWORK_MODE=%s is not implemented yet: no credentialed adapter exists (ADR 0001 rollout step 5)", mode)
	default:
		return nil, fmt.Errorf("unknown network mode %q", mode)
	}
}

// StubGateway implements the network protocol against nothing at all. It
// records what was submitted so tests and local runs can assert on it,
// and returns whatever demand was seeded into it.
//
// It is a deliberate, first-class part of the design rather than test
// scaffolding: ADR 0001 §4 requires the whole service to be runnable,
// end to end, with no credentials in existence.
type StubGateway struct {
	mu sync.Mutex

	pending []contract.InboundDemand

	acknowledged map[shared.NetworkRef]bool
	confirmed    map[shared.NetworkRef]bool

	logger *slog.Logger
}

func NewStubGateway(logger *slog.Logger) *StubGateway {
	if logger == nil {
		logger = slog.Default()
	}
	return &StubGateway{
		acknowledged: make(map[shared.NetworkRef]bool),
		confirmed:    make(map[shared.NetworkRef]bool),
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
func (g *StubGateway) SubmitAcknowledgement(_ context.Context, ref shared.NetworkRef, accepted bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.acknowledged[ref] = accepted
	g.pending = removeByRef(g.pending, ref)
	g.logger.Info("stub network acknowledgement", "networkRef", ref, "accepted", accepted)
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

func (g *StubGateway) SubmitShipmentConfirmation(_ context.Context, ref shared.NetworkRef) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.confirmed[ref] = true
	g.logger.Info("stub network shipment confirmation", "networkRef", ref)
	return nil
}

// Acknowledgement reports what was submitted for a ref, and whether
// anything was.
func (g *StubGateway) Acknowledgement(ref shared.NetworkRef) (accepted bool, submitted bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.acknowledged[ref]
	return v, ok
}
