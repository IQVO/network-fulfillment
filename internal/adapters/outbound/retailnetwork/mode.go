// Package retailnetwork holds the outbound adapters for the external retail
// network (the fleet's own retail-network, ADR 0009) and the NETWORK_MODE
// switch that decides which one is wired: the credential-free StubGateway and
// the LiveGateway that calls the network's Vendor API (ADR 0026).
//
// This package is the ONLY place in the codebase that may know the network's
// own vocabulary. Nothing above it — no use case, no domain type, no other
// adapter — names a purchase order number, a listing, a node id, a package
// number, a transaction id or a network reason code (ADR 0001 §2, ADR 0009 §2).
package retailnetwork

import "strings"

// Mode selects which gateway implementation is wired at boot.
type Mode string

const (
	// ModeStub talks to nobody. It is the DEFAULT, deliberately: the
	// kind cluster and the e2e suite must never need credentials, and a
	// live network call must be impossible to make by accident (ADR
	// 0009 §4).
	ModeStub Mode = "stub"

	// ModeLive calls the network's Vendor API at NETWORK_BASE_URL as the
	// vendor NETWORK_VENDOR_ID (ADR 0026). There is no credential: the
	// fleet has none and neither has the Vendor API.
	ModeLive Mode = "live"
)

// ParseMode reads NETWORK_MODE. Anything unrecognised — including the
// empty string, and the retired "sandbox" (ADR 0009 §4 removed it: the
// network has no separate sandbox tier) — is stub. Failing CLOSED matters
// more than failing loudly here: a typo in a deployment manifest must not
// be the reason this service starts submitting real acknowledgements to
// a real network.
func ParseMode(s string) Mode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case string(ModeLive):
		return ModeLive
	default:
		return ModeStub
	}
}
