package retailnetwork

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// seedFile is the wire shape of a stub demand file.
//
// This is the ONE place a hand-written demand document is parsed, and it
// exists so a stub run can be driven declaratively — the same pattern the
// fleet already uses for PATH_CATALOGUE_FILE, and the honest alternative
// to a write endpoint that exists only for testing. The network offers no
// push (ADR 0001 §5), so an HTTP intake would be a second inbound path
// with no production counterpart.
//
// It is JSON in OUR vocabulary, not the network's: this is not a
// recording of an Amazon payload, and nothing here is a wire contract
// with anybody. When a real credentialed adapter lands, its own fixtures
// replace this and the ACL translation happens inside it.
type seedFile struct {
	Demands []seedDemand `json:"demands"`
}

type seedDemand struct {
	NetworkRef     string     `json:"networkRef"`
	SiteId         string     `json:"siteId"`
	RequiredShipBy string     `json:"requiredShipBy"`
	Lines          []seedLine `json:"lines"`
}

type seedLine struct {
	NetworkLineRef   string `json:"networkLineRef"`
	NetworkProductId string `json:"networkProductId"`
	Quantity         int    `json:"quantity"`
}

// LoadSeedFile reads demand from path and seeds it into g.
//
// Every failure is returned rather than logged-and-ignored: this runs at
// startup, and a seed file that silently failed to load would leave a
// stub deployment looking healthy while the inbound leg had nothing to
// deliver — indistinguishable from a broken poller, which is exactly the
// confusion this file is meant to avoid.
//
// requiredShipBy accepts either an RFC 3339 instant or a Go duration
// RELATIVE to now ("+36h"). Relative is the useful form for a file
// committed to a repo or mounted from a ConfigMap: a fixed instant goes
// stale and every order in the file becomes instantly infeasible, which
// looks like a promise bug rather than an expired fixture.
func LoadSeedFile(g *StubGateway, path string, now time.Time) (int, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is operator-controlled config (a boot-time env value), not user input.
	if err != nil {
		return 0, fmt.Errorf("read seed file: %w", err)
	}

	var file seedFile
	// DisallowUnknownFields, deliberately: a typo'd key in a hand-written
	// fixture would otherwise be silently dropped, and the resulting
	// "why did nothing arrive" is far more expensive than a startup error
	// naming the field.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return 0, fmt.Errorf("parse seed file %s: %w", path, err)
	}

	demands := make([]contract.InboundDemand, 0, len(file.Demands))
	for i, d := range file.Demands {
		shipBy, err := parseWhen(d.RequiredShipBy, now)
		if err != nil {
			return 0, fmt.Errorf("demand %d (%s): requiredShipBy: %w", i, d.NetworkRef, err)
		}

		lines := make([]contract.InboundLine, 0, len(d.Lines))
		for _, l := range d.Lines {
			lines = append(lines, contract.InboundLine{
				NetworkLineRef:   shared.NetworkLineRef(l.NetworkLineRef),
				NetworkProductId: shared.NetworkProductId(l.NetworkProductId),
				Quantity:         l.Quantity,
			})
		}

		demands = append(demands, contract.InboundDemand{
			NetworkRef:     shared.NetworkRef(d.NetworkRef),
			SiteId:         shared.SiteId(d.SiteId),
			RequiredShipBy: shipBy,
			Lines:          lines,
		})
	}

	// Validation stays in the domain: these go in as-is, and demand with
	// an empty ref or a zero quantity is rejected by NewLine/Receive with
	// the same error it would get from a real network. Pre-validating
	// here would duplicate those rules and let the two drift.
	g.Seed(demands...)
	return len(demands), nil
}

// parseWhen accepts an RFC 3339 instant or a relative Go duration.
func parseWhen(v string, now time.Time) (time.Time, error) {
	if v == "" {
		return time.Time{}, fmt.Errorf("must not be empty")
	}
	if v[0] == '+' || v[0] == '-' {
		d, err := time.ParseDuration(v)
		if err != nil {
			return time.Time{}, fmt.Errorf("not a duration: %w", err)
		}
		return now.Add(d), nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("not an RFC 3339 instant or a +duration: %w", err)
	}
	return t, nil
}
