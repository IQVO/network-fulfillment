package http

import (
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// capabilityOfferResponse is one CapabilityOffer (ADR 0001 §8) as this
// context's REST surface exposes it.
type capabilityOfferResponse struct {
	SKU                string `json:"sku"`
	SiteId             string `json:"siteId"`
	AdvertisedQuantity int    `json:"advertisedQuantity"`
	Basis              string `json:"basis"`
	ComputedAt         string `json:"computedAt"`
}

func toCapabilityOfferResponse(o capabilityoffer.CapabilityOffer) capabilityOfferResponse {
	return capabilityOfferResponse{
		SKU:                string(o.SKU()),
		SiteId:             string(o.SiteId()),
		AdvertisedQuantity: o.AdvertisedQuantity(),
		Basis:              string(o.Basis()),
		ComputedAt:         o.ComputedAt().UTC().Format(time.RFC3339),
	}
}

// networkOrderResponse is a NetworkOrder as this context's REST surface
// exposes it.
//
// WHAT IS ABSENT IS THE POINT. This context holds the network's
// ship-to name, address and phone (ADR 0001 hard rule 2: customer PII
// stops here), and no field of this struct can carry any of it. The
// aggregate does not model PII today, so this DTO is not yet removing
// anything — it is the place that must keep refusing to add it when the
// ship-to detail the outbound legs need does arrive.
//
// The network's own identifiers ARE exposed: a reader of this surface is
// an operator asking "what did we tell the network about purchase order
// X", and that question is unanswerable without them. They are the
// network's references, not its customer's identity.
type networkOrderResponse struct {
	NetworkRef     string `json:"networkRef"`
	SiteId         string `json:"siteId"`
	State          string `json:"state"`
	RequiredShipBy string `json:"requiredShipBy"`
	AcknowledgeBy  string `json:"acknowledgeBy"`
	ReceivedAt     string `json:"receivedAt"`

	// Absent until acknowledged. A pointer rather than an empty string so
	// "not linked yet" is distinguishable from "linked to nothing", which
	// is the correlation this fleet already got wrong once (OM ADR 0018).
	LocalOrderId *string `json:"localOrderId,omitempty"`

	// Derived, not stored: whether the acknowledgement window has closed
	// with this order still unanswered. Computed by the domain predicate
	// the sweep itself uses, so the surface cannot disagree with the
	// sweep about what overdue means.
	AcknowledgementOverdue bool `json:"acknowledgementOverdue"`

	Lines []networkOrderLineResponse `json:"lines"`
}

// networkOrderLineResponse is one line, in both vocabularies.
//
// Both are present deliberately: the SKU is what the rest of this fleet
// acted on, the network product id is what the network asked for, and an
// operator reconciling a disputed order needs to see that the
// translation between them was what they expect. Publishing only one
// makes the Anti-Corruption Layer's decision invisible.
type networkOrderLineResponse struct {
	NetworkLineRef   string `json:"networkLineRef"`
	NetworkProductId string `json:"networkProductId"`
	SKU              string `json:"sku"`
	Quantity         int    `json:"quantity"`
}

func toNetworkOrderResponse(o *networkorder.NetworkOrder, now time.Time) networkOrderResponse {
	lines := make([]networkOrderLineResponse, 0, len(o.Lines()))
	for _, l := range o.Lines() {
		lines = append(lines, networkOrderLineResponse{
			NetworkLineRef:   string(l.NetworkLineRef()),
			NetworkProductId: string(l.NetworkProductId()),
			SKU:              string(l.SKU()),
			Quantity:         l.Quantity(),
		})
	}

	var localOrderId *string
	if id := o.LocalOrderId(); id != nil {
		s := string(*id)
		localOrderId = &s
	}

	return networkOrderResponse{
		NetworkRef:             string(o.NetworkRef()),
		SiteId:                 string(o.SiteId()),
		State:                  string(o.State()),
		RequiredShipBy:         o.RequiredShipBy().UTC().Format(time.RFC3339),
		AcknowledgeBy:          o.AcknowledgeBy().UTC().Format(time.RFC3339),
		ReceivedAt:             o.ReceivedAt().UTC().Format(time.RFC3339),
		LocalOrderId:           localOrderId,
		AcknowledgementOverdue: o.AcknowledgementOverdue(now),
		Lines:                  lines,
	}
}

// inboundStatusResponse answers "is the inbound leg alive".
//
// It exists because the poller's failure mode is SILENCE: a poller that
// stops fetching produces no orders, no errors and a perfectly healthy
// /healthz, while the 24h acknowledgement clock keeps running against
// wall time (ADR 0001: a poller outage is an SLA breach, not a delayed
// batch). Polls and since are how an operator tells a quiet network from
// a dead poller.
type inboundStatusResponse struct {
	// The gateway mode actually in force, echoed so it can be verified
	// rather than assumed — every *_MODE in this fleet defaults
	// permissive and several sat wrong in the cluster for weeks.
	NetworkMode string `json:"networkMode"`

	Polls    int `json:"polls"`
	Received int `json:"received"`
	Failed   int `json:"failed"`

	// The watermark the next poll will use. Absent on a cold start that
	// has not completed a clean pass yet, which is itself the signal:
	// zero here after minutes of uptime means no pass has ever succeeded.
	Since *string `json:"since,omitempty"`

	// Orders still unanswered, and how many of those have already missed
	// the window. Overdue > 0 is the fact the sweep reports and the one
	// worth alerting on.
	Unanswered int `json:"unanswered"`
	Overdue    int `json:"overdue"`
}

func toInboundStatusResponse(mode string, s poller.Stats, unanswered, overdue int) inboundStatusResponse {
	var since *string
	if !s.Since.IsZero() {
		v := s.Since.UTC().Format(time.RFC3339)
		since = &v
	}
	return inboundStatusResponse{
		NetworkMode: mode,
		Polls:       s.Polls,
		Received:    s.Received,
		Failed:      s.Failed,
		Since:       since,
		Unanswered:  unanswered,
		Overdue:     overdue,
	}
}
