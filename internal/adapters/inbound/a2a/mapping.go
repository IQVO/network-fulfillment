package a2a

import (
	"fmt"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// problemTypeBase namespaces this context's problem types. It is the SAME
// namespace as the REST adapter's problemBaseURI (internal/adapters/inbound/
// http/errors.go) so a delegating agent sees the type a REST caller would.
const problemTypeBase = "https://errors.network-fulfillment.warehouse-systems.dev"

// problem is the RFC 7807-shaped rejection an agent client receives instead of
// a result: domain rejections and request-shape failures are business answers,
// not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "/malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "/unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidArgument(detail string) *problem {
	return &problem{Type: problemTypeBase + "/invalid-argument", Title: "Invalid skill argument", Detail: detail, Status: 400}
}

func problemOrderNotFound(ref string) *problem {
	return &problem{Type: problemTypeBase + "/network-order-not-found", Title: "Network order not found", Detail: fmt.Sprintf("no network order has the network reference %q", ref), Status: 404}
}

func problemDependencyUnavailable(what string) *problem {
	return &problem{Type: problemTypeBase + "/dependency-unavailable", Title: "A dependency is not available, retry later", Detail: what, Status: 503}
}

func problemInternal() *problem {
	return &problem{Type: problemTypeBase + "/internal-error", Title: "An unexpected internal error occurred", Detail: "the skill failed; see server logs", Status: 500}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id the
// answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field, keeping the envelope
// flat and versioned as one contract.
type skillDTO struct {
	NetworkDemand          *networkDemand          `json:"networkDemand,omitempty"`
	AdvertisedAvailability *advertisedAvailability `json:"advertisedAvailability,omitempty"`
	summary                string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// networkDemand is the network_demand_status answer: the matching orders,
// always a list (one element for a networkRef lookup).
type networkDemand struct {
	Count  int               `json:"count"`
	Orders []networkOrderDTO `json:"orders"`
}

// advertisedAvailability is the advertised_availability answer.
type advertisedAvailability struct {
	Count  int                  `json:"count"`
	Offers []capabilityOfferDTO `json:"offers"`
}

// networkOrderDTO mirrors the REST networkOrderResponse and the MCP
// networkOrderDTO field for field (same absence of customer PII per AGENTS.md
// hard rule 2): the three surfaces are the same read on three transports and
// must never disagree about it.
type networkOrderDTO struct {
	NetworkRef             string                `json:"networkRef"`
	SiteId                 string                `json:"siteId"`
	State                  string                `json:"state"`
	RequiredShipBy         string                `json:"requiredShipBy"`
	AcknowledgeBy          string                `json:"acknowledgeBy"`
	ReceivedAt             string                `json:"receivedAt"`
	LocalOrderId           *string               `json:"localOrderId,omitempty"`
	AcknowledgementOverdue bool                  `json:"acknowledgementOverdue"`
	Lines                  []networkOrderLineDTO `json:"lines"`
}

// networkOrderLineDTO carries both vocabularies, like the REST line.
type networkOrderLineDTO struct {
	NetworkLineRef   string `json:"networkLineRef"`
	NetworkProductId string `json:"networkProductId"`
	SKU              string `json:"sku"`
	Quantity         int    `json:"quantity"`
}

func toNetworkOrderDTO(o *networkorder.NetworkOrder, now time.Time) networkOrderDTO {
	lines := make([]networkOrderLineDTO, 0, len(o.Lines()))
	for _, l := range o.Lines() {
		lines = append(lines, networkOrderLineDTO{
			NetworkLineRef:   string(l.NetworkLineRef()),
			NetworkProductId: string(l.NetworkProductId()),
			SKU:              string(l.SKU()),
			Quantity:         l.Quantity(),
		})
	}
	var localOrderId *string
	if id := o.CommittedLocalOrderId(); id != nil {
		s := string(*id)
		localOrderId = &s
	}
	return networkOrderDTO{
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

// capabilityOfferDTO mirrors the REST capabilityOfferResponse and the MCP
// capabilityOfferDTO field for field.
type capabilityOfferDTO struct {
	SKU                string `json:"sku"`
	SiteId             string `json:"siteId"`
	AdvertisedQuantity int    `json:"advertisedQuantity"`
	Basis              string `json:"basis"`
	ComputedAt         string `json:"computedAt"`
}

func toCapabilityOfferDTO(o capabilityoffer.CapabilityOffer) capabilityOfferDTO {
	return capabilityOfferDTO{
		SKU:                string(o.SKU()),
		SiteId:             string(o.SiteId()),
		AdvertisedQuantity: o.AdvertisedQuantity(),
		Basis:              string(o.Basis()),
		ComputedAt:         o.ComputedAt().UTC().Format(time.RFC3339),
	}
}
