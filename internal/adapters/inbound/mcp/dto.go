package mcp

import (
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// networkOrderDTO is a NetworkOrder as this MCP surface exposes it. It
// deliberately mirrors internal/adapters/inbound/http's
// networkOrderResponse field-for-field (same absence of customer PII per
// ADR 0001 hard rule 2), because the two are the same read projected onto
// two different transports and must never disagree about it.
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

// networkOrderLineDTO carries both vocabularies, same reasoning as the
// HTTP adapter's networkOrderLineResponse.
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
	if id := o.LocalOrderId(); id != nil {
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
