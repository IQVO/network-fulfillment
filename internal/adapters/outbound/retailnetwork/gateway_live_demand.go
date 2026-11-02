package retailnetwork

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

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
