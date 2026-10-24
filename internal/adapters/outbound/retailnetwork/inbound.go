package retailnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The inbound half of the Vendor API (ADR 0028): the SHIPPED inbound shipments
// bound for the vendor's nodes, and the receipt the dock reports back. The wire
// shapes below are the network's Published Language and stay in this package.

var (
	pathInboundShipments = []string{"vendor", "inbound-shipments"}
	pathInboundReceipts  = []string{"vendor", "inbound-receipts"}
)

// ---- wire shapes (apis/openapi.yaml of vendor-connectivity) ----

type shipmentItemDTO struct {
	GTIN      string `json:"gtin"`
	SKU       string `json:"sku,omitempty"`
	QtyEaches int64  `json:"qtyEaches"`
	Lot       string `json:"lot,omitempty"`
	Expiry    string `json:"expiry,omitempty"`
}

type dimsDTO struct {
	L int64 `json:"l"`
	W int64 `json:"w"`
	H int64 `json:"h"`
}

type shipmentCartonDTO struct {
	SSCC         string            `json:"sscc"`
	ParentSSCC   string            `json:"parentSscc,omitempty"`
	Kind         string            `json:"kind"`
	Contents     []shipmentItemDTO `json:"contents"`
	GrossWeightG int64             `json:"grossWeightG,omitempty"`
	DimsMm       *dimsDTO          `json:"dimsMm,omitempty"`
}

type shipmentPalletDTO struct {
	SSCC    string              `json:"sscc"`
	Cartons []shipmentCartonDTO `json:"cartons"`
}

type inboundShipmentDTO struct {
	ShipmentID string              `json:"shipmentId"`
	SellerID   string              `json:"sellerId,omitempty"`
	NodeID     string              `json:"nodeId"`
	ShippedAt  time.Time           `json:"shippedAt"`
	Status     string              `json:"status"`
	Pallets    []shipmentPalletDTO `json:"pallets"`
	Cartons    []shipmentCartonDTO `json:"cartons"`
}

type inboundShipmentPageDTO struct {
	InboundShipments []inboundShipmentDTO `json:"inboundShipments"`
	NextToken        string               `json:"nextToken,omitempty"`
}

type receiptItemDTO struct {
	GTIN      string `json:"gtin"`
	QtyEaches int64  `json:"qtyEaches"`
}

type receiptCartonDTO struct {
	SSCC            string           `json:"sscc"`
	Status          string           `json:"status"`
	Items           []receiptItemDTO `json:"items"`
	ComplianceCodes []string         `json:"complianceCodes"`
}

type inboundReceiptRequestDTO struct {
	VendorID   string             `json:"vendorId"`
	ShipmentID string             `json:"shipmentId"`
	NodeID     string             `json:"nodeId"`
	ReceivedAt time.Time          `json:"receivedAt"`
	Cartons    []receiptCartonDTO `json:"cartons"`
}

const stateShipped = "SHIPPED"

// ---- client ----

func (c *apiClient) inboundPage(ctx context.Context, nodeID string, createdAfter time.Time, nextToken string) (inboundShipmentPageDTO, error) {
	q := c.vendorQuery()
	q.Set("limit", strconv.Itoa(pollPageLimit))
	if nextToken != "" {
		q.Set("nextToken", nextToken)
	} else {
		q.Set("nodeId", nodeID)
		if !createdAfter.IsZero() {
			q.Set("createdAfter", createdAfter.UTC().Format(time.RFC3339Nano))
		}
	}
	var page inboundShipmentPageDTO
	_, err := c.call(ctx, request{op: "pollInboundShipments", method: http.MethodGet, path: pathInboundShipments, query: q}, &page)
	return page, err
}

// pollInboundAll pages through every shipment of a node shipped after
// createdAfter; a repeated token or an absurd page count is an error, not a loop.
func (c *apiClient) pollInboundAll(ctx context.Context, nodeID string, createdAfter time.Time) ([]inboundShipmentDTO, error) {
	var all []inboundShipmentDTO
	seen := map[string]bool{}
	token := ""
	for range maxPollPages {
		page, err := c.inboundPage(ctx, nodeID, createdAfter, token)
		if err != nil {
			return nil, err
		}
		all = append(all, page.InboundShipments...)
		if page.NextToken == "" {
			return all, nil
		}
		if seen[page.NextToken] {
			return nil, fmt.Errorf("retail-network: pollInboundShipments: the network repeated page token %q", page.NextToken)
		}
		seen[page.NextToken] = true
		token = page.NextToken
	}
	return nil, fmt.Errorf("retail-network: pollInboundShipments: more than %d pages", maxPollPages)
}

// ---- request builder ----

func buildInboundReceiptBody(vendorID, nodeID string, r contract.InboundReceipt) ([]byte, error) {
	req := inboundReceiptRequestDTO{
		VendorID: vendorID, ShipmentID: r.ShipmentRef, NodeID: nodeID, ReceivedAt: r.ReceivedAt.UTC(),
		Cartons: make([]receiptCartonDTO, len(r.Cartons)),
	}
	for i, c := range r.Cartons {
		dto := receiptCartonDTO{SSCC: c.SSCC, Status: string(c.Status), Items: make([]receiptItemDTO, len(c.Items)), ComplianceCodes: append([]string{}, c.ComplianceCodes...)}
		for j, it := range c.Items {
			dto.Items[j] = receiptItemDTO{GTIN: it.GTIN, QtyEaches: it.QtyEaches}
		}
		req.Cartons[i] = dto
	}
	return json.Marshal(req)
}

// ---- gateway ----

var _ ports.InboundGateway = (*LiveGateway)(nil)

// PollInboundShipments returns the SHIPPED shipments bound for site's node
// shipped after since (less the overlap of ADR 0026 Decision 5), flattened to
// cartons. A site without a node yet has nothing to poll: that is an empty
// answer, not an error, because nodes converge on the demand poll.
func (g *LiveGateway) PollInboundShipments(ctx context.Context, site shared.SiteId, since time.Time) ([]contract.InboundShipment, error) {
	nodeID, ok, err := g.store.NodeIDForSite(ctx, string(site))
	if err != nil {
		return nil, err
	}
	if !ok {
		g.logger.Debug("retail-network: no inbound poll, the site has no node yet", "site", site)
		return nil, nil
	}
	dtos, err := g.client.pollInboundAll(ctx, nodeID, pollWindowStart(since))
	if err != nil {
		return nil, err
	}
	out := make([]contract.InboundShipment, 0, len(dtos))
	seen := make(map[string]bool, len(dtos))
	for _, d := range dtos {
		if d.Status != stateShipped || seen[d.ShipmentID] {
			continue
		}
		seen[d.ShipmentID] = true
		out = append(out, shipmentFromDTO(d, site))
	}
	return out, nil
}

func shipmentFromDTO(d inboundShipmentDTO, site shared.SiteId) contract.InboundShipment {
	s := contract.InboundShipment{Ref: d.ShipmentID, OwnerRef: d.SellerID, SiteId: site, ShippedAt: d.ShippedAt}
	for _, p := range d.Pallets {
		for _, c := range p.Cartons {
			if c.ParentSSCC == "" {
				c.ParentSSCC = p.SSCC
			}
			s.Cartons = append(s.Cartons, cartonFromDTO(c))
		}
	}
	for _, c := range d.Cartons {
		s.Cartons = append(s.Cartons, cartonFromDTO(c))
	}
	return s
}

func cartonFromDTO(c shipmentCartonDTO) contract.InboundCarton {
	out := contract.InboundCarton{SSCC: c.SSCC, ParentSSCC: c.ParentSSCC, Kind: contract.CartonKind(c.Kind), GrossWeightG: c.GrossWeightG}
	if c.DimsMm != nil {
		out.Dims = &contract.CartonDims{LengthMm: c.DimsMm.L, WidthMm: c.DimsMm.W, HeightMm: c.DimsMm.H}
	}
	for _, it := range c.Contents {
		out.Contents = append(out.Contents, contract.InboundContent{GTIN: it.GTIN, QtyEaches: it.QtyEaches, Lot: it.Lot, Expiry: it.Expiry})
	}
	return out
}

// ErrUnknownSite: a receipt names a site that has no node on the network.
var ErrUnknownSite = errors.New("retail-network: the site has no node on the network")

// SubmitInboundReceipt reports a receipt exactly once per shipment. The ledger
// row (kind INBOUND_RECEIPT, subject = shipment) holds the exact body and a
// deterministic Idempotency-Key before the first call; a retry sends the stored
// body; a row that already has a transaction is never sent again. A network
// FAILURE of the transaction is returned as an error naming the network's
// errors, so the caller records it; a definitive refusal (for example a carton
// that is not on the manifest) is ErrWriteRefused.
func (g *LiveGateway) SubmitInboundReceipt(ctx context.Context, r contract.InboundReceipt) error {
	nodeID, ok, err := g.store.NodeIDForSite(ctx, string(r.SiteId))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownSite, r.SiteId)
	}
	body, err := buildInboundReceiptBody(g.client.vendorID, nodeID, r)
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindInboundReceipt, Subject: r.ShipmentRef, IdempotencyKey: idempotencyKey("rcpt", r.ShipmentRef), Body: body,
	})
	if err != nil {
		return err
	}
	if row, err = g.send(ctx, row, "submitInboundReceipt", pathInboundReceipts); err != nil {
		return err
	}
	if row.Status == StatusSuccess {
		return nil
	}
	status, err := g.reconcile(ctx, row)
	if err != nil {
		return err
	}
	if status == contract.SubmissionFailure {
		failed, _, _ := g.store.Submission(ctx, row.Kind, row.Subject)
		return fmt.Errorf("%w: the network failed the inbound receipt of %s: %s", ErrWriteRefused, r.ShipmentRef, failed.Errors)
	}
	return nil
}
