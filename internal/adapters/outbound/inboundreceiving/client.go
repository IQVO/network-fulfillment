// Package inboundreceiving is the outbound adapter to the fleet's
// inbound-receiving context REST API (ADR 0028): it registers the ASN a polled
// shipment becomes. The wire shapes are inbound-receiving's and stop here.
package inboundreceiving

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// DependencyName labels the breaker's series.
const DependencyName = "inbound-receiving"

// Client implements ports.AsnRegistrar.
type Client struct{ rest *fleetrest.Client }

var _ ports.AsnRegistrar = (*Client)(nil)

// New wraps a fleetrest client built for inbound-receiving's base URL.
func New(rest *fleetrest.Client) *Client { return &Client{rest: rest} }

type contentDTO struct {
	LineNo int    `json:"lineNo"`
	SKU    string `json:"sku"`
	GTIN   string `json:"gtin"`
	Qty    int64  `json:"qty"`
	Lot    string `json:"lot,omitempty"`
	Expiry string `json:"expiry,omitempty"`
}

type dimsDTO struct {
	LengthMm int64 `json:"lengthMm"`
	WidthMm  int64 `json:"widthMm"`
	HeightMm int64 `json:"heightMm"`
}

type cartonDTO struct {
	SSCC         string       `json:"sscc"`
	ParentSSCC   string       `json:"parentSscc,omitempty"`
	Kind         string       `json:"kind"`
	Contents     []contentDTO `json:"contents"`
	GrossWeightG int64        `json:"grossWeightG,omitempty"`
	Dims         *dimsDTO     `json:"dims,omitempty"`
}

type lineDTO struct {
	LineNo      int    `json:"lineNo"`
	SKU         string `json:"sku"`
	ExpectedQty int64  `json:"expectedQty"`
}

type registerAsnDTO struct {
	AsnNumber   string      `json:"asnNumber"`
	SupplierRef string      `json:"supplierRef"`
	OwnerRef    string      `json:"ownerRef,omitempty"`
	Lines       []lineDTO   `json:"lines"`
	Cartons     []cartonDTO `json:"cartons"`
}

// Body is the exact JSON of POST /asns for asn.
func Body(asn contract.AsnRegistration) ([]byte, error) {
	dto := registerAsnDTO{AsnNumber: asn.AsnNumber, SupplierRef: asn.SupplierRef, OwnerRef: asn.OwnerRef}
	for _, l := range asn.Lines {
		dto.Lines = append(dto.Lines, lineDTO{LineNo: l.LineNo, SKU: l.SKU, ExpectedQty: l.ExpectedQty})
	}
	for _, c := range asn.Cartons {
		out := cartonDTO{SSCC: c.SSCC, ParentSSCC: c.ParentSSCC, Kind: string(c.Kind), GrossWeightG: c.GrossWeightG}
		if c.Dims != nil {
			out.Dims = &dimsDTO{LengthMm: c.Dims.LengthMm, WidthMm: c.Dims.WidthMm, HeightMm: c.Dims.HeightMm}
		}
		for _, x := range c.Contents {
			out.Contents = append(out.Contents, contentDTO{LineNo: x.LineNo, SKU: x.SKU, GTIN: x.GTIN, Qty: x.Qty, Lot: x.Lot, Expiry: x.Expiry})
		}
		dto.Cartons = append(dto.Cartons, out)
	}
	return json.Marshal(dto)
}

// idempotencyKey is deterministic per ASN number: a replay is a replay.
func idempotencyKey(asnNumber string) string { return "nf-asn-" + asnNumber }

// retryableProblems are refusals that waiting can fix: the product projection of
// inbound-receiving has not caught up with an SKU product-master already knows.
var retryableProblems = map[string]bool{"unknown-sku": true, "concurrent-update": true}

// RegisterAsn registers the ASN. 201 is success; 409 asn-already-exists is
// success too (this ASN was registered by an earlier attempt: its number is
// derived from the shipment, so it can only be ours); a 422 is a verdict, except
// unknown-sku which is retried; 400 is a verdict; anything else is an error.
func (c *Client) RegisterAsn(ctx context.Context, asn contract.AsnRegistration) error {
	body, err := Body(asn)
	if err != nil {
		return err
	}
	resp, err := c.rest.Do(ctx, fleetrest.Request{
		Method: http.MethodPost, Path: []string{"asns"}, Body: body,
		Header: map[string]string{"Idempotency-Key": idempotencyKey(asn.AsnNumber)},
	})
	if err != nil {
		return err
	}
	problem, detail := problemOf(resp.Body)
	switch {
	case resp.Status == http.StatusCreated || resp.Status == http.StatusOK:
		return nil
	case resp.Status == http.StatusConflict && problem == "asn-already-exists":
		return nil
	case resp.Status == http.StatusUnprocessableEntity || resp.Status == http.StatusBadRequest:
		return &contract.AsnRefusedError{Problem: problem, Detail: detail, Retryable: retryableProblems[problem]}
	}
	return fmt.Errorf("inbound-receiving: POST /asns: %d %s: %s", resp.Status, problem, detail)
}

// problemOf reads the slug (the last path element of `type`) and the detail of
// an application/problem+json body.
func problemOf(body []byte) (slug, detail string) {
	var p struct {
		Type   string `json:"type"`
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(body, &p)
	for i := len(p.Type) - 1; i >= 0; i-- {
		if p.Type[i] == '/' {
			return p.Type[i+1:], p.Detail
		}
	}
	return p.Type, p.Detail
}
