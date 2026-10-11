// Package ordermanagement is the outbound adapter for order-management's
// REST API. It is the downstream half of this context's Anti-Corruption
// Layer: everything it sends is already in the fleet's shared
// vocabulary, and nothing the network said reaches it.
package ordermanagement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Planner implements ports.FulfillmentPlanner against order-management's
// REST API (its ADR 0020 endpoints).
type Planner struct {
	BaseURL string
	HTTP    *http.Client
}

func NewPlanner(baseURL string, client *http.Client) *Planner {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Planner{BaseURL: baseURL, HTTP: client}
}

type orderLineRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type receiveOrderRequest struct {
	Lines []orderLineRequest `json:"lines"`
	// ReleaseOnAllocation false is the hold: order-management allocates
	// and computes a promise, but puts no work on the floor until we
	// acknowledge to the network (its ADR 0020 §1).
	ReleaseOnAllocation bool `json:"releaseOnAllocation"`
	// AllowPartialShipment is forced false. The network's protocol is
	// fill-or-kill and order-management REJECTS a held order that allows
	// partial shipment (its ADR 0020 §4) — so this is not a default we
	// are choosing, it is an invariant we must satisfy.
	AllowPartialShipment bool `json:"allowPartialShipment"`
	// RequiredShipBy hands the network's deadline to order-management so
	// IT decides feasibility via PromisePolicy.FeasibleBy. This is what
	// ADR 0001 §7 means by "asked, never recomputed here".
	RequiredShipBy time.Time `json:"requiredShipBy"`
}

type orderResponse struct {
	ID          string     `json:"id"`
	PromiseDate *time.Time `json:"promiseDate"`
	Lines       []struct {
		Status string `json:"status"`
	} `json:"lines"`
}

// RaiseHeldOrder creates the held order and asks for a feasibility
// verdict, by sending the network's deadline as requiredShipBy.
//
// order-management answers with PromisePolicy.FeasibleBy: a promise
// means the deadline is makeable, and NO promise means it is not. We do
// not compare instants here and we do not reimplement the rule — the
// promise math lives in exactly one place in this fleet and it is not
// this context (ADR 0001 §7).
//
// An absent promiseDate is therefore a VERDICT, not a missing field.
// That is the one subtlety worth remembering when reading this: for an
// ordinary order a null promise means "not computed yet", but for an
// order carrying a deadline it means "we cannot meet it".
func (p *Planner) RaiseHeldOrder(ctx context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	// order-management's POST /orders is wrapped in a transactional
	// Idempotency-Key middleware and answers 400 idempotency-key-required
	// without the header (its #105). The key is derived from the NETWORK
	// ref, not generated: this call is retried by NF's poll loop (and after a
	// crash between saving the order and raising the hold), and a stable key
	// makes every retry replay the same local order instead of creating a
	// second held order that would reserve stock twice. An empty ref would
	// make every order share one key and silently dedupe unrelated orders,
	// so it is refused before any call is made.
	if req.NetworkRef == "" {
		return contract.HeldOrderResult{}, errors.New("raise held order: network ref is required (it is the idempotency key)")
	}

	lines := make([]orderLineRequest, 0, len(req.Lines))
	for sku, qty := range req.Lines {
		lines = append(lines, orderLineRequest{SKU: string(sku), Quantity: qty})
	}

	body := receiveOrderRequest{
		Lines:                lines,
		ReleaseOnAllocation:  false,
		AllowPartialShipment: false,
		RequiredShipBy:       req.RequiredShipBy,
	}

	var out orderResponse
	headers := map[string]string{idempotencyKeyHeader: raiseIdempotencyKey(req.NetworkRef)}
	if err := p.doWithHeaders(ctx, http.MethodPost, "/orders", headers, body, &out); err != nil {
		return contract.HeldOrderResult{}, err
	}

	// No promise means order-management could not find a window at or
	// before the deadline — not feasible, and explicitly NOT an error:
	// "we cannot make your date" is a legitimate answer that must reach
	// the network as a rejection.
	if out.PromiseDate == nil {
		return contract.HeldOrderResult{
			LocalOrderId: shared.LocalOrderId(out.ID),
			Feasible:     false,
		}, nil
	}

	// A promise came back, so order-management has committed to a
	// departure at or before the deadline. We take its word rather than
	// re-checking: a second opinion computed here could only ever
	// disagree with the authority, and disagreeing with the authority is
	// how two services end up promising different things.
	return contract.HeldOrderResult{
		LocalOrderId:   shared.LocalOrderId(out.ID),
		Feasible:       true,
		PromisedCutoff: *out.PromiseDate,
	}, nil
}

func (p *Planner) ReleaseHeldOrder(ctx context.Context, id shared.LocalOrderId) error {
	return p.do(ctx, http.MethodPost, fmt.Sprintf("/orders/%s/release", id), nil, nil)
}

// CancelHeldOrder frees the hold. order-management exposes cancellation
// as DELETE /orders/{id}, not a POST sub-resource -- verified against its
// OpenAPI spec on origin/develop rather than assumed, because this is the
// call that releases inventory reserved for demand we refused, and a 404
// here would silently recreate the orphaned-hold gap both ADRs flagged.
func (p *Planner) CancelHeldOrder(ctx context.Context, id shared.LocalOrderId) error {
	return p.do(ctx, http.MethodDelete, fmt.Sprintf("/orders/%s", id), nil, nil)
}

// idempotencyKeyHeader is the request header order-management's create
// endpoint requires.
const idempotencyKeyHeader = "Idempotency-Key"

// raiseIdempotencyKey is the stable key for raising the hold of one network
// order: the same network order always maps to the same key.
func raiseIdempotencyKey(ref shared.NetworkRef) string {
	return "netful-raise-" + string(ref)
}

func (p *Planner) do(ctx context.Context, method, path string, in, out any) error {
	return p.doWithHeaders(ctx, method, path, nil, in, out)
}

func (p *Planner) doWithHeaders(ctx context.Context, method, path string, headers map[string]string, in, out any) error {
	var buf *bytes.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(b)
	} else {
		buf = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, p.BaseURL+path, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := p.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("order-management %s %s: status %d", method, path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
