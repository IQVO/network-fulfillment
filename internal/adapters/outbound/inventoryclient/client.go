// Package inventoryclient is the outbound adapter for inventory-storage's
// REST API, implementing ports.InventoryAvailability against its
// `GET /inventory/{sku}/usable` endpoint.
//
// This is a SYNCHRONOUS call, not a Kafka-fed cache -- a deliberate,
// documented departure from ADR 0001 §8's literal "three Kafka-fed
// local caches" wording for the inventory leg specifically. Why:
// inventory-storage publishes its stock-ledger events (StockReceived,
// ItemStowed, StockReserved, ReservationExpired, ReservationRevoked,
// StockPicked) to `warehouse.inventory.analytics` ONLY -- an
// ANALYTICS-ONLY topic, not a cross-context integration topic this
// service is meant to build a read model from. Rebuilding
// inventory-storage's own usable-inventory projection (on-hand minus
// active reservations minus held/unlocated stock) from that ledger in a
// second repository would be exactly the kind of promise-math
// duplication ADR 0001 §7 already rejected for order-management's
// figures ("duplicating ADR 0014's promise math in a second repository
// guarantees the two copies drift") -- the same reasoning applies
// symmetrically here, and inventory-storage already exposes the
// computed figure directly over REST.
//
// RecomputeCapabilityOffers (the one caller) runs this from its own
// background schedule, never from a request's hot path, so the
// synchronous round trip costs latency on a job already built to
// tolerate it, exactly like ordermanagement.Planner's own synchronous
// calls from ReceiveNetworkDemand.
package inventoryclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Client implements ports.InventoryAvailability.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func NewClient(baseURL string, client *http.Client) *Client {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{BaseURL: baseURL, HTTP: client}
}

type usableInventoryResponse struct {
	SKU    string `json:"sku"`
	Usable int    `json:"usable"`
}

// UsableQuantity calls GET /inventory/{sku}/usable. An unknown SKU
// reports 0, by that endpoint's own documented contract -- never an
// error, since "we hold none of this" is a legitimate, common answer.
func (c *Client) UsableQuantity(ctx context.Context, sku shared.SKU) (int, error) {
	path := fmt.Sprintf("/inventory/%s/usable", url.PathEscape(string(sku)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("inventory-storage GET %s: status %d", path, resp.StatusCode)
	}
	var out usableInventoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("inventory-storage GET %s: decode response: %w", path, err)
	}
	return out.Usable, nil
}
