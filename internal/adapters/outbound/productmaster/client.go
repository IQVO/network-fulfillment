// Package productmaster is the outbound adapter to the fleet's product-master
// context REST API (ADR 0028): GTIN resolution for the ASN mapping, and
// (later) pack-level declaration. product-master's own docs say no service
// should call GET /gtins on a hot path: this one is not on a request path, it
// runs in a background loop, and a Postgres cache (CachingResolver) means a GTIN
// is asked for once.
package productmaster

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
const DependencyName = "product-master"

// Client implements ports.GtinResolver.
type Client struct{ rest *fleetrest.Client }

var _ ports.GtinResolver = (*Client)(nil)

// New wraps a fleetrest client built for product-master's base URL.
func New(rest *fleetrest.Client) *Client { return &Client{rest: rest} }

type gtinDTO struct {
	GTIN         string `json:"gtin"`
	SKU          string `json:"sku"`
	Level        string `json:"level"`
	UnitsPerPack int    `json:"unitsPerPack"`
	Active       bool   `json:"active"`
}

// ResolveGtin asks GET /gtins/{gtin}. 404 is contract.ErrGtinNotFound.
func (c *Client) ResolveGtin(ctx context.Context, gtin string) (contract.GtinResolution, error) {
	resp, err := c.rest.Do(ctx, fleetrest.Request{Method: http.MethodGet, Path: []string{"gtins", gtin}})
	if err != nil {
		return contract.GtinResolution{}, err
	}
	switch resp.Status {
	case http.StatusOK:
		var d gtinDTO
		if err := json.Unmarshal(resp.Body, &d); err != nil {
			return contract.GtinResolution{}, fmt.Errorf("product-master: GET /gtins/%s: undecodable answer: %w", gtin, err)
		}
		if d.SKU == "" || d.Level == "" {
			return contract.GtinResolution{}, fmt.Errorf("product-master: GET /gtins/%s: the answer names no sku or level", gtin)
		}
		return contract.GtinResolution{GTIN: gtin, SKU: d.SKU, Level: d.Level, UnitsPerPack: d.UnitsPerPack, Active: d.Active}, nil
	case http.StatusNotFound:
		return contract.GtinResolution{}, contract.ErrGtinNotFound
	}
	return contract.GtinResolution{}, fmt.Errorf("product-master: GET /gtins/%s: %d", gtin, resp.Status)
}

// CachingResolver answers from the local cache and falls back to product-master;
// only an ANSWER is cached, so a GTIN declared later is found on the next try.
type CachingResolver struct {
	Inner ports.GtinResolver
	Cache ports.GtinCache
}

var _ ports.GtinResolver = (*CachingResolver)(nil)

// ResolveGtin implements ports.GtinResolver.
func (r *CachingResolver) ResolveGtin(ctx context.Context, gtin string) (contract.GtinResolution, error) {
	if cached, ok, err := r.Cache.CachedGtin(ctx, gtin); err != nil {
		return contract.GtinResolution{}, err
	} else if ok {
		return cached, nil
	}
	res, err := r.Inner.ResolveGtin(ctx, gtin)
	if err != nil {
		return contract.GtinResolution{}, err
	}
	if err := r.Cache.CacheGtin(ctx, res); err != nil {
		return contract.GtinResolution{}, err
	}
	return res, nil
}
