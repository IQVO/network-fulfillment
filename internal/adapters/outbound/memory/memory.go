// Package memory holds in-memory outbound adapters. They are TEST DOUBLES
// for the unit and BDD tests only: no production binary (cmd/**) may
// import this package, and the architecture sensor
// TestRuntimeBinaryHasNoInMemoryAdapters fails the build if one does.
// The service runs on the Postgres adapters.
package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// NetworkOrderRepo is an in-memory ports.NetworkOrderRepo.
type NetworkOrderRepo struct {
	mu     sync.RWMutex
	orders map[shared.NetworkRef]*networkorder.NetworkOrder
}

func NewNetworkOrderRepo() *NetworkOrderRepo {
	return &NetworkOrderRepo{orders: make(map[shared.NetworkRef]*networkorder.NetworkOrder)}
}

func (r *NetworkOrderRepo) Save(_ context.Context, o *networkorder.NetworkOrder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orders[o.NetworkRef()] = o
	return nil
}

// FindByRef returns (nil, nil) when no order has this ref — "not found"
// is the application's concern, not the repository's.
func (r *NetworkOrderRepo) FindByRef(_ context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.orders[ref], nil
}

// ListUnanswered returns every order still in NEW, soonest acknowledgement
// deadline first (ties broken by networkRef so the order is total and
// stable). apis/openapi.yaml promises "soonest deadline first" and the
// Postgres adapter does `ORDER BY acknowledge_by`; this double sorts the
// same way so tests see the same contract. The sweep must still not
// depend on the order — it does not — but the REST surface does.
func (r *NetworkOrderRepo) ListUnanswered(_ context.Context) ([]*networkorder.NetworkOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*networkorder.NetworkOrder
	for _, o := range r.orders {
		if o.State() == networkorder.StateNew {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].AcknowledgeBy().Equal(out[j].AcknowledgeBy()) {
			return out[i].AcknowledgeBy().Before(out[j].AcknowledgeBy())
		}
		return out[i].NetworkRef() < out[j].NetworkRef()
	})
	return out, nil
}

// ListSubmitted returns every order currently SUBMITTED — accepted and
// told to the network, but not yet reconciled. See ListUnanswered's
// comment on iteration order; the same applies here.
func (r *NetworkOrderRepo) ListSubmitted(_ context.Context) ([]*networkorder.NetworkOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*networkorder.NetworkOrder
	for _, o := range r.orders {
		if o.State() == networkorder.StateSubmitted {
			out = append(out, o)
		}
	}
	return out, nil
}

// ListAll returns every order regardless of state. Iteration order is
// deliberately not specified, for the same reason as ListUnanswered.
func (r *NetworkOrderRepo) ListAll(_ context.Context) ([]*networkorder.NetworkOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*networkorder.NetworkOrder, 0, len(r.orders))
	for _, o := range r.orders {
		out = append(out, o)
	}
	return out, nil
}

// ListOpenHolds returns every order whose order-management hold is recorded
// and has no outcome, in NEW or REJECTED (ADR 0024 §3), oldest acknowledgement
// deadline first so a pass frees the longest-leaked hold first. It matches
// the Postgres query's narrowing, not the orphan rule: the sweep still asks
// the domain whether acknowledgeBy plus the grace has passed.
func (r *NetworkOrderRepo) ListOpenHolds(_ context.Context) ([]*networkorder.NetworkOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*networkorder.NetworkOrder
	for _, o := range r.orders {
		if !o.HoldOpen() {
			continue
		}
		if o.State() == networkorder.StateNew || o.State() == networkorder.StateRejected {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].AcknowledgeBy().Equal(out[j].AcknowledgeBy()) {
			return out[i].AcknowledgeBy().Before(out[j].AcknowledgeBy())
		}
		return out[i].NetworkRef() < out[j].NetworkRef()
	})
	return out, nil
}

// CapabilityOfferRepo is an in-memory ports.CapabilityOfferRepo, keyed
// by (SKU, SiteId) so Save genuinely upserts the one current snapshot
// per pair rather than accumulating history.
type CapabilityOfferRepo struct {
	mu     sync.RWMutex
	offers map[capabilityOfferKey]capabilityoffer.CapabilityOffer
}

type capabilityOfferKey struct {
	sku    shared.SKU
	siteId shared.SiteId
}

func NewCapabilityOfferRepo() *CapabilityOfferRepo {
	return &CapabilityOfferRepo{offers: make(map[capabilityOfferKey]capabilityoffer.CapabilityOffer)}
}

func (r *CapabilityOfferRepo) Save(_ context.Context, o capabilityoffer.CapabilityOffer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offers[capabilityOfferKey{sku: o.SKU(), siteId: o.SiteId()}] = o
	return nil
}

func (r *CapabilityOfferRepo) ListAll(_ context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]capabilityoffer.CapabilityOffer, 0, len(r.offers))
	for _, o := range r.offers {
		out = append(out, o)
	}
	return out, nil
}
