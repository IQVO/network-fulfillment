// Package memory holds in-memory outbound adapters. They back the unit
// tests and the zero-configuration local run: with no DATABASE_URL the
// service is fully functional over REST on these, matching the
// convention every sibling service in this fleet follows.
package memory

import (
	"context"
	"sync"

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

// ListUnanswered returns every order still in NEW. Iteration order is
// deliberately not specified: the sweep must not depend on it, and a
// map gives no guarantee anyway.
func (r *NetworkOrderRepo) ListUnanswered(_ context.Context) ([]*networkorder.NetworkOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*networkorder.NetworkOrder
	for _, o := range r.orders {
		if o.State() == networkorder.StateNew {
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

// ProductTranslation is an in-memory ports.ProductTranslation: the
// Anti-Corruption Layer's dictionary, seeded explicitly.
//
// A static table is the correct v1. The mapping is a business fact
// somebody must curate, and pretending it can be derived — by string
// convention, or by asking a catalogue that does not know the network's
// identifiers — is how an ACL quietly stops being one.
type ProductTranslation struct {
	mu  sync.RWMutex
	skm map[shared.NetworkProductId]shared.SKU
}

func NewProductTranslation() *ProductTranslation {
	return &ProductTranslation{skm: make(map[shared.NetworkProductId]shared.SKU)}
}

func (t *ProductTranslation) Add(id shared.NetworkProductId, sku shared.SKU) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.skm[id] = sku
}

func (t *ProductTranslation) ToSKU(_ context.Context, id shared.NetworkProductId) (shared.SKU, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	sku, ok := t.skm[id]
	if !ok {
		return "", shared.ErrUnknownProduct
	}
	return sku, nil
}
