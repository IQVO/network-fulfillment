// Package producttranslation is the Anti-Corruption Layer's product
// dictionary: the network's product identifier to our SKU. It is a
// production adapter, not a test double. The mapping is curated
// configuration loaded at boot from the JSON file named by
// PRODUCT_TRANSLATION_FILE (see LoadFile).
package producttranslation

import (
	"context"
	"sync"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Dictionary is a ports.ProductTranslation backed by a table held in
// the process, seeded from the curated file (LoadFile) or, in tests, with
// Add.
//
// A static table is the correct v1. The mapping is a business fact
// somebody must curate, and pretending it can be derived — by string
// convention, or by asking a catalogue that does not know the network's
// identifiers — is how an ACL quietly stops being one.
type Dictionary struct {
	mu  sync.RWMutex
	skm map[shared.NetworkProductId]shared.SKU
}

// New returns an empty dictionary.
func New() *Dictionary {
	return &Dictionary{skm: make(map[shared.NetworkProductId]shared.SKU)}
}

// Add maps one network product identifier to a SKU.
func (t *Dictionary) Add(id shared.NetworkProductId, sku shared.SKU) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.skm[id] = sku
}

// ToSKU translates id, or returns shared.ErrUnknownProduct.
func (t *Dictionary) ToSKU(_ context.Context, id shared.NetworkProductId) (shared.SKU, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	sku, ok := t.skm[id]
	if !ok {
		return "", shared.ErrUnknownProduct
	}
	return sku, nil
}

// KnownSKUs lists every distinct SKU this dictionary maps to.
func (t *Dictionary) KnownSKUs(_ context.Context) ([]shared.SKU, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	seen := make(map[shared.SKU]struct{}, len(t.skm))
	out := make([]shared.SKU, 0, len(t.skm))
	for _, sku := range t.skm {
		if _, dup := seen[sku]; dup {
			continue
		}
		seen[sku] = struct{}{}
		out = append(out, sku)
	}
	return out, nil
}
