// Package producttranslation is the Anti-Corruption Layer's product
// dictionary: the network's product identifier to our SKU, and (ADR 0027
// part 2) the same table read the other way for one site, to feed the
// network the quantity of a SKU under the identifier it knows it by. It is a
// production adapter, not a test double. The mapping is curated
// configuration loaded at boot from the JSON file named by
// PRODUCT_TRANSLATION_FILE (see LoadFile).
package producttranslation

import (
	"context"
	"slices"
	"sync"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// everySite is the site of an entry that names none: it applies to all of them.
const everySite shared.SiteId = ""

// listingKey is one (site, SKU) pair of the inverse table.
type listingKey struct {
	site shared.SiteId
	sku  shared.SKU
}

// listingEntry is the network product id a (site, SKU) pair is fed under.
// ambiguous is set when a second, different id claims the same pair: the
// dictionary has always allowed several network ids for one SKU on the way in,
// but on the way out that names no single listing, so the pair is not fed.
type listingEntry struct {
	id        shared.NetworkProductId
	ambiguous bool
}

// Dictionary is a ports.ProductTranslation backed by a table held in
// the process, seeded from the curated file (LoadFile) or, in tests, with
// Add and AddAt.
//
// A static table is the correct v1. The mapping is a business fact
// somebody must curate, and pretending it can be derived — by string
// convention, or by asking a catalogue that does not know the network's
// identifiers — is how an ACL quietly stops being one.
type Dictionary struct {
	mu       sync.RWMutex
	skm      map[shared.NetworkProductId]shared.SKU
	listings map[listingKey]listingEntry
}

// New returns an empty dictionary.
func New() *Dictionary {
	return &Dictionary{
		skm:      make(map[shared.NetworkProductId]shared.SKU),
		listings: make(map[listingKey]listingEntry),
	}
}

// Add maps one network product identifier to a SKU, at every site.
func (t *Dictionary) Add(id shared.NetworkProductId, sku shared.SKU) {
	t.AddAt(everySite, id, sku)
}

// AddAt maps one network product identifier to a SKU at one site (the empty
// site is every site).
func (t *Dictionary) AddAt(site shared.SiteId, id shared.NetworkProductId, sku shared.SKU) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.skm[id] = sku
	key := listingKey{site: site, sku: sku}
	switch existing, ok := t.listings[key]; {
	case !ok:
		t.listings[key] = listingEntry{id: id}
	case existing.id != id:
		t.listings[key] = listingEntry{id: existing.id, ambiguous: true}
	}
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

// ListingFor is the inverse of ToSKU for one site: the network product id the
// SKU is listed under there. An entry of the site wins over one for every site.
// ok is false when the SKU has no listing at the site, or when several ids
// claim it (nothing is guessed).
func (t *Dictionary) ListingFor(site shared.SiteId, sku shared.SKU) (shared.NetworkProductId, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, s := range []shared.SiteId{site, everySite} {
		if e, ok := t.listings[listingKey{site: s, sku: sku}]; ok {
			return e.id, !e.ambiguous
		}
	}
	return "", false
}

// ListedSKUs lists, sorted, the SKUs that have a single listing at site: what a
// full inventory feed of that site must account for (ADR 0027 §10).
func (t *Dictionary) ListedSKUs(_ context.Context, site shared.SiteId) ([]shared.SKU, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	seen := make(map[shared.SKU]struct{})
	out := []shared.SKU{}
	for key, e := range t.listings {
		if e.ambiguous || (key.site != site && key.site != everySite) {
			continue
		}
		if _, dup := seen[key.sku]; dup {
			continue
		}
		seen[key.sku] = struct{}{}
		out = append(out, key.sku)
	}
	slices.Sort(out)
	return out, nil
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
