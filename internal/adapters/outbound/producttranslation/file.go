package producttranslation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// translationFile is the wire shape of the Anti-Corruption Layer's
// dictionary: the network's product identifier to our SKU.
//
// A curated file is the correct v1 and not a placeholder. The mapping is
// a business fact somebody must own, and pretending it can be DERIVED —
// by string convention, or by asking a catalogue that has never heard of
// the network's identifiers — is how an ACL quietly stops being one.
//
// Without this, the dictionary is empty in every deployment and EVERY
// order rejects as untranslatable: the inbound leg looks alive, answers
// the network in the negative every time, and the cause is invisible
// because refusing unknown products is also correct behaviour.
type translationFile struct {
	Products []productMapping `json:"products"`
}

type productMapping struct {
	NetworkProductId string `json:"networkProductId"`
	SKU              string `json:"sku"`
	// Site, when present, restricts the entry to that site (ADR 0027 §10): the
	// network keys stock by a listing, one item at one node, so the same SKU is
	// listed under another id at each site. Absent means every site.
	Site *string `json:"site,omitempty"`
}

// LoadFile reads path into t.
//
// Every failure is returned rather than logged: a dictionary that
// silently failed to load is indistinguishable, from the outside, from a
// network asking only for products we do not stock.
func LoadFile(t *Dictionary, path string) (int, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is operator-controlled config (a boot-time env value), not user input.
	if err != nil {
		return 0, fmt.Errorf("read product translation file: %w", err)
	}

	var file translationFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	// A typo'd key would otherwise be dropped in silence, and the symptom
	// — every order rejected — points at the network rather than at this
	// file.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return 0, fmt.Errorf("parse product translation file %s: %w", path, err)
	}

	// Validated BEFORE anything is added, so a bad entry late in the file
	// cannot leave a half-loaded dictionary: that would translate some
	// lines and reject others from the same order, which the protocol has
	// no way to express (acknowledgement is all-or-nothing).
	seen := make(map[shared.NetworkProductId]shared.SKU, len(file.Products))
	sites := make([]shared.SiteId, len(file.Products))
	listed := make(skuListings, len(file.Products))
	for i, p := range file.Products {
		if p.NetworkProductId == "" {
			return 0, fmt.Errorf("product %d: networkProductId must not be empty", i)
		}
		if p.SKU == "" {
			return 0, fmt.Errorf("product %d (%s): sku must not be empty", i, p.NetworkProductId)
		}
		id := shared.NetworkProductId(p.NetworkProductId)
		// A duplicate is ambiguous, and silently keeping the last one
		// would mean the dictionary depends on file order.
		if existing, dup := seen[id]; dup {
			return 0, fmt.Errorf("product %d: %s is mapped twice (%s and %s)",
				i, p.NetworkProductId, existing, p.SKU)
		}
		site, err := entrySite(i, p)
		if err != nil {
			return 0, err
		}
		if err := listed.add(i, site, shared.SKU(p.SKU), id); err != nil {
			return 0, err
		}
		seen[id], sites[i] = shared.SKU(p.SKU), site
	}

	for i, p := range file.Products {
		t.AddAt(sites[i], shared.NetworkProductId(p.NetworkProductId), shared.SKU(p.SKU))
	}
	return len(seen), nil
}

// entrySite is the site an entry names; no site key means every site. A key
// that is there but blank is a mistake, not "every site".
func entrySite(i int, p productMapping) (shared.SiteId, error) {
	if p.Site == nil {
		return everySite, nil
	}
	site := strings.TrimSpace(*p.Site)
	if site == "" {
		return "", fmt.Errorf("product %d (%s): site must not be empty (leave the key out for every site)", i, p.NetworkProductId)
	}
	return shared.SiteId(site), nil
}

// skuListings tracks, per SKU, where the file lists it, to refuse two listings
// of one SKU at one site (ADR 0027 §10: a listing is one item at one node, so
// the inverse the feed needs must name exactly one). Several ids for a SKU
// without any site stay allowed: that is the many-to-one dictionary of
// ADR 0013, which the feed simply does not use.
type skuListings map[shared.SKU]*skuSites

type skuSites struct {
	everySite shared.NetworkProductId
	bySite    map[shared.SiteId]shared.NetworkProductId
	// firstSite and firstID are the first entry that named a site, for the message.
	firstSite shared.SiteId
	firstID   shared.NetworkProductId
}

func (l skuListings) add(i int, site shared.SiteId, sku shared.SKU, id shared.NetworkProductId) error {
	s := l[sku]
	if s == nil {
		s = &skuSites{bySite: make(map[shared.SiteId]shared.NetworkProductId)}
		l[sku] = s
	}
	if site == everySite {
		if s.firstSite != everySite {
			return clashError(i, sku, s.firstSite, id, s.firstID)
		}
		s.everySite = id
		return nil
	}
	if s.everySite != "" {
		return clashError(i, sku, site, id, s.everySite)
	}
	if otherID, dup := s.bySite[site]; dup {
		return clashError(i, sku, site, id, otherID)
	}
	if len(s.bySite) == 0 {
		s.firstSite, s.firstID = site, id
	}
	s.bySite[site] = id
	return nil
}

func clashError(i int, sku shared.SKU, site shared.SiteId, id, other shared.NetworkProductId) error {
	return fmt.Errorf("product %d: two listings of %s at site %s (%s and %s): a SKU has one listing per site",
		i, sku, site, id, other)
}
