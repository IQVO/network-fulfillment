package producttranslation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

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
		seen[id] = shared.SKU(p.SKU)
	}

	for id, sku := range seen {
		t.Add(id, sku)
	}
	return len(seen), nil
}
