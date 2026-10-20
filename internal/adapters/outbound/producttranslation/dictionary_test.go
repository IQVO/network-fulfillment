package producttranslation_test

import (
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/producttranslation"
)

func TestDictionary_KnownSKUsDeduplicates(t *testing.T) {
	tr := producttranslation.New()
	tr.Add("ASIN-1", "SKU-1")
	tr.Add("ASIN-2", "SKU-1") // two network products map to the same SKU
	tr.Add("ASIN-3", "SKU-2")

	skus, err := tr.KnownSKUs(t.Context())
	if err != nil {
		t.Fatalf("KnownSKUs: %v", err)
	}
	if len(skus) != 2 {
		t.Fatalf("len(skus) = %d, want 2 (deduplicated)", len(skus))
	}
}

func TestDictionary_KnownSKUsEmptyWhenNoneLoaded(t *testing.T) {
	tr := producttranslation.New()
	skus, err := tr.KnownSKUs(t.Context())
	if err != nil {
		t.Fatalf("KnownSKUs: %v", err)
	}
	if len(skus) != 0 {
		t.Fatalf("len(skus) = %d, want 0", len(skus))
	}
}
