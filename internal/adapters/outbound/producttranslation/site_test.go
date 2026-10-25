package producttranslation_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/producttranslation"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestLoadFile_AnEntryMayNameItsSiteAndOneWithoutAppliesToEverySite(t *testing.T) {
	path := writeTranslations(t, `{
	  "products": [
	    {"networkProductId": "L-A1", "sku": "sku-1", "site": "site-a"},
	    {"networkProductId": "L-B1", "sku": "sku-1", "site": "site-b"},
	    {"networkProductId": "L-ALL", "sku": "sku-2"}
	  ]
	}`)
	tr := producttranslation.New()
	n, err := producttranslation.LoadFile(tr, path)
	if err != nil || n != 3 {
		t.Fatalf("LoadFile = %d, %v; want 3 entries and no error", n, err)
	}

	for _, tc := range []struct {
		site shared.SiteId
		sku  shared.SKU
		want shared.NetworkProductId
		ok   bool
	}{
		{"site-a", "sku-1", "L-A1", true},
		{"site-b", "sku-1", "L-B1", true},
		{"site-c", "sku-1", "", false}, // sku-1 is listed at a and b only
		{"site-a", "sku-2", "L-ALL", true},
		{"site-z", "sku-2", "L-ALL", true}, // no site: every site
		{"site-a", "sku-9", "", false},
	} {
		got, ok := tr.ListingFor(tc.site, tc.sku)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ListingFor(%s, %s) = %q, %v; want %q, %v", tc.site, tc.sku, got, ok, tc.want, tc.ok)
		}
	}

	// The forward lookup is unchanged by the site: the network's product id names one SKU.
	for id, want := range map[shared.NetworkProductId]shared.SKU{"L-A1": "sku-1", "L-B1": "sku-1", "L-ALL": "sku-2"} {
		if got, err := tr.ToSKU(t.Context(), id); err != nil || got != want {
			t.Errorf("ToSKU(%s) = %q, %v; want %q", id, got, err, want)
		}
	}
}

func TestDictionary_ListedSKUsAreThoseWithAListingAtTheSiteSorted(t *testing.T) {
	tr := producttranslation.New()
	tr.AddAt("site-a", "L-A2", "sku-2")
	tr.AddAt("site-a", "L-A1", "sku-1")
	tr.AddAt("site-b", "L-B3", "sku-3")
	tr.Add("L-ALL", "sku-0")

	got, err := tr.ListedSKUs(t.Context(), "site-a")
	if err != nil {
		t.Fatalf("ListedSKUs: %v", err)
	}
	if want := []shared.SKU{"sku-0", "sku-1", "sku-2"}; !slices.Equal(got, want) {
		t.Fatalf("ListedSKUs(site-a) = %v, want %v", got, want)
	}
	got, _ = tr.ListedSKUs(t.Context(), "site-nowhere")
	if want := []shared.SKU{"sku-0"}; !slices.Equal(got, want) {
		t.Fatalf("ListedSKUs(site-nowhere) = %v, want only what applies to every site: %v", got, want)
	}
}

func TestDictionary_ASiteEntryWinsOverOneForEverySiteAndAmbiguityFailsClosed(t *testing.T) {
	tr := producttranslation.New()
	tr.Add("L-ALL", "sku-1")
	tr.AddAt("site-a", "L-A1", "sku-1")
	if got, ok := tr.ListingFor("site-a", "sku-1"); !ok || got != "L-A1" {
		t.Fatalf("ListingFor(site-a) = %q, %v; want the site's own entry", got, ok)
	}

	// Two network ids for one SKU with no site is what a dictionary has always
	// allowed (many-to-one on the way in). On the way out it names no single listing.
	legacy := producttranslation.New()
	legacy.Add("ASIN-1", "sku-1")
	legacy.Add("ASIN-2", "sku-1")
	if got, ok := legacy.ListingFor("site-a", "sku-1"); ok {
		t.Fatalf("ListingFor = %q, true; want no listing when two ids claim the SKU", got)
	}
	if skus, _ := legacy.ListedSKUs(t.Context(), "site-a"); len(skus) != 0 {
		t.Fatalf("ListedSKUs = %v; an ambiguous SKU must not be fed", skus)
	}
	if skus, _ := legacy.KnownSKUs(t.Context()); len(skus) != 1 {
		t.Fatalf("KnownSKUs = %v; the forward side must keep deduplicating", skus)
	}
}

func TestLoadFile_ADuplicateListingForTheSameSiteIsALoadError(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"same sku twice at one site", `{"products":[
			{"networkProductId":"L-1","sku":"s","site":"site-a"},
			{"networkProductId":"L-2","sku":"s","site":"site-a"}]}`, "two listings"},
		{"a site entry and one for every site", `{"products":[
			{"networkProductId":"L-1","sku":"s"},
			{"networkProductId":"L-2","sku":"s","site":"site-a"}]}`, "two listings"},
		{"the same, in the other order", `{"products":[
			{"networkProductId":"L-2","sku":"s","site":"site-a"},
			{"networkProductId":"L-1","sku":"s"}]}`, "two listings"},
		{"the same network id at two sites", `{"products":[
			{"networkProductId":"L-1","sku":"s","site":"site-a"},
			{"networkProductId":"L-1","sku":"s","site":"site-b"}]}`, "mapped twice"},
		{"an empty site", `{"products":[{"networkProductId":"L-1","sku":"s","site":""}]}`, "site must not be empty"},
		{"a blank site", `{"products":[{"networkProductId":"L-1","sku":"s","site":"  "}]}`, "site must not be empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := producttranslation.New()
			_, err := producttranslation.LoadFile(tr, writeTranslations(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadFile = %v, want an error mentioning %q", err, tc.want)
			}
			if _, err := tr.ToSKU(t.Context(), "L-1"); err == nil {
				t.Fatal("a rejected file must leave nothing loaded")
			}
		})
	}
}

func TestLoadFile_TwoNetworkIdsForOneSkuWithoutASiteStillLoad(t *testing.T) {
	path := writeTranslations(t, `{"products":[
		{"networkProductId":"ASIN-1","sku":"s"},
		{"networkProductId":"ASIN-2","sku":"s"}]}`)
	if n, err := producttranslation.LoadFile(producttranslation.New(), path); err != nil || n != 2 {
		t.Fatalf("LoadFile = %d, %v; a many-to-one dictionary without sites must keep loading", n, err)
	}
}
