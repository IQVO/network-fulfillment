package ordermanagement_test

import (
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The network's site id is not always a warehouse site code (the live node is
// "site-1", the building that serves it is "AUS3"). NETWORK_SITE_MAP, passed in
// as WithSiteMap, translates the one to the other BEFORE the shape check, so a
// held order for a mapped site carries a fulfillmentSiteId order-management
// accepts. ADR 0035.
func TestRaiseHeldOrder_SiteMap(t *testing.T) {
	siteMap := map[shared.SiteId]shared.SiteId{"site-1": "AUS3", "NODE 2": "DAL1"}
	cases := []struct {
		name    string
		siteMap map[shared.SiteId]shared.SiteId
		site    shared.SiteId
		want    any // nil = key absent
	}{
		{"a mapped site is sent as its warehouse site", siteMap, "site-1", "AUS3"},
		{"a site that is not even a site code can be mapped", siteMap, "NODE 2", "DAL1"},
		{"an unmapped valid site passes through unchanged", siteMap, "WH1", "WH1"},
		{"an unmapped invalid site is omitted", siteMap, "site-9", nil},
		{"an empty site stays omitted", siteMap, "", nil},
		{"the lookup is exact: another case is another site", siteMap, "SITE-1", nil},
		{"a mapped valid site is translated, not passed through", map[shared.SiteId]shared.SiteId{"WH1": "AUS3"}, "WH1", "AUS3"},
		{"an empty map is today's behaviour: valid passes", map[shared.SiteId]shared.SiteId{}, "WH1", "WH1"},
		{"an empty map is today's behaviour: invalid is omitted", map[shared.SiteId]shared.SiteId{}, "site-1", nil},
		{"a nil map is today's behaviour", nil, "site-1", nil},
		// Boot refuses a map whose value is not a site code, so this cannot be
		// configured. The planner still never sends what order-management would
		// answer 400 for: the shape check runs after the translation too.
		{"a mapped value that is not a site code is omitted", map[shared.SiteId]shared.SiteId{"site-1": "aus3"}, "site-1", nil},
		{"a mapped empty value is omitted", map[shared.SiteId]shared.SiteId{"site-1": ""}, "site-1", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := raiseBody(t, tc.site, ordermanagement.WithSiteMap(tc.siteMap))
			got, present := body["fulfillmentSiteId"]
			if tc.want == nil {
				if present {
					t.Fatalf("fulfillmentSiteId = %v, want the key absent", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("fulfillmentSiteId = %v, want %v", got, tc.want)
			}
		})
	}
}

// Without the option the planner is exactly what it was: nothing is mapped.
func TestRaiseHeldOrder_NoSiteMapOptionChangesNothing(t *testing.T) {
	if got := raiseBody(t, "site-1"); got["fulfillmentSiteId"] != nil {
		t.Fatalf("fulfillmentSiteId = %v, want the key absent", got["fulfillmentSiteId"])
	}
	if got := raiseBody(t, "WH1"); got["fulfillmentSiteId"] != "WH1" {
		t.Fatalf("fulfillmentSiteId = %v, want WH1", got["fulfillmentSiteId"])
	}
}

// WithSiteMap copies the map: a caller that keeps editing its own map after
// building the planner must not change what the planner sends.
func TestWithSiteMap_CopiesTheMap(t *testing.T) {
	m := map[shared.SiteId]shared.SiteId{"site-1": "AUS3"}
	opt := ordermanagement.WithSiteMap(m)
	m["site-1"] = "DAL1"
	m["site-2"] = "DAL2"
	body := raiseBody(t, "site-1", opt)
	if got := body["fulfillmentSiteId"]; got != "AUS3" {
		t.Fatalf("fulfillmentSiteId = %v, want AUS3 (the map as it was when the planner was built)", got)
	}
	if got := raiseBody(t, "site-2", opt)["fulfillmentSiteId"]; got != nil {
		t.Fatalf("fulfillmentSiteId = %v, want the key absent for a site added later", got)
	}
}

func TestValidSiteCode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"AUS3", true}, {"A", true}, {"0", true}, {"", false}, {"aus3", false},
		{"AUS-3", false}, {"AUS 3", false}, {"AUS3\n", false}, {"ÅUS3", false},
	} {
		if got := ordermanagement.ValidSiteCode(tc.in); got != tc.want {
			t.Errorf("ValidSiteCode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
