package ordermanagement

import (
	"log/slog"
	"maps"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// PlannerOption configures a Planner at construction. Options are the way
// configuration reaches the adapter: the composition root passes what it read
// at boot, and the planner reads no environment and holds no global.
type PlannerOption func(*Planner)

// WithSiteMap sets the translation from the network's site id to the warehouse
// site id that is sent as fulfillmentSiteId (ADR 0035). The network's id is
// whatever the network node is called (the live node is "site-1"); the
// warehouse site is the facility-layout code of the building that serves it
// ("AUS3"). A site with no entry is decided as before: sent as it is when it
// is a site code, omitted when it is not.
//
// The map is copied when the option is built, so the caller can keep editing
// its own. The composition
// root validates it at boot (cmd/netfulfil, NETWORK_SITE_MAP); the planner
// still checks the shape of what it sends, translated or not, because it is
// the last stop before order-management answers 400.
func WithSiteMap(siteMap map[shared.SiteId]shared.SiteId) PlannerOption {
	copied := maps.Clone(siteMap)
	return func(p *Planner) { p.siteMap = copied }
}

// ValidSiteCode mirrors order-management's FulfillmentSiteID rule (its ADR
// 0044, itself a mirror of facility-layout's site code): at least one
// character, uppercase ASCII letters and digits only. It is a mirror, not a
// lookup: an unknown but well-formed code is sent as is. The composition root
// uses it to validate NETWORK_SITE_MAP with the very rule the planner applies.
func ValidSiteCode(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// fulfillmentSite returns the warehouse site to send, or "" to omit the
// field. The order's site is translated through the site map first (ADR
// 0035); an unmapped site is decided as ADR 0029 says. A site that is still
// not a site code is logged and left out rather than sent: order-management
// would answer 400 for it, and the hold (and the network's answer) must not
// fail on a field that is optional there.
func (p *Planner) fulfillmentSite(ref shared.NetworkRef, site shared.SiteId) string {
	if site == "" {
		return ""
	}
	if warehouse, ok := p.siteMap[site]; ok {
		if !ValidSiteCode(string(warehouse)) {
			slog.Warn("mapped warehouse site is not a site code; fulfillmentSiteId omitted",
				"networkRef", string(ref), "siteId", string(site), "mappedTo", string(warehouse))
			return ""
		}
		return string(warehouse)
	}
	if !ValidSiteCode(string(site)) {
		slog.Warn("network order site is not a site code; fulfillmentSiteId omitted",
			"networkRef", string(ref), "siteId", string(site))
		return ""
	}
	return string(site)
}
