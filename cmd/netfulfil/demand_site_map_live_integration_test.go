//go:build integration

package main

import (
	"context"
	"slices"
	"testing"
	"time"
)

// NETWORK_SITE_MAP (ADR 0035) through the REAL entry point, in the shape the
// live cluster has: the network node is called "site-1" (SITE_ID, the node's
// vendorNodeCode), which is not a site code, so before the map the held order
// went to order-management with no fulfillmentSiteId. With site-1=AUS3 the
// hold carries AUS3, and nothing on the network side moves: the node is still
// adopted by SITE_ID and the order still records the network's own site.
//
// Named to sort before e2e_integration_test.go (see demand_live_integration_test.go).
func TestLiveDemand_SiteMapSendsTheWarehouseSite(t *testing.T) {
	const networkSite, warehouseSite = "site-1", "AUS3"
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startLiveRig(t, ctx)
	expectEqual(t, "adopted node", rig.network.addNode(networkSite), "N-1")
	rig.network.issue(livePO, networkSite, liveListing, 2, time.Now().UTC())
	stop := rig.start(t, ctx, map[string]string{
		"SITE_ID":          networkSite,
		"NETWORK_SITE_MAP": networkSite + "=" + warehouseSite,
	})

	e2eWaitFor(t, ctx, 90*time.Second, "the purchase order is an ACKNOWLEDGED network order", func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM network_orders WHERE state = 'ACKNOWLEDGED'`) == 1
	})
	ref := rig.text(t, ctx, `SELECT network_ref FROM network_orders`)

	t.Run("the held order carries the warehouse site", func(t *testing.T) {
		if sites := rig.planner.raisedSites(); !slices.Equal(sites, []string{warehouseSite}) {
			t.Fatalf("fulfillmentSiteId sent on the hold = %v, want [%s]", sites, warehouseSite)
		}
	})
	t.Run("the network's own ids do not move", func(t *testing.T) {
		expectEqual(t, "order site", rig.text(t, ctx, `SELECT site_id FROM network_orders WHERE network_ref = $1`, ref), networkSite)
		expectEqual(t, "adopted node", rig.text(t, ctx, `SELECT node_id FROM network_nodes WHERE site_code = $1`, networkSite), "N-1")
		expectEqual(t, "node registrations (the node was adopted)", rig.network.writeCount("/vendor/nodes"), 0)
		expectEqual(t, "nodes keyed by the warehouse site", rig.count(t, ctx, `SELECT count(*) FROM network_nodes WHERE site_code = $1`, warehouseSite), 0)
	})

	mustStop(t, stop)
}
