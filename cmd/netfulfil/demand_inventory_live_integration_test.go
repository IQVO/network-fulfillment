//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
)

// The inventory feed (ADR 0027 part 2) through the REAL entry point: run(ctx)
// with NETWORK_MODE=live against a real Postgres and a real Kafka, the stateful
// fake of the Vendor API (now with POST /vendor/inventory and the network's
// listing rule), a fake inventory-storage and the two cache topics the profile
// test feeds. Named to sort before e2e_integration_test.go (see
// demand_live_integration_test.go).
//
// In the first test the capability offers are made by the real
// RecomputeCapabilityOffers in a first run in which both workers are switched off
// with 0 (which is itself asserted: nothing reaches the network); the second writes
// the offers of a pass to Postgres directly, because it is not about how they are
// made, and a whole service run is the most expensive thing these tests do. The
// feed is then watched in runs where the recompute is parked at one hour, so the
// offers are exactly the ones of that pass and every later "pass" is the test's own
// doing (the moment of the offers is moved in Postgres): a feed that reads the
// offers while a recompute rewrites them would otherwise be racing the test, and
// a partial feed in between is correct behaviour that no assertion here should
// depend on.

const (
	invSKUA      = "SKU-A"
	invSKUB      = "SKU-B"
	invSKUC      = "SKU-C"
	invListingA  = "LST-A"
	invListingB  = "LST-B"
	invListingC  = "LST-C"
	invElsewhere = "site-elsewhere"

	inventoryEveryTick = "300ms"
	inventoryQuiet     = 2 * time.Second // several passes at inventoryEveryTick
	inventoryPath      = "/vendor/inventory"
)

// writeInventoryProducts is the dictionary of these tests: A is listed at every
// site, B only at the site under test, C only elsewhere (it has an offer here and
// no listing here, which is the expected "not fed" case).
func writeInventoryProducts(t *testing.T, rig *liveRig) {
	t.Helper()
	body := fmt.Sprintf(`{"products":[`+
		`{"networkProductId":%q,"sku":%q},`+
		`{"networkProductId":%q,"sku":%q,"site":%q},`+
		`{"networkProductId":%q,"sku":%q,"site":%q}]}`,
		invListingA, invSKUA, invListingB, invSKUB, liveSite, invListingC, invSKUC, invElsewhere)
	if err := os.WriteFile(filepath.Join(rig.env.dir, "products.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// offersAsOf is the moment of the offers Postgres holds: the pass a feed describes.
func (r *liveRig) offersAsOf(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	var asOf time.Time
	if err := r.pool.QueryRow(ctx, `SELECT max(computed_at) FROM capability_offers`).Scan(&asOf); err != nil {
		t.Fatalf("read the moment of the offers: %v", err)
	}
	return asOf.UTC()
}

// newerPass moves every offer to a later moment, as a newer recompute pass with
// the same figures would; change, when it is not empty, is an extra assignment
// (a figure that differs).
func (r *liveRig) newerPass(t *testing.T, ctx context.Context, by time.Duration, change string) {
	t.Helper()
	set := fmt.Sprintf("computed_at = computed_at + interval '%d seconds'", int(by.Seconds()))
	if change != "" {
		set += ", " + change
	}
	if _, err := r.pool.Exec(ctx, "UPDATE capability_offers SET "+set); err != nil {
		t.Fatalf("a newer pass of offers: %v", err)
	}
}

// seedOffers runs the service once with the recompute on and both workers off, so
// that the offers of three SKUs exist in Postgres and the site is mapped to its
// node, then stops it. Nothing may have reached the network meanwhile: 0 switches
// the profile and the inventory feed off. It returns the moment of the offers.
func seedOffers(t *testing.T, ctx context.Context, rig *liveRig, stock *usableStock) time.Time {
	t.Helper()
	stop := rig.start(t, ctx, map[string]string{
		"INVENTORY_STORAGE_URL": stock.srv.URL, "RECOMPUTE_INTERVAL": "500ms", "POLL_INTERVAL": "500ms",
		"NETWORK_PROFILE_INTERVAL": "0", "NETWORK_INVENTORY_INTERVAL": "0",
	})
	e2eWaitFor(t, ctx, 90*time.Second, "the capability offers of the three SKUs and the node of the site", func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM capability_offers WHERE site_id = $1`, liveSite) == 3 &&
			rig.count(t, ctx, `SELECT count(*) FROM network_nodes WHERE site_code = $1`, liveSite) == 1
	})
	mustStop(t, stop)
	// A stop that fell in the middle of a pass would leave the offers of two
	// passes; make them the last pass's, whole.
	if _, err := rig.pool.Exec(ctx, `UPDATE capability_offers SET computed_at = (SELECT max(computed_at) FROM capability_offers)`); err != nil {
		t.Fatalf("make the offers one pass: %v", err)
	}
	expectEqual(t, "inventory POSTs with both workers off", rig.network.requestCount(http.MethodPost, inventoryPath), 0)
	expectEqual(t, "profile PUTs with both workers off", rig.network.requestCount(http.MethodPut, profilePath), 0)
	return rig.offersAsOf(t, ctx)
}

// seedOffersBySQL writes the offers of one pass straight into Postgres, migrating
// the schema first as the service does at boot: SKU-A throughput-constrained at 30,
// SKU-B at 5 and SKU-C (listed elsewhere) at 7, the figures the first test gets from
// a real recompute. It returns the moment of the pass.
func seedOffersBySQL(t *testing.T, ctx context.Context, rig *liveRig) time.Time {
	t.Helper()
	if err := postgres.RunMigrations(rig.env.dsn, "../../migrations"); err != nil {
		t.Fatalf("migrate before seeding: %v", err)
	}
	asOf := time.Now().UTC().Truncate(time.Microsecond)
	for _, o := range []struct {
		sku, basis string
		qty        int
	}{{invSKUA, "THROUGHPUT_CONSTRAINED", 30}, {invSKUB, "PHYSICAL", 5}, {invSKUC, "PHYSICAL", 7}} {
		if _, err := rig.pool.Exec(ctx, `INSERT INTO capability_offers (sku, site_id, advertised_quantity, basis, computed_at) VALUES ($1, $2, $3, $4, $5)`,
			o.sku, liveSite, o.qty, o.basis, asOf); err != nil {
			t.Fatalf("seed the offer of %s: %v", o.sku, err)
		}
	}
	return asOf
}

func waitForInventoryFeeds(t *testing.T, ctx context.Context, rig *liveRig, n int) {
	t.Helper()
	e2eWaitFor(t, ctx, 90*time.Second, fmt.Sprintf("%d inventory feeds on the network, the last one settled", n), func() bool {
		feeds := rig.network.inventoryFeeds()
		return len(feeds) == n && feeds[n-1].Status != "PROCESSING"
	})
	// The verdict is stored a moment after the network gives it.
	e2eWaitFor(t, ctx, 30*time.Second, fmt.Sprintf("%d settled INVENTORY ledger rows", n), func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'INVENTORY' AND status IN ('SUCCESS', 'FAILURE')`) == n
	})
}

func expectNoMoreInventoryFeeds(t *testing.T, rig *liveRig, feeds int) {
	t.Helper()
	time.Sleep(inventoryQuiet)
	expectEqual(t, "POST /vendor/inventory requests (replays included)", rig.network.requestCount(http.MethodPost, inventoryPath), feeds)
	expectEqual(t, "inventory feeds", len(rig.network.inventoryFeeds()), feeds)
}

func feedKey(asOf time.Time) string {
	return "nf-inventory-" + liveSite + "-" + asOf.UTC().Format(time.RFC3339Nano)
}

func feedSubject(asOf time.Time) string {
	return liveSite + "#" + asOf.UTC().Format(time.RFC3339Nano)
}

// expectFeed checks one feed on the network: when it is as of, what it carries, how
// its transaction ended, and the ledger row of the same attempt.
func expectFeed(t *testing.T, ctx context.Context, rig *liveRig, f fakeInventoryFeed, asOf time.Time, lines []fakeInventoryLine, status string) {
	t.Helper()
	expectEqual(t, "idempotency key", f.Key, feedKey(asOf))
	expectEqual(t, "node", f.NodeID, "N-1")
	if !f.AsOf.Equal(asOf) {
		t.Fatalf("feed asOf = %s, want %s", f.AsOf, asOf)
	}
	if !slices.Equal(f.Lines, lines) {
		t.Fatalf("feed lines = %+v, want %+v", f.Lines, lines)
	}
	expectEqual(t, "transaction", f.Status, status)
	expectEqual(t, "ledger row", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'INVENTORY' AND subject = $1`, feedSubject(asOf)), status)
}

func expectStock(t *testing.T, rig *liveRig, listing string, want int) {
	t.Helper()
	got, ok := rig.network.stockOf("N-1", listing)
	if !ok || got != want {
		t.Fatalf("the network holds %d (known: %v) of %s, want %d", got, ok, listing, want)
	}
}

func expectNoStock(t *testing.T, rig *liveRig, listing string) {
	t.Helper()
	if got, ok := rig.network.stockOf("N-1", listing); ok {
		t.Fatalf("the network holds %d of %s, want it never told", got, listing)
	}
}

// publishInventoryCaches publishes what makes the offers throughput-constrained:
// a path, its capacity (30 at each cutoff) and the site's schedule. SKU-A has 100
// usable, so it is offered 30 on the THROUGHPUT_CONSTRAINED basis; SKU-B has 5 and
// is offered 5 on the PHYSICAL one.
func publishInventoryCaches(t *testing.T, ctx context.Context, rig *liveRig) {
	t.Helper()
	feed := newProfileFeed(t, ctx, rig.env.brokers)
	feed.publishPath()
	feed.publishCapacity()
	feed.publishSchedule(allDayNames)
}

// TestLiveInventory_FedWithTheProfileAfterTheOffersExistAndNotResentWhileUnchanged
// is the inventory feed end to end: one run declares the profile and feeds
// the inventory of the offers a recompute pass made; an unchanged feed is not sent
// again, not by later passes and not after a restart, until an hour has passed
// since the one the network holds; a figure that changed is sent at once.
func TestLiveInventory_FedWithTheProfileAfterTheOffersExistAndNotResentWhileUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 285*time.Second)
	defer cancel()
	rig := startLiveRig(t, ctx)
	writeInventoryProducts(t, rig)
	expectEqual(t, "adopted node", rig.network.addNode(liveSite), "N-1")
	rig.network.setNodeListings("N-1", invListingA, invListingB)
	publishInventoryCaches(t, ctx, rig)
	stock := newUsableStock(t, map[string]int{invSKUA: 100, invSKUB: 5, invSKUC: 7})

	asOf := seedOffers(t, ctx, rig, stock)

	env := map[string]string{
		"INVENTORY_STORAGE_URL": stock.srv.URL, "RECOMPUTE_INTERVAL": "1h", "POLL_INTERVAL": "500ms",
		"NETWORK_PROFILE_INTERVAL": profileEveryTick, "NETWORK_INVENTORY_INTERVAL": inventoryEveryTick,
	}
	stop := rig.start(t, ctx, env)
	waitForProfileWrites(t, ctx, rig, 1)
	waitForInventoryFeeds(t, ctx, rig, 1)

	fed := []fakeInventoryLine{
		{ListingID: invListingA, AvailableQty: 30, Basis: "THROUGHPUT_CONSTRAINED"},
		{ListingID: invListingB, AvailableQty: 5, Basis: "PHYSICAL"},
	}
	t.Run("one run declares the profile and feeds the offers of the pass, complete, listed SKUs only", func(t *testing.T) {
		expectEqual(t, "profile writes", len(rig.network.profileWrites()), 1)
		f := rig.network.inventoryFeeds()[0]
		expectFeed(t, ctx, rig, f, asOf, fed, "SUCCESS")
		expectEqual(t, "fullUpdate (every listed SKU has a fresh offer)", f.Full, true)
		expectEqual(t, "feed state", rig.count(t, ctx, `SELECT count(*) FROM network_inventory_feeds WHERE site_code = $1 AND part = 1`, liveSite), 1)
		expectStock(t, rig, invListingA, 30)
		expectStock(t, rig, invListingB, 5)
		expectNoStock(t, rig, invListingC) // SKU-C has an offer and no listing at this site
	})
	t.Run("later passes over the same pass of offers send nothing", func(t *testing.T) { expectNoMoreInventoryFeeds(t, rig, 1) })

	mustStop(t, stop)
	rig.newerPass(t, ctx, 10*time.Minute, "")
	stop = rig.start(t, ctx, env)
	t.Run("a restart does not re-send a feed that is unchanged inside the hour, whatever the pass", func(t *testing.T) {
		expectNoMoreInventoryFeeds(t, rig, 1)
	})

	rig.newerPass(t, ctx, 2*time.Hour, "")
	waitForInventoryFeeds(t, ctx, rig, 2)
	t.Run("an unchanged feed is repeated once an hour has passed, as of the newer pass", func(t *testing.T) {
		expectFeed(t, ctx, rig, rig.network.inventoryFeeds()[1], asOf.Add(2*time.Hour+10*time.Minute), fed, "SUCCESS")
		expectEqual(t, "INVENTORY ledger rows", rig.count(t, ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'INVENTORY'`), 2)
	})

	rig.newerPass(t, ctx, time.Minute, `advertised_quantity = CASE WHEN sku = 'SKU-B' THEN 9 ELSE advertised_quantity END`)
	waitForInventoryFeeds(t, ctx, rig, 3)
	t.Run("a feed whose figures changed is sent at once", func(t *testing.T) {
		changed := []fakeInventoryLine{fed[0], {ListingID: invListingB, AvailableQty: 9, Basis: "PHYSICAL"}}
		expectFeed(t, ctx, rig, rig.network.inventoryFeeds()[2], asOf.Add(2*time.Hour+11*time.Minute), changed, "SUCCESS")
		expectStock(t, rig, invListingA, 30)
		expectStock(t, rig, invListingB, 9)
	})
	t.Run("the profile was declared once through all of it", func(t *testing.T) {
		expectEqual(t, "profile writes", len(rig.network.profileWrites()), 1)
	})

	mustStop(t, stop)
}

// TestLiveInventory_AListingTheNetworkRefusesIsLeftOutOfTheNextFeeds: the node does
// not carry LST-B, so the network fails the whole first feed with
// ErrListingNotOnNode at the line of that listing. The service records the refusal,
// does not repeat the same feed, and sends the next one without the listing, so one
// wrong listing does not silence the rest. The refusal survives a restart. The site
// is mapped to its node by the first poll of the run, so the first pass of the feed
// may find no node yet and wait for the next.
func TestLiveInventory_AListingTheNetworkRefusesIsLeftOutOfTheNextFeeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 285*time.Second)
	defer cancel()
	rig := startLiveRig(t, ctx)
	writeInventoryProducts(t, rig)
	expectEqual(t, "adopted node", rig.network.addNode(liveSite), "N-1")
	rig.network.setNodeListings("N-1", invListingA)
	asOf := seedOffersBySQL(t, ctx, rig)

	env := map[string]string{
		"RECOMPUTE_INTERVAL": "1h", "POLL_INTERVAL": "500ms",
		"NETWORK_PROFILE_INTERVAL": "0", "NETWORK_INVENTORY_INTERVAL": inventoryEveryTick,
	}
	stop := rig.start(t, ctx, env)
	waitForInventoryFeeds(t, ctx, rig, 1)

	t.Run("the whole first feed fails at the line of the listing and applies nothing", func(t *testing.T) {
		f := rig.network.inventoryFeeds()[0]
		both := []fakeInventoryLine{
			{ListingID: invListingA, AvailableQty: 30, Basis: "THROUGHPUT_CONSTRAINED"},
			{ListingID: invListingB, AvailableQty: 5, Basis: "PHYSICAL"},
		}
		expectFeed(t, ctx, rig, f, asOf, both, "FAILURE")
		if len(f.Errors) != 1 || f.Errors[0]["code"] != "ErrListingNotOnNode" || f.Errors[0]["path"] != "$.lines[1].listingId" {
			t.Fatalf("errors = %v, want one ErrListingNotOnNode at $.lines[1].listingId", f.Errors)
		}
		expectNoStock(t, rig, invListingA)
		expectEqual(t, "recorded refusals of "+invListingB, rig.count(t, ctx,
			`SELECT count(*) FROM network_rejected_listings WHERE site_code = $1 AND listing_id = $2`, liveSite, invListingB), 1)
	})
	t.Run("the refused feed is not repeated while nothing about it changed", func(t *testing.T) { expectNoMoreInventoryFeeds(t, rig, 1) })

	rig.newerPass(t, ctx, time.Minute, "")
	waitForInventoryFeeds(t, ctx, rig, 2)
	onlyA := []fakeInventoryLine{{ListingID: invListingA, AvailableQty: 30, Basis: "THROUGHPUT_CONSTRAINED"}}
	t.Run("the next feed leaves the refused listing out and goes through", func(t *testing.T) {
		expectFeed(t, ctx, rig, rig.network.inventoryFeeds()[1], asOf.Add(time.Minute), onlyA, "SUCCESS")
		expectStock(t, rig, invListingA, 30)
		expectNoStock(t, rig, invListingB)
	})

	mustStop(t, stop)
	stop = rig.start(t, ctx, env)
	rig.newerPass(t, ctx, 2*time.Hour, "")
	waitForInventoryFeeds(t, ctx, rig, 3)
	t.Run("after a restart the refusal is remembered: the repeated feed still leaves it out", func(t *testing.T) {
		expectFeed(t, ctx, rig, rig.network.inventoryFeeds()[2], asOf.Add(2*time.Hour+time.Minute), onlyA, "SUCCESS")
		expectEqual(t, "failed feeds in all", rig.count(t, ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'INVENTORY' AND status = 'FAILURE'`), 1)
	})

	mustStop(t, stop)
}
