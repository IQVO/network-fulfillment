package usecases

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var (
	passAt  = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	earlier = passAt.Add(-time.Minute)
)

func offerAt(t *testing.T, site shared.SiteId, sku shared.SKU, qty int, basis capabilityoffer.Basis, at time.Time) capabilityoffer.CapabilityOffer {
	t.Helper()
	o, err := capabilityoffer.New(sku, site, qty, qty+10, basis, at)
	if err != nil {
		t.Fatalf("offer %s/%s: %v", site, sku, err)
	}
	return o
}

func physicalAt(t *testing.T, site shared.SiteId, sku shared.SKU, qty int, at time.Time) capabilityoffer.CapabilityOffer {
	return offerAt(t, site, sku, qty, capabilityoffer.BasisPhysical, at)
}

func skuNames(n int) []shared.SKU {
	out := make([]shared.SKU, n)
	for i := range out {
		out[i] = shared.SKU(fmt.Sprintf("sku-%05d", i))
	}
	return out
}

func offersFor(t *testing.T, site shared.SiteId, skus []shared.SKU, at time.Time) []capabilityoffer.CapabilityOffer {
	out := make([]capabilityoffer.CapabilityOffer, len(skus))
	for i, s := range skus {
		out[i] = physicalAt(t, site, s, i, at)
	}
	return out
}

func TestPlanInventoryFeeds_CompleteOnlyWhenEveryListedSkuHasAFreshOffer(t *testing.T) {
	const site shared.SiteId = "FC-1"
	listed := []shared.SKU{"sku-a", "sku-b", "sku-c"}
	fresh := func(sku shared.SKU) capabilityoffer.CapabilityOffer { return physicalAt(t, site, sku, 7, passAt) }

	cases := []struct {
		name         string
		offers       []capabilityoffer.CapabilityOffer
		listed       []shared.SKU
		wantFeeds    int
		wantComplete bool
		wantSKUs     []shared.SKU
		wantStale    []shared.SKU
		wantUnlisted []shared.SKU
	}{
		{"every listed SKU is fresh", []capabilityoffer.CapabilityOffer{fresh("sku-c"), fresh("sku-a"), fresh("sku-b")}, listed,
			1, true, []shared.SKU{"sku-a", "sku-b", "sku-c"}, nil, nil},
		{"one listed SKU has no offer (its inventory lookup failed)", []capabilityoffer.CapabilityOffer{fresh("sku-a"), fresh("sku-c")}, listed,
			1, false, []shared.SKU{"sku-a", "sku-c"}, []shared.SKU{"sku-b"}, nil},
		{"one listed SKU only has an offer of an earlier pass: it is not fresh and is not fed", []capabilityoffer.CapabilityOffer{
			fresh("sku-a"), physicalAt(t, site, "sku-b", 99, earlier), fresh("sku-c")}, listed,
			1, false, []shared.SKU{"sku-a", "sku-c"}, []shared.SKU{"sku-b"}, nil},
		{"a SKU with no listing at the site is not fed and does not make the feed partial", []capabilityoffer.CapabilityOffer{
			fresh("sku-a"), fresh("sku-b"), fresh("sku-c"), fresh("sku-z")}, listed,
			1, true, []shared.SKU{"sku-a", "sku-b", "sku-c"}, nil, []shared.SKU{"sku-z"}},
		{"the offers of another site are not this site's", []capabilityoffer.CapabilityOffer{
			fresh("sku-a"), fresh("sku-b"), fresh("sku-c"), physicalAt(t, "FC-2", "sku-a", 1, passAt.Add(time.Hour))}, listed,
			1, true, []shared.SKU{"sku-a", "sku-b", "sku-c"}, nil, nil},
		{"no offer at all: nothing to feed", nil, listed, 0, false, nil, nil, nil},
		{"offers but no SKU listed at the site: nothing to feed", []capabilityoffer.CapabilityOffer{fresh("sku-a")}, nil,
			0, false, nil, nil, []shared.SKU{"sku-a"}},
		{"only stale offers of listed SKUs: the newest pass is the stale one, so those are fresh", []capabilityoffer.CapabilityOffer{
			physicalAt(t, site, "sku-a", 5, earlier), physicalAt(t, site, "sku-b", 5, earlier)}, listed,
			1, false, []shared.SKU{"sku-a", "sku-b"}, []shared.SKU{"sku-c"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := planInventoryFeeds(site, tc.listed, tc.offers)
			if len(plan.Feeds) != tc.wantFeeds {
				t.Fatalf("%d feeds, want %d: %+v", len(plan.Feeds), tc.wantFeeds, plan.Feeds)
			}
			if !equalSKUs(plan.Stale, tc.wantStale) || !equalSKUs(plan.Unlisted, tc.wantUnlisted) {
				t.Errorf("stale = %v unlisted = %v; want %v and %v", plan.Stale, plan.Unlisted, tc.wantStale, tc.wantUnlisted)
			}
			if tc.wantFeeds == 0 {
				return
			}
			feed := plan.Feeds[0]
			if feed.Complete != tc.wantComplete {
				t.Errorf("Complete = %v, want %v", feed.Complete, tc.wantComplete)
			}
			var got []shared.SKU
			for _, l := range feed.Lines {
				got = append(got, l.SKU)
			}
			if !equalSKUs(got, tc.wantSKUs) {
				t.Errorf("lines = %v, want %v (sorted)", got, tc.wantSKUs)
			}
			if feed.SiteId != site || feed.Part != 1 || feed.Parts != 1 || feed.Fingerprint == "" {
				t.Errorf("feed header = %+v", feed)
			}
		})
	}
}

func equalSKUs(a, b []shared.SKU) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPlanInventoryFeeds_AsOfIsThePassesClockReadingNotTheLatestOfAnyOffer(t *testing.T) {
	const site shared.SiteId = "FC-1"
	plan := planInventoryFeeds(site, []shared.SKU{"sku-a", "sku-b"}, []capabilityoffer.CapabilityOffer{
		physicalAt(t, site, "sku-a", 1, passAt), physicalAt(t, site, "sku-b", 1, earlier),
	})
	if len(plan.Feeds) != 1 || !plan.Feeds[0].AsOf.Equal(passAt) || !plan.AsOf.Equal(passAt) {
		t.Fatalf("plan = %+v, want one feed as of %v", plan, passAt)
	}
}

func TestPlanInventoryFeeds_CarriesQuantityAndBasisAndFeedsZero(t *testing.T) {
	const site shared.SiteId = "FC-1"
	plan := planInventoryFeeds(site, []shared.SKU{"sku-a", "sku-b"}, []capabilityoffer.CapabilityOffer{
		offerAt(t, site, "sku-a", 0, capabilityoffer.BasisPhysical, passAt),
		offerAt(t, site, "sku-b", 4, capabilityoffer.BasisThroughputConstrained, passAt),
	})
	want := []contract.FeedLine{
		{SKU: "sku-a", Quantity: 0, Basis: capabilityoffer.BasisPhysical},
		{SKU: "sku-b", Quantity: 4, Basis: capabilityoffer.BasisThroughputConstrained},
	}
	got := plan.Feeds[0].Lines
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("lines = %+v, want %+v (a zero is a fact the network must hear)", got, want)
	}
}

func TestPlanInventoryFeeds_SplitsOver5000LinesIntoPartialFeeds(t *testing.T) {
	const site shared.SiteId = "FC-1"
	cases := []struct {
		lines     int
		wantParts []int
		complete  bool
	}{
		{1, []int{1}, true},
		{contract.MaxFeedLines, []int{5000}, true},
		{contract.MaxFeedLines + 1, []int{5000, 1}, false},
		{2 * contract.MaxFeedLines, []int{5000, 5000}, false},
		{2*contract.MaxFeedLines + 1, []int{5000, 5000, 1}, false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d lines", tc.lines), func(t *testing.T) {
			skus := skuNames(tc.lines)
			plan := planInventoryFeeds(site, skus, offersFor(t, site, skus, passAt))
			if len(plan.Feeds) != len(tc.wantParts) {
				t.Fatalf("%d feeds, want %d", len(plan.Feeds), len(tc.wantParts))
			}
			next := 0
			for i, f := range plan.Feeds {
				if len(f.Lines) != tc.wantParts[i] || f.Part != i+1 || f.Parts != len(tc.wantParts) || f.Complete != tc.complete {
					t.Errorf("part %d: %d lines part %d/%d complete=%v; want %d lines, %d/%d, complete=%v",
						i+1, len(f.Lines), f.Part, f.Parts, f.Complete, tc.wantParts[i], i+1, len(tc.wantParts), tc.complete)
				}
				if !f.AsOf.Equal(passAt) {
					t.Errorf("part %d as of %v, want the pass's %v", i+1, f.AsOf, passAt)
				}
				for _, l := range f.Lines {
					if l.SKU != skus[next] {
						t.Fatalf("part %d: line %v out of order, want %v", i+1, l.SKU, skus[next])
					}
					next++
				}
			}
			if next != tc.lines {
				t.Errorf("%d lines fed, want all %d", next, tc.lines)
			}
		})
	}
}

func TestPlanInventoryFeeds_AFingerprintIsOfTheContentNotOfTheMoment(t *testing.T) {
	const site shared.SiteId = "FC-1"
	listed := []shared.SKU{"sku-a", "sku-b"}
	fp := func(at time.Time, qtyB int, basisB capabilityoffer.Basis, drop bool) string {
		offers := []capabilityoffer.CapabilityOffer{physicalAt(t, site, "sku-a", 3, at)}
		if !drop {
			offers = append(offers, offerAt(t, site, "sku-b", qtyB, basisB, at))
		}
		return planInventoryFeeds(site, listed, offers).Feeds[0].Fingerprint
	}
	base := fp(passAt, 5, capabilityoffer.BasisPhysical, false)
	if again := fp(passAt.Add(time.Hour), 5, capabilityoffer.BasisPhysical, false); again != base {
		t.Error("the same lines at another moment must have the same fingerprint")
	}
	for name, other := range map[string]string{
		"another quantity":                     fp(passAt, 6, capabilityoffer.BasisPhysical, false),
		"another basis":                        fp(passAt, 5, capabilityoffer.BasisThroughputConstrained, false),
		"a line fewer (and so a partial feed)": fp(passAt, 0, "", true),
	} {
		if other == base {
			t.Errorf("%s: the fingerprint did not change", name)
		}
	}
}

// ---- the use case ----

type feedCall struct{ feed contract.InventoryFeed }

type fakeFeeder struct {
	calls  []feedCall
	result func(contract.InventoryFeed) (contract.FeedResult, error)
}

func (f *fakeFeeder) SubmitInventoryFeed(_ context.Context, feed contract.InventoryFeed) (contract.FeedResult, error) {
	f.calls = append(f.calls, feedCall{feed})
	if f.result != nil {
		return f.result(feed)
	}
	return contract.FeedResult{Outcome: contract.FeedSent, Sent: len(feed.Lines)}, nil
}

type fakeCatalogue struct {
	listed map[shared.SiteId][]shared.SKU
	err    map[shared.SiteId]error
}

func (f fakeCatalogue) ListedSKUs(_ context.Context, site shared.SiteId) ([]shared.SKU, error) {
	return f.listed[site], f.err[site]
}

func feedFixture(t *testing.T, offers []capabilityoffer.CapabilityOffer, catalogue fakeCatalogue, sites ...shared.SiteId) (*FeedInventory, *fakeFeeder, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	feeder := &fakeFeeder{}
	return &FeedInventory{
		Feeder: feeder, Catalogue: catalogue, Offers: &fakeOffersRepo{saved: offers},
		Logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), Sites: sites,
	}, feeder, logs
}

func TestFeedInventory_OneFeedPerSitePerPassCarryingThePersistedOffers(t *testing.T) {
	offers := append(offersFor(t, "FC-1", []shared.SKU{"sku-a", "sku-b"}, passAt), physicalAt(t, "FC-2", "sku-a", 9, passAt))
	u, feeder, _ := feedFixture(t, offers, fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{
		"FC-1": {"sku-a", "sku-b"}, "FC-2": {"sku-a", "sku-b"},
	}}, "FC-1", "FC-2")

	res := u.Execute(t.Context())

	if len(feeder.calls) != 2 {
		t.Fatalf("%d feeds, want one per site", len(feeder.calls))
	}
	fc1, fc2 := feeder.calls[0].feed, feeder.calls[1].feed
	if fc1.SiteId != "FC-1" || !fc1.Complete || len(fc1.Lines) != 2 {
		t.Errorf("FC-1 feed = %+v, want a complete feed of both SKUs", fc1)
	}
	if fc2.SiteId != "FC-2" || fc2.Complete || len(fc2.Lines) != 1 {
		t.Errorf("FC-2 feed = %+v, want a partial feed: sku-b has no offer there", fc2)
	}
	if res.Outcomes[contract.FeedSent] != 2 {
		t.Errorf("outcomes = %v, want two sent", res.Outcomes)
	}
}

func TestFeedInventory_ASkuWithNoListingIsLoggedOncePerSiteAcrossPasses(t *testing.T) {
	offers := []capabilityoffer.CapabilityOffer{physicalAt(t, "FC-1", "sku-a", 1, passAt), physicalAt(t, "FC-1", "sku-z", 1, passAt)}
	u, feeder, logs := feedFixture(t, offers, fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{"FC-1": {"sku-a"}}}, "FC-1")

	u.Execute(t.Context())
	u.Execute(t.Context())

	if got := strings.Count(logs.String(), "has no listing at the site"); got != 1 {
		t.Fatalf("logged %d times, want once:\n%s", got, logs)
	}
	if !strings.Contains(logs.String(), "sku-z") || strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("logs = %s, want sku-z named and no error: an unlisted SKU is an expected state", logs)
	}
	if len(feeder.calls) != 2 || len(feeder.calls[0].feed.Lines) != 1 {
		t.Errorf("calls = %+v, want the feed of sku-a only, each pass", feeder.calls)
	}
}

func TestFeedInventory_AFailureOfOneSiteNeverStopsTheOthers(t *testing.T) {
	offers := append(offersFor(t, "FC-1", []shared.SKU{"sku-a"}, passAt), offersFor(t, "FC-2", []shared.SKU{"sku-a"}, passAt)...)
	u, feeder, logs := feedFixture(t, offers, fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{"FC-1": {"sku-a"}, "FC-2": {"sku-a"}}}, "FC-1", "FC-2", "FC-3")
	feeder.result = func(f contract.InventoryFeed) (contract.FeedResult, error) {
		if f.SiteId == "FC-1" {
			return contract.FeedResult{}, errors.New("network unreachable")
		}
		return contract.FeedResult{Outcome: contract.FeedConfirmed, Sent: 1}, nil
	}

	res := u.Execute(t.Context())

	if len(feeder.calls) != 2 {
		t.Fatalf("%d calls, want both sites with offers attempted", len(feeder.calls))
	}
	want := map[contract.FeedOutcome]int{contract.FeedFailed: 1, contract.FeedConfirmed: 1, contract.FeedSkippedNoOffers: 1}
	for k, v := range want {
		if res.Outcomes[k] != v {
			t.Errorf("outcomes = %v, want %v", res.Outcomes, want)
			break
		}
	}
	if !strings.Contains(logs.String(), "network unreachable") {
		t.Errorf("the failure was not logged: %s", logs)
	}
}

func TestFeedInventory_ASiteWithoutANodeIsSkippedNotFailed(t *testing.T) {
	u, feeder, logs := feedFixture(t, offersFor(t, "FC-1", []shared.SKU{"sku-a"}, passAt),
		fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{"FC-1": {"sku-a"}}}, "FC-1")
	feeder.result = func(contract.InventoryFeed) (contract.FeedResult, error) {
		return contract.FeedResult{}, fmt.Errorf("wrapped: %w", contract.ErrSiteNotRegistered)
	}

	res := u.Execute(t.Context())

	if res.Outcomes[contract.FeedSkippedNoNode] != 1 || len(res.Outcomes) != 1 {
		t.Errorf("outcomes = %v, want one skipped_no_node", res.Outcomes)
	}
	if strings.Contains(logs.String(), "level=ERROR") || strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("logs = %s, a site not registered yet is a state, not a fault", logs)
	}
}

func TestFeedInventory_NoOffersAndNoListingsAreSkippedWithAReason(t *testing.T) {
	u, feeder, _ := feedFixture(t, []capabilityoffer.CapabilityOffer{physicalAt(t, "FC-2", "sku-a", 1, passAt)},
		fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{}}, "FC-1", "FC-2")

	res := u.Execute(t.Context())

	if len(feeder.calls) != 0 {
		t.Fatalf("%d feeds, want none", len(feeder.calls))
	}
	if res.Outcomes[contract.FeedSkippedNoOffers] != 1 || res.Outcomes[contract.FeedSkippedNoListings] != 1 {
		t.Errorf("outcomes = %v, want one of each skip", res.Outcomes)
	}
}

func TestFeedInventory_ACatalogueThatCannotBeReadLeavesTheSiteForTheNextPass(t *testing.T) {
	u, feeder, logs := feedFixture(t, offersFor(t, "FC-1", []shared.SKU{"sku-a"}, passAt),
		fakeCatalogue{err: map[shared.SiteId]error{"FC-1": errors.New("dictionary gone")}}, "FC-1")

	res := u.Execute(t.Context())

	if len(feeder.calls) != 0 || res.Outcomes[contract.FeedFailed] != 1 || !strings.Contains(logs.String(), "dictionary gone") {
		t.Errorf("calls = %d outcomes = %v logs = %s", len(feeder.calls), res.Outcomes, logs)
	}
}

func TestFeedInventory_AnUnreadableOfferStoreFailsEverySiteAndSendsNothing(t *testing.T) {
	u, feeder, _ := feedFixture(t, nil, fakeCatalogue{}, "FC-1", "FC-2")
	u.Offers = &failingOffers{}

	res := u.Execute(t.Context())

	if len(feeder.calls) != 0 || res.Outcomes[contract.FeedFailed] != 2 {
		t.Errorf("calls = %d outcomes = %v, want two failed and nothing sent", len(feeder.calls), res.Outcomes)
	}
}

type failingOffers struct{ fakeOffersRepo }

func (*failingOffers) ListAll(context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	return nil, errors.New("offers unreadable")
}

func TestFeedInventory_ACancelledContextStopsBeforeTheNextSite(t *testing.T) {
	offers := append(offersFor(t, "FC-1", []shared.SKU{"sku-a"}, passAt), offersFor(t, "FC-2", []shared.SKU{"sku-a"}, passAt)...)
	u, feeder, _ := feedFixture(t, offers, fakeCatalogue{listed: map[shared.SiteId][]shared.SKU{"FC-1": {"sku-a"}, "FC-2": {"sku-a"}}}, "FC-1", "FC-2")
	ctx, cancel := context.WithCancel(t.Context())
	feeder.result = func(contract.InventoryFeed) (contract.FeedResult, error) {
		cancel()
		return contract.FeedResult{Outcome: contract.FeedSent}, nil
	}

	u.Execute(ctx)

	if len(feeder.calls) != 1 {
		t.Fatalf("%d calls, want the pass to stop after the first site", len(feeder.calls))
	}
}
