package usecases

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// FeedInventory tells the network, for each site this service runs, what the
// site claims it can ship (ADR 0027 part 2): one feed per site, built from the
// CapabilityOffers the last RecomputeCapabilityOffers pass persisted. It reads
// only local data (the offers and the product dictionary): there is never a
// synchronous call to a neighbour, so an outage elsewhere cannot stop a feed.
//
// What it sends and when are two different questions. This use case decides the
// content (planInventoryFeeds: which SKUs, whether the feed is complete, how it
// splits); the gateway owns delivery: it sends a feed when its fingerprint
// changes, repeats an unchanged one at most hourly, and keeps out of the next
// feed a line the network refused. Calling this every few minutes with unchanged
// offers sends nothing.
type FeedInventory struct {
	Feeder    ports.InventoryFeeder
	Catalogue ports.FeedCatalogue
	Offers    ports.CapabilityOfferRepo
	Logger    *slog.Logger

	// Sites are the sites to feed.
	Sites []shared.SiteId

	mu    sync.Mutex
	noted map[unlistedKey]struct{}
}

type unlistedKey struct {
	site shared.SiteId
	sku  shared.SKU
}

// FeedReport summarises one pass: how many feeds (parts) ended in each outcome.
type FeedReport struct {
	Outcomes map[contract.FeedOutcome]int
}

// Execute feeds every site in turn. One site's failure never stops the others;
// a cancelled context stops the pass before the next site.
func (u *FeedInventory) Execute(ctx context.Context) FeedReport {
	rep := FeedReport{Outcomes: map[contract.FeedOutcome]int{}}
	offers, err := u.Offers.ListAll(ctx)
	if err != nil {
		u.log(ctx, slog.LevelError, "inventory feed: the persisted offers could not be read; the next pass retries", "", "err", err)
		rep.Outcomes[contract.FeedFailed] += len(u.Sites)
		return rep
	}
	for _, site := range u.Sites {
		if ctx.Err() != nil {
			break
		}
		for _, outcome := range u.feedSite(ctx, site, offers) {
			rep.Outcomes[outcome]++
		}
	}
	return rep
}

func (u *FeedInventory) feedSite(ctx context.Context, site shared.SiteId, offers []capabilityoffer.CapabilityOffer) []contract.FeedOutcome {
	if !hasOffersFor(site, offers) {
		u.log(ctx, slog.LevelDebug, "inventory feed: no capability offer for the site yet; nothing is fed", site)
		return []contract.FeedOutcome{contract.FeedSkippedNoOffers}
	}
	listed, err := u.Catalogue.ListedSKUs(ctx, site)
	if err != nil {
		u.log(ctx, slog.LevelError, "inventory feed: the product dictionary could not be read; the next pass retries", site, "err", err)
		return []contract.FeedOutcome{contract.FeedFailed}
	}
	plan := planInventoryFeeds(site, listed, offers)
	u.noteUnlisted(ctx, site, plan.Unlisted)
	if len(plan.Stale) > 0 {
		u.log(ctx, slog.LevelWarn, "inventory feed: listed SKUs without a fresh offer in the pass are left out and the feed is not complete",
			site, "skus", len(plan.Stale), "first", plan.Stale[0])
	}
	if len(plan.Feeds) == 0 {
		u.log(ctx, slog.LevelDebug, "inventory feed: nothing the site lists has a fresh offer; nothing is fed", site)
		return []contract.FeedOutcome{contract.FeedSkippedNoListings}
	}
	out := make([]contract.FeedOutcome, 0, len(plan.Feeds))
	for _, feed := range plan.Feeds {
		if ctx.Err() != nil {
			break
		}
		out = append(out, u.send(ctx, site, feed))
	}
	return out
}

func (u *FeedInventory) send(ctx context.Context, site shared.SiteId, feed contract.InventoryFeed) contract.FeedOutcome {
	res, err := u.Feeder.SubmitInventoryFeed(ctx, feed)
	switch {
	case errors.Is(err, contract.ErrSiteNotRegistered):
		u.log(ctx, slog.LevelInfo, "inventory feed: the site has no node on the network yet; fed once it has one", site)
		return contract.FeedSkippedNoNode
	case err != nil:
		u.log(ctx, slog.LevelError, "inventory feed: the feed could not be sent; the next pass retries", site,
			"part", feed.Part, "of", feed.Parts, "err", err)
		return contract.FeedFailed
	}
	u.log(ctx, slog.LevelDebug, "inventory feed: pass handled", site, "outcome", res.Outcome,
		"part", feed.Part, "of", feed.Parts, "lines", res.Sent, "dropped", res.Dropped, "complete", feed.Complete)
	return res.Outcome
}

func hasOffersFor(site shared.SiteId, offers []capabilityoffer.CapabilityOffer) bool {
	for _, o := range offers {
		if o.SiteId() == site {
			return true
		}
	}
	return false
}

// noteUnlisted logs, once per process, a SKU the site has an offer for and no
// listing for: an expected state (the dictionary lists what the network sells at
// each node), so it is information and never an error.
func (u *FeedInventory) noteUnlisted(ctx context.Context, site shared.SiteId, skus []shared.SKU) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.noted == nil {
		u.noted = map[unlistedKey]struct{}{}
	}
	for _, sku := range skus {
		key := unlistedKey{site: site, sku: sku}
		if _, done := u.noted[key]; done {
			continue
		}
		u.noted[key] = struct{}{}
		u.log(ctx, slog.LevelInfo, "inventory feed: SKU has no listing at the site; it is not fed (said once)", site, "sku", sku)
	}
}

func (u *FeedInventory) log(ctx context.Context, level slog.Level, msg string, site shared.SiteId, args ...any) {
	if u.Logger == nil {
		return
	}
	if site != "" {
		args = append([]any{"site", site}, args...)
	}
	u.Logger.Log(ctx, level, msg, args...)
}
