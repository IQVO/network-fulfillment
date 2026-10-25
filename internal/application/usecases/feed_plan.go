package usecases

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// feedPlan is what one site's feed comes to for one recompute pass: pure data,
// no network types, built by planInventoryFeeds.
type feedPlan struct {
	// AsOf is the pass's clock reading: the ComputedAt its fresh offers share.
	AsOf time.Time
	// Feeds are the parts to send, in order; none when there is nothing to feed.
	Feeds []contract.InventoryFeed
	// Stale are the SKUs the site has a listing for that have no fresh offer in
	// the pass (their inventory lookup failed, or they were never computed).
	// They are not fed, and they make the feed incomplete.
	Stale []shared.SKU
	// Unlisted are the SKUs with a fresh offer that the site has no listing for.
	Unlisted []shared.SKU
}

// planInventoryFeeds builds the feed of one site from the offers one recompute
// pass persisted (ADR 0027 §9, §10). It is pure.
//
//   - A pass is the set of the site's offers that share the newest ComputedAt:
//     an offer of an earlier pass is not fresh. Its SKU is not fed (a stale
//     figure presented under a new moment would be a lie) and it keeps the feed
//     from being complete.
//   - Only a SKU the site has a listing for is fed. Offers carry only the bases
//     PHYSICAL and THROUGHPUT_CONSTRAINED; a figure that is not known is never
//     an offer, so none is ever fed as unknown.
//   - The feed is Complete only when every listed SKU has a fresh offer: the
//     network zeroes what a complete feed omits, so a SKU whose lookup failed
//     must not be left out of one.
//   - More than contract.MaxFeedLines lines are split into parts, each partial
//     (an omitted listing is zeroed by a complete feed, and the other parts
//     carry it), all as of the same moment.
func planInventoryFeeds(site shared.SiteId, listed []shared.SKU, offers []capabilityoffer.CapabilityOffer) feedPlan {
	mine := make([]capabilityoffer.CapabilityOffer, 0, len(offers))
	var asOf time.Time
	for _, o := range offers {
		if o.SiteId() != site {
			continue
		}
		mine = append(mine, o)
		if o.ComputedAt().After(asOf) {
			asOf = o.ComputedAt()
		}
	}
	if len(mine) == 0 {
		return feedPlan{}
	}

	listedSet := make(map[shared.SKU]struct{}, len(listed))
	for _, s := range listed {
		listedSet[s] = struct{}{}
	}
	fed := make(map[shared.SKU]contract.FeedLine, len(mine))
	unlisted := map[shared.SKU]struct{}{}
	for _, o := range mine {
		if !o.ComputedAt().Equal(asOf) {
			continue
		}
		if _, ok := listedSet[o.SKU()]; !ok {
			unlisted[o.SKU()] = struct{}{}
			continue
		}
		line := contract.FeedLine{SKU: o.SKU(), Quantity: o.AdvertisedQuantity(), Basis: o.Basis()}
		// One offer per SKU and site is the repository's rule; were two ever to
		// share a pass, the smaller figure is the one that can be honoured.
		if prev, dup := fed[line.SKU]; !dup || line.Quantity < prev.Quantity {
			fed[line.SKU] = line
		}
	}

	plan := feedPlan{AsOf: asOf, Unlisted: sortedSKUs(unlisted)}
	for s := range listedSet {
		if _, ok := fed[s]; !ok {
			plan.Stale = append(plan.Stale, s)
		}
	}
	slices.Sort(plan.Stale)

	lines := make([]contract.FeedLine, 0, len(fed))
	for _, l := range fed {
		lines = append(lines, l)
	}
	slices.SortFunc(lines, func(a, b contract.FeedLine) int { return strings.Compare(string(a.SKU), string(b.SKU)) })
	plan.Feeds = splitFeeds(site, asOf, len(plan.Stale) == 0, lines)
	return plan
}

func sortedSKUs(set map[shared.SKU]struct{}) []shared.SKU {
	if len(set) == 0 {
		return nil
	}
	out := make([]shared.SKU, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

// splitFeeds cuts the sorted lines into parts of at most contract.MaxFeedLines.
// Only a feed that fits in one part can be complete.
func splitFeeds(site shared.SiteId, asOf time.Time, allFresh bool, lines []contract.FeedLine) []contract.InventoryFeed {
	if len(lines) == 0 {
		return nil
	}
	parts := (len(lines) + contract.MaxFeedLines - 1) / contract.MaxFeedLines
	complete := allFresh && parts == 1
	out := make([]contract.InventoryFeed, 0, parts)
	for i := range parts {
		chunk := lines[i*contract.MaxFeedLines : min((i+1)*contract.MaxFeedLines, len(lines))]
		out = append(out, contract.InventoryFeed{
			SiteId: site, AsOf: asOf, Complete: complete, Lines: chunk,
			Part: i + 1, Parts: parts, Fingerprint: feedFingerprint(complete, chunk),
		})
	}
	return out
}

// feedFingerprint hashes what a part says, and not when it was said: equal
// fingerprints are the same feed. Whether the part is complete is part of what
// it says (a complete feed zeroes what it omits), so it is hashed with the lines.
func feedFingerprint(complete bool, lines []contract.FeedLine) string {
	h := sha256.New()
	fmt.Fprintf(h, "complete=%t\n", complete)
	for _, l := range lines {
		fmt.Fprintf(h, "%s\x00%d\x00%s\n", l.SKU, l.Quantity, l.Basis)
	}
	return hex.EncodeToString(h.Sum(nil))
}
