package contract

import (
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// MaxFeedLines is the most lines one inventory feed may carry: the network takes
// at most 5000 per feed (ADR 0027 §9), so a bigger site is fed in parts.
const MaxFeedLines = 5000

// InventoryFeed is SubmitInventoryFeed's argument (ADR 0027 §9): what one site
// claims it can ship, in OUR terms, taken from the CapabilityOffers one
// recompute pass persisted. The network's own vocabulary (the identifier it
// keys stock by, which node, the flag that makes a feed "full") stays inside
// the adapter.
type InventoryFeed struct {
	SiteId shared.SiteId
	// AsOf is the clock reading of the recompute pass that produced the offers:
	// the moment the figures describe, not the moment of sending.
	AsOf time.Time
	// Complete is true only when every SKU the site has a listing for is in
	// Lines with a fresh offer. A complete feed says the SKUs it omits have no
	// stock; an incomplete one says nothing about them (ADR 0027 §10).
	Complete bool
	// Lines are sorted by SKU, one per SKU, at most MaxFeedLines.
	Lines []FeedLine
	// Part and Parts say which piece of a split feed this is (1 of 1 when the
	// site fits in one). Every part has the same AsOf; only a single-part feed
	// can be Complete.
	Part, Parts int
	// Fingerprint identifies the content of this part (Complete, Lines): equal
	// fingerprints are the same feed, whatever the AsOf.
	Fingerprint string
}

// FeedLine is one SKU of an InventoryFeed. Basis is never anything but the two
// bases an offer can have: an offer whose figure is unknown is not fed.
type FeedLine struct {
	SKU      shared.SKU
	Quantity int
	Basis    capabilityoffer.Basis
}

// FeedOutcome is what one attempt to feed a site came to, and the label of its
// log line.
type FeedOutcome string

const (
	// FeedSent: the feed was written to the network in this call; its outcome is
	// not read yet.
	FeedSent FeedOutcome = "sent"
	// FeedConfirmed: the network settled the feed as applied in this call (a feed
	// it already held counts as applied).
	FeedConfirmed FeedOutcome = "confirmed"
	// FeedPending: a feed written earlier is still being processed.
	FeedPending FeedOutcome = "pending"
	// FeedUnchanged: the network already holds this very feed (same fingerprint,
	// sent less than an hour ago); nothing was sent.
	FeedUnchanged FeedOutcome = "unchanged"
	// FeedFailed: the network failed the feed, or refused it.
	FeedFailed FeedOutcome = "failed"
	// FeedSkippedNoNode: the site has no node on the network yet.
	FeedSkippedNoNode FeedOutcome = "skipped_no_node"
	// FeedSkippedNoOffers: no recompute pass has persisted an offer for the site.
	FeedSkippedNoOffers FeedOutcome = "skipped_no_offers"
	// FeedSkippedNoListings: the site has offers but not one of its SKUs is listed
	// there, so there is nothing to feed.
	FeedSkippedNoListings FeedOutcome = "skipped_no_listings"
)

// FeedResult is SubmitInventoryFeed's answer.
type FeedResult struct {
	Outcome FeedOutcome
	// Sent is how many lines the network was given; Dropped how many the adapter
	// held back because the network had refused that listing earlier.
	Sent, Dropped int
	// Errors carries the network's error list for a FeedFailed outcome.
	Errors string
}
