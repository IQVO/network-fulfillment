package retailnetwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The inventory feed (ADR 0027 part 2): POST /vendor/inventory, an asynchronous
// write like the others. It reuses the ledger, send and the transaction reading
// of the demand loop and the profile as they are (Decision 12).

var _ ports.InventoryFeeder = (*LiveGateway)(nil)

var pathInventory = []string{"vendor", "inventory"}

const (
	// inventoryRepeatEvery is how long an unchanged feed is held back: it is
	// repeated at most hourly so the network's view carries a recent asOf
	// (ADR 0027 §11).
	inventoryRepeatEvery = time.Hour
	// rejectedListingTTL is how long a listing the network refused as not on the
	// node stays out of the feeds. After it the listing is tried again, once, so a
	// listing the network later learns of is not silenced for good, and one that is
	// still wrong costs one failed feed a day and one ERROR line.
	rejectedListingTTL = 24 * time.Hour
)

// ListingResolver is the product dictionary read the other way for one site: the
// network product id a SKU is fed under there (it is the listing id, ADR 0026
// Decision 4). ok is false when the SKU has no single listing at the site.
type ListingResolver interface {
	ListingFor(site shared.SiteId, sku shared.SKU) (shared.NetworkProductId, bool)
}

// feedSubject is the ledger subject of a feed: <site>#<asOf>, with #<part> from
// the second part of a split feed.
func feedSubject(site string, asOf time.Time, part, parts int) string {
	s := site + "#" + asOf.UTC().Format(time.RFC3339Nano)
	if parts > 1 && part > 1 {
		s += "#" + strconv.Itoa(part)
	}
	return s
}

// feedKey is the idempotency key of a feed: nf-inventory-<site>-<asOf>, with
// -<part> from the second part. Deterministic, so a retry after a crash replays.
func feedKey(site string, asOf time.Time, part, parts int) string {
	k := site + "-" + asOf.UTC().Format(time.RFC3339Nano)
	if parts > 1 && part > 1 {
		k += "-" + strconv.Itoa(part)
	}
	return idempotencyKey("inventory", k)
}

// SubmitInventoryFeed feeds one part of a site's inventory and settles what it can
// in one call.
//
//   - A pass is fed once. The ledger subject is <site>#<asOf>, so a second look at
//     the same pass (a restart, a worker tick before the next recompute) reports
//     where the first one stands and sends nothing.
//   - A feed is sent when its fingerprint changed since the last attempt; an
//     unchanged one is repeated when an hour has passed since the pass it described.
//   - A listing the network refused as not on the node (ErrListingNotOnNode) is
//     kept out of the following feeds, so that one wrong listing cannot silence the
//     rest; a feed the network already applied (ErrFeedAlreadyApplied) is a success.
//
// A feed the network could not take is ErrBadFeed before any I/O; a site with no
// node is contract.ErrSiteNotRegistered. A refusal by the network is a result
// (FeedFailed with its error list), a call that could not be made is an error.
func (g *LiveGateway) SubmitInventoryFeed(ctx context.Context, feed contract.InventoryFeed) (contract.FeedResult, error) {
	if err := validateFeed(feed); err != nil {
		return contract.FeedResult{}, err
	}
	if g.listings == nil {
		return contract.FeedResult{}, ErrNoDictionary
	}
	site := string(feed.SiteId)
	nodeID, mapped, err := g.store.NodeIDForSite(ctx, site)
	if err != nil {
		return contract.FeedResult{}, err
	}
	if !mapped {
		return contract.FeedResult{}, fmt.Errorf("%w: %s", contract.ErrSiteNotRegistered, site)
	}

	state, found, err := g.store.InventoryFeedState(ctx, site, feed.Part)
	if err != nil {
		return contract.FeedResult{}, err
	}
	var prev Submission
	havePrev := false
	if found {
		// Settle the last attempt first: it may teach us a listing to drop.
		if prev, havePrev, err = g.settleLastFeed(ctx, site, state); err != nil {
			return contract.FeedResult{}, err
		}
		if !feed.AsOf.After(state.AsOf) {
			return g.standing(site, prev, havePrev), nil
		}
	}

	plan, err := g.planWire(ctx, feed)
	if err != nil {
		return contract.FeedResult{}, err
	}
	if len(plan.lines) == 0 {
		g.logger.Warn("retail-network: every line of the inventory feed is held back; nothing is sent",
			"site", site, "heldBack", plan.dropped)
		return contract.FeedResult{Outcome: contract.FeedSkippedNoListings, Dropped: plan.dropped}, nil
	}
	if found && state.Fingerprint == plan.fingerprint && feed.AsOf.Sub(state.AsOf) < inventoryRepeatEvery {
		res := g.standing(site, prev, havePrev)
		res.Dropped = plan.dropped
		return res, nil
	}
	return g.sendFeed(ctx, nodeID, feed, plan)
}

// standing reports where the last attempt stands, without sending anything.
func (g *LiveGateway) standing(site string, prev Submission, havePrev bool) contract.FeedResult {
	switch {
	case !havePrev || prev.Status == StatusSuccess:
		return contract.FeedResult{Outcome: contract.FeedUnchanged}
	case prev.Status == StatusFailure:
		g.logger.Warn("retail-network: the last inventory feed failed and its content has not changed; it is sent again when the content changes or an hour has passed",
			"site", site, "errors", prev.Errors)
		return contract.FeedResult{Outcome: contract.FeedFailed, Errors: prev.Errors}
	default:
		return contract.FeedResult{Outcome: contract.FeedPending}
	}
}

func validateFeed(f contract.InventoryFeed) error {
	switch {
	case f.SiteId == "":
		return fmt.Errorf("%w: no site", ErrBadFeed)
	case f.AsOf.IsZero():
		return fmt.Errorf("%w: no moment (asOf)", ErrBadFeed)
	case f.Fingerprint == "":
		return fmt.Errorf("%w: no fingerprint", ErrBadFeed)
	case f.Parts < 1 || f.Part < 1 || f.Part > f.Parts:
		return fmt.Errorf("%w: part %d of %d", ErrBadFeed, f.Part, f.Parts)
	case len(f.Lines) == 0 || len(f.Lines) > maxInventoryLines:
		return fmt.Errorf("%w: %d lines (a feed has 1 to %d)", ErrBadFeed, len(f.Lines), maxInventoryLines)
	}
	for i, l := range f.Lines {
		if l.SKU == "" || l.Quantity < 0 {
			return fmt.Errorf("%w: line %d has no SKU or a negative quantity", ErrBadFeed, i)
		}
		if _, err := wireBasis(l.Basis); err != nil {
			return err
		}
	}
	return nil
}

// wirePlan is what a feed comes to on the wire, after listings are found and
// refused ones are left out.
type wirePlan struct {
	lines       []inventoryLineDTO
	full        bool
	dropped     int    // lines held back because the network refused that listing
	fingerprint string // of the feed as it goes out
}

// planWire turns the feed's SKUs into the network's listings. A SKU with no single
// listing at the site (the use case only offers listed ones, so this is a
// dictionary that changed under it) is left out and makes the feed incomplete.
func (g *LiveGateway) planWire(ctx context.Context, feed contract.InventoryFeed) (wirePlan, error) {
	site := string(feed.SiteId)
	refused, err := g.activeRejections(ctx, site)
	if err != nil {
		return wirePlan{}, err
	}
	plan := wirePlan{full: feed.Complete, lines: make([]inventoryLineDTO, 0, len(feed.Lines))}
	var held []string
	for _, l := range feed.Lines {
		listing, ok := g.listings.ListingFor(feed.SiteId, l.SKU)
		if !ok {
			g.logger.Warn("retail-network: a SKU of the inventory feed has no listing at the site any more; it is left out and the feed is not complete",
				"site", site, "sku", l.SKU)
			plan.full = false
			continue
		}
		if refused[string(listing)] {
			held = append(held, string(listing))
			continue
		}
		basis, err := wireBasis(l.Basis)
		if err != nil {
			return wirePlan{}, err
		}
		plan.lines = append(plan.lines, inventoryLineDTO{ListingID: string(listing), AvailableQty: l.Quantity, Basis: basis})
	}
	plan.dropped = len(held)
	plan.fingerprint = feed.Fingerprint
	if len(held) > 0 || plan.full != feed.Complete {
		slices.Sort(held)
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%t|%s", feed.Fingerprint, plan.full, strings.Join(held, ","))))
		plan.fingerprint = hex.EncodeToString(sum[:])
	}
	return plan, nil
}

// activeRejections are the listings of the site the network refused less than
// rejectedListingTTL ago.
func (g *LiveGateway) activeRejections(ctx context.Context, site string) (map[string]bool, error) {
	all, err := g.store.RejectedListings(ctx, site)
	if err != nil {
		return nil, err
	}
	now := g.client.now()
	out := make(map[string]bool, len(all))
	for _, r := range all {
		if now.Sub(r.RejectedAt) < rejectedListingTTL {
			out[r.ListingID] = true
		}
	}
	return out, nil
}

// sendFeed writes the feed: the exact body is stored in the ledger and the state
// is advanced before the call, so a crash resumes it under the same key.
func (g *LiveGateway) sendFeed(ctx context.Context, nodeID string, feed contract.InventoryFeed, plan wirePlan) (contract.FeedResult, error) {
	site := string(feed.SiteId)
	body, err := buildInventoryBody(g.client.vendorID, nodeID, feed.AsOf, plan.full, plan.lines)
	if err != nil {
		return contract.FeedResult{}, err
	}
	subject := feedSubject(site, feed.AsOf, feed.Part, feed.Parts)
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindInventory, Subject: subject, IdempotencyKey: feedKey(site, feed.AsOf, feed.Part, feed.Parts), Body: body,
	})
	if err != nil {
		return contract.FeedResult{}, err
	}
	if err := g.store.SaveInventoryFeedState(ctx, InventoryFeedState{
		SiteCode: site, Part: feed.Part, Subject: subject, Fingerprint: plan.fingerprint, AsOf: feed.AsOf.UTC(),
	}); err != nil {
		return contract.FeedResult{}, err
	}
	sentNow := row.TransactionID == ""
	row, err = g.send(ctx, row, "submitInventory", pathInventory)
	if errors.Is(err, ErrWriteRefused) {
		g.logger.Error("retail-network: the network refused an inventory feed", "site", site, "asOf", feed.AsOf, "errors", err.Error())
		return contract.FeedResult{Outcome: contract.FeedFailed, Dropped: plan.dropped, Errors: err.Error()}, nil
	}
	if err != nil {
		return contract.FeedResult{}, err
	}
	res := contract.FeedResult{Sent: len(plan.lines), Dropped: plan.dropped}
	settled, err := g.readFeedTransaction(ctx, site, row)
	switch {
	case err != nil:
		// The write is accepted and its id is stored: the next pass reads it.
		g.logger.Warn("retail-network: could not read the outcome of an inventory feed yet; the next pass will", "site", site, "err", err)
		res.Outcome = contract.FeedSent
	case settled.Status == StatusSuccess:
		res.Outcome = contract.FeedConfirmed
	case settled.Status == StatusFailure:
		res.Outcome, res.Errors = contract.FeedFailed, settled.Errors
	case sentNow:
		res.Outcome = contract.FeedSent
	default:
		res.Outcome = contract.FeedPending
	}
	return res, nil
}

// settleLastFeed takes the last attempt as far as the network lets it go: it
// sends it if it never got its transaction id (the stored body and key, so the
// network sees one write) and reads its transaction once. An unreadable
// transaction leaves it in flight; the next pass asks again.
func (g *LiveGateway) settleLastFeed(ctx context.Context, site string, st InventoryFeedState) (Submission, bool, error) {
	row, ok, err := g.store.Submission(ctx, KindInventory, st.Subject)
	if err != nil || !ok || row.Status.Terminal() {
		return row, ok, err
	}
	if row, err = g.send(ctx, row, "submitInventory", pathInventory); err != nil && !errors.Is(err, ErrWriteRefused) {
		return row, true, err
	}
	if row.TransactionID != "" {
		if _, err := g.readFeedTransaction(ctx, site, row); err != nil {
			g.logger.Warn("retail-network: could not read the outcome of an inventory feed yet; the next pass will", "site", site, "err", err)
		}
	}
	row, ok, err = g.store.Submission(ctx, KindInventory, st.Subject)
	return row, ok, err
}

// readFeedTransaction reads the row's transaction by its persisted id and stores
// the verdict, learning from a failure what it can. A transaction still
// PROCESSING leaves the row as it is.
func (g *LiveGateway) readFeedTransaction(ctx context.Context, site string, row Submission) (Submission, error) {
	tx, err := g.client.getTransaction(ctx, row.TransactionID)
	if err != nil {
		return row, err
	}
	switch tx.Status {
	case txSuccess:
		if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusSuccess, ""); err != nil {
			return row, err
		}
		row.Status = StatusSuccess
	case txFailure:
		applied, err := g.learnFromFailure(ctx, site, row, tx.Errors)
		if err != nil {
			return row, err
		}
		if applied {
			row.Status = StatusSuccess
			return row, g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusSuccess, "")
		}
		row.Status, row.Errors = StatusFailure, joinErrors(tx.Errors)
		g.logger.Error("retail-network: the network failed an inventory feed", "site", site, "subject", row.Subject, "errors", row.Errors)
		return row, g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusFailure, row.Errors)
	}
	return row, nil
}

// learnFromFailure acts on the errors of a failed feed. applied is true when
// the network only said it already holds this feed (ErrFeedAlreadyApplied), which
// is a success. A listing it refused as not on the node is recorded BEFORE the
// outcome is, so a crash in between re-reads the failure and records it again.
func (g *LiveGateway) learnFromFailure(ctx context.Context, site string, row Submission, errs []submissionErrorDTO) (bool, error) {
	onlyApplied := len(errs) > 0
	for _, e := range errs {
		switch e.Code {
		case errFeedAlreadyApplied:
			g.logger.Info("retail-network: the network already held this inventory feed; counted as applied", "site", site, "subject", row.Subject)
		case errListingNotOnNode:
			onlyApplied = false
			if err := g.rejectListingOf(ctx, site, row, e); err != nil {
				return false, err
			}
		default:
			onlyApplied = false
		}
	}
	return onlyApplied, nil
}

var lineIndexPattern = regexp.MustCompile(`lines\[(\d+)\]`)

// rejectListingOf finds the listing an ErrListingNotOnNode points at (the JSON
// path indexes the lines of the body we stored) and keeps it out of the next feeds.
func (g *LiveGateway) rejectListingOf(ctx context.Context, site string, row Submission, e submissionErrorDTO) error {
	listing, ok := listingAtPath(row.Body, e.Path)
	if !ok {
		g.logger.Error("retail-network: the network refused a listing as not on the node but the error names no line of the feed; nothing can be left out",
			"site", site, "subject", row.Subject, "path", e.Path, "message", e.Message)
		return nil
	}
	g.logger.Error("retail-network: the network refused a listing as not on the node; it is left out of the next inventory feeds",
		"site", site, "listing", listing, "path", e.Path, "message", e.Message)
	return g.store.RejectListing(ctx, site, RejectedListing{ListingID: listing, Reason: e.Message, RejectedAt: g.client.now().UTC()})
}

// listingAtPath is the listing id of the line a JSON path like
// $.lines[3].listingId points at, in the body that was sent.
func listingAtPath(body []byte, path string) (string, bool) {
	m := lineIndexPattern.FindStringSubmatch(path)
	if m == nil {
		return "", false
	}
	idx, err := strconv.Atoi(m[1])
	if err != nil {
		return "", false
	}
	var sent inventoryFeedRequestDTO
	if err := json.Unmarshal(body, &sent); err != nil || idx < 0 || idx >= len(sent.Lines) {
		return "", false
	}
	return sent.Lines[idx].ListingID, true
}
