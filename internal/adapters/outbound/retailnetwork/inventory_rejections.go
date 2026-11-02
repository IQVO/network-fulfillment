package retailnetwork

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
)

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
