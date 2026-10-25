package retailnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The inventory feed through the live gateway (ADR 0027 part 2), against the
// stateful fake Vendor API of gateway_live_support_test.go.

var asOf0 = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

// listingTable is a product dictionary read for one site.
type listingTable map[string]shared.NetworkProductId

func (l listingTable) ListingFor(site shared.SiteId, sku shared.SKU) (shared.NetworkProductId, bool) {
	id, ok := l[string(site)+"|"+string(sku)]
	return id, ok
}

func testListings() listingTable {
	return listingTable{
		"FC-1|sku-a": "L-A", "FC-1|sku-b": "L-B", "FC-1|sku-c": "L-C",
		"FC-2|sku-a": "L-OTHER-A",
	}
}

func feedFixture(t *testing.T) *liveFixture {
	t.Helper()
	f := profileFixture(t) // FC-1 is mapped to N-1
	f.g.listings = testListings()
	return f
}

func physical(sku shared.SKU, qty int) contract.FeedLine {
	return contract.FeedLine{SKU: sku, Quantity: qty, Basis: capabilityoffer.BasisPhysical}
}

func constrained(sku shared.SKU, qty int) contract.FeedLine {
	return contract.FeedLine{SKU: sku, Quantity: qty, Basis: capabilityoffer.BasisThroughputConstrained}
}

// feedAt is a one-part feed of FC-1 whose fingerprint follows its content.
func feedAt(asOf time.Time, complete bool, lines ...contract.FeedLine) contract.InventoryFeed {
	return contract.InventoryFeed{
		SiteId: "FC-1", AsOf: asOf, Complete: complete, Lines: lines, Part: 1, Parts: 1,
		Fingerprint: fmt.Sprintf("%t|%v", complete, lines),
	}
}

func threeLines() []contract.FeedLine {
	return []contract.FeedLine{physical("sku-a", 5), constrained("sku-b", 2), physical("sku-c", 0)}
}

func (f *liveFixture) feed(t *testing.T, feed contract.InventoryFeed) contract.FeedResult {
	t.Helper()
	res, err := f.g.SubmitInventoryFeed(context.Background(), feed)
	if err != nil {
		t.Fatalf("SubmitInventoryFeed: %v", err)
	}
	return res
}

func (f *liveFixture) inventoryPosts() []seen {
	var out []seen
	for _, r := range f.fake.requests() {
		if r.Method == http.MethodPost && r.Path == "/vendor/inventory" {
			out = append(out, r)
		}
	}
	return out
}

func decodeFeed(t *testing.T, body string) inventoryFeedRequestDTO {
	t.Helper()
	var d inventoryFeedRequestDTO
	if err := json.Unmarshal([]byte(body), &d); err != nil {
		t.Fatalf("body %q: %v", body, err)
	}
	return d
}

func listingsOf(d inventoryFeedRequestDTO) []string {
	var out []string
	for _, l := range d.Lines {
		out = append(out, l.ListingID)
	}
	return out
}

func refuseListing(path string) submissionErrorDTO {
	return submissionErrorDTO{Code: errListingNotOnNode, Message: "listing is not on this node", Path: path}
}

func TestSubmitInventoryFeed_ASiteWithNoNodeIsNotRegisteredAndNothingIsSent(t *testing.T) {
	f := readyFixture(t, nil) // on the network, not mapped here yet
	f.g.listings = testListings()

	_, err := f.g.SubmitInventoryFeed(context.Background(), feedAt(asOf0, true, threeLines()...))

	if !errors.Is(err, contract.ErrSiteNotRegistered) {
		t.Fatalf("err = %v, want ErrSiteNotRegistered", err)
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Fatalf("%d requests; a site with no node feeds nothing", n)
	}
	if _, found, _ := f.store.InventoryFeedState(context.Background(), "FC-1", 1); found {
		t.Fatal("a feed state was kept for a site that has no node")
	}
}

func TestSubmitInventoryFeed_TheFirstFeedIsOnePostWithItsLedgerRowKeyAndExactBody(t *testing.T) {
	f := feedFixture(t)

	res := f.feed(t, feedAt(asOf0, true, threeLines()...))

	if res.Outcome != contract.FeedConfirmed || res.Sent != 3 || res.Dropped != 0 {
		t.Fatalf("result = %+v, want confirmed with 3 lines sent", res)
	}
	posts := f.inventoryPosts()
	if len(posts) != 1 {
		t.Fatalf("POSTs = %d, want 1", len(posts))
	}
	if posts[0].IdemKey != "nf-inventory-FC-1-2026-10-24T12:00:00Z" {
		t.Errorf("key = %q", posts[0].IdemKey)
	}
	expectFirstFeedBody(t, decodeFeed(t, posts[0].Body))
	row, ok := f.store.row(KindInventory, "FC-1#2026-10-24T12:00:00Z")
	if !ok || row.Status != StatusSuccess || row.IdempotencyKey != posts[0].IdemKey || string(row.Body) != posts[0].Body || row.TransactionID == "" {
		t.Fatalf("ledger row = %+v ok=%v, want SUCCESS with the key and the exact body that were sent", row, ok)
	}
}

// expectFirstFeedBody checks the body of the first feed: whose it is, that it is a
// full update as of the pass, and its three lines in order.
func expectFirstFeedBody(t *testing.T, d inventoryFeedRequestDTO) {
	t.Helper()
	want := []inventoryLineDTO{
		{ListingID: "L-A", AvailableQty: 5, Basis: "PHYSICAL"},
		{ListingID: "L-B", AvailableQty: 2, Basis: "THROUGHPUT_CONSTRAINED"},
		{ListingID: "L-C", AvailableQty: 0, Basis: "PHYSICAL"},
	}
	if d.VendorID != "V-WS" || d.NodeID != "N-1" || !d.FullUpdate || !d.AsOf.Equal(asOf0) || len(d.Lines) != len(want) {
		t.Fatalf("body = %+v", d)
	}
	for i, l := range want {
		if d.Lines[i] != l {
			t.Errorf("line %d = %+v, want %+v", i, d.Lines[i], l)
		}
	}
}

func TestSubmitInventoryFeed_APartialFeedIsNotAFullUpdate(t *testing.T) {
	f := feedFixture(t)
	f.feed(t, feedAt(asOf0, false, physical("sku-a", 5)))
	if d := decodeFeed(t, f.inventoryPosts()[0].Body); d.FullUpdate {
		t.Fatal("an incomplete feed went out as a full update: the network would zero the listings it omits")
	}
}

func TestSubmitInventoryFeed_ASkuWithNoListingIsLeftOutAndTheFeedIsNotFull(t *testing.T) {
	f := feedFixture(t)

	res := f.feed(t, feedAt(asOf0, true, physical("sku-a", 5), physical("sku-nowhere", 9)))

	d := decodeFeed(t, f.inventoryPosts()[0].Body)
	if got := listingsOf(d); len(got) != 1 || got[0] != "L-A" {
		t.Errorf("listings = %v, want only L-A", got)
	}
	if d.FullUpdate || res.Sent != 1 {
		t.Errorf("fullUpdate = %v sent = %d; a SKU the feed could not place must make it partial", d.FullUpdate, res.Sent)
	}
	if !strings.Contains(f.logs.String(), "sku-nowhere") {
		t.Errorf("the SKU left out was not logged:\n%s", f.logs)
	}
}

func TestSubmitInventoryFeed_ThePassIsFedOnceAndARestartDoesNotSendItAgain(t *testing.T) {
	f := feedFixture(t)
	feed := feedAt(asOf0, true, threeLines()...)
	f.feed(t, feed)

	if res := f.feed(t, feed); res.Outcome != contract.FeedUnchanged {
		t.Fatalf("the same pass again = %+v, want unchanged", res)
	}

	// A new process over the same database: same store, same network.
	again := newLiveGateway(f.g.client, f.store, LiveConfig{Sites: []string{"FC-1"}, Logger: f.g.logger})
	again.listings = testListings()
	res, err := again.SubmitInventoryFeed(context.Background(), feedAt(asOf0.Add(5*time.Minute), true, threeLines()...))
	if err != nil || res.Outcome != contract.FeedUnchanged {
		t.Fatalf("a restart over unchanged content = %+v, %v; want unchanged", res, err)
	}
	if n := len(f.inventoryPosts()); n != 1 {
		t.Fatalf("POSTs = %d, want 1 across the repeat and the restart", n)
	}
}

func TestSubmitInventoryFeed_AnUnchangedFeedIsRepeatedWhenAnHourHasPassed(t *testing.T) {
	f := feedFixture(t)
	f.feed(t, feedAt(asOf0, true, threeLines()...))

	if res := f.feed(t, feedAt(asOf0.Add(inventoryRepeatEvery-time.Second), true, threeLines()...)); res.Outcome != contract.FeedUnchanged {
		t.Fatalf("one second short of the hour = %+v, want unchanged", res)
	}
	if res := f.feed(t, feedAt(asOf0.Add(inventoryRepeatEvery), true, threeLines()...)); res.Outcome != contract.FeedConfirmed {
		t.Fatalf("at the hour = %+v, want it sent again", res)
	}
	posts := f.inventoryPosts()
	if len(posts) != 2 || posts[1].IdemKey != "nf-inventory-FC-1-2026-10-24T13:00:00Z" {
		t.Fatalf("POSTs = %+v, want a second one with its own key", posts)
	}
}

func TestSubmitInventoryFeed_AChangedFeedIsSentAtOnce(t *testing.T) {
	f := feedFixture(t)
	f.feed(t, feedAt(asOf0, true, threeLines()...))

	changed := []contract.FeedLine{physical("sku-a", 4), constrained("sku-b", 2), physical("sku-c", 0)}
	if res := f.feed(t, feedAt(asOf0.Add(time.Minute), true, changed...)); res.Outcome != contract.FeedConfirmed {
		t.Fatalf("a changed feed = %+v, want sent", res)
	}
	if n := len(f.inventoryPosts()); n != 2 {
		t.Fatalf("POSTs = %d, want 2", n)
	}
}

func TestSubmitInventoryFeed_AFeedTheNetworkAlreadyAppliedIsASuccess(t *testing.T) {
	f := feedFixture(t)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: errFeedAlreadyApplied, Message: "asOf is not newer than the one held"})

	res := f.feed(t, feedAt(asOf0, true, threeLines()...))

	if res.Outcome != contract.FeedConfirmed {
		t.Fatalf("result = %+v, want confirmed", res)
	}
	if row, _ := f.store.row(KindInventory, "FC-1#2026-10-24T12:00:00Z"); row.Status != StatusSuccess {
		t.Fatalf("row = %+v, want SUCCESS", row)
	}
	if strings.Contains(f.logs.String(), "level=ERROR") {
		t.Errorf("an already applied feed is not an error:\n%s", f.logs)
	}
}

func TestSubmitInventoryFeed_AListingNotOnTheNodeFailsTheFeedAndIsLeftOutOfTheNext(t *testing.T) {
	f := feedFixture(t)
	f.sim.setOutcome(txFailure, refuseListing("$.lines[1].listingId"))

	first := f.feed(t, feedAt(asOf0, true, threeLines()...))

	if first.Outcome != contract.FeedFailed || !strings.Contains(first.Errors, errListingNotOnNode) {
		t.Fatalf("first = %+v, want failed with the network's error", first)
	}
	if row, _ := f.store.row(KindInventory, "FC-1#2026-10-24T12:00:00Z"); row.Status != StatusFailure {
		t.Fatalf("row = %+v, want FAILURE", row)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "listing=L-B") || !strings.Contains(logs, "$.lines[1].listingId") {
		t.Errorf("the refused listing and its path must be logged at ERROR:\n%s", logs)
	}

	// The same pass is not retried; the next one is, without the listing.
	f.sim.setOutcome(txSuccess)
	if res := f.feed(t, feedAt(asOf0, true, threeLines()...)); res.Outcome != contract.FeedFailed {
		t.Fatalf("the failed pass again = %+v, want its standing failure and no resend", res)
	}
	next := f.feed(t, feedAt(asOf0.Add(time.Minute), true, threeLines()...))

	if next.Outcome != contract.FeedConfirmed || next.Dropped != 1 || next.Sent != 2 {
		t.Fatalf("next = %+v, want confirmed with L-B held back", next)
	}
	posts := f.inventoryPosts()
	if len(posts) != 2 {
		t.Fatalf("POSTs = %d, want 2", len(posts))
	}
	if got := listingsOf(decodeFeed(t, posts[1].Body)); strings.Join(got, ",") != "L-A,L-C" {
		t.Errorf("second feed listings = %v, want L-A and L-C", got)
	}
	// And it stays out while the content is unchanged.
	if res := f.feed(t, feedAt(asOf0.Add(2*time.Minute), true, threeLines()...)); res.Outcome != contract.FeedUnchanged || res.Dropped != 1 {
		t.Fatalf("third = %+v, want unchanged", res)
	}
}

func TestSubmitInventoryFeed_ARefusedListingIsTriedAgainAfterTheTTL(t *testing.T) {
	f := feedFixture(t)
	f.sim.setOutcome(txFailure, refuseListing("$.lines[1].listingId"))
	f.feed(t, feedAt(asOf0, true, threeLines()...))
	f.sim.setOutcome(txSuccess)

	later := asOf0.Add(rejectedListingTTL + time.Minute)
	f.g.client.now = func() time.Time { return later }
	res := f.feed(t, feedAt(later, true, threeLines()...))

	if res.Dropped != 0 || res.Sent != 3 {
		t.Fatalf("result = %+v, want the listing tried again after %v", res, rejectedListingTTL)
	}
}

func TestSubmitInventoryFeed_AFailureThatNamesNoLineLeavesNothingOutAndSaysSo(t *testing.T) {
	f := feedFixture(t)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: errListingNotOnNode, Message: "unknown listing"})

	f.feed(t, feedAt(asOf0, true, threeLines()...))

	if rejected, _ := f.store.RejectedListings(context.Background(), "FC-1"); len(rejected) != 0 {
		t.Fatalf("rejected = %+v, want none: the error did not say which line", rejected)
	}
	if !strings.Contains(f.logs.String(), "names no line") {
		t.Errorf("the missing path was not reported:\n%s", f.logs)
	}
}

func TestSubmitInventoryFeed_AnotherFailureIsLoggedAndTheSameContentWaitsForTheHour(t *testing.T) {
	f := feedFixture(t)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: "ErrUpstreamUnavailable", Message: "offer-availability is down"})
	first := f.feed(t, feedAt(asOf0, true, threeLines()...))
	f.sim.setOutcome(txSuccess)

	if first.Outcome != contract.FeedFailed || !strings.Contains(first.Errors, "ErrUpstreamUnavailable") {
		t.Fatalf("first = %+v", first)
	}
	if res := f.feed(t, feedAt(asOf0.Add(time.Minute), true, threeLines()...)); res.Outcome != contract.FeedFailed {
		t.Fatalf("the same content a minute later = %+v, want its standing failure, not a resend", res)
	}
	if res := f.feed(t, feedAt(asOf0.Add(inventoryRepeatEvery), true, threeLines()...)); res.Outcome != contract.FeedConfirmed {
		t.Fatalf("an hour later = %+v, want the retry", res)
	}
}

func TestSubmitInventoryFeed_AWriteTheNetworkRefusesOutrightIsFinalForThatPass(t *testing.T) {
	f := feedFixture(t)
	f.sim.inject = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Path == "/vendor/inventory" {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return true
		}
		return false
	}

	res := f.feed(t, feedAt(asOf0, true, threeLines()...))
	again := f.feed(t, feedAt(asOf0, true, threeLines()...))

	if res.Outcome != contract.FeedFailed || again.Outcome != contract.FeedFailed {
		t.Fatalf("results = %+v, %+v; want failed both times", res, again)
	}
	if n := len(f.inventoryPosts()); n != 1 {
		t.Fatalf("POSTs = %d, want the refused one only", n)
	}
	if !strings.Contains(f.logs.String(), "level=ERROR") {
		t.Errorf("a refusal must be logged at ERROR:\n%s", f.logs)
	}
}

func TestSubmitInventoryFeed_AFeedStillProcessingIsReadOnTheNextPassAndNotSentAgain(t *testing.T) {
	f := feedFixture(t)
	f.sim.setOutcome(txProcessing)
	if res := f.feed(t, feedAt(asOf0, true, threeLines()...)); res.Outcome != contract.FeedSent {
		t.Fatalf("first = %+v, want sent and not yet settled", res)
	}

	if res := f.feed(t, feedAt(asOf0.Add(time.Minute), true, threeLines()...)); res.Outcome != contract.FeedPending {
		t.Fatalf("second = %+v, want pending", res)
	}
	f.sim.finish("tx-1", txSuccess)
	if res := f.feed(t, feedAt(asOf0.Add(2*time.Minute), true, threeLines()...)); res.Outcome != contract.FeedUnchanged {
		t.Fatalf("third = %+v, want unchanged once the network settled it", res)
	}
	if n := len(f.inventoryPosts()); n != 1 {
		t.Fatalf("POSTs = %d, want 1", n)
	}
}

func TestSubmitInventoryFeed_ACrashBetweenTheLedgerAndTheCallResumesUnderTheSameKey(t *testing.T) {
	f := feedFixture(t)
	body, _ := buildInventoryBody("V-WS", "N-1", asOf0, true, []inventoryLineDTO{{ListingID: "L-A", AvailableQty: 5, Basis: "PHYSICAL"}})
	subject := "FC-1#2026-10-24T12:00:00Z"
	ctx := context.Background()
	if _, err := f.store.BeginSubmission(ctx, Submission{Kind: KindInventory, Subject: subject, IdempotencyKey: "nf-inventory-FC-1-2026-10-24T12:00:00Z", Body: body}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SaveInventoryFeedState(ctx, InventoryFeedState{SiteCode: "FC-1", Part: 1, Subject: subject, Fingerprint: "old", AsOf: asOf0}); err != nil {
		t.Fatal(err)
	}

	f.feed(t, feedAt(asOf0.Add(time.Minute), true, threeLines()...))

	posts := f.inventoryPosts()
	if len(posts) != 2 || posts[0].IdemKey != "nf-inventory-FC-1-2026-10-24T12:00:00Z" || posts[0].Body != string(body) {
		t.Fatalf("POSTs = %+v, want the stored row sent first with its own key and body", posts)
	}
	if row, _ := f.store.row(KindInventory, subject); row.Status != StatusSuccess || row.TransactionID == "" {
		t.Fatalf("the resumed row = %+v, want settled", row)
	}
}

func TestSubmitInventoryFeed_ASplitFeedHasOneLedgerRowAndKeyPerPart(t *testing.T) {
	f := feedFixture(t)
	one, two := feedAt(asOf0, false, physical("sku-a", 1)), feedAt(asOf0, false, physical("sku-b", 2))
	one.Part, one.Parts, two.Part, two.Parts = 1, 2, 2, 2

	f.feed(t, one)
	f.feed(t, two)
	f.feed(t, two) // the same pass again

	posts := f.inventoryPosts()
	if len(posts) != 2 || posts[0].IdemKey != "nf-inventory-FC-1-2026-10-24T12:00:00Z" || posts[1].IdemKey != "nf-inventory-FC-1-2026-10-24T12:00:00Z-2" {
		t.Fatalf("POSTs = %+v, want two with distinct keys", posts)
	}
	if _, ok := f.store.row(KindInventory, "FC-1#2026-10-24T12:00:00Z#2"); !ok {
		t.Fatal("part 2 has no ledger row of its own")
	}
}

func TestSubmitInventoryFeed_EveryLineHeldBackSendsNothing(t *testing.T) {
	f := feedFixture(t)
	if err := f.store.RejectListing(context.Background(), "FC-1", RejectedListing{ListingID: "L-A", RejectedAt: asOf0}); err != nil {
		t.Fatal(err)
	}

	res := f.feed(t, feedAt(asOf0, false, physical("sku-a", 5)))

	if res.Outcome != contract.FeedSkippedNoListings || res.Dropped != 1 || len(f.inventoryPosts()) != 0 {
		t.Fatalf("result = %+v posts = %d, want nothing sent", res, len(f.inventoryPosts()))
	}
}

func TestSubmitInventoryFeed_AFeedThatCannotBeSentIsRefusedBeforeAnyIO(t *testing.T) {
	f := feedFixture(t)
	huge := make([]contract.FeedLine, maxInventoryLines+1)
	for i := range huge {
		huge[i] = physical(shared.SKU(fmt.Sprintf("s-%d", i)), 1)
	}
	cases := map[string]contract.InventoryFeed{
		"no lines":       feedAt(asOf0, true),
		"too many lines": feedAt(asOf0, false, huge...),
		"negative":       feedAt(asOf0, true, physical("sku-a", -1)),
		"unknown basis":  feedAt(asOf0, true, contract.FeedLine{SKU: "sku-a", Quantity: 1, Basis: "UNKNOWN"}),
		"no moment":      feedAt(time.Time{}, true, physical("sku-a", 1)),
		"no site":        {AsOf: asOf0, Lines: []contract.FeedLine{physical("sku-a", 1)}, Part: 1, Parts: 1, Fingerprint: "f"},
		"no fingerprint": {SiteId: "FC-1", AsOf: asOf0, Lines: []contract.FeedLine{physical("sku-a", 1)}, Part: 1, Parts: 1},
		"part past last": {SiteId: "FC-1", AsOf: asOf0, Lines: []contract.FeedLine{physical("sku-a", 1)}, Part: 3, Parts: 2, Fingerprint: "f"},
		"no parts":       {SiteId: "FC-1", AsOf: asOf0, Lines: []contract.FeedLine{physical("sku-a", 1)}, Fingerprint: "f"},
		"no SKU":         feedAt(asOf0, true, contract.FeedLine{Quantity: 1, Basis: capabilityoffer.BasisPhysical}),
	}
	for name, feed := range cases {
		if _, err := f.g.SubmitInventoryFeed(context.Background(), feed); !errors.Is(err, ErrBadFeed) {
			t.Errorf("%s: err = %v, want ErrBadFeed", name, err)
		}
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Fatalf("%d requests; a bad feed makes none", n)
	}
}

func TestSubmitInventoryFeed_WithoutADictionaryItCannotFeed(t *testing.T) {
	f := feedFixture(t)
	f.g.listings = nil
	if _, err := f.g.SubmitInventoryFeed(context.Background(), feedAt(asOf0, true, threeLines()...)); !errors.Is(err, ErrNoDictionary) {
		t.Fatalf("err = %v, want ErrNoDictionary", err)
	}
}

func TestFeedSubjectAndKey(t *testing.T) {
	for _, tc := range []struct {
		part, parts  int
		subject, key string
	}{
		{1, 1, "FC-1#2026-10-24T12:00:00Z", "nf-inventory-FC-1-2026-10-24T12:00:00Z"},
		{1, 3, "FC-1#2026-10-24T12:00:00Z", "nf-inventory-FC-1-2026-10-24T12:00:00Z"},
		{2, 3, "FC-1#2026-10-24T12:00:00Z#2", "nf-inventory-FC-1-2026-10-24T12:00:00Z-2"},
	} {
		if got := feedSubject("FC-1", asOf0, tc.part, tc.parts); got != tc.subject {
			t.Errorf("subject(%d/%d) = %q, want %q", tc.part, tc.parts, got, tc.subject)
		}
		if got := feedKey("FC-1", asOf0, tc.part, tc.parts); got != tc.key {
			t.Errorf("key(%d/%d) = %q, want %q", tc.part, tc.parts, got, tc.key)
		}
	}
	// A moment with a fraction keeps it, and a local zone is read as UTC.
	frac := time.Date(2026, 10, 24, 9, 0, 0, 500, time.FixedZone("x", -3*3600))
	if got := feedSubject("FC-1", frac, 1, 1); got != "FC-1#2026-10-24T12:00:00.0000005Z" {
		t.Errorf("subject = %q", got)
	}
	long := strings.Repeat("s", 200)
	if got := feedKey(long, asOf0, 1, 1); len(got) > maxIdempotencyKeyLength || !strings.HasPrefix(got, "nf-inventory-") {
		t.Errorf("a key over the limit = %q, want it hashed within %d", got, maxIdempotencyKeyLength)
	}
}

func TestListingAtPath(t *testing.T) {
	body := []byte(`{"lines":[{"listingId":"L-0"},{"listingId":"L-1"},{"listingId":"L-2"}]}`)
	for _, tc := range []struct {
		path string
		want string
		ok   bool
	}{
		{"$.lines[0].listingId", "L-0", true},
		{"$.lines[2].listingId", "L-2", true},
		{"lines[1]", "L-1", true},
		{"$.lines[3].listingId", "", false},
		{"$.lines[x].listingId", "", false},
		{"$.nodeId", "", false},
		{"", "", false},
		{"$.lines[99999999999999999999].listingId", "", false},
	} {
		if got, ok := listingAtPath(body, tc.path); got != tc.want || ok != tc.ok {
			t.Errorf("listingAtPath(%q) = %q, %v; want %q, %v", tc.path, got, ok, tc.want, tc.ok)
		}
	}
	if _, ok := listingAtPath([]byte("not json"), "$.lines[0]"); ok {
		t.Error("an unreadable stored body must not name a listing")
	}
}

func TestWireBasis(t *testing.T) {
	for in, want := range map[capabilityoffer.Basis]string{
		capabilityoffer.BasisPhysical: "PHYSICAL", capabilityoffer.BasisThroughputConstrained: "THROUGHPUT_CONSTRAINED",
	} {
		if got, err := wireBasis(in); err != nil || got != want {
			t.Errorf("wireBasis(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []capabilityoffer.Basis{"", "UNKNOWN", "physical"} {
		if _, err := wireBasis(bad); !errors.Is(err, ErrBadFeed) {
			t.Errorf("wireBasis(%q) = %v, want ErrBadFeed: an unknown figure is never sent", bad, err)
		}
	}
}
