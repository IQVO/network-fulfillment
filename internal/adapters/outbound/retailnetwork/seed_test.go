package retailnetwork_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
)

func writeSeed(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "demand.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	return path
}

func seedNow() time.Time { return time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC) }

func TestLoadSeedFile_SeedsDemandThePollerCanFetch(t *testing.T) {
	path := writeSeed(t, `{
	  "demands": [
	    {
	      "networkRef": "po-1",
	      "siteId": "site-1",
	      "requiredShipBy": "2026-09-25T12:00:00Z",
	      "lines": [
	        {"networkLineRef": "1", "networkProductId": "ASIN-1", "quantity": 2},
	        {"networkLineRef": "2", "networkProductId": "ASIN-2", "quantity": 1}
	      ]
	    }
	  ]
	}`)

	g := retailnetwork.NewStubGateway(nil)
	n, err := retailnetwork.LoadSeedFile(g, path, seedNow())
	if err != nil {
		t.Fatalf("LoadSeedFile: %v", err)
	}
	if n != 1 {
		t.Fatalf("loaded %d demands, want 1", n)
	}

	got, err := g.PollDemand(t.Context(), time.Time{})
	if err != nil {
		t.Fatalf("PollDemand: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("polled %d demands, want 1", len(got))
	}
	d := got[0]
	if d.NetworkRef != "po-1" || d.SiteId != "site-1" {
		t.Fatalf("ref/site = %q/%q, want po-1/site-1", d.NetworkRef, d.SiteId)
	}
	if !d.RequiredShipBy.Equal(time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("requiredShipBy = %v", d.RequiredShipBy)
	}
	if len(d.Lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(d.Lines))
	}
	if d.Lines[0].NetworkProductId != "ASIN-1" || d.Lines[0].Quantity != 2 {
		t.Fatalf("line 0 = %+v", d.Lines[0])
	}
	// The network's product id must arrive UNTRANSLATED: translating it is
	// the receiving use case's job, and a seed file that pre-resolved SKUs
	// would bypass the Anti-Corruption Layer entirely.
	if d.Lines[0].NetworkLineRef != "1" {
		t.Fatalf("networkLineRef = %q, want 1", d.Lines[0].NetworkLineRef)
	}
}

// A relative deadline is the useful form for a committed fixture: a fixed
// instant goes stale and every order becomes instantly infeasible, which
// reads like a promise bug rather than an expired file.
func TestLoadSeedFile_AcceptsARelativeDeadline(t *testing.T) {
	path := writeSeed(t, `{
	  "demands": [
	    {"networkRef": "po-rel", "siteId": "site-1", "requiredShipBy": "+36h",
	     "lines": [{"networkLineRef": "1", "networkProductId": "ASIN-1", "quantity": 1}]}
	  ]
	}`)

	g := retailnetwork.NewStubGateway(nil)
	if _, err := retailnetwork.LoadSeedFile(g, path, seedNow()); err != nil {
		t.Fatalf("LoadSeedFile: %v", err)
	}

	got, _ := g.PollDemand(t.Context(), time.Time{})
	want := seedNow().Add(36 * time.Hour)
	if !got[0].RequiredShipBy.Equal(want) {
		t.Fatalf("requiredShipBy = %v, want %v (now + 36h)", got[0].RequiredShipBy, want)
	}
}

// Every failure must be returned, not swallowed. A seed file that
// silently failed to load leaves a stub deployment looking healthy with
// nothing to deliver — indistinguishable from a broken poller.
func TestLoadSeedFile_FailuresAreReturnedNotSwallowed(t *testing.T) {
	cases := []struct {
		name, body, wantSubstring string
	}{
		{
			"unknown field",
			`{"demands":[{"networkRef":"po-1","siteId":"s","requiredShipBy":"+1h","shipToName":"Ada","lines":[]}]}`,
			"shipToName",
		},
		{
			"malformed json",
			`{"demands":[`,
			"parse seed file",
		},
		{
			"empty deadline",
			`{"demands":[{"networkRef":"po-1","siteId":"s","requiredShipBy":"","lines":[]}]}`,
			"must not be empty",
		},
		{
			"deadline neither instant nor duration",
			`{"demands":[{"networkRef":"po-1","siteId":"s","requiredShipBy":"tomorrow","lines":[]}]}`,
			"RFC 3339",
		},
		{
			"unparseable duration",
			`{"demands":[{"networkRef":"po-1","siteId":"s","requiredShipBy":"+later","lines":[]}]}`,
			"not a duration",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := retailnetwork.NewStubGateway(nil)
			_, err := retailnetwork.LoadSeedFile(g, writeSeed(t, tc.body), seedNow())
			if err == nil {
				t.Fatalf("expected an error naming the problem, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSubstring) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.wantSubstring)
			}
			// Nothing partial may be seeded: a half-loaded file is worse
			// than none, because the missing orders look like a poller
			// problem.
			got, _ := g.PollDemand(t.Context(), time.Time{})
			if len(got) != 0 {
				t.Fatalf("a failed load seeded %d demands", len(got))
			}
		})
	}
}

func TestLoadSeedFile_MissingFileIsAnError(t *testing.T) {
	g := retailnetwork.NewStubGateway(nil)
	_, err := retailnetwork.LoadSeedFile(g, filepath.Join(t.TempDir(), "absent.json"), seedNow())
	if err == nil {
		t.Fatal("a missing seed file must be an error, not a silent empty load")
	}
	if !strings.Contains(err.Error(), "read seed file") {
		t.Fatalf("error = %q, want it to name the read failure", err)
	}
}

// Validation belongs to the domain, so invalid demand must reach the use
// case and be rejected there with the same error a real network would
// produce — not be filtered out by the loader.
func TestLoadSeedFile_DoesNotPreValidateDomainRules(t *testing.T) {
	path := writeSeed(t, `{
	  "demands": [
	    {"networkRef": "po-bad", "siteId": "site-1", "requiredShipBy": "+1h",
	     "lines": [{"networkLineRef": "1", "networkProductId": "ASIN-1", "quantity": 0}]}
	  ]
	}`)

	g := retailnetwork.NewStubGateway(nil)
	n, err := retailnetwork.LoadSeedFile(g, path, seedNow())
	if err != nil {
		t.Fatalf("the loader must not enforce domain rules, got %v", err)
	}
	if n != 1 {
		t.Fatalf("loaded %d, want the invalid demand passed through", n)
	}
	got, _ := g.PollDemand(t.Context(), time.Time{})
	if got[0].Lines[0].Quantity != 0 {
		t.Fatalf("quantity = %d, want 0 preserved for the domain to reject", got[0].Lines[0].Quantity)
	}
}
