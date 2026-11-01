package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

type pageBody struct {
	Items []struct {
		NetworkRef string `json:"networkRef"`
		SKU        string `json:"sku"`
		SiteID     string `json:"siteId"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

// walk follows nextCursor from the first page and returns every item key.
func walk(t *testing.T, do func(path string) (int, []byte), base string, limit int, key func(i int, b pageBody) string) []string {
	t.Helper()
	var seen []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatalf("more than 100 pages: the cursor does not advance")
		}
		path := fmt.Sprintf("%s?limit=%d", base, limit)
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		code, raw := do(path)
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d %s", path, code, raw)
		}
		var b pageBody
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(b.Items) > limit {
			t.Fatalf("page of %d items, limit %d", len(b.Items), limit)
		}
		for i := range b.Items {
			seen = append(seen, key(i, b))
		}
		if b.NextCursor == "" {
			return seen
		}
		cursor = b.NextCursor
	}
}

func assertExactlyOnce(t *testing.T, seen []string, want int) {
	t.Helper()
	counts := map[string]int{}
	for _, k := range seen {
		counts[k]++
		if counts[k] > 1 {
			t.Fatalf("%s returned twice: %v", k, seen)
		}
	}
	if len(counts) != want {
		t.Fatalf("walked %d distinct rows, want %d (skipped rows): %v", len(counts), want, seen)
	}
}

func TestListUnanswered_WalksEveryPageWithTies(t *testing.T) {
	e := newTestEnv(t)
	const n = 11
	for i := 0; i < n; i++ {
		// Three orders share each acknowledgeBy: ties on the sort key.
		e.seed(t, fmt.Sprintf("po-%02d", i), time.Duration(i/3)*time.Hour)
	}
	do := func(path string) (int, []byte) {
		rec := e.do(t, http.MethodGet, path)
		return rec.Code, rec.Body.Bytes()
	}
	seen := walk(t, do, "/network-orders", 2, func(i int, b pageBody) string { return b.Items[i].NetworkRef })
	assertExactlyOnce(t, seen, n)
}

func TestListCapabilityOffers_WalksEveryPageWithTies(t *testing.T) {
	offers := memory.NewCapabilityOfferRepo()
	const n = 9
	for i := 0; i < n; i++ {
		// Three sites per SKU: ties on the sort key.
		o, err := capabilityoffer.New(shared.SKU(fmt.Sprintf("sku-%d", i/3)), shared.SiteId(fmt.Sprintf("site-%d", i%3)), 1, 1, capabilityoffer.BasisPhysical, now())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := offers.Save(context.Background(), o); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	srv := (&inboundhttp.Server{Orders: memory.NewNetworkOrderRepo(), Poller: fakeStats{}, Clock: fixedClock{t: now()}, Offers: offers}).Routes()
	e := &testEnv{handler: srv}
	do := func(path string) (int, []byte) {
		rec := e.do(t, http.MethodGet, path)
		return rec.Code, rec.Body.Bytes()
	}
	seen := walk(t, do, "/capability-offers", 2, func(i int, b pageBody) string { return b.Items[i].SKU + "/" + b.Items[i].SiteID })
	assertExactlyOnce(t, seen, n)
}

func TestCollections_RejectBadPagingParameters(t *testing.T) {
	offers := memory.NewCapabilityOfferRepo()
	srv := (&inboundhttp.Server{Orders: memory.NewNetworkOrderRepo(), Poller: fakeStats{}, Clock: fixedClock{t: now()}, Offers: offers}).Routes()
	e := &testEnv{handler: srv}
	cases := map[string]string{
		"?limit=0":            "invalid-limit",
		"?limit=501":          "invalid-limit",
		"?limit=abc":          "invalid-limit",
		"?cursor=!!!":         "invalid-cursor",
		"?cursor=bm90LWpzb24": "invalid-cursor",
		"?after=x":            "unknown-query-parameter",
		"?offset=10":          "unknown-query-parameter",
		"?nextToken=x":        "unknown-query-parameter",
		"?from=x":             "unknown-query-parameter",
	}
	for _, base := range []string{"/network-orders", "/capability-offers"} {
		for q, want := range cases {
			rec := e.do(t, http.MethodGet, base+q)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "/"+want) {
				t.Errorf("GET %s%s = %d %s, want 400 %s", base, q, rec.Code, rec.Body.String(), want)
			}
		}
	}
	// A cursor that decodes but whose sort key is not a timestamp is invalid
	// for the network-orders keyset.
	rec := e.do(t, http.MethodGet, "/network-orders?cursor=eyJzIjoibm90LWEtdGltZSIsImkiOiJwby0xIn0")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "/invalid-cursor") {
		t.Errorf("non-time cursor = %d %s, want 400 invalid-cursor", rec.Code, rec.Body.String())
	}
}
