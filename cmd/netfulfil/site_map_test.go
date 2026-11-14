package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// NETWORK_SITE_MAP (ADR 0035): optional, comma-separated networkSite=warehouseSite
// pairs. Strict at boot: a map the planner could not use is a boot error that
// names the variable and the entry, never a silently ignored one.

func TestParseNetworkSiteMap(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    networkSiteMap // compared by content; nil and empty are the same "no map"
		wantErr string         // substring; empty means success
	}{
		{name: "unset is no map", raw: "", want: networkSiteMap{}},
		{name: "blank is no map", raw: " \t ", want: networkSiteMap{}},
		{name: "one pair", raw: "site-1=AUS3", want: networkSiteMap{"site-1": "AUS3"}},
		{name: "two pairs", raw: "site-1=AUS3,node-b=DAL1", want: networkSiteMap{"site-1": "AUS3", "node-b": "DAL1"}},
		{name: "whitespace around everything is trimmed", raw: "  site-1 = AUS3 ,\tnode-b\t=\tDAL1  ",
			want: networkSiteMap{"site-1": "AUS3", "node-b": "DAL1"}},
		{name: "two network sites may share one building", raw: "site-1=AUS3,site-2=AUS3",
			want: networkSiteMap{"site-1": "AUS3", "site-2": "AUS3"}},
		{name: "a digits-only warehouse site is a site code", raw: "site-1=7", want: networkSiteMap{"site-1": "7"}},
		{name: "a network site that is already a site code may be remapped", raw: "WH1=AUS3", want: networkSiteMap{"WH1": "AUS3"}},

		{name: "duplicate key", raw: "site-1=AUS3,site-1=DAL1", wantErr: `duplicate network site "site-1"`},
		{name: "duplicate key with the same value", raw: "site-1=AUS3,site-1=AUS3", wantErr: `duplicate network site "site-1"`},
		{name: "duplicate key after trimming", raw: "site-1=AUS3, site-1 =DAL1", wantErr: `duplicate network site "site-1"`},

		{name: "lowercase warehouse site", raw: "site-1=aus3", wantErr: `warehouse site "aus3"`},
		{name: "dash in the warehouse site", raw: "site-1=AUS-3", wantErr: `warehouse site "AUS-3"`},
		{name: "inner space in the warehouse site", raw: "site-1=AUS 3", wantErr: `warehouse site "AUS 3"`},
		{name: "a second = lands in the warehouse site", raw: "site-1=AUS3=X", wantErr: `warehouse site "AUS3=X"`},
		{name: "non-ASCII warehouse site", raw: "site-1=ÅUS3", wantErr: `warehouse site "ÅUS3"`},

		{name: "empty warehouse site", raw: "site-1=", wantErr: "empty warehouse site"},
		{name: "blank warehouse site", raw: "site-1=  ", wantErr: "empty warehouse site"},
		{name: "empty network site", raw: "=AUS3", wantErr: "empty network site"},
		{name: "blank network site", raw: " =AUS3", wantErr: "empty network site"},
		{name: "only a separator", raw: "=", wantErr: "empty network site"},
		{name: "no separator", raw: "site-1", wantErr: `"site-1"`},
		{name: "no separator in the second entry", raw: "site-1=AUS3,site-2", wantErr: `"site-2"`},
		{name: "trailing comma", raw: "site-1=AUS3,", wantErr: "empty entry"},
		{name: "leading comma", raw: ",site-1=AUS3", wantErr: "empty entry"},
		{name: "doubled comma", raw: "site-1=AUS3,,site-2=DAL1", wantErr: "empty entry"},
		{name: "only a comma", raw: ",", wantErr: "empty entry"},
		{name: "a blank entry between commas", raw: "site-1=AUS3, ,site-2=DAL1", wantErr: "empty entry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseNetworkSiteMap(tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseNetworkSiteMap(%q) = %v, want an error containing %q", tc.raw, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				if !strings.Contains(err.Error(), "NETWORK_SITE_MAP") {
					t.Fatalf("error %q does not name NETWORK_SITE_MAP", err)
				}
				if got != nil {
					t.Fatalf("a refused map must return nothing, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNetworkSiteMap(%q): %v", tc.raw, err)
			}
			if len(got) != len(tc.want) || (len(got) > 0 && !reflect.DeepEqual(got, tc.want)) {
				t.Fatalf("parseNetworkSiteMap(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// The error names the entry that is wrong, so with several pairs the operator
// does not have to bisect the value.
func TestParseNetworkSiteMap_NamesTheOffendingEntry(t *testing.T) {
	_, err := parseNetworkSiteMap("site-1=AUS3,site-2=bad,site-3=DAL1")
	if err == nil || !strings.Contains(err.Error(), `"site-2=bad"`) {
		t.Fatalf("error %v does not quote the offending entry %q", err, "site-2=bad")
	}
}

func TestLoadBootConfig_NetworkSiteMap(t *testing.T) {
	t.Run("unset leaves no map and changes nothing else", func(t *testing.T) {
		cfg, err := loadBootConfig(env(bootVars(nil)))
		if err != nil {
			t.Fatalf("loadBootConfig: %v", err)
		}
		if len(cfg.siteMap) != 0 {
			t.Fatalf("siteMap = %v, want none", cfg.siteMap)
		}
	})
	t.Run("a valid map is parsed into the boot config", func(t *testing.T) {
		cfg, err := loadBootConfig(env(bootVars(map[string]string{"NETWORK_SITE_MAP": " site-1=AUS3 "})))
		if err != nil {
			t.Fatalf("loadBootConfig: %v", err)
		}
		if want := (networkSiteMap{"site-1": "AUS3"}); !reflect.DeepEqual(cfg.siteMap, want) {
			t.Fatalf("siteMap = %v, want %v", cfg.siteMap, want)
		}
	})
	t.Run("an invalid map refuses the boot and names the variable", func(t *testing.T) {
		_, err := loadBootConfig(env(bootVars(map[string]string{"NETWORK_SITE_MAP": "site-1=aus3"})))
		if err == nil || !strings.Contains(err.Error(), "NETWORK_SITE_MAP") {
			t.Fatalf("err = %v, want a boot error naming NETWORK_SITE_MAP", err)
		}
	})
	t.Run("it is read in stub mode too: the planner is wired in every mode", func(t *testing.T) {
		_, err := loadBootConfig(env(bootVars(map[string]string{"NETWORK_MODE": "stub", "NETWORK_SITE_MAP": "=AUS3"})))
		if err == nil {
			t.Fatal("an invalid NETWORK_SITE_MAP must be refused in stub mode as well")
		}
	})
}

// The boot gate end to end: run() with an invalid map exits 1 at the
// configuration check, before telemetry, the database or any dial, and says
// why. A map that is wrong but ignored would route held orders nowhere and
// look healthy.
func TestRun_RefusesToBootOnAnInvalidSiteMap(t *testing.T) {
	t.Setenv("DATABASE_URL", testDSN)
	t.Setenv("KAFKA_BROKERS", testBrokers)
	t.Setenv("NETWORK_SITE_MAP", "site-1=aus3")

	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w // run builds its logger on os.Stdout
	start := time.Now()
	code := run(context.Background())
	os.Stdout = stdout
	_ = w.Close()
	logged, _ := io.ReadAll(r)

	if code != 1 {
		t.Fatalf("run() = %d, want 1 (boot refused)", code)
	}
	if !strings.Contains(string(logged), "invalid configuration") || !strings.Contains(string(logged), "NETWORK_SITE_MAP") {
		t.Fatalf("run() did not log the refusal naming NETWORK_SITE_MAP: %s", logged)
	}
	// The database retry alone costs 15 s: a fast return proves nothing was dialled.
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("run() took %v: it got past the configuration check", elapsed)
	}
}

// The map reaches the planner through the wiring, not a global: the planner
// wirePlanner builds sends the warehouse site, and an unmapped site as before.
func TestWirePlanner_AppliesTheSiteMap(t *testing.T) {
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"ord-1","promiseDate":"2026-09-24T08:00:00Z","lines":[]}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("ORDER_MANAGEMENT_URL", srv.URL)

	planner := wirePlanner(telemetry.NewCircuitBreakerMetrics(), networkSiteMap{"site-1": "AUS3"})
	for _, site := range []shared.SiteId{"site-1", "WH1", "site-9"} {
		if _, err := planner.RaiseHeldOrder(context.Background(), contract.HeldOrderRequest{
			NetworkRef: shared.NetworkRef("po-" + string(site)), SiteId: site,
			RequiredShipBy: time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC),
			Lines:          map[shared.SKU]int{"SKU-1": 1},
		}); err != nil {
			t.Fatalf("RaiseHeldOrder(%s): %v", site, err)
		}
	}
	got := make([]any, len(bodies))
	for i, b := range bodies {
		got[i] = b["fulfillmentSiteId"]
	}
	if want := []any{"AUS3", "WH1", nil}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fulfillmentSiteId sent for site-1, WH1, site-9 = %v, want %v", got, want)
	}
}
