package main

import (
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestLoadInboundConfig(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	t.Run("stub mode has no inbound loop and reads nothing", func(t *testing.T) {
		cfg, err := loadInboundConfig(get(map[string]string{"INBOUND_RECEIVING_URL": "http://x"}), retailnetwork.ModeStub)
		if err != nil || cfg.enabled || cfg.inboundReceivingURL != "" {
			t.Fatalf("cfg=%+v err=%v", cfg, err)
		}
	})
	t.Run("live reads and trims both URLs", func(t *testing.T) {
		cfg, err := loadInboundConfig(get(map[string]string{"INBOUND_RECEIVING_URL": " http://r ", "PRODUCT_MASTER_URL": "http://p\t", "INBOUND_RECEIPT_CONSUMER_GROUP": " grp "}), retailnetwork.ModeLive)
		if err != nil || !cfg.enabled || cfg.inboundReceivingURL != "http://r" || cfg.productMasterURL != "http://p" || cfg.receiptConsumerGroup != "grp" {
			t.Fatalf("cfg=%+v err=%v", cfg, err)
		}
	})
	t.Run("a missing URL is a boot error naming exactly the missing ones", func(t *testing.T) { checkMissingInboundURLs(t, get) })
}

func checkMissingInboundURLs(t *testing.T, get func(map[string]string) func(string) string) {
	t.Helper()
	cases := []struct {
		name       string
		vars       map[string]string
		want, miss []string
	}{
		{"none names all three", nil, []string{"INBOUND_RECEIVING_URL", "PRODUCT_MASTER_URL", "INBOUND_RECEIPT_CONSUMER_GROUP"}, nil},
		{"no consumer group names only it", map[string]string{"INBOUND_RECEIVING_URL": "http://r", "PRODUCT_MASTER_URL": "http://p"}, []string{"INBOUND_RECEIPT_CONSUMER_GROUP"}, []string{"INBOUND_RECEIVING_URL", "PRODUCT_MASTER_URL"}},
		{"no receiving URL names only it", map[string]string{"PRODUCT_MASTER_URL": "http://p", "INBOUND_RECEIPT_CONSUMER_GROUP": "g"}, []string{"INBOUND_RECEIVING_URL"}, []string{"PRODUCT_MASTER_URL"}},
		{"no product-master URL names only it", map[string]string{"INBOUND_RECEIVING_URL": "http://r", "INBOUND_RECEIPT_CONSUMER_GROUP": "g"}, []string{"PRODUCT_MASTER_URL"}, []string{"INBOUND_RECEIVING_URL"}},
		{"blank is missing", map[string]string{"INBOUND_RECEIVING_URL": "  ", "PRODUCT_MASTER_URL": "\t", "INBOUND_RECEIPT_CONSUMER_GROUP": "g"}, []string{"INBOUND_RECEIVING_URL", "PRODUCT_MASTER_URL"}, []string{"INBOUND_RECEIPT_CONSUMER_GROUP"}},
	}
	for _, tc := range cases {
		_, err := loadInboundConfig(get(tc.vars), retailnetwork.ModeLive)
		if err == nil {
			t.Fatalf("%s: want a boot error", tc.name)
		}
		for _, w := range tc.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: %q does not name %s", tc.name, err, w)
			}
		}
		for _, m := range tc.miss {
			if strings.Contains(err.Error(), m) {
				t.Errorf("%s: %q names %s, which is set", tc.name, err, m)
			}
		}
	}
}

func TestLoadBootConfig_LiveWithoutTheInboundURLsIsRefused(t *testing.T) {
	vars := map[string]string{"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers, "NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://n", "NETWORK_VENDOR_ID": "V"}
	_, err := loadBootConfig(func(k string) string { return vars[k] })
	if err == nil || !strings.Contains(err.Error(), "INBOUND_RECEIVING_URL") {
		t.Fatalf("err = %v, want the missing inbound URL named", err)
	}
	vars["INBOUND_RECEIVING_URL"], vars["PRODUCT_MASTER_URL"], vars["INBOUND_RECEIPT_CONSUMER_GROUP"] = "http://r", "http://p", "g"
	cfg, err := loadBootConfig(func(k string) string { return vars[k] })
	if err != nil || !cfg.inbound.enabled {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}

func TestNetworkSites_AreSiteIDPlusTheNodeDefinitionsWithoutDuplicates(t *testing.T) {
	t.Setenv("SITE_ID", "FC-1")
	got := networkSites([]retailnetwork.NodeDefinition{{SiteCode: "FC-2"}, {SiteCode: "FC-1"}, {SiteCode: "FC-2"}})
	want := []shared.SiteId{"FC-1", "FC-2"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("sites = %v, want %v", got, want)
	}
}

func TestInboundIntakeInterval(t *testing.T) {
	t.Setenv("INBOUND_INTAKE_INTERVAL", "")
	if got := inboundIntakeInterval(); got != 30*time.Second {
		t.Errorf("default = %v", got)
	}
	t.Setenv("INBOUND_INTAKE_INTERVAL", "5s")
	if got := inboundIntakeInterval(); got != 5*time.Second {
		t.Errorf("set = %v", got)
	}
}
