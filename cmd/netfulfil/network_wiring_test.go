package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/producttranslation"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// networkEnvKeys is every variable the network wiring decisions read. Each
// case sets ALL of them (absent ones to empty) with t.Setenv, so nothing a
// previous case or the developer's shell exported can leak into a decision.
var networkEnvKeys = []string{
	"DATABASE_URL", "KAFKA_BROKERS", "EVENT_PUBLISHER", "CAPABILITY_OFFER_ENABLED",
	"NETWORK_MODE", "NETWORK_BASE_URL", "NETWORK_VENDOR_ID", "NETWORK_NODES_FILE", "NETWORK_SEED_FILE",
	"SITE_ID", "INBOUND_RECEIVING_URL", "PRODUCT_MASTER_URL", "INBOUND_RECEIPT_CONSUMER_GROUP",
}

func setNetworkEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	for _, k := range networkEnvKeys {
		t.Setenv(k, vars[k])
	}
}

func writeFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const wiringNodeJSON = `{"nodes":[{"siteCode":"site-1","nodeType":"FULFILLMENT_CENTER","regionId":"SP",` +
	`"timezone":"America/Sao_Paulo","postalCode":"01000-000","countryCode":"BR"}]}`

// bootVars returns the mandatory configuration plus the given variables.
func bootVars(extra map[string]string) map[string]string {
	vars := map[string]string{"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers}
	for k, v := range extra {
		vars[k] = v
	}
	return vars
}

// liveVars returns a complete, valid live-mode environment plus the given variables.
func liveVars(extra map[string]string) map[string]string {
	vars := bootVars(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1",
		"INBOUND_RECEIVING_URL": "http://inbound-receiving.test", "PRODUCT_MASTER_URL": "http://product-master.test", "INBOUND_RECEIPT_CONSUMER_GROUP": "nf-test"})
	for k, v := range extra {
		vars[k] = v
	}
	return vars
}

// bootNetwork runs the same three steps bootInfra does for the network, on the
// real process environment, and returns what they decided. The pool is nil:
// wiring only hands it to the store, which touches it on the first call.
func bootNetwork(t *testing.T) (cfg bootConfig, nodes []retailnetwork.NodeDefinition, gateway ports.NetworkGateway, err error) {
	t.Helper()
	cfg, err = loadBootConfig(os.Getenv)
	if err != nil {
		return cfg, nil, nil, err
	}
	nodes, err = loadNetworkNodes(quietLogger(), cfg.network)
	if err != nil {
		return cfg, nil, nil, err
	}
	gateway, err = wireGateway(quietLogger(), cfg.network, nodes, producttranslation.New(), nil, nil)
	return cfg, nodes, gateway, err
}

// TestNetworkWiring_StubIsTheDefault: anything but "live" is the stub, and the
// stub never reads the live-only variables.
func TestNetworkWiring_StubIsTheDefault(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
	}{
		{"unset mode", bootVars(nil)},
		{"stub spelled out", bootVars(map[string]string{"NETWORK_MODE": "stub"})},
		{"a retired or mistyped mode fails closed", bootVars(map[string]string{"NETWORK_MODE": "sandbox"})},
		{"live-only variables are ignored", bootVars(map[string]string{
			"NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1", "NETWORK_NODES_FILE": "/does/not/exist.json",
		})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setNetworkEnv(t, tc.vars)
			cfg, nodes, gateway, err := bootNetwork(t)
			if err != nil {
				t.Fatalf("unexpected boot error: %v", err)
			}
			if _, ok := gateway.(*retailnetwork.StubGateway); !ok || cfg.network.mode != retailnetwork.ModeStub || len(nodes) != 0 {
				t.Fatalf("mode %q, gateway %T, %d node definitions; want the stub with none", cfg.network.mode, gateway, len(nodes))
			}
		})
	}
}

// TestNetworkWiring_LiveWires: a complete live configuration builds the live
// gateway, with or without node definitions.
func TestNetworkWiring_LiveWires(t *testing.T) {
	cases := []struct {
		name      string
		vars      map[string]string
		wantNodes int
	}{
		{"adopting its nodes needs no node file", liveVars(nil), 0},
		{"registering nodes from the node file", liveVars(map[string]string{"NETWORK_NODES_FILE": writeFile(t, "nodes.json", wiringNodeJSON)}), 1},
		{"the mode is case-insensitive", liveVars(map[string]string{"NETWORK_MODE": "LIVE"}), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setNetworkEnv(t, tc.vars)
			cfg, nodes, gateway, err := bootNetwork(t)
			if err != nil {
				t.Fatalf("unexpected boot error: %v", err)
			}
			if _, ok := gateway.(*retailnetwork.LiveGateway); !ok || cfg.network.mode != retailnetwork.ModeLive || len(nodes) != tc.wantNodes {
				t.Fatalf("mode %q, gateway %T, %d node definitions; want the live gateway with %d", cfg.network.mode, gateway, len(nodes), tc.wantNodes)
			}
		})
	}
}

// TestNetworkWiring_LiveRefusals: every way a live configuration is wrong stops
// the boot with an error that names the cause.
func TestNetworkWiring_LiveRefusals(t *testing.T) {
	cases := []struct {
		name     string
		vars     map[string]string
		wantErr  []string // every substring the boot error must contain
		notNamed []string // variables the error must NOT mention: they are set
	}{
		{
			name:     "no base URL names NETWORK_BASE_URL only",
			vars:     bootVars(map[string]string{"NETWORK_MODE": "live", "NETWORK_VENDOR_ID": "V-1"}),
			wantErr:  []string{"NETWORK_MODE=live", "NETWORK_BASE_URL"},
			notNamed: []string{"NETWORK_VENDOR_ID"},
		},
		{
			name:     "no vendor id names NETWORK_VENDOR_ID only",
			vars:     bootVars(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test"}),
			wantErr:  []string{"NETWORK_MODE=live", "NETWORK_VENDOR_ID"},
			notNamed: []string{"NETWORK_BASE_URL"},
		},
		{name: "neither names both", vars: bootVars(map[string]string{"NETWORK_MODE": "live"}), wantErr: []string{"NETWORK_BASE_URL", "NETWORK_VENDOR_ID"}},
		{
			name:    "blank values count as missing",
			vars:    bootVars(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "  ", "NETWORK_VENDOR_ID": "\t"}),
			wantErr: []string{"NETWORK_BASE_URL", "NETWORK_VENDOR_ID"},
		},
		{
			name: "never boots without DATABASE_URL (ADR 0025)",
			vars: map[string]string{
				"KAFKA_BROKERS": testBrokers, "NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1",
			},
			wantErr: []string{"DATABASE_URL"},
		},
		{
			name:    "a node file that is not there",
			vars:    liveVars(map[string]string{"NETWORK_NODES_FILE": filepath.Join(t.TempDir(), "absent.json")}),
			wantErr: []string{"read nodes file"},
		},
		{
			name:    "a node file with a missing field",
			vars:    liveVars(map[string]string{"NETWORK_NODES_FILE": writeFile(t, "bad-nodes.json", `{"nodes":[{"siteCode":"site-1"}]}`)}),
			wantErr: []string{"nodeType is required"},
		},
		{
			name:    "a base URL that is not absolute",
			vars:    liveVars(map[string]string{"NETWORK_BASE_URL": "network.test"}),
			wantErr: []string{"NETWORK_BASE_URL"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setNetworkEnv(t, tc.vars)
			_, _, _, err := bootNetwork(t)
			if err == nil {
				t.Fatalf("boot succeeded, want an error containing %q", tc.wantErr)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
			for _, other := range tc.notNamed {
				if strings.Contains(err.Error(), other) {
					t.Fatalf("error %q names %s, which is set", err, other)
				}
			}
		})
	}
}

// TestNetworkWiring_SeedFile: NETWORK_SEED_FILE seeds the stub and is refused
// against the live gateway, where the seeded demand would never be delivered.
func TestNetworkWiring_SeedFile(t *testing.T) {
	seed := writeFile(t, "seed.json", `{"demands":[]}`)
	t.Run("live refuses a seed file", func(t *testing.T) {
		setNetworkEnv(t, liveVars(map[string]string{"NETWORK_SEED_FILE": seed}))
		_, _, gateway, err := bootNetwork(t)
		if err != nil {
			t.Fatalf("wiring: %v", err)
		}
		err = seedStubDemand(gateway, quietLogger())
		if err == nil || !strings.Contains(err.Error(), "NETWORK_SEED_FILE") {
			t.Fatalf("seedStubDemand on the live gateway = %v, want an error naming NETWORK_SEED_FILE", err)
		}
	})
	t.Run("the stub takes it", func(t *testing.T) {
		setNetworkEnv(t, bootVars(map[string]string{"NETWORK_SEED_FILE": seed}))
		_, _, gateway, err := bootNetwork(t)
		if err != nil {
			t.Fatalf("wiring: %v", err)
		}
		if err := seedStubDemand(gateway, quietLogger()); err != nil {
			t.Fatalf("seedStubDemand on the stub: %v", err)
		}
	})
}
