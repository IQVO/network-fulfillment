package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// networkEnvKeys is every variable the network wiring decisions read. Each
// case sets ALL of them (absent ones to empty) with t.Setenv, so nothing a
// previous case or the developer's shell exported can leak into a decision.
var networkEnvKeys = []string{
	"DATABASE_URL", "KAFKA_BROKERS", "EVENT_PUBLISHER", "CAPABILITY_OFFER_ENABLED",
	"NETWORK_MODE", "NETWORK_BASE_URL", "NETWORK_VENDOR_ID", "NETWORK_NODES_FILE", "NETWORK_SEED_FILE",
	"SITE_ID",
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
	gateway, err = wireGateway(quietLogger(), cfg.network, nodes, nil, nil)
	return cfg, nodes, gateway, err
}

func TestNetworkWiring_Decisions(t *testing.T) {
	base := map[string]string{"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers}
	with := func(extra map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	nodesFile := writeFile(t, "nodes.json", wiringNodeJSON)
	badNodesFile := writeFile(t, "bad-nodes.json", `{"nodes":[{"siteCode":"site-1"}]}`)

	cases := []struct {
		name      string
		vars      map[string]string
		wantErr   []string // every substring the boot error must contain
		notNamed  []string // variables the boot error must NOT mention (they are set)
		wantMode  retailnetwork.Mode
		wantLive  bool
		wantNodes int
	}{
		{name: "unset mode is the stub", vars: with(nil), wantMode: retailnetwork.ModeStub},
		{name: "stub spelled out", vars: with(map[string]string{"NETWORK_MODE": "stub"}), wantMode: retailnetwork.ModeStub},
		{name: "a retired or mistyped mode fails closed to the stub", vars: with(map[string]string{"NETWORK_MODE": "sandbox"}), wantMode: retailnetwork.ModeStub},
		{
			name:     "the stub ignores live-only variables",
			vars:     with(map[string]string{"NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1", "NETWORK_NODES_FILE": "/does/not/exist.json"}),
			wantMode: retailnetwork.ModeStub,
		},
		{
			name:     "live without a base URL names NETWORK_BASE_URL only",
			vars:     with(map[string]string{"NETWORK_MODE": "live", "NETWORK_VENDOR_ID": "V-1"}),
			wantErr:  []string{"NETWORK_MODE=live", "NETWORK_BASE_URL"},
			notNamed: []string{"NETWORK_VENDOR_ID"},
		},
		{
			name:     "live without a vendor id names NETWORK_VENDOR_ID only",
			vars:     with(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test"}),
			wantErr:  []string{"NETWORK_MODE=live", "NETWORK_VENDOR_ID"},
			notNamed: []string{"NETWORK_BASE_URL"},
		},
		{
			name:    "live with neither names both",
			vars:    with(map[string]string{"NETWORK_MODE": "LIVE"}),
			wantErr: []string{"NETWORK_BASE_URL", "NETWORK_VENDOR_ID"},
		},
		{
			name:    "blank values count as missing",
			vars:    with(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "  ", "NETWORK_VENDOR_ID": "\t"}),
			wantErr: []string{"NETWORK_BASE_URL", "NETWORK_VENDOR_ID"},
		},
		{
			name: "live never boots without DATABASE_URL (ADR 0025)",
			vars: map[string]string{
				"KAFKA_BROKERS": testBrokers, "NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1",
			},
			wantErr: []string{"DATABASE_URL"},
		},
		{
			name:     "live adopting its nodes needs no node file",
			vars:     with(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1"}),
			wantMode: retailnetwork.ModeLive, wantLive: true,
		},
		{
			name: "live registering nodes from the node file",
			vars: with(map[string]string{
				"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1", "NETWORK_NODES_FILE": nodesFile,
			}),
			wantMode: retailnetwork.ModeLive, wantLive: true, wantNodes: 1,
		},
		{
			name: "a node file that is not there stops the live boot",
			vars: with(map[string]string{
				"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1", "NETWORK_NODES_FILE": filepath.Join(t.TempDir(), "absent.json"),
			}),
			wantErr: []string{"read nodes file"},
		},
		{
			name: "a node file with a missing field stops the live boot",
			vars: with(map[string]string{
				"NETWORK_MODE": "live", "NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1", "NETWORK_NODES_FILE": badNodesFile,
			}),
			wantErr: []string{"nodeType is required"},
		},
		{
			name:    "a base URL that is not absolute stops the live boot",
			vars:    with(map[string]string{"NETWORK_MODE": "live", "NETWORK_BASE_URL": "network.test", "NETWORK_VENDOR_ID": "V-1"}),
			wantErr: []string{"NETWORK_BASE_URL"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setNetworkEnv(t, tc.vars)
			cfg, nodes, gateway, err := bootNetwork(t)
			if len(tc.wantErr) > 0 {
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
				return
			}
			if err != nil {
				t.Fatalf("unexpected boot error: %v", err)
			}
			if cfg.network.mode != tc.wantMode {
				t.Fatalf("mode = %q, want %q", cfg.network.mode, tc.wantMode)
			}
			if len(nodes) != tc.wantNodes {
				t.Fatalf("node definitions = %d, want %d", len(nodes), tc.wantNodes)
			}
			_, isLive := gateway.(*retailnetwork.LiveGateway)
			_, isStub := gateway.(*retailnetwork.StubGateway)
			if isLive != tc.wantLive || isStub == tc.wantLive {
				t.Fatalf("gateway = %T, want live=%v", gateway, tc.wantLive)
			}
		})
	}
}

// TestNetworkWiring_SeedFile: NETWORK_SEED_FILE seeds the stub and is refused
// against the live gateway, where the seeded demand would never be delivered.
func TestNetworkWiring_SeedFile(t *testing.T) {
	seed := writeFile(t, "seed.json", `{"demands":[]}`)
	live := map[string]string{
		"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers, "NETWORK_MODE": "live",
		"NETWORK_BASE_URL": "http://network.test", "NETWORK_VENDOR_ID": "V-1", "NETWORK_SEED_FILE": seed,
	}
	t.Run("live refuses a seed file", func(t *testing.T) {
		setNetworkEnv(t, live)
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
		vars := map[string]string{"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers, "NETWORK_SEED_FILE": seed}
		setNetworkEnv(t, vars)
		_, _, gateway, err := bootNetwork(t)
		if err != nil {
			t.Fatalf("wiring: %v", err)
		}
		if err := seedStubDemand(gateway, quietLogger()); err != nil {
			t.Fatalf("seedStubDemand on the stub: %v", err)
		}
	})
}
