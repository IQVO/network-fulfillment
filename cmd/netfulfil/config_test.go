package main

import (
	"reflect"
	"strings"
	"testing"
)

// env builds the lookup function loadBootConfig takes, so each case states
// exactly the variables it sets and nothing leaks in from the real process.
func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

const (
	testDSN     = "postgres://u:p@db:5432/nf"
	testBrokers = "kafka-0:9092,kafka-1:9092"
)

// TestLoadBootConfig_Mandatory proves the two real dependencies are
// mandatory and that every refusal names the variable, so an operator reads
// the cause in the first log line (the real-wiring ADR).
func TestLoadBootConfig_Mandatory(t *testing.T) {
	cases := []struct {
		name    string
		vars    map[string]string
		wantErr string // substring; empty means success
	}{
		{"missing DATABASE_URL", map[string]string{"KAFKA_BROKERS": testBrokers}, "DATABASE_URL"},
		{"blank DATABASE_URL", map[string]string{"DATABASE_URL": "   ", "KAFKA_BROKERS": testBrokers}, "DATABASE_URL"},
		{"missing KAFKA_BROKERS", map[string]string{"DATABASE_URL": testDSN}, "KAFKA_BROKERS"},
		{"blank KAFKA_BROKERS", map[string]string{"DATABASE_URL": testDSN, "KAFKA_BROKERS": " , ,"}, "KAFKA_BROKERS"},
		{"both present", map[string]string{"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadBootConfig(env(tc.vars))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want an error naming %s, got none", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not name %s", err, tc.wantErr)
			}
		})
	}
}

// TestLoadBootConfig_Values proves the parsed configuration: the DSN is
// trimmed and the broker list is split, trimmed and stripped of blanks.
func TestLoadBootConfig_Values(t *testing.T) {
	cfg, err := loadBootConfig(env(map[string]string{
		"DATABASE_URL":  "  " + testDSN + "  ",
		"KAFKA_BROKERS": " kafka-0:9092 , ,kafka-1:9092 ",
	}))
	if err != nil {
		t.Fatalf("loadBootConfig: %v", err)
	}
	if cfg.databaseURL != testDSN {
		t.Fatalf("databaseURL = %q, want %q", cfg.databaseURL, testDSN)
	}
	if want := []string{"kafka-0:9092", "kafka-1:9092"}; !reflect.DeepEqual(cfg.kafkaBrokers, want) {
		t.Fatalf("kafkaBrokers = %v, want %v", cfg.kafkaBrokers, want)
	}
}

// TestLoadBootConfig_EventPublisher: EVENT_PUBLISHER stays accepted because
// warehouse-infra still sets it, but only as unset or kafka. The old default
// (unset meant a log sink) and log itself must fail the boot loudly.
func TestLoadBootConfig_EventPublisher(t *testing.T) {
	cases := []struct {
		value   string
		wantErr bool
	}{
		{"", false},
		{"kafka", false},
		{"KAFKA", false},
		{"log", true},
		{"direct", true},
		{"none", true},
	}
	for _, tc := range cases {
		t.Run("EVENT_PUBLISHER="+tc.value, func(t *testing.T) {
			_, err := loadBootConfig(env(map[string]string{
				"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers, "EVENT_PUBLISHER": tc.value,
			}))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("EVENT_PUBLISHER=%q must be refused", tc.value)
				}
				if !strings.Contains(err.Error(), "EVENT_PUBLISHER") {
					t.Fatalf("error %q does not name EVENT_PUBLISHER", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("EVENT_PUBLISHER=%q must be accepted: %v", tc.value, err)
			}
		})
	}
}

// TestLoadBootConfig_CapabilityOffer: the capability-offer pipeline is
// always on, so the only accepted values are unset and true; anything that
// would switch it off is a boot error, never a silently disabled consumer.
func TestLoadBootConfig_CapabilityOffer(t *testing.T) {
	cases := []struct {
		value   string
		wantErr bool
	}{
		{"", false},
		{"true", false},
		{"TRUE", false},
		{"false", true},
		{"0", true},
		{"off", true},
	}
	for _, tc := range cases {
		t.Run("CAPABILITY_OFFER_ENABLED="+tc.value, func(t *testing.T) {
			_, err := loadBootConfig(env(map[string]string{
				"DATABASE_URL": testDSN, "KAFKA_BROKERS": testBrokers, "CAPABILITY_OFFER_ENABLED": tc.value,
			}))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("CAPABILITY_OFFER_ENABLED=%q must be refused", tc.value)
				}
				if !strings.Contains(err.Error(), "CAPABILITY_OFFER_ENABLED") {
					t.Fatalf("error %q does not name CAPABILITY_OFFER_ENABLED", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CAPABILITY_OFFER_ENABLED=%q must be accepted: %v", tc.value, err)
			}
		})
	}
}
