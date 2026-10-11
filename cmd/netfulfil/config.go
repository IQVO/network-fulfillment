package main

// Environment-variable configuration for the composition root. Every
// knob read at startup lives here so the env surface can be audited in
// one file; the wiring files call these accessors rather than reading
// the environment inline.

import (
	"os"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func kafkaBrokers() string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return v
	}
	return "localhost:9092"
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed value (logged only implicitly: the relay interval is a
// tuning knob, not a contract worth failing boot over).
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// capabilityOfferEnabled reports whether CAPABILITY_OFFER_ENABLED=true
// (default false). See wireCapabilityOffer's own doc comment for why
// this feature is opt-in rather than always-on.
func capabilityOfferEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("CAPABILITY_OFFER_ENABLED")), "true")
}

// siteId is the single site CapabilityOffer recompute targets (ADR 0001
// §8's inherited single-site simplification — see
// RecomputeCapabilityOffers's own doc comment on SiteId).
func siteId() shared.SiteId {
	if v := os.Getenv("SITE_ID"); v != "" {
		return shared.SiteId(v)
	}
	return shared.SiteId("site-1")
}

// recomputeInterval bounds how often RecomputeCapabilityOffers runs.
func recomputeInterval() time.Duration {
	return durationEnv("RECOMPUTE_INTERVAL", time.Minute)
}

func pollInterval() time.Duration {
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	// Well under the 24h acknowledgement window, so a restart or a brief
	// outage cannot eat a meaningful fraction of it.
	return time.Minute
}

func sweepInterval() time.Duration {
	if v := os.Getenv("SWEEP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return time.Minute
}

func addr() string {
	if p := os.Getenv("PORT"); p != "" {
		return ":" + p
	}
	return ":8080"
}

// migrationsPath is where the migrations live in the container image (see
// Dockerfile), overridable for a local run from the repo root.
func migrationsPath() string {
	if p := os.Getenv("MIGRATIONS_PATH"); p != "" {
		return p
	}
	return "/app/migrations"
}

// getenv returns the environment variable key, or fallback when it is unset
// or empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
