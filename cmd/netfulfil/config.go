package main

// Environment-variable configuration for the composition root. Every
// knob read at startup lives here so the env surface can be audited in
// one file; the wiring files call these accessors rather than reading
// the environment inline.

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
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

// outboxMaxAttempts parses OUTBOX_MAX_ATTEMPTS (ADR 0021): how many failed
// sends exclude an outbox row from the relay's claims. Unset means
// postgres.DefaultOutboxMaxAttempts (20); 0 means unlimited (the pre-ADR
// behaviour, where one poison row blocks the relay forever).
func outboxMaxAttempts(logger *slog.Logger) int {
	return envInt(logger, "OUTBOX_MAX_ATTEMPTS", postgres.DefaultOutboxMaxAttempts)
}

// envInt parses a non-negative integer env var. Unset yields def; an invalid
// or negative value logs a warning and yields def, so a typo can never
// silently remove a limit. "0" is valid and its meaning is the caller's.
func envInt(logger *slog.Logger, key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		logger.Warn("invalid integer env var, using default", "key", key, "value", raw, "default", def, "error", err)
		return def
	}
	return n
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

// defaultOrphanHoldGrace is how long past acknowledgeBy the orphaned-hold
// sweep (ADR 0024) waits before it counts a hold as orphaned. It leaves room
// for RejectOverdueOrders, which runs on the same ticker, to do its own work
// first, so the sweep is a backstop and never a second actor racing it.
const defaultOrphanHoldGrace = time.Hour

// orphanHoldSweepEnabled reports ORPHAN_HOLD_SWEEP_ENABLED (ADR 0024 §5).
// Unlike CAPABILITY_OFFER_ENABLED this is ON by default: a leaked hold keeps
// real inventory reserved forever, which is the worse failure. Anything
// strconv.ParseBool accepts is honoured case-insensitively; an unparseable
// value keeps it ON and logs a warning, so a typo can never switch the safety
// net off silently.
func orphanHoldSweepEnabled(logger *slog.Logger) bool {
	raw := strings.TrimSpace(os.Getenv("ORPHAN_HOLD_SWEEP_ENABLED"))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		logger.Warn("invalid ORPHAN_HOLD_SWEEP_ENABLED, keeping the orphaned-hold sweep on", "value", raw, "error", err)
		return true
	}
	return enabled
}

// orphanHoldGrace parses ORPHAN_HOLD_GRACE (ADR 0024 §5), a Go duration added
// to acknowledgeBy. Unset yields defaultOrphanHoldGrace. "0" (or "0s") is valid
// and means no grace, which is why this cannot reuse durationEnv: that treats a
// zero duration as malformed. A negative or malformed value logs a warning and
// yields the default.
func orphanHoldGrace(logger *slog.Logger) time.Duration {
	raw := strings.TrimSpace(os.Getenv("ORPHAN_HOLD_GRACE"))
	if raw == "" {
		return defaultOrphanHoldGrace
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("invalid ORPHAN_HOLD_GRACE, using the default", "value", raw, "default", defaultOrphanHoldGrace, "error", err)
		return defaultOrphanHoldGrace
	}
	return d
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

// newLogger builds the process logger: JSON to stdout at the level LOG_LEVEL
// names (debug, info, warn or warning, error; case-insensitive). Unset or
// any other value is info. Same vocabulary as the projector and reports
// binaries (gap fix D22).
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
