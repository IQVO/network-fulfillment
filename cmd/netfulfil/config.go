package main

// Environment-variable configuration for the composition root. Every
// knob read at startup lives here so the env surface can be audited in
// one file; the wiring files call these accessors rather than reading
// the environment inline.

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// bootConfig is the mandatory configuration of the OLTP service: the two
// real dependencies it cannot run without. loadBootConfig refuses to
// return one that is incomplete, so the process exits before it binds the
// listener or dials anything (the real-wiring ADR).
type bootConfig struct {
	databaseURL  string
	kafkaBrokers []string
	network      networkConfig
}

// networkConfig is the retail-network side of the boot configuration (ADR 0009
// §4, ADR 0026 Decision 10). Stub is the default; everything else is read only
// in live mode.
type networkConfig struct {
	mode     retailnetwork.Mode
	baseURL  string // NETWORK_BASE_URL, required in live mode
	vendorID string // NETWORK_VENDOR_ID, required in live mode
	// nodesFile is NETWORK_NODES_FILE, optional: without it live mode relies
	// on every site already being a node on the network (adoption).
	nodesFile string
}

// loadNetworkConfig reads NETWORK_MODE and, in live mode, the variables the
// Vendor API adapter cannot run without. A missing one stops the boot with one
// error naming every variable that is absent, before anything is dialled.
func loadNetworkConfig(get func(string) string) (networkConfig, error) {
	cfg := networkConfig{mode: retailnetwork.ParseMode(get("NETWORK_MODE"))}
	if cfg.mode != retailnetwork.ModeLive {
		// Not read in stub mode: a stale NETWORK_BASE_URL on a stub deployment
		// is harmless, and the stub never dials it.
		return cfg, nil
	}
	cfg.baseURL = strings.TrimSpace(get("NETWORK_BASE_URL"))
	cfg.vendorID = strings.TrimSpace(get("NETWORK_VENDOR_ID"))
	cfg.nodesFile = strings.TrimSpace(get("NETWORK_NODES_FILE"))
	var missing []string
	if cfg.baseURL == "" {
		missing = append(missing, "NETWORK_BASE_URL")
	}
	if cfg.vendorID == "" {
		missing = append(missing, "NETWORK_VENDOR_ID")
	}
	if len(missing) > 0 {
		return networkConfig{}, fmt.Errorf("NETWORK_MODE=live requires %s to be set", strings.Join(missing, " and "))
	}
	return cfg, nil
}

// loadBootConfig reads and validates the mandatory configuration through
// get (os.Getenv in production, a map lookup in the tests). Every problem
// is an error that names the variable.
func loadBootConfig(get func(string) string) (bootConfig, error) {
	var cfg bootConfig
	cfg.databaseURL = strings.TrimSpace(get("DATABASE_URL"))
	if cfg.databaseURL == "" {
		return bootConfig{}, errors.New("DATABASE_URL is required: this context runs on a real Postgres")
	}
	cfg.kafkaBrokers = splitBrokers(get("KAFKA_BROKERS"))
	if len(cfg.kafkaBrokers) == 0 {
		return bootConfig{}, errors.New("KAFKA_BROKERS is required: this context runs on a real Kafka (comma-separated host:port list)")
	}
	if err := checkEventPublisher(get("EVENT_PUBLISHER")); err != nil {
		return bootConfig{}, err
	}
	if err := checkCapabilityOffer(get("CAPABILITY_OFFER_ENABLED")); err != nil {
		return bootConfig{}, err
	}
	// After DATABASE_URL on purpose: live mode keeps its submission ledger and
	// identifier maps in Postgres (ADR 0025, ADR 0026 Decision 4), so a live
	// boot without a database is refused by the check above, by name.
	network, err := loadNetworkConfig(get)
	if err != nil {
		return bootConfig{}, err
	}
	cfg.network = network
	return cfg, nil
}

// splitBrokers splits a comma-separated broker list, dropping blanks.
func splitBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// checkEventPublisher keeps EVENT_PUBLISHER accepted (warehouse-infra sets
// it) but only with the value kafka: events always leave through the
// transactional outbox and its relay. The old default (unset = log) and
// log itself are refused so a stale deployment fails loudly instead of
// silently dropping every event.
func checkEventPublisher(raw string) error {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" || v == "kafka" {
		return nil
	}
	return fmt.Errorf("EVENT_PUBLISHER=%s is not supported: this context publishes through the transactional outbox to real Kafka (unset it or use kafka)", raw)
}

// checkCapabilityOffer keeps CAPABILITY_OFFER_ENABLED accepted
// (warehouse-infra sets it to true) but the capability-offer pipeline and
// its two Kafka cache consumers are always on now: anything that turns it
// off is a boot error, never a silently disabled consumer.
func checkCapabilityOffer(raw string) error {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" || v == "true" {
		return nil
	}
	return fmt.Errorf("CAPABILITY_OFFER_ENABLED=%s is not supported: the capability-offer Kafka caches always run on this context (unset it or use true)", raw)
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

// defaultProfileInterval is how often the node profile is re-derived and, when
// its content changed, declared (ADR 0027 §6). The profile changes with the
// schedule and the cycle times, not with orders, so minutes are plenty.
const defaultProfileInterval = 5 * time.Minute

// profileInterval parses NETWORK_PROFILE_INTERVAL (ADR 0027 §6), a Go duration.
// Unset yields defaultProfileInterval; "0" (or "0s") is valid and disables the
// declaration worker, which is why this cannot reuse durationEnv (it treats a
// zero duration as malformed). A negative or malformed value logs a warning and
// yields the default, so a typo cannot silently switch the declaration off.
// The bool is false when the worker is disabled.
func profileInterval(logger *slog.Logger) (time.Duration, bool) {
	raw := strings.TrimSpace(os.Getenv("NETWORK_PROFILE_INTERVAL"))
	if raw == "" {
		return defaultProfileInterval, true
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("invalid NETWORK_PROFILE_INTERVAL, using the default", "value", raw, "default", defaultProfileInterval, "error", err)
		return defaultProfileInterval, true
	}
	return d, d > 0
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
