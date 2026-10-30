// Command a2a is the composition root for the network-fulfillment A2A agent:
// it wires env config to the read repositories, repositories to Deps, and Deps
// to the inbound A2A adapter, then serves the Agent Card and the A2A JSON-RPC
// endpoint over HTTP. It is a deployable beside cmd/netfulfil (the HTTP
// service) and cmd/mcp, cloned from cmd/mcp's composition (ADR 0030).
//
// The agent is READ-ONLY, like the MCP server (ADR 0002): it wires only the
// same ports.NetworkOrderRepo and ports.CapabilityOfferRepo the MCP adapter
// reads, with no use case, event publisher, outbox or relay - there is no Kafka
// in this process. Shipment confirmation stays REST only (ADR 0014).
//
// There is no authentication layer in front of this agent: A2A is
// unauthenticated by the same fleet-wide decision as REST and MCP. It is
// reachable only from inside the cluster (or through the Kong route).
//
// Configuration: DATABASE_URL (required), A2A_AGENT_URL (required, the absolute
// http(s) URL the Agent Card advertises), A2A_ADDR (default :8093).

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inbounda2a "github.com/claudioed/network-fulfillment/internal/adapters/inbound/a2a"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

func main() {
	if err := run(); err != nil {
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Fail fast on a bad configuration: before telemetry, any connection and
	// before the listener binds.
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	// OTLP trace + metric export and Go runtime metrics (ADR-0013 Tier 1
	// item 1). Never blocks on the Collector. MUST run before newRouter:
	// otelchi binds the global MeterProvider when the middleware is built.
	serviceName := getenv("OTEL_SERVICE_NAME", "network-fulfillment-a2a")
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownTelemetry, err := telemetry.Setup(otelCtx, serviceName,
		getenv("SERVICE_VERSION", "dev"), getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	otelCancel()
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry flush failed on shutdown", "error", err)
		}
	}()

	// See cmd/netfulfil/main.go's wireOrders doc comment for the full
	// "why" (session-scoped pg_advisory_lock vs PgBouncer
	// transaction-pooling incompatibility; docs/adr/0007-migrations-
	// direct-postgres-connection.md, mirroring order-management's
	// ADR-0029). This binary also runs migrations on start
	// (buildOrders below), so it needs the same direct-connection
	// split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", cfg.databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "/app/migrations")

	orders, pool, closeOrders, err := buildOrders(context.Background(), cfg.databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeOrders()

	offers := buildOffers(pool)

	// The A2A adapter reads the SAME repositories the HTTP and MCP adapters
	// read: no write use case, publisher or outbox is wired, and no write
	// skill exists - delegation must never widen what this context exposes
	// (the MCP surface is read-only, so is this one).
	deps := inbounda2a.Deps{
		Orders: orders,
		Offers: offers,
		Clock:  system.Clock{},
	}
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: cfg.agentURL, Version: getenv("SERVICE_VERSION", "dev")})
	handler := newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, serviceName)

	srv := &http.Server{Addr: cfg.addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("a2a agent listening (JSON-RPC)", "addr", cfg.addr, "agent_url", cfg.agentURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("a2a agent failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// errMissingDatabaseURL is the boot error when DATABASE_URL is empty: this
// process reads the same Postgres cmd/netfulfil writes, and has no other
// backend.
var errMissingDatabaseURL = errors.New("DATABASE_URL is required: the A2A agent reads the real Postgres that cmd/netfulfil writes")

// errMissingAgentURL is the boot error when A2A_AGENT_URL is empty: the Agent
// Card must advertise a URL agent clients can dial, and there is no sensible
// default outside a cluster.
var errMissingAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute http(s) URL agent clients dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/)")

// bootConfig is everything run() needs from the environment, validated before
// anything starts.
type bootConfig struct {
	addr        string
	databaseURL string
	agentURL    string
}

// loadBootConfig reads and validates the environment. Every refusal names its
// variable; none waits on a dependency.
func loadBootConfig() (bootConfig, error) {
	databaseURL, err := requireDatabaseURL(os.Getenv("DATABASE_URL"))
	if err != nil {
		return bootConfig{}, err
	}
	agentURL, err := requireAgentURL(os.Getenv("A2A_AGENT_URL"))
	if err != nil {
		return bootConfig{}, err
	}
	return bootConfig{addr: getenv("A2A_ADDR", ":8093"), databaseURL: databaseURL, agentURL: agentURL}, nil
}

// requireAgentURL returns the trimmed A2A_AGENT_URL, which must be an absolute
// http(s) URL with a host.
func requireAgentURL(raw string) (string, error) {
	agentURL := strings.TrimSpace(raw)
	if agentURL == "" {
		return "", errMissingAgentURL
	}
	u, err := url.Parse(agentURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL", agentURL)
	}
	return agentURL, nil
}

// requireDatabaseURL returns the trimmed DATABASE_URL or errMissingDatabaseURL.
func requireDatabaseURL(raw string) (string, error) {
	url := strings.TrimSpace(raw)
	if url == "" {
		return "", errMissingDatabaseURL
	}
	return url, nil
}

// buildOrders opens the Postgres pool (running migrations first) and returns
// the NetworkOrderRepo over it. Kept independent of cmd/netfulfil so the A2A
// process can be deployed and scaled separately. buildOffers reuses the
// returned pool instead of opening a second one.
func buildOrders(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (ports.NetworkOrderRepo, *pgxpool.Pool, func(), error) {
	if err := postgres.RunMigrations(migrationsDatabaseURL, migrationsPath); err != nil {
		return nil, nil, nil, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, nil, nil, err
	}
	logger.Info("order repository wired", "backend", "postgres")
	return postgres.NewNetworkOrderRepo(pool), pool, pool.Close, nil
}

// buildOffers wires ports.CapabilityOfferRepo for the advertised_availability
// skill over the SAME already-migrated pool buildOrders opened, so the A2A
// process and cmd/netfulfil share the offers cmd/netfulfil's recompute pass
// writes.
func buildOffers(pool *pgxpool.Pool) ports.CapabilityOfferRepo {
	return postgres.NewCapabilityOfferRepo(pool)
}

// newLogger builds the process logger: JSON to stdout at the level LOG_LEVEL
// names (debug, info, warn or warning, error; case-insensitive). Unset or
// any other value is info. Same vocabulary as every other binary in this
// repository (gap fix D22).
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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
