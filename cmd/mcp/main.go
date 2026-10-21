// Command mcp is the composition root for the network-fulfillment MCP
// server: it wires env config to outbound adapters, adapters to Deps, and
// Deps to the inbound MCP adapter, then serves MCP over Streamable HTTP.
// It is a second, independent deployable alongside cmd/netfulfil (the
// HTTP service), following the exact composition pattern
// facility-layout's cmd/mcp/main.go uses.
//
// network-fulfillment's REST surface is read-only, so this server wires
// only reads: the same ports.NetworkOrderRepo the HTTP adapter reads, and
// (when REPORTS_BASE_URL is set) a client of the netfulfil-reports REST
// service for the curated acknowledgement-report tool.
//
// There is no authentication layer in front of this server: the fleet's
// static-bearer-key rollout was removed. It is reachable only from
// inside the cluster.
//
// network-fulfillment has no business-metric OTel package, but it does
// now call telemetry.Setup (ADR-0013 Tier 1 item 1) and wire otelchi
// HTTP RED metrics (Tier 1 item 2) on this router, closing the gap
// ADR-0019 explicitly left open for this binary.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	inboundmcp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// OTLP trace + metric export and Go runtime metrics (ADR-0013 Tier 1
	// item 1). Never blocks on the Collector. MUST run before newRouter:
	// otelchi binds the global MeterProvider when the middleware is built.
	serviceName := getenv("OTEL_SERVICE_NAME", "network-fulfillment-mcp")
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

	mcpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL, err := requireDatabaseURL(os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	// See cmd/netfulfil/main.go's wireOrders doc comment for the full
	// "why" (session-scoped pg_advisory_lock vs PgBouncer
	// transaction-pooling incompatibility; docs/adr/0007-migrations-
	// direct-postgres-connection.md, mirroring order-management's
	// ADR-0029). This binary also runs migrations on start
	// (buildOrders below), so it needs the same direct-connection
	// split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "/app/migrations")

	orders, pool, closeOrders, err := buildOrders(context.Background(), databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeOrders()

	offers := buildOffers(pool)

	// The MCP adapter reuses the SAME repository the HTTP adapter reads
	// (internal/adapters/inbound/http.Server.Orders): no write use case
	// is wired, and no write tool — this context's map of network
	// demand is consumed, not mutated, through either surface.
	//
	// When REPORTS_BASE_URL is set, the curated acknowledgement-report
	// tool is additionally wired to call the netfulfil-reports REST
	// service; when it is unset the tool is simply not registered.
	deps := inboundmcp.Deps{
		Orders: orders,
		Clock:  system.Clock{},
		Offers: offers,
	}
	if reportsURL := os.Getenv("REPORTS_BASE_URL"); reportsURL != "" {
		deps.Reports = inboundmcp.NewReportsRESTClient(reportsURL, nil)
		logger.Info("acknowledgement report tool wired", "reports_base_url", reportsURL)
	}
	server := inboundmcp.NewServer(deps)

	handler := newRouter(inboundmcp.Handler(server), serviceName)

	srv := &http.Server{Addr: mcpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", mcpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("mcp server failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// newRouter wraps the MCP handler in the process's HTTP surface:
//
//   - GET /healthz answers 200 {"status":"ok"}, for the Kubernetes
//     liveness/readiness probes.
//   - The MCP Streamable HTTP endpoint is mounted at BOTH "/" and "/mcp",
//     matching facility-layout's convention.
//
// Middleware order follows ADR-0013 Tier 1 item 2 verbatim: RequestID,
// otelchi (tracing), otelchimetric (request duration + active requests),
// the request logger, then Recoverer.
func newRouter(mcpHandler http.Handler, serviceName string) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	r.Use(middleware.Recoverer)
	r.Get("/healthz", healthz)
	r.Handle("/", mcpHandler)
	r.Mount("/mcp", mcpHandler)
	return r
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// errMissingDatabaseURL is the boot error when DATABASE_URL is empty: this
// process reads the same Postgres cmd/netfulfil writes, and has no other
// backend.
var errMissingDatabaseURL = errors.New("DATABASE_URL is required: the MCP server reads the real Postgres that cmd/netfulfil writes")

// requireDatabaseURL returns the trimmed DATABASE_URL or errMissingDatabaseURL.
func requireDatabaseURL(raw string) (string, error) {
	url := strings.TrimSpace(raw)
	if url == "" {
		return "", errMissingDatabaseURL
	}
	return url, nil
}

// buildOrders opens the Postgres pool (running migrations first) and returns
// the NetworkOrderRepo over it. Kept independent of cmd/netfulfil so the MCP
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

// buildOffers wires ports.CapabilityOfferRepo for the list_capability_offers
// tool over the SAME already-migrated pool buildOrders opened, so the MCP
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
