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
// network-fulfillment has no OTel/observability package (AGENTS.md), so
// unlike facility-layout's cmd/mcp/main.go this composition root wires no
// telemetry.Setup — the process logs plain structured JSON only.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	inboundmcp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	mcpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "/app/migrations")

	orders, closeOrders, err := buildOrders(context.Background(), databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeOrders()

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
		Clock:  systemClock{},
	}
	if reportsURL := os.Getenv("REPORTS_BASE_URL"); reportsURL != "" {
		deps.Reports = inboundmcp.NewReportsRESTClient(reportsURL, nil)
		logger.Info("acknowledgement report tool wired", "reports_base_url", reportsURL)
	}
	server := inboundmcp.NewServer(deps)

	handler := newRouter(inboundmcp.Handler(server))

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
func newRouter(mcpHandler http.Handler) http.Handler {
	r := chi.NewRouter()
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

// buildOrders chooses the NetworkOrderRepo implementation, exactly as
// cmd/netfulfil/main.go's wireOrders does: in-memory with no DATABASE_URL,
// Postgres (with migrations run) when one is set. Kept independent of
// cmd/netfulfil so the MCP process can be deployed and scaled separately.
func buildOrders(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (ports.NetworkOrderRepo, func(), error) {
	if databaseURL == "" {
		logger.Info("order repository wired", "backend", "memory")
		return memory.NewNetworkOrderRepo(), func() {}, nil
	}

	if err := postgres.RunMigrations(databaseURL, migrationsPath); err != nil {
		return nil, nil, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	logger.Info("order repository wired", "backend", "postgres")
	return postgres.NewNetworkOrderRepo(pool), pool.Close, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
