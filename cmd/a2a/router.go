package main

import (
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"
)

// newRouter wraps the A2A handler in the process's HTTP surface:
//
//   - GET /healthz answers 200 {"status":"ok"}, for the Kubernetes
//     liveness/readiness probes. A2A is unauthenticated by decision, so probes
//     need no credentials, and the route leaks no skill or state.
//   - The Agent Card is served at the well-known path
//     /.well-known/agent-card.json (the discovery contract, ADR 0030).
//   - The JSON-RPC endpoint is mounted at BOTH "/" (the canonical root) and
//     "/a2a" (the fleet's <CTX>_A2A_ENDPOINT convention, mirroring cmd/mcp's
//     dual "/" and "/mcp" mount).
//
// Middleware order follows ADR-0013 Tier 1 item 2 verbatim, as in cmd/mcp:
// RequestID, otelchi (tracing), otelchimetric (request duration + active
// requests), then Recoverer.
func newRouter(a2aHandler http.Handler, card *a2a.AgentCard, serviceName string) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	r.Use(middleware.Recoverer)
	r.Get("/healthz", healthz)
	r.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	r.Handle("/a2a", a2aHandler)
	r.Handle("/", a2aHandler)
	return r
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
