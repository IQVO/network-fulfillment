// Package http is this context's inbound REST adapter.
//
// It is deliberately READ-ONLY. Demand enters this context by polling the
// network (ADR 0001 §5) and by nothing else: the network offers us no
// push, so an HTTP intake endpoint would be a second, fictional inbound
// path with no counterpart in production — and the only thing it could
// genuinely be used for is injecting test demand, which the stub
// gateway's file-seeded mode already does honestly.
//
// So what is here is observation: what did we tell the network, and is
// the inbound leg alive. Both are questions an operator has during an
// incident and cannot currently answer without reading logs.
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/cors"

	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// StatsSource is the poller, seen from here as just its counters. A local
// interface so the server's tests need no real poller, gateway or ticker.
type StatsSource interface {
	Stats() poller.Stats
}

// Server holds this adapter's dependencies.
//
// It takes the repository directly rather than through a read use case:
// every handler here is a pure projection of stored state with no
// orchestration, and a use case per read would be a layer that only
// forwards. Anything that DECIDES something still belongs in usecases.
type Server struct {
	Orders      ports.NetworkOrderRepo
	Poller      StatsSource
	Clock       ports.Clock
	NetworkMode string
}

// Routes returns this adapter's handler.
//
// Only GETs, and that is a design statement rather than an omission —
// see the package comment.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /network-orders/{networkRef}", s.handleGetNetworkOrder)
	mux.HandleFunc("GET /network-orders", s.handleListUnanswered)
	mux.HandleFunc("GET /inbound-status", s.handleInboundStatus)
	return corsMiddleware()(mux)
}

// corsMiddleware allows the warehouse-console browser SPA (and this
// service's own netfulfil_mfe remote dev origin, :5188) to call this
// read-only API directly from the browser. CORS_ALLOWED_ORIGINS overrides
// the local-dev default (comma-separated) for staging/prod deployments.
// Same shape as every sibling context's inbound HTTP adapter (see e.g.
// facility-layout's corsMiddleware) -- only GET/OPTIONS are allowed here,
// matching this context's read-only REST surface.
func corsMiddleware() func(http.Handler) http.Handler {
	origins := []string{"http://localhost:5173", "http://localhost:5188"}
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		origins = strings.Split(v, ",")
	}
	return cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{http.MethodGet, http.MethodOptions},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: false,
		MaxAge:           300,
	})
}

// handleHealthz stays liveness-only: it must not consult Postgres or the
// poller. A readiness signal that fails when a dependency is slow turns
// one degraded dependency into a restart loop, and this pod's whole job
// is to keep a 24h clock running.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetNetworkOrder(w http.ResponseWriter, r *http.Request) {
	ref := shared.NetworkRef(r.PathValue("networkRef"))
	if ref == "" {
		writeError(w, r, shared.ErrEmptyNetworkRef)
		return
	}

	o, err := s.Orders.FindByRef(r.Context(), ref)
	if err != nil {
		writeError(w, r, err)
		return
	}
	// The repository's (nil, nil) convention becomes the application's
	// not-found here, which is exactly where that translation belongs.
	if o == nil {
		writeError(w, r, usecases.ErrOrderNotFound)
		return
	}

	writeJSON(w, http.StatusOK, toNetworkOrderResponse(o, s.Clock.Now()))
}

// handleListUnanswered lists orders still awaiting an answer.
//
// Unanswered is the only list worth exposing: it is the working set the
// sweep acts on and the only one whose size is an operational signal. A
// general "all orders" listing would need paging and would answer no
// question anybody has during an incident.
func (s *Server) handleListUnanswered(w http.ResponseWriter, r *http.Request) {
	orders, err := s.Orders.ListUnanswered(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}

	now := s.Clock.Now()
	out := make([]networkOrderResponse, 0, len(orders))
	for _, o := range orders {
		out = append(out, toNetworkOrderResponse(o, now))
	}
	writeJSON(w, http.StatusOK, map[string]any{"networkOrders": out})
}

func (s *Server) handleInboundStatus(w http.ResponseWriter, r *http.Request) {
	unanswered, overdue, err := s.countUnanswered(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK,
		toInboundStatusResponse(s.NetworkMode, s.Poller.Stats(), unanswered, overdue))
}

func (s *Server) countUnanswered(ctx context.Context) (unanswered, overdue int, err error) {
	orders, err := s.Orders.ListUnanswered(ctx)
	if err != nil {
		return 0, 0, err
	}
	now := s.Clock.Now()
	for _, o := range orders {
		// The domain predicate, not a local comparison: a second
		// definition of "overdue" here would be free to drift from the
		// one the sweep enforces.
		if o.AcknowledgementOverdue(now) {
			overdue++
		}
	}
	return len(orders), overdue, nil
}

func writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, statusFor(err), problemFor(err), err.Error(), r.URL.Path)
}

type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetails{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
