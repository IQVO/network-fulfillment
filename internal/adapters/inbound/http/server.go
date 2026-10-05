// Package http is this context's inbound REST adapter.
//
// It is almost entirely READ-ONLY. Demand enters this context by polling
// the network (ADR 0001 §5) and by nothing else: the network offers us no
// push, so an HTTP intake endpoint would be a second, fictional inbound
// path with no counterpart in production — and the only thing it could
// genuinely be used for is injecting test demand, which the stub
// gateway's file-seeded mode already does honestly.
//
// So most of what is here is observation: what did we tell the network,
// and is the inbound leg alive. Both are questions an operator has during
// an incident and cannot currently answer without reading logs. The one
// exception is POST /network-orders/{networkRef}/shipment-confirmation
// (docs/adr/0014-explicit-shipment-confirmation-endpoint.md): a narrow,
// explicitly-ADR'd write endpoint for a fact this context is TOLD, not
// demand it decides on.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/cors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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
	// Readiness backs GET /readyz (ADR 0004 §graceful shutdown). A nil
	// Readiness (the zero value, and every pre-existing caller/test)
	// means /readyz always reports ready — see Readiness's own doc
	// comment.
	Readiness *Readiness
	// MetricsRegistry, when non-nil, backs GET /metrics (ADR 0004's
	// circuit_breaker_state gauge). A nil registry means /metrics is
	// simply not registered — every pre-existing caller/test that does
	// not care about metrics is unaffected.
	MetricsRegistry *prometheus.Registry
	// Offers backs GET /capability-offers (ADR 0001 §8). A nil Offers
	// means that route is simply not registered, matching every other
	// optional dependency in this Server.
	Offers ports.CapabilityOfferRepo
	// ConfirmShipment backs the one write endpoint this adapter has
	// (ADR 0014). A nil value means that route is not registered.
	ConfirmShipment *usecases.ConfirmNetworkOrderShipment
}

// Routes returns this adapter's handler.
//
// Only GETs, and that is a design statement rather than an omission —
// see the package comment.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /network-orders/{networkRef}", s.handleGetNetworkOrder)
	mux.HandleFunc("GET /network-orders", s.handleListUnanswered)
	mux.HandleFunc("GET /inbound-status", s.handleInboundStatus)
	if s.MetricsRegistry != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.MetricsRegistry, promhttp.HandlerOpts{}))
	}
	if s.Offers != nil {
		mux.HandleFunc("GET /capability-offers", s.handleListCapabilityOffers)
	}
	if s.ConfirmShipment != nil {
		mux.HandleFunc("POST /network-orders/{networkRef}/shipment-confirmation", s.handleConfirmShipment)
	}
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

// handleListCapabilityOffers lists every currently-advertised
// CapabilityOffer (ADR 0001 §8): the figure this context would tell the
// network it can ship for each (SKU, site) it knows about.
func (s *Server) handleListCapabilityOffers(w http.ResponseWriter, r *http.Request) {
	offers, err := s.Offers.ListAll(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]capabilityOfferResponse, 0, len(offers))
	for _, o := range offers {
		out = append(out, toCapabilityOfferResponse(o))
	}
	writeJSON(w, http.StatusOK, map[string]any{"capabilityOffers": out})
}

// handleConfirmShipment is this adapter's one write endpoint (ADR 0014):
// POST /network-orders/{networkRef}/shipment-confirmation, empty body,
// 204 on success. See ConfirmNetworkOrderShipment's own doc comment for
// why this is an explicit call rather than a PackageManifested
// correlation.
func (s *Server) handleConfirmShipment(w http.ResponseWriter, r *http.Request) {
	ref := shared.NetworkRef(r.PathValue("networkRef"))
	if ref == "" {
		writeError(w, r, shared.ErrEmptyNetworkRef)
		return
	}
	if _, err := s.ConfirmShipment.Execute(r.Context(), ref); err != nil {
		if errors.Is(err, usecases.ErrOrderNotFound) {
			writeError(w, r, usecases.ErrOrderNotFound)
			return
		}
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
