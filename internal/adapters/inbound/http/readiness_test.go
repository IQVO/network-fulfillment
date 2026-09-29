package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
)

// TestReadyz_ZeroValueServerAlwaysReady proves a Server built without
// wiring a Readiness (every pre-existing caller/test) behaves exactly
// as before ADR 0004: /readyz always reports 200 ready.
func TestReadyz_ZeroValueServerAlwaysReady(t *testing.T) {
	env := newTestEnv(t)

	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (zero-value Readiness is always ready)", rec.Code)
	}
}

// TestReadyz_ReflectsSetNotReady proves GET /readyz flips to 503 once
// SetNotReady has been called -- the graceful-shutdown signal a
// Kubernetes readinessProbe is meant to observe.
func TestReadyz_ReflectsSetNotReady(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	server := &inboundhttp.Server{
		Orders:    newTestEnv(t).orders,
		Poller:    fakeStats{},
		Clock:     fixedClock{t: now()},
		Readiness: readiness,
	}
	handler := server.Routes()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("before SetNotReady: status = %d, want 200", rec.Code)
	}

	readiness.SetNotReady()

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("after SetNotReady: status = %d, want 503", rec.Code)
	}
}

// TestMetrics_NotRegisteredWhenRegistryIsNil proves a Server built
// without a MetricsRegistry (every pre-existing caller/test) exposes no
// GET /metrics route at all, rather than a route that panics or serves
// an empty body.
func TestMetrics_NotRegisteredWhenRegistryIsNil(t *testing.T) {
	env := newTestEnv(t)

	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (no MetricsRegistry wired)", rec.Code)
	}
}

// TestMetrics_ServesRegisteredGaugeFamily proves GET /metrics, once a
// MetricsRegistry IS wired, actually serves that registry's own
// families in Prometheus text format.
func TestMetrics_ServesRegisteredGaugeFamily(t *testing.T) {
	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_probe_gauge"})
	gauge.Set(2)
	registry.MustRegister(gauge)

	server := &inboundhttp.Server{
		Orders:          newTestEnv(t).orders,
		Poller:          fakeStats{},
		Clock:           fixedClock{t: now()},
		MetricsRegistry: registry,
	}
	handler := server.Routes()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "test_probe_gauge 2") {
		t.Fatalf("body = %q, want it to contain the registered gauge's value", body)
	}
}
