package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The Grafana per-context dashboard (warehouse-infra
// scripts/gen-context-dashboards.py) reads
// http_server_request_duration_seconds_* by route. This pins that Routes()
// records http.server.request.duration with the chi route pattern as
// http.route (via otelchi, ADR-0013 Tier 1 item 2), so those panels are not
// silently empty again.
func TestRoutes_RecordsRequestDurationByRoute(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	srv := (&Server{}).Routes()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(h.DataPoints) == 0 {
				t.Fatalf("unexpected data for %s: %#v", m.Name, m.Data)
			}
			for _, dp := range h.DataPoints {
				if v, ok := dp.Attributes.Value("http.route"); ok && v.AsString() == "/healthz" {
					return
				}
			}
			t.Fatalf("no data point with http.route=/healthz: %#v", h.DataPoints)
		}
	}
	t.Fatal("http.server.request.duration was not recorded")
}

// ADR-0013 Tier 1 item 2: otelchimetric.NewServerActiveRequests must also
// be wired (not just NewServerRequestDuration) — both lines are part of
// the Tier 1 contract this repo's sibling services (inventory-storage,
// wes-work-planning) already follow verbatim.
func TestRoutes_RecordsActiveRequests(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	srv := (&Server{}).Routes()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "http.server.active_requests" {
				return
			}
		}
	}
	t.Fatal("http.server.active_requests was not recorded")
}
