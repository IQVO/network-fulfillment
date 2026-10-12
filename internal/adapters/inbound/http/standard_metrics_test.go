package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
)

// collectedMetricNames installs a manual-reader MeterProvider globally for
// the duration of the test and returns a func that reports the names of
// every metric collected from it. It must be called BEFORE the router
// under test is built: otelchimetric binds to otel.GetMeterProvider() at
// construction time.
func collectedMetricNames(t *testing.T) func() map[string]bool {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	return func() map[string]bool {
		t.Helper()
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("collect metrics: %v", err)
		}
		names := map[string]bool{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				names[m.Name] = true
			}
		}
		return names
	}
}

// ADR-0013 Tier 1: every HTTP server in the fleet emits
// http.server.request.duration and http.server.active_requests via
// otelchimetric. The reports router previously registered neither.
func TestReportsRouter_EmitsStandardHTTPServerMetrics(t *testing.T) {
	collect := collectedMetricNames(t)
	router := inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{}, nil, "network-fulfillment-reports")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}

	names := collect()
	if !names["http.server.request.duration"] {
		t.Errorf("http.server.request.duration not emitted by the reports router; got %v", names)
	}
	if !names["http.server.active_requests"] {
		t.Errorf("http.server.active_requests not emitted by the reports router; got %v", names)
	}
}
