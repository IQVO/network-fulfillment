package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
)

// The REST server of the composition root serves the keyed read of a purchase
// order (ADR 0036) in every network mode: the poNumber -> networkRef map is in
// this service's Postgres, empty in stub mode, and the route answers 404 for a
// number nothing recorded rather than not existing.
func TestWireAPI_ServesTheKeyedReadOfAPurchaseOrder(t *testing.T) {
	api := wireAPI(apiDeps{
		mode:    retailnetwork.ModeStub,
		metrics: telemetry.NewCircuitBreakerMetrics(),
		logger:  quietLogger(),
	})
	if api.PurchaseOrders == nil {
		t.Fatal("wireAPI left the purchase-order index unset: GET /purchase-orders/{poNumber} would not be registered")
	}

	// A malformed number is answered by the route's own shape check, before any
	// database read: a route that was not registered would be a plain 404.
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/purchase-orders/not%20valid", nil))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid-purchase-order-number") {
		t.Fatalf("GET /purchase-orders/not%%20valid = %d %s, want 400 invalid-purchase-order-number", rec.Code, rec.Body.String())
	}
}
