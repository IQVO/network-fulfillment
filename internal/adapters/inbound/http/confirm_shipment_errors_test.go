package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// Confirming shipment for an order that was never acknowledged is a
// business-rule violation the caller can fix (wait for / check the
// acknowledgement), not a server bug: 409 with its own problem type, not
// the 500 internal-error it fell through to before.
func TestHandleConfirmShipment_BeforeAcknowledgeReturns409(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	line, err := networkorder.NewLine("1", "ASIN-1", "sku-1", 1)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	o, err := networkorder.Receive("po-new", "site-1", at.Add(48*time.Hour), []networkorder.Line{line}, at)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	srv := &inboundhttp.Server{
		Orders: orders,
		Poller: fakeStats{},
		Clock:  fixedClock{t: at},
		ConfirmShipment: &usecases.ConfirmNetworkOrderShipment{
			Orders:  orders,
			Gateway: nopGateway{},
			Events:  nopPublisher{},
			Clock:   fixedClock{t: at},
		},
	}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/network-orders/po-new/shipment-confirmation", nil))

	body := assertProblem(t, rec, http.StatusConflict)
	if !strings.HasSuffix(body.Type, "/confirm-before-acknowledge") {
		t.Fatalf("problem.type = %q, want the confirm-before-acknowledge slug", body.Type)
	}
	if strings.Contains(body.Title, "unexpected internal error") {
		t.Fatalf("a deliberate business rule must not be titled as an internal error: %q", body.Title)
	}
}

// The browser SPA (warehouse-console) calls the shipment-confirmation
// POST cross-origin, so the CORS preflight must allow POST.
func TestCORSPreflightAllowsPostForShipmentConfirmation(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	srv := &inboundhttp.Server{
		Orders: orders,
		Poller: fakeStats{},
		Clock:  fixedClock{t: now()},
		ConfirmShipment: &usecases.ConfirmNetworkOrderShipment{
			Orders:  orders,
			Gateway: nopGateway{},
			Events:  nopPublisher{},
			Clock:   fixedClock{t: now()},
		},
	}
	req := httptest.NewRequest(http.MethodOptions, "/network-orders/po-1/shipment-confirmation", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != http.MethodPost {
		t.Fatalf("Access-Control-Allow-Methods = %q, want POST to be allowed (status %d)", got, rec.Code)
	}
}
