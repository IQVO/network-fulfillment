package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

type nopGateway struct{}

func (nopGateway) PollDemand(context.Context, time.Time) ([]contract.InboundDemand, error) {
	return nil, nil
}

func (nopGateway) SubmitAcknowledgement(context.Context, shared.NetworkRef, bool) error { return nil }
func (nopGateway) SubmitShipmentConfirmation(context.Context, shared.NetworkRef) error  { return nil }

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, any) error { return nil }

// --- GET /capability-offers --------------------------------------------

func TestHandleListCapabilityOffers_NotRegisteredWhenOffersNil(t *testing.T) {
	e := newTestEnv(t) // Offers is unset (nil) in the base fixture
	rec := e.do(t, http.MethodGet, "/capability-offers")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when Offers is nil (route not registered)", rec.Code)
	}
}

func TestHandleListCapabilityOffers_ReturnsEveryOffer(t *testing.T) {
	offers := memory.NewCapabilityOfferRepo()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	o, err := capabilityoffer.New("sku-1", "site-1", 40, 100, capabilityoffer.BasisThroughputConstrained, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := offers.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	srv := &inboundhttp.Server{
		Orders: memory.NewNetworkOrderRepo(),
		Poller: fakeStats{},
		Clock:  fixedClock{t: now},
		Offers: offers,
	}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/capability-offers", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		CapabilityOffers []struct {
			SKU                string `json:"sku"`
			AdvertisedQuantity int    `json:"advertisedQuantity"`
			Basis              string `json:"basis"`
		} `json:"capabilityOffers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body.CapabilityOffers) != 1 {
		t.Fatalf("len = %d, want 1: %s", len(body.CapabilityOffers), rec.Body.String())
	}
	if body.CapabilityOffers[0].SKU != "sku-1" || body.CapabilityOffers[0].AdvertisedQuantity != 40 ||
		body.CapabilityOffers[0].Basis != "THROUGHPUT_CONSTRAINED" {
		t.Fatalf("unexpected offer: %+v", body.CapabilityOffers[0])
	}
}

// --- POST /network-orders/{networkRef}/shipment-confirmation -----------

func TestHandleConfirmShipment_NotRegisteredWhenNil(t *testing.T) {
	e := newTestEnv(t) // ConfirmShipment is unset (nil) in the base fixture
	rec := e.do(t, http.MethodPost, "/network-orders/po-1/shipment-confirmation")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when ConfirmShipment is nil (route not registered)", rec.Code)
	}
}

func TestHandleConfirmShipment_SuccessReturns204(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	line, err := networkorder.NewLine("1", "ASIN-1", "sku-1", 1)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	o, err := networkorder.Receive("po-1", "site-1", now.Add(48*time.Hour), []networkorder.Line{line}, now)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	confirm := &usecases.ConfirmNetworkOrderShipment{
		Orders:  orders,
		Gateway: nopGateway{},
		Events:  nopPublisher{},
		Clock:   fixedClock{t: now},
	}
	srv := &inboundhttp.Server{
		Orders:          orders,
		Poller:          fakeStats{},
		Clock:           fixedClock{t: now},
		ConfirmShipment: confirm,
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/network-orders/po-1/shipment-confirmation", nil)
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", rec.Code, rec.Body.String())
	}

	got, err := orders.FindByRef(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if got.State() != networkorder.StateConfirmed {
		t.Fatalf("state = %v, want CONFIRMED", got.State())
	}
}

func TestHandleConfirmShipment_UnknownRefReturns404(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	confirm := &usecases.ConfirmNetworkOrderShipment{
		Orders:  orders,
		Gateway: nopGateway{},
		Events:  nopPublisher{},
		Clock:   fixedClock{t: now()},
	}
	srv := &inboundhttp.Server{
		Orders:          orders,
		Poller:          fakeStats{},
		Clock:           fixedClock{t: now()},
		ConfirmShipment: confirm,
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/network-orders/po-missing/shipment-confirmation", nil)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
}
