package retailnetwork

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func TestStubInbound_PollReturnsOnlyTheSitesShipmentsAfterTheCursor(t *testing.T) {
	g := NewStubGateway(nil)
	g.SeedInbound(
		contract.InboundShipment{Ref: "old", SiteId: "FC-1", ShippedAt: t0},
		contract.InboundShipment{Ref: "new", SiteId: "FC-1", ShippedAt: t0.Add(time.Hour)},
		contract.InboundShipment{Ref: "other", SiteId: "FC-2", ShippedAt: t0.Add(time.Hour)},
	)
	got, err := g.PollInboundShipments(context.Background(), "FC-1", t0)
	if err != nil || len(got) != 1 || got[0].Ref != "new" {
		t.Fatalf("got %+v, %v: want only 'new' (strictly after the cursor, this site)", got, err)
	}
	all, _ := g.PollInboundShipments(context.Background(), "FC-1", time.Time{})
	if len(all) != 2 {
		t.Errorf("zero cursor returned %d, want 2", len(all))
	}
}

func TestStubInbound_TheFirstReceiptOfAShipmentWins(t *testing.T) {
	g := NewStubGateway(nil)
	first := contract.InboundReceipt{ShipmentRef: "S", Cartons: []contract.ReceiptCarton{{SSCC: ssccA, Status: contract.ReceiptReceived}}}
	second := contract.InboundReceipt{ShipmentRef: "S", Cartons: []contract.ReceiptCarton{{SSCC: ssccA, Status: contract.ReceiptMissing}}}
	_ = g.SubmitInboundReceipt(context.Background(), first)
	_ = g.SubmitInboundReceipt(context.Background(), second)
	got, ok := g.InboundReceipt("S")
	if !ok || got.Cartons[0].Status != contract.ReceiptReceived {
		t.Errorf("receipt = %+v, %v", got, ok)
	}
	if _, ok := g.InboundReceipt("none"); ok {
		t.Error("an unknown shipment has no receipt")
	}
}
