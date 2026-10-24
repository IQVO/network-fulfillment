package network

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Covers the three StubGateway operations the rest of the suite never calls
// (the BDD world wraps the gateway in a recordingGateway, whose forwarding
// tests live elsewhere; these pin the stub's own contract).

func newTestStubGateway() *StubGateway {
	return NewStubGateway(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestStubGateway_SubmitAvailabilityIsANoop(t *testing.T) {
	g := newTestStubGateway()
	err := g.SubmitAvailability(context.Background(), contract.AvailabilityUpdate{
		SiteId: "site-1", SKU: "sku-1", Quantity: 5,
	})
	if err != nil {
		t.Fatalf("SubmitAvailability = %v, want nil (the stub makes no external call)", err)
	}
}

func TestStubGateway_DeclareCapabilityIsANoop(t *testing.T) {
	g := newTestStubGateway()
	err := g.DeclareCapability(context.Background(), contract.CapabilityDeclaration{
		SiteId: "site-1", SKU: "sku-1", AdvertisedQuantity: 5,
	})
	if err != nil {
		t.Fatalf("DeclareCapability = %v, want nil (documented no-op: the network API has no such operation)", err)
	}
}

func TestStubGateway_RequestLabelIsDeterministicAndPIIFree(t *testing.T) {
	g := newTestStubGateway()
	for _, ref := range []string{"po-1", "po-2"} {
		label, err := g.RequestLabel(context.Background(), shared.NetworkRef(ref))
		if err != nil {
			t.Fatalf("RequestLabel(%s): %v", ref, err)
		}
		if !strings.HasPrefix(label.LabelRef, "stub-label-"+ref) {
			t.Errorf("labelRef = %q, want the stub-label-<ref> prefix", label.LabelRef)
		}
		if !strings.HasPrefix(label.TrackingNumber, "stub-tracking-"+ref) {
			t.Errorf("trackingNumber = %q, want the stub-tracking-<ref> prefix", label.TrackingNumber)
		}
		if label.Carrier != "STUB_CARRIER" {
			t.Errorf("carrier = %q, want STUB_CARRIER", label.Carrier)
		}
	}
}
