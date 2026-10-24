package retailnetwork

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
		labels, err := g.RequestLabel(context.Background(), shared.NetworkRef(ref), 1)
		if err != nil {
			t.Fatalf("RequestLabel(%s): %v", ref, err)
		}
		if len(labels) != 1 {
			t.Fatalf("RequestLabel(%s) = %d labels, want 1", ref, len(labels))
		}
		label := labels[0]
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

func TestStubGateway_RequestLabelReturnsOneLabelPerPackage(t *testing.T) {
	g := newTestStubGateway()
	labels, err := g.RequestLabel(context.Background(), "po-1", 3)
	if err != nil {
		t.Fatalf("RequestLabel: %v", err)
	}
	if len(labels) != 3 {
		t.Fatalf("labels = %d, want 3", len(labels))
	}
	seen := map[string]bool{}
	for _, l := range labels {
		if seen[l.TrackingNumber] {
			t.Fatalf("tracking number %q repeated: every package needs its own", l.TrackingNumber)
		}
		seen[l.TrackingNumber] = true
	}
	// A package count below one still labels the one package an order has.
	labels, err = g.RequestLabel(context.Background(), "po-1", 0)
	if err != nil || len(labels) != 1 {
		t.Fatalf("RequestLabel(0) = %d labels, %v; want 1, nil", len(labels), err)
	}
}

func TestStubGateway_RecordsTheRejectionReason(t *testing.T) {
	g := newTestStubGateway()
	if err := g.SubmitAcknowledgement(context.Background(), "po-1", contract.Acknowledgement{Reason: shared.RejectionReasonInfeasibleDeadline}); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}
	if got := g.RejectionReason("po-1"); got != shared.RejectionReasonInfeasibleDeadline {
		t.Fatalf("reason = %q, want INFEASIBLE_DEADLINE", got)
	}
	if got := g.RejectionReason("po-unknown"); got != "" {
		t.Fatalf("reason of an unanswered order = %q, want empty", got)
	}
}
