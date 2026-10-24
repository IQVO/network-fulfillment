package retailnetwork

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

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

func TestStubGateway_DeclareNodeProfileRecordsItAndSendsNothing(t *testing.T) {
	g := newTestStubGateway()
	profile := contract.NodeProfile{
		SiteId: "site-1", Timezone: "UTC", HandlingTime: time.Hour, ThroughputPerDay: 10, Fingerprint: "f1",
		Cutoffs: []contract.ProfileCutoff{{LocalTime: "18:00", Days: []time.Weekday{time.Monday}, MaxOrders: 4}},
	}
	res, err := g.DeclareNodeProfile(context.Background(), profile)
	if err != nil {
		t.Fatalf("DeclareNodeProfile = %v, want nil (the stub makes no external call)", err)
	}
	if res.Outcome != contract.ProfileUnchanged || res.Revision != 0 {
		t.Errorf("result = %+v, want %q with no revision: nothing was sent", res, contract.ProfileUnchanged)
	}
	got, ok := g.DeclaredProfile("site-1")
	if !ok || got.Fingerprint != "f1" || len(got.Cutoffs) != 1 {
		t.Errorf("DeclaredProfile = %+v ok=%v, want the profile that was declared", got, ok)
	}
	if _, ok := g.DeclaredProfile("site-2"); ok {
		t.Error("DeclaredProfile found a profile for a site that never declared one")
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
