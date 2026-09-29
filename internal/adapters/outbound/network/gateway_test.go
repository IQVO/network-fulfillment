package network

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func TestParseMode_FailsClosedToStub(t *testing.T) {
	cases := map[string]Mode{
		"stub":       ModeStub,
		"sandbox":    ModeSandbox,
		"live":       ModeLive,
		"LIVE":       ModeLive,
		"  live  ":   ModeLive,
		"":           ModeStub,
		"lives":      ModeStub,
		"production": ModeStub,
		"true":       ModeStub,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			// Failing CLOSED matters more than failing loudly: a typo in
			// a deployment manifest must never be the reason this
			// service starts submitting real acknowledgements to a real
			// retailer.
			if got := ParseMode(in); got != want {
				t.Fatalf("ParseMode(%q) = %v, want %v", in, got, want)
			}
		})
	}
}

func TestNewGateway_RefusesUnimplementedModesRatherThanDegrading(t *testing.T) {
	// A deployment that ASKED for a real network and silently got a
	// stub would look healthy while answering nobody — the worst
	// possible failure for this context.
	for _, mode := range []Mode{ModeSandbox, ModeLive} {
		if _, err := NewGateway(mode, nil); err == nil {
			t.Fatalf("NewGateway(%v) must fail until a credentialed adapter exists", mode)
		}
	}

	g, err := NewGateway(ModeStub, nil)
	if err != nil {
		t.Fatalf("NewGateway(stub): %v", err)
	}
	if g == nil {
		t.Fatal("stub gateway must be usable")
	}
}

func TestStubGateway_PollDemandRePresentsUnacknowledgedUnits(t *testing.T) {
	g := NewStubGateway(nil)
	g.Seed(contract.InboundDemand{NetworkRef: "po-1"})

	first, err := g.PollDemand(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("PollDemand: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first poll = %d demands, want 1", len(first))
	}

	// A real network's cursor only advances once WE have acknowledged
	// an order (poller.go's own contract: a failed Execute must be
	// re-fetched on the next pass, not lost). Nothing has been
	// acknowledged yet, so the same unit must still be there.
	second, err := g.PollDemand(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("PollDemand: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("second poll (before ack) = %d demands, want 1", len(second))
	}

	if err := g.SubmitAcknowledgement(context.Background(), "po-1", true); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}

	// Acknowledging the order is what retires it: the next poll must
	// not hand it back again.
	third, err := g.PollDemand(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("PollDemand: %v", err)
	}
	if len(third) != 0 {
		t.Fatalf("third poll (after ack) = %d demands, want 0", len(third))
	}
}

func TestStubGateway_RecordsAcknowledgements(t *testing.T) {
	g := NewStubGateway(nil)

	if _, submitted := g.Acknowledgement("po-1"); submitted {
		t.Fatal("nothing submitted yet")
	}
	if err := g.SubmitAcknowledgement(context.Background(), "po-1", true); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}
	accepted, submitted := g.Acknowledgement("po-1")
	if !submitted || !accepted {
		t.Fatalf("submitted=%v accepted=%v, want true/true", submitted, accepted)
	}

	if err := g.SubmitAcknowledgement(context.Background(), "po-2", false); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}
	accepted, submitted = g.Acknowledgement("po-2")
	if !submitted || accepted {
		t.Fatalf("submitted=%v accepted=%v, want true/false", submitted, accepted)
	}
}
