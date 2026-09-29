package networkorder

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func testNow() time.Time { return time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC) }

func mustLine(t *testing.T) Line {
	t.Helper()
	l, err := NewLine("line-1", "ASIN-1", "SKU-1", 2)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return l
}

func mustReceive(t *testing.T) *NetworkOrder {
	t.Helper()
	o, err := Receive("po-1", "site-1", testNow().Add(48*time.Hour), []Line{mustLine(t)}, testNow())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return o
}

func TestNewLine_Validation(t *testing.T) {
	cases := []struct {
		name    string
		ref     shared.NetworkLineRef
		product shared.NetworkProductId
		sku     shared.SKU
		qty     int
		wantErr error
	}{
		{"valid", "line-1", "ASIN-1", "SKU-1", 1, nil},
		{"empty line ref", "", "ASIN-1", "SKU-1", 1, shared.ErrEmptyNetworkLineRef},
		{"empty product id", "line-1", "", "SKU-1", 1, shared.ErrEmptyNetworkProductId},
		{"untranslated sku", "line-1", "ASIN-1", "", 1, shared.ErrUnknownProduct},
		{"zero quantity", "line-1", "ASIN-1", "SKU-1", 0, shared.ErrNonPositiveQuantity},
		{"negative quantity", "line-1", "ASIN-1", "SKU-1", -1, shared.ErrNonPositiveQuantity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewLine(tc.ref, tc.product, tc.sku, tc.qty)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestReceive_RejectsEmptyRefAndNoLines(t *testing.T) {
	if _, err := Receive("", "site-1", testNow(), []Line{mustLine(t)}, testNow()); !errors.Is(err, shared.ErrEmptyNetworkRef) {
		t.Fatalf("empty ref: err = %v, want ErrEmptyNetworkRef", err)
	}
	if _, err := Receive("po-1", "site-1", testNow(), nil, testNow()); !errors.Is(err, shared.ErrNoLines) {
		t.Fatalf("no lines: err = %v, want ErrNoLines", err)
	}
}

func TestReceive_DerivesAcknowledgementDeadlineFromArrival(t *testing.T) {
	// The deadline must come from when the demand ARRIVED, not from an
	// ambient clock: a poll returning a batch after an outage may carry
	// orders whose windows are already near closing.
	arrived := testNow().Add(-20 * time.Hour)
	o, err := Receive("po-1", "site-1", testNow(), []Line{mustLine(t)}, arrived)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	want := arrived.Add(AcknowledgementWindow)
	if !o.AcknowledgeBy().Equal(want) {
		t.Fatalf("acknowledgeBy = %v, want %v", o.AcknowledgeBy(), want)
	}
}

// TestAcknowledgementWindow_IsExactlyTwentyFourHours pins the SLA's
// literal value against a hardcoded duration rather than the constant
// itself. TestReceive_DerivesAcknowledgementDeadlineFromArrival above
// computes its expectation FROM AcknowledgementWindow, so a mutant that
// changes the constant's arithmetic (e.g. 24*time.Hour -> 24/time.Hour)
// mutates both sides of that assertion identically and can never be
// caught by it — this test is the one that actually pins the number
// ADR 0001 §6 names.
func TestAcknowledgementWindow_IsExactlyTwentyFourHours(t *testing.T) {
	if AcknowledgementWindow != 24*time.Hour {
		t.Fatalf("AcknowledgementWindow = %v, want exactly 24h (ADR 0001 §6, Amazon Vendor Direct Fulfillment's published SLA)", AcknowledgementWindow)
	}
}

func TestAcknowledge_ThenSecondAnswerIsRejected(t *testing.T) {
	o := mustReceive(t)
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if o.State() != StateAcknowledged {
		t.Fatalf("state = %v, want ACKNOWLEDGED", o.State())
	}
	if err := o.Acknowledge(); !errors.Is(err, ErrAlreadyAnswered) {
		t.Fatalf("second acknowledge: err = %v, want ErrAlreadyAnswered", err)
	}
	if err := o.Reject(); !errors.Is(err, ErrAlreadyAnswered) {
		t.Fatalf("reject after acknowledge: err = %v, want ErrAlreadyAnswered", err)
	}
}

func TestReject_IsTerminal(t *testing.T) {
	o := mustReceive(t)
	if err := o.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if err := o.Acknowledge(); !errors.Is(err, ErrAlreadyAnswered) {
		t.Fatalf("acknowledge after reject: err = %v, want ErrAlreadyAnswered", err)
	}
}

func TestConfirmShipment_RequiresAcknowledgementFirst(t *testing.T) {
	o := mustReceive(t)
	if err := o.ConfirmShipment(); !errors.Is(err, ErrConfirmBeforeAcknowledge) {
		t.Fatalf("confirm while NEW: err = %v, want ErrConfirmBeforeAcknowledge", err)
	}

	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := o.ConfirmShipment(); err != nil {
		t.Fatalf("ConfirmShipment: %v", err)
	}
	if o.State() != StateConfirmed {
		t.Fatalf("state = %v, want CONFIRMED", o.State())
	}
}

func TestLinkLocalOrder_OnlyOnceAndOnlyWhenAcknowledged(t *testing.T) {
	o := mustReceive(t)

	// Local work must never exist for demand we have not committed to.
	if err := o.LinkLocalOrder("ord-1"); !errors.Is(err, ErrNotAcknowledged) {
		t.Fatalf("link while NEW: err = %v, want ErrNotAcknowledged", err)
	}

	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := o.LinkLocalOrder("ord-1"); err != nil {
		t.Fatalf("LinkLocalOrder: %v", err)
	}
	if got := o.LocalOrderId(); got == nil || *got != "ord-1" {
		t.Fatalf("localOrderId = %v, want ord-1", got)
	}

	// Re-linking would orphan real allocated work in order-management
	// with nothing pointing at it.
	if err := o.LinkLocalOrder("ord-2"); !errors.Is(err, ErrLocalOrderAlreadyLinked) {
		t.Fatalf("relink: err = %v, want ErrLocalOrderAlreadyLinked", err)
	}
}

func TestAcknowledgementOverdue(t *testing.T) {
	o := mustReceive(t)
	deadline := o.AcknowledgeBy()

	if o.AcknowledgementOverdue(deadline) {
		t.Fatal("must not be overdue exactly AT the deadline instant")
	}
	if !o.AcknowledgementOverdue(deadline.Add(time.Nanosecond)) {
		t.Fatal("must be overdue one instant past the deadline")
	}

	// An answered order is never overdue, however late the clock is:
	// the window only applies to an order still awaiting an answer.
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if o.AcknowledgementOverdue(deadline.Add(72 * time.Hour)) {
		t.Fatal("an acknowledged order must never be reported overdue")
	}
}

func TestSKUQuantities_SumsAndLeaksNoNetworkVocabulary(t *testing.T) {
	l1, _ := NewLine("line-1", "ASIN-1", "SKU-1", 2)
	l2, _ := NewLine("line-2", "ASIN-2", "SKU-1", 3) // same SKU, different network line
	l3, _ := NewLine("line-3", "ASIN-3", "SKU-2", 1)

	o, err := Receive("po-1", "site-1", testNow(), []Line{l1, l2, l3}, testNow())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	got := o.SKUQuantities()
	if got["SKU-1"] != 5 {
		t.Fatalf("SKU-1 = %d, want 5 (two network lines of the same SKU must sum)", got["SKU-1"])
	}
	if got["SKU-2"] != 1 {
		t.Fatalf("SKU-2 = %d, want 1", got["SKU-2"])
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
}

func TestLines_ReturnsACopy(t *testing.T) {
	o := mustReceive(t)
	lines := o.Lines()
	lines[0] = Line{} // mutate the caller's copy

	if o.Lines()[0].SKU() != "SKU-1" {
		t.Fatal("mutating the returned slice must not reach inside the aggregate")
	}
}

func TestReceiveUntranslatable_IsLinelessAndRejectable(t *testing.T) {
	o, err := ReceiveUntranslatable("po-9", "site-1", testNow().Add(48*time.Hour), testNow())
	if err != nil {
		t.Fatalf("ReceiveUntranslatable: %v", err)
	}
	if len(o.Lines()) != 0 {
		t.Fatalf("lines = %d, want 0", len(o.Lines()))
	}
	// It still carries a real deadline, so the sweep treats it like any
	// other unanswered order if the rejection submission fails.
	if !o.AcknowledgeBy().Equal(testNow().Add(AcknowledgementWindow)) {
		t.Fatal("untranslatable order must still carry the acknowledgement window")
	}
	if err := o.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
}

func TestRehydrate_RoundTrips(t *testing.T) {
	local := shared.LocalOrderId("ord-7")
	o := Rehydrate("po-1", "site-1", testNow().Add(48*time.Hour), testNow().Add(AcknowledgementWindow), testNow(),
		[]Line{mustLine(t)}, StateAcknowledged, &local)

	if o.State() != StateAcknowledged {
		t.Fatalf("state = %v, want ACKNOWLEDGED", o.State())
	}
	if got := o.LocalOrderId(); got == nil || *got != "ord-7" {
		t.Fatalf("localOrderId = %v, want ord-7", got)
	}
	// A rehydrated acknowledged order must still refuse a second answer.
	if err := o.Acknowledge(); !errors.Is(err, ErrAlreadyAnswered) {
		t.Fatalf("err = %v, want ErrAlreadyAnswered", err)
	}
}

func TestAccessors_RoundTrip(t *testing.T) {
	// In the domain package deliberately: Go measures coverage per
	// package under test, so an accessor exercised only from an
	// application-layer test still reads as 0% here.
	o, err := Receive("po-1", "site-9", testNow().Add(72*time.Hour), []Line{mustLine(t)}, testNow())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	if o.NetworkRef() != "po-1" {
		t.Fatalf("networkRef = %q", o.NetworkRef())
	}
	if o.SiteId() != "site-9" {
		t.Fatalf("siteId = %q", o.SiteId())
	}
	if !o.RequiredShipBy().Equal(testNow().Add(72 * time.Hour)) {
		t.Fatalf("requiredShipBy = %v", o.RequiredShipBy())
	}
	if !o.ReceivedAt().Equal(testNow()) {
		t.Fatalf("receivedAt = %v", o.ReceivedAt())
	}
	if o.LocalOrderId() != nil {
		t.Fatal("a fresh order must have no local order")
	}

	l := o.Lines()[0]
	if l.NetworkLineRef() != "line-1" {
		t.Fatalf("networkLineRef = %q", l.NetworkLineRef())
	}
	if l.NetworkProductId() != "ASIN-1" {
		t.Fatalf("networkProductId = %q", l.NetworkProductId())
	}
	if l.SKU() != "SKU-1" {
		t.Fatalf("sku = %q", l.SKU())
	}
	if l.Quantity() != 2 {
		t.Fatalf("quantity = %d", l.Quantity())
	}
}

func TestReceiveUntranslatable_RejectsEmptyRef(t *testing.T) {
	if _, err := ReceiveUntranslatable("", "site-1", testNow(), testNow()); !errors.Is(err, shared.ErrEmptyNetworkRef) {
		t.Fatalf("err = %v, want ErrEmptyNetworkRef", err)
	}
}
