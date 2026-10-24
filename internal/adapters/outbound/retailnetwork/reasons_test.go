package retailnetwork

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestAckDecision_MapsOurReasonsToTheNetworksAndNeverInventsOne(t *testing.T) {
	cases := []struct {
		name       string
		in         contract.Acknowledgement
		want       wireDecision
		wantSend   bool
		wantBadAck bool
	}{
		{"accept", contract.Acknowledgement{Accepted: true}, wireDecision{Decision: "ACCEPT"}, true, false},
		{"infeasible deadline", contract.Acknowledgement{Reason: shared.RejectionReasonInfeasibleDeadline}, wireDecision{Decision: "REJECT", ReasonCode: "CANNOT_MEET_SHIP_BY"}, true, false},
		{"untranslatable sku", contract.Acknowledgement{Reason: shared.RejectionReasonUntranslatableSKU}, wireDecision{Decision: "REJECT", ReasonCode: "UNKNOWN_LISTING"}, true, false},
		{"deadline missed is not written", contract.Acknowledgement{Reason: shared.RejectionReasonAcknowledgementDeadlineMissed}, wireDecision{}, false, false},
		{"accept with a reason", contract.Acknowledgement{Accepted: true, Reason: shared.RejectionReasonInfeasibleDeadline}, wireDecision{}, false, true},
		{"reject without a reason", contract.Acknowledgement{}, wireDecision{}, false, true},
		{"submission failed stays internal", contract.Acknowledgement{Reason: shared.RejectionReasonSubmissionFailed}, wireDecision{}, false, true},
		{"unknown reason", contract.Acknowledgement{Reason: "SOMETHING_NEW"}, wireDecision{}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, send, err := ackDecision(tc.in)
			if tc.wantBadAck != errors.Is(err, ErrBadAcknowledgement) {
				t.Fatalf("err = %v, want ErrBadAcknowledgement=%v", err, tc.wantBadAck)
			}
			if send != tc.wantSend || got != tc.want {
				t.Fatalf("ackDecision = (%+v, send=%v), want (%+v, send=%v)", got, send, tc.want, tc.wantSend)
			}
		})
	}
}

func TestSubmissionStatusMappings(t *testing.T) {
	cases := []struct {
		wire   string
		port   contract.SubmissionStatusValue
		ledger SubmissionStatus
	}{
		{"SUCCESS", contract.SubmissionSuccess, StatusSuccess},
		{"FAILURE", contract.SubmissionFailure, StatusFailure},
		{"PROCESSING", contract.SubmissionPending, StatusProcessing},
		{"", contract.SubmissionPending, StatusProcessing},
		{"SOMETHING_NEW", contract.SubmissionPending, StatusProcessing},
	}
	for _, tc := range cases {
		if got := submissionStatus(tc.wire); got != tc.port {
			t.Errorf("submissionStatus(%q) = %v, want %v: an unrecognised status must stay pending, never a guessed verdict", tc.wire, got, tc.port)
		}
		if got := ledgerStatus(tc.wire); got != tc.ledger {
			t.Errorf("ledgerStatus(%q) = %v, want %v", tc.wire, got, tc.ledger)
		}
	}
}

func TestIdempotencyKey_IsDeterministicAndWithinTheNetworksLimit(t *testing.T) {
	if got := idempotencyKey("ack", "NR-1"); got != "nf-ack-NR-1" {
		t.Fatalf("key = %q, want nf-ack-NR-1", got)
	}
	if idempotencyKey("ack", "NR-1") == idempotencyKey("ship", "NR-1") {
		t.Fatal("two kinds of write for one order must not share a key: the network scopes a key to a route, but a ledger must tell them apart")
	}
	long := strings.Repeat("x", 300)
	a, b := idempotencyKey("ack", long), idempotencyKey("ack", long)
	if a != b {
		t.Fatalf("a long subject gave two keys, %q and %q: it must be deterministic", a, b)
	}
	if len(a) > maxIdempotencyKeyLength {
		t.Fatalf("key length = %d, want at most %d", len(a), maxIdempotencyKeyLength)
	}
	if a == idempotencyKey("ack", strings.Repeat("x", 301)) {
		t.Fatal("two different long subjects collided")
	}
}

func TestBuildLabelBody_NumbersPackagesFromOneAndRefusesNone(t *testing.T) {
	body, err := buildLabelBody("V-WS", "PO-1", 3)
	if err != nil {
		t.Fatalf("buildLabelBody: %v", err)
	}
	want := `{"vendorId":"V-WS","poNumber":"PO-1","packages":[{"packageNo":1},{"packageNo":2},{"packageNo":3}]}`
	if string(body) != want {
		t.Fatalf("body = %s, want %s", body, want)
	}
	for _, n := range []int{0, -1} {
		if _, err := buildLabelBody("V-WS", "PO-1", n); !errors.Is(err, ErrBadPackageCount) {
			t.Fatalf("package count %d: err = %v, want ErrBadPackageCount", n, err)
		}
	}
}

func TestBuildAcknowledgementBody_EchoesEveryLineWithItsQuantity(t *testing.T) {
	po := PurchaseOrder{PoNumber: "PO-1", Lines: []PurchaseOrderLine{{LineNo: 1, Quantity: 2}, {LineNo: 2, Quantity: 5}}}
	body, err := buildAcknowledgementBody("V-WS", po, wireDecision{Decision: "REJECT", ReasonCode: "CANNOT_MEET_SHIP_BY"})
	if err != nil {
		t.Fatalf("buildAcknowledgementBody: %v", err)
	}
	want := `{"vendorId":"V-WS","poNumber":"PO-1","decision":"REJECT","reasonCode":"CANNOT_MEET_SHIP_BY","lines":[{"lineNo":1,"quantity":2},{"lineNo":2,"quantity":5}]}`
	if string(body) != want {
		t.Fatalf("body = %s\nwant   %s", body, want)
	}
	accept, _ := buildAcknowledgementBody("V-WS", po, wireDecision{Decision: "ACCEPT"})
	if strings.Contains(string(accept), "reasonCode") {
		t.Fatalf("an ACCEPT carries no reasonCode: %s", accept)
	}
}

func TestBuildShipmentConfirmationBody_IsUTCAndKeepsTrackingOrder(t *testing.T) {
	at := time.Date(2026, 10, 24, 9, 30, 0, 0, time.FixedZone("BRT", -3*3600))
	body, err := buildShipmentConfirmationBody("V-WS", "PO-1", at, []string{"TRK-1", "TRK-2"})
	if err != nil {
		t.Fatalf("buildShipmentConfirmationBody: %v", err)
	}
	want := `{"vendorId":"V-WS","poNumber":"PO-1","shippedAt":"2026-10-24T12:30:00Z","trackingNumbers":["TRK-1","TRK-2"]}`
	if string(body) != want {
		t.Fatalf("body = %s, want %s", body, want)
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		value  string
		want   time.Duration
		wantOK bool
	}{
		{"seconds", "7", 7 * time.Second, true},
		{"zero", "0", 0, true},
		{"padded", " 3 ", 3 * time.Second, true},
		{"http date in the future", now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second, true},
		{"http date in the past", now.Add(-time.Minute).UTC().Format(http.TimeFormat), 0, true},
		{"negative", "-4", 0, false},
		{"garbage", "soon", 0, false},
		{"absent", "", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.value != "" {
				h.Set("Retry-After", tc.value)
			}
			got, ok := retryAfter(h, now)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("retryAfter(%q) = %v, %v; want %v, %v", tc.value, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestProblem_ReadsTheSlugOfAnAPIError(t *testing.T) {
	err := errors.Join(errors.New("context"), &APIError{Type: "ack-window-closed"})
	if got := Problem(err); got != "ack-window-closed" {
		t.Fatalf("Problem = %q", got)
	}
	if got := Problem(errors.New("plain")); got != "" {
		t.Fatalf("Problem of a plain error = %q, want empty", got)
	}
}
