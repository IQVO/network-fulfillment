package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

const (
	sA = "006141410000000012"
	sB = "006141410000000029"
	sC = "006141410000000036"
	sX = "006141410000000043"
)

func manifest(sscc ...string) contract.InboundShipment {
	s := contract.InboundShipment{Ref: "SHP-1", SiteId: "FC-1", OwnerRef: "S-100"}
	for _, x := range sscc {
		s.Cartons = append(s.Cartons, contract.InboundCarton{SSCC: x, Kind: contract.CartonMixed})
	}
	return s
}

func got(sscc, gtin string, qty int64, cond string) contract.ReceivedCartonFact {
	return contract.ReceivedCartonFact{SSCC: sscc, Contents: []contract.ReceivedContent{{GTIN: gtin, Qty: qty, Condition: cond}}}
}

var closedAt = time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC)

func facts(m contract.InboundShipment, cartons []contract.ReceivedCartonFact, findings ...contract.CartonFinding) contract.ReceiptFacts {
	return contract.ReceiptFacts{Shipment: m, ClosedAt: closedAt, Cartons: cartons, Findings: findings}
}

func cartonOf(t *testing.T, r contract.InboundReceipt, sscc string) contract.ReceiptCarton {
	t.Helper()
	for _, c := range r.Cartons {
		if c.SSCC == sscc {
			return c
		}
	}
	t.Fatalf("receipt has no carton %s: %+v", sscc, r.Cartons)
	return contract.ReceiptCarton{}
}

func TestBuildReceipt_EveryCartonStatus(t *testing.T) {
	r, err := usecases.BuildReceipt(facts(manifest(sA, sB, sC),
		[]contract.ReceivedCartonFact{
			got(sA, gEachA, 12, "Good"),
			{SSCC: sB, Contents: []contract.ReceivedContent{{GTIN: gEachA, Qty: 3, Condition: "Good"}, {GTIN: gEachB, Qty: 2, Condition: "Damaged"}, {GTIN: gEachA, Qty: 1, Condition: "Damaged"}}},
			got(sX, gEachA, 5, "Good"),
		},
		contract.CartonFinding{Kind: "MissingCarton", Expected: sC, At: closedAt},
	), quiet)
	if err != nil {
		t.Fatal(err)
	}
	expectCartonStatuses(t, r)
}

func expectCartonStatuses(t *testing.T, r contract.InboundReceipt) {
	t.Helper()
	if r.ShipmentRef != "SHP-1" || r.SiteId != "FC-1" || !r.ReceivedAt.Equal(closedAt) {
		t.Errorf("header = %+v", r)
	}
	if len(r.Cartons) != 4 || r.Cartons[0].SSCC != sA || r.Cartons[3].SSCC != sX {
		t.Errorf("order = %+v: manifest order, unexpected last", r.Cartons)
	}
	if c := cartonOf(t, r, sA); c.Status != contract.ReceiptReceived || fmt.Sprint(c.Items) != "[{00614141000036 12}]" {
		t.Errorf("A = %+v", c)
	}
	if c := cartonOf(t, r, sB); c.Status != contract.ReceiptDamaged || fmt.Sprint(c.Items) != "[{00614141000036 4} {00614141000043 2}]" {
		t.Errorf("B = %+v: DAMAGED, every counted unit per GTIN summed and in GTIN order", c)
	}
	expectMissingAndUnexpected(t, r)
}

func expectMissingAndUnexpected(t *testing.T, r contract.InboundReceipt) {
	t.Helper()
	if c := cartonOf(t, r, sC); c.Status != contract.ReceiptMissing || c.Items == nil || len(c.Items) != 0 {
		t.Errorf("C = %+v: MISSING carries an empty items list", c)
	}
	if c := cartonOf(t, r, sX); c.Status != contract.ReceiptUnexpected || len(c.Items) != 1 || c.Items[0].QtyEaches != 5 {
		t.Errorf("X = %+v: a carton off the manifest that the dock accepted is UNEXPECTED with its count", c)
	}
}

func TestBuildReceipt_ANeverReceivedManifestCartonIsMissingEvenWithoutAFinding(t *testing.T) {
	r, err := usecases.BuildReceipt(facts(manifest(sA, sB), []contract.ReceivedCartonFact{got(sA, gEachA, 1, "Good")}), quiet)
	if err != nil || cartonOf(t, r, sB).Status != contract.ReceiptMissing {
		t.Fatalf("receipt = %+v, %v", r, err)
	}
}

func TestBuildReceipt_AnUnexpectedFindingWithoutAnAcceptedCartonIsReportedEmpty(t *testing.T) {
	r, err := usecases.BuildReceipt(facts(manifest(sA), []contract.ReceivedCartonFact{got(sA, gEachA, 1, "Good")},
		contract.CartonFinding{Kind: "UnexpectedCarton", SSCC: sX}), quiet)
	if err != nil {
		t.Fatal(err)
	}
	if c := cartonOf(t, r, sX); c.Status != contract.ReceiptUnexpected || c.Items == nil || len(c.Items) != 0 {
		t.Errorf("X = %+v", c)
	}
}

func TestBuildReceipt_ComplianceCodesComeFromTheCartonsOwnFindings(t *testing.T) {
	r, err := usecases.BuildReceipt(facts(manifest(sA, sB),
		[]contract.ReceivedCartonFact{got(sA, gEachA, 11, "Good"), got(sB, gEachA, 1, "Good")},
		contract.CartonFinding{Kind: "CaseQtyMismatch", SSCC: sA, Expected: "12", Actual: "11"},
		contract.CartonFinding{Kind: "CartonOverLimit", SSCC: sA},
		contract.CartonFinding{Kind: "CaseQtyMismatch", SSCC: sA},
		contract.CartonFinding{Kind: "UnknownGtin"}, // no SSCC: it cannot be tied to a carton
	), quiet)
	if err != nil {
		t.Fatal(err)
	}
	if c := cartonOf(t, r, sA); fmt.Sprint(c.ComplianceCodes) != "[CARTON_OVER_LIMIT CASE_QTY_MISMATCH]" {
		t.Errorf("A codes = %v, want sorted and without duplicates", c.ComplianceCodes)
	}
	if c := cartonOf(t, r, sB); c.ComplianceCodes == nil || len(c.ComplianceCodes) != 0 {
		t.Errorf("B codes = %#v, want an empty list (the field is always present)", c.ComplianceCodes)
	}
}

func TestBuildReceipt_CodesAreCappedAtTwentyAndAlwaysValid(t *testing.T) {
	var findings []contract.CartonFinding
	for i := range 25 {
		findings = append(findings, contract.CartonFinding{Kind: fmt.Sprintf("Kind%c%c", 'A'+i, 'a'+i), SSCC: sA})
	}
	findings = append(findings, contract.CartonFinding{Kind: "9bad", SSCC: sA}, contract.CartonFinding{Kind: "", SSCC: sA})
	r, err := usecases.BuildReceipt(facts(manifest(sA), []contract.ReceivedCartonFact{got(sA, gEachA, 1, "Good")}, findings...), quiet)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(cartonOf(t, r, sA).ComplianceCodes); n != 20 {
		t.Errorf("codes = %d, want the cap of 20", n)
	}
}

func TestBuildReceipt_ACartonWithoutAnSsccIsNotReported(t *testing.T) {
	r, err := usecases.BuildReceipt(facts(manifest(sA),
		[]contract.ReceivedCartonFact{got(sA, gEachA, 1, "Good"), {LPN: "LPN-1", Contents: []contract.ReceivedContent{{GTIN: gEachA, Qty: 9, Condition: "Good"}}}}), quiet)
	if err != nil || len(r.Cartons) != 1 {
		t.Fatalf("receipt = %+v, %v: the internal LPN is unknown to the network", r, err)
	}
}

func TestBuildReceipt_NoCartonsAtAllIsAnError(t *testing.T) {
	if _, err := usecases.BuildReceipt(facts(manifest(), nil), quiet); err == nil {
		t.Fatal("want an error: a receipt names at least one carton")
	}
}

func TestComplianceCode(t *testing.T) {
	for in, want := range map[string]string{"CaseQtyMismatch": "CASE_QTY_MISMATCH", "CartonOverLimit": "CARTON_OVER_LIMIT", "DuplicateSscc": "DUPLICATE_SSCC", "UnknownGtin": "UNKNOWN_GTIN", "Kind": "KIND", "": ""} {
		if got := usecases.ComplianceCode(in); got != want {
			t.Errorf("ComplianceCode(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---- ReturnInboundReceipts ----

type receiptQueue struct {
	due      []contract.ReceiptFacts
	sent     []string
	retried  map[string]time.Time
	retryMsg map[string]string
	rejected map[string]string
	dueErr   error
}

func (q *receiptQueue) RecordCartonReceived(context.Context, string, contract.ReceivedCartonFact) error {
	return nil
}
func (q *receiptQueue) RecordFinding(context.Context, string, contract.CartonFinding) error {
	return nil
}
func (q *receiptQueue) RecordClosed(context.Context, string, contract.ReceiptClosedFact, time.Time) error {
	return nil
}
func (q *receiptQueue) DueReceipts(context.Context, time.Time, int) ([]contract.ReceiptFacts, error) {
	return q.due, q.dueErr
}
func (q *receiptQueue) MarkReceiptSent(_ context.Context, asn string, _ time.Time) error {
	q.sent = append(q.sent, asn)
	return nil
}
func (q *receiptQueue) MarkReceiptRetry(_ context.Context, asn, msg string, next time.Time) error {
	q.retried[asn], q.retryMsg[asn] = next, msg
	return nil
}
func (q *receiptQueue) MarkReceiptRejected(_ context.Context, asn, msg string) error {
	q.rejected[asn] = msg
	return nil
}

type receiptGateway struct {
	err  error
	sent []contract.InboundReceipt
}

func (g *receiptGateway) PollInboundShipments(context.Context, shared.SiteId, time.Time) ([]contract.InboundShipment, error) {
	return nil, nil
}
func (g *receiptGateway) SubmitInboundReceipt(_ context.Context, r contract.InboundReceipt) error {
	g.sent = append(g.sent, r)
	return g.err
}

func newReturn(gw *receiptGateway, f ...contract.ReceiptFacts) (*usecases.ReturnInboundReceipts, *receiptQueue) {
	q := &receiptQueue{due: f, retried: map[string]time.Time{}, retryMsg: map[string]string{}, rejected: map[string]string{}}
	return &usecases.ReturnInboundReceipts{Receipts: q, Gateway: gw, Clock: &movableClock{t: intakeNow}, Logger: quiet}, q
}

func oneReceipt() contract.ReceiptFacts {
	return facts(manifest(sA), []contract.ReceivedCartonFact{got(sA, gEachA, 12, "Good")})
}

func TestReturn_ReportsEachClosedReceiptOnceAndMarksItSent(t *testing.T) {
	gw := &receiptGateway{}
	uc, q := newReturn(gw, oneReceipt())
	res, err := uc.Execute(context.Background())
	if err != nil || res != (usecases.ReturnResult{Sent: 1}) {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if len(gw.sent) != 1 || gw.sent[0].Cartons[0].Items[0].QtyEaches != 12 || len(q.sent) != 1 || q.sent[0] != "NF-SHP-1" {
		t.Errorf("sent=%+v marked=%v", gw.sent, q.sent)
	}
}

func TestReturn_AFailureRetriesWithBackoffAndTheAttemptCount(t *testing.T) {
	gw := &receiptGateway{err: errors.New("network down")}
	f := oneReceipt()
	f.Attempts = 2
	uc, q := newReturn(gw, f)
	res, err := uc.Execute(context.Background())
	if err != nil || res.Retrying != 1 || len(q.sent) != 0 {
		t.Fatalf("res=%+v err=%v sent=%v", res, err, q.sent)
	}
	if want := intakeNow.Add(4 * time.Minute); !q.retried["NF-SHP-1"].Equal(want) || !strings.Contains(q.retryMsg["NF-SHP-1"], "network down") {
		t.Errorf("retry = %v %q, want %v: the third failure waits four minutes", q.retried["NF-SHP-1"], q.retryMsg["NF-SHP-1"], want)
	}
}

func TestReturn_ARefusalParksTheReceiptAndANoCartonReceiptToo(t *testing.T) {
	gw := &receiptGateway{err: fmt.Errorf("%w: carton not on the manifest", contract.ErrReceiptRefused)}
	uc, q := newReturn(gw, oneReceipt())
	res, err := uc.Execute(context.Background())
	if err != nil || res.Rejected != 1 || !strings.Contains(q.rejected["NF-SHP-1"], "not on the manifest") {
		t.Fatalf("res=%+v err=%v rejected=%v", res, err, q.rejected)
	}
	empty := facts(manifest(), nil)
	empty.Shipment.Ref = "SHP-2"
	uc, q = newReturn(&receiptGateway{}, empty)
	if res, _ = uc.Execute(context.Background()); res.Rejected != 1 || q.rejected["NF-SHP-2"] == "" {
		t.Errorf("a receipt with no carton can never be reported: %+v %v", res, q.rejected)
	}
}

func TestReturn_ADueQueryFailureIsReturned(t *testing.T) {
	uc, q := newReturn(&receiptGateway{})
	q.dueErr = errors.New("db down")
	if _, err := uc.Execute(context.Background()); err == nil {
		t.Fatal("want the query failure")
	}
}
