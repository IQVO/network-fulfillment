//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

type receiptRig struct {
	shipments *postgres.InboundShipmentRepo
	repo      *postgres.ReceiptProjectionRepo
	asn, ref  string
	now       time.Time
}

func newReceiptRig(t *testing.T) *receiptRig {
	t.Helper()
	pool := newDB(t)
	rig := &receiptRig{
		shipments: postgres.NewInboundShipmentRepo(pool), repo: postgres.NewReceiptProjectionRepo(pool),
		ref: "SHP-" + uniqueSuffix(t), now: time.Now().UTC().Truncate(time.Microsecond),
	}
	rig.asn = "NF-" + rig.ref
	if _, err := rig.shipments.Record(context.Background(), inboundShipmentFixture(rig.ref, rig.now.Add(-time.Hour)), rig.asn, rig.now); err != nil {
		t.Fatal(err)
	}
	return rig
}

func (r *receiptRig) closeWithFacts(t *testing.T, suffix string) {
	t.Helper()
	ctx := context.Background()
	if err := r.repo.RecordCartonReceived(ctx, "c1"+suffix, contract.ReceivedCartonFact{
		ReceiptID: "rcpt-1", AsnNumber: r.asn, SSCC: "006141410000000012", ReceivedAt: r.now,
		Contents: []contract.ReceivedContent{{GTIN: "00614141000036", Qty: 12, Condition: "Good"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.repo.RecordCartonReceived(ctx, "c2"+suffix, contract.ReceivedCartonFact{ReceiptID: "rcpt-1", AsnNumber: r.asn, LPN: "LPN-1", ReceivedAt: r.now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := r.repo.RecordFinding(ctx, "f1"+suffix, contract.CartonFinding{ReceiptID: "rcpt-1", AsnNumber: r.asn, SSCC: "006141410000000012", Kind: "CaseQtyMismatch", Expected: "12", Actual: "11", At: r.now}); err != nil {
		t.Fatal(err)
	}
	if err := r.repo.RecordClosed(ctx, "z1"+suffix, contract.ReceiptClosedFact{ReceiptID: "rcpt-1", AsnNumber: r.asn, ClosedAt: r.now}, r.now); err != nil {
		t.Fatal(err)
	}
}

func TestReceiptProjection_ACloseCarriesTheCartonsAndFindingsOfItsAsn(t *testing.T) {
	rig := newReceiptRig(t)
	rig.closeWithFacts(t, "")
	due, err := rig.repo.DueReceipts(context.Background(), rig.now, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("due = %d, %v", len(due), err)
	}
	f := due[0]
	if f.Shipment.Ref != rig.ref || f.ReceiptID != "rcpt-1" || !f.ClosedAt.Equal(rig.now) || len(f.Cartons) != 2 || len(f.Findings) != 1 {
		t.Fatalf("facts = %+v", f)
	}
	if c := f.Cartons[0]; c.SSCC != "006141410000000012" || c.Contents[0].Qty != 12 || c.Contents[0].Condition != "Good" {
		t.Errorf("carton = %+v", c)
	}
	if c := f.Cartons[1]; c.SSCC != "" || c.LPN != "LPN-1" {
		t.Errorf("labelless carton = %+v: keyed by its LPN", c)
	}
	if fd := f.Findings[0]; fd.Kind != "CaseQtyMismatch" || fd.Expected != "12" || fd.Actual != "11" {
		t.Errorf("finding = %+v", fd)
	}
}

func TestReceiptProjection_AReplayOfEveryEventChangesNothing(t *testing.T) {
	rig := newReceiptRig(t)
	rig.closeWithFacts(t, "")
	rig.closeWithFacts(t, "") // the same CloudEvents ids again: a redelivery
	due, _ := rig.repo.DueReceipts(context.Background(), rig.now, 10)
	if len(due) != 1 || len(due[0].Cartons) != 2 || len(due[0].Findings) != 1 {
		t.Fatalf("after a replay: %+v", due)
	}
}

func TestReceiptProjection_AnAsnThatIsNotOursIsDroppedWithoutError(t *testing.T) {
	rig := newReceiptRig(t)
	ctx := context.Background()
	if err := rig.repo.RecordClosed(ctx, "z-other", contract.ReceiptClosedFact{ReceiptID: "r", AsnNumber: "SOMEONE-ELSES-ASN", ClosedAt: rig.now}, rig.now); err != nil {
		t.Fatal(err)
	}
	if err := rig.repo.RecordFinding(ctx, "f-other", contract.CartonFinding{AsnNumber: "SOMEONE-ELSES-ASN", Kind: "UnexpectedCarton"}); err != nil {
		t.Fatal(err)
	}
	if due, _ := rig.repo.DueReceipts(ctx, rig.now, 10); len(due) != 0 {
		t.Errorf("due = %d, want none", len(due))
	}
}

func TestReceiptProjection_TheFirstCloseWinsAndTheQueueFollowsTheStatus(t *testing.T) {
	rig := newReceiptRig(t)
	ctx := context.Background()
	rig.closeWithFacts(t, "")
	if err := rig.repo.RecordClosed(ctx, "z2", contract.ReceiptClosedFact{ReceiptID: "rcpt-2", AsnNumber: rig.asn, ClosedAt: rig.now.Add(time.Hour)}, rig.now); err != nil {
		t.Fatal(err)
	}
	due, _ := rig.repo.DueReceipts(ctx, rig.now, 10)
	if len(due) != 1 || due[0].ReceiptID != "rcpt-1" {
		t.Fatalf("a second close replaced the first: %+v", due)
	}
	if err := rig.repo.MarkReceiptRetry(ctx, rig.asn, "network down", rig.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if due, _ = rig.repo.DueReceipts(ctx, rig.now, 10); len(due) != 0 {
		t.Fatal("a receipt in backoff is due")
	}
	due, _ = rig.repo.DueReceipts(ctx, rig.now.Add(2*time.Minute), 10)
	if len(due) != 1 || due[0].Attempts != 1 {
		t.Fatalf("after the backoff: %+v", due)
	}
	if err := rig.repo.MarkReceiptSent(ctx, rig.asn, rig.now); err != nil {
		t.Fatal(err)
	}
	if err := rig.repo.MarkReceiptRetry(ctx, rig.asn, "late", rig.now); err != nil {
		t.Fatal(err)
	}
	if due, _ = rig.repo.DueReceipts(ctx, rig.now.Add(time.Hour), 10); len(due) != 0 {
		t.Fatal("a SENT receipt became due again")
	}
}

func TestReceiptProjection_ARejectedReceiptLeavesTheQueue(t *testing.T) {
	rig := newReceiptRig(t)
	ctx := context.Background()
	rig.closeWithFacts(t, "")
	if err := rig.repo.MarkReceiptRejected(ctx, rig.asn, "carton not on the manifest"); err != nil {
		t.Fatal(err)
	}
	if due, _ := rig.repo.DueReceipts(ctx, rig.now.Add(time.Hour), 10); len(due) != 0 {
		t.Fatal("a REJECTED receipt is still due")
	}
}
