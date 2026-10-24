//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
)

// The receipt return (ADR 0028 step 3) through the real entry point: events that
// inbound-receiving would publish are produced onto a real Kafka topic, this
// service's own consumer group projects them into Postgres, and the vendor
// receipt reaches the (fake) Vendor API exactly once.

const inCartX = "006141410000000043"

// inboundReceivingEvent builds a CloudEvents message the way inbound-receiving
// publishes it: its own type and source, snake_case data.
func inboundReceivingEvent(t *testing.T, id, typ string, data any) []byte {
	t.Helper()
	e := ce.New("1.0")
	e.SetID(id)
	e.SetSource("/warehouse/inbound-receiving")
	e.SetType(typ)
	e.SetSubject("rcpt-1")
	e.SetTime(time.Now().UTC())
	if err := e.SetData("application/json", data); err != nil {
		t.Fatal(err)
	}
	raw, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (r *inboundRig) produce(t *testing.T, ctx context.Context, key string, values ...[]byte) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(r.env.brokers...), Topic: inboundkafka.ReceiptTopic, Balancer: &kafkago.Hash{}, RequiredAcks: kafkago.RequireAll}
	defer func() { _ = w.Close() }()
	msgs := make([]kafkago.Message, len(values))
	for i, v := range values {
		msgs[i] = kafkago.Message{Key: []byte(key), Value: v}
	}
	e2eWaitFor(t, ctx, 60*time.Second, "the broker accepts the receipt events", func() bool { return w.WriteMessages(ctx, msgs...) == nil })
}

// closedReceiptEvents is a receipt of SHP-R: carton A never arrived (MissingCarton
// at close), carton B arrived with one damaged GTIN and over the limit, and a
// carton off the manifest was flagged and one was accepted.
func closedReceiptEvents(t *testing.T, asn string) [][]byte {
	t.Helper()
	const typ = "com.warehouse.wms.inbound-receiving.receipt."
	return [][]byte{
		inboundReceivingEvent(t, "ev-carton-b", typ+"ReceiptCartonReceived", map[string]any{
			"receipt_id": "rcpt-1", "asn_number": asn, "sscc": inCartB, "sealed": true, "lpn": inCartB, "received_at": time.Now().UTC(),
			"contents": []map[string]any{
				{"line_no": 1, "sku": "SKU-A", "gtin": inGtinA, "qty": 6, "condition": "Good"},
				{"line_no": 2, "sku": "SKU-B", "gtin": inGtinB, "qty": 4, "condition": "Damaged"},
			},
		}),
		inboundReceivingEvent(t, "ev-over-limit", typ+"CartonDiscrepancyRaised", map[string]any{
			"receipt_id": "rcpt-1", "sscc": inCartB, "kind": "CartonOverLimit", "expected": "grossWeightG:1000", "actual": "grossWeightG:2100", "at": time.Now().UTC(),
		}),
		inboundReceivingEvent(t, "ev-unexpected", typ+"CartonDiscrepancyRaised", map[string]any{
			"receipt_id": "rcpt-1", "sscc": inCartX, "kind": "UnexpectedCarton", "at": time.Now().UTC(),
		}),
		inboundReceivingEvent(t, "ev-missing", typ+"CartonDiscrepancyRaised", map[string]any{
			"receipt_id": "rcpt-1", "kind": "MissingCarton", "expected": inCartA, "at": time.Now().UTC(),
		}),
		inboundReceivingEvent(t, "ev-closed", typ+"ReceiptClosed", map[string]any{
			"receipt_id": "rcpt-1", "asn_number": asn, "closed_at": "2026-10-25T09:30:00Z", "discrepancies": []any{},
		}),
		inboundReceivingEvent(t, "ev-closed", typ+"ReceiptClosed", map[string]any{ // the same id again: a redelivery
			"receipt_id": "rcpt-1", "asn_number": asn, "closed_at": "2026-10-25T09:30:00Z", "discrepancies": []any{},
		}),
	}
}

func (r *inboundRig) receiptStatus(t *testing.T, ctx context.Context, asn string) string {
	t.Helper()
	return r.text(t, ctx, `SELECT status FROM inbound_receipts WHERE asn_number = $1`, asn)
}

// TestInboundReceipt_ClosedReceiptIsReportedExactlyOnce: from ReceiptClosed (and
// the carton events before it) to POST /vendor/inbound-receipts, once, whatever
// is redelivered, restarted or produced for another ASN.
func TestInboundReceipt_ClosedReceiptIsReportedExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startInboundRig(t, ctx)
	rig.master.declare(inGtinA, "SKU-A", "EACH", 1)
	rig.master.declare(inGtinB, "SKU-B", "EACH", 1)
	rig.inbound.ship(shipmentTree("SHP-R"))
	stop := rig.start(t, ctx, nil)
	e2eWaitFor(t, ctx, 90*time.Second, "SHP-R is REGISTERED", func() bool { return rig.shipmentStatus(t, ctx, "SHP-R") == "REGISTERED" })

	// An ASN that is not ours is dropped; ours is projected, then closed.
	rig.produce(t, ctx, "SOMEONE-ELSES-ASN", inboundReceivingEvent(t, "ev-foreign", "com.warehouse.wms.inbound-receiving.receipt.ReceiptClosed",
		map[string]any{"receipt_id": "rcpt-9", "asn_number": "SOMEONE-ELSES-ASN", "closed_at": "2026-10-25T09:30:00Z"}))
	rig.produce(t, ctx, "NF-SHP-R", closedReceiptEvents(t, "NF-SHP-R")...)

	e2eWaitFor(t, ctx, 90*time.Second, "the receipt is SENT", func() bool { return rig.receiptStatus(t, ctx, "NF-SHP-R") == "SENT" })

	t.Run("one receipt with the statuses, items and compliance codes", func(t *testing.T) {
		expectEqual(t, "POST /vendor/inbound-receipts", rig.inbound.receiptPosts(), 1)
		var body struct {
			VendorID   string `json:"vendorId"`
			ShipmentID string `json:"shipmentId"`
			NodeID     string `json:"nodeId"`
			ReceivedAt string `json:"receivedAt"`
			Cartons    []struct {
				SSCC   string `json:"sscc"`
				Status string `json:"status"`
				Items  []struct {
					GTIN      string `json:"gtin"`
					QtyEaches int64  `json:"qtyEaches"`
				} `json:"items"`
				ComplianceCodes []string `json:"complianceCodes"`
			} `json:"cartons"`
		}
		if err := json.Unmarshal([]byte(rig.inbound.receiptBody("SHP-R")), &body); err != nil {
			t.Fatal(err)
		}
		expectEqual(t, "vendorId", body.VendorID, liveVendorID)
		expectEqual(t, "shipmentId", body.ShipmentID, "SHP-R")
		expectEqual(t, "nodeId", body.NodeID, "N-1")
		expectEqual(t, "receivedAt is the close", body.ReceivedAt, "2026-10-25T09:30:00Z")
		got := map[string]string{}
		for _, c := range body.Cartons {
			got[c.SSCC] = fmt.Sprintf("%s items=%v codes=%v", c.Status, c.Items, c.ComplianceCodes)
		}
		expectEqual(t, "cartons reported", len(body.Cartons), 3)
		expectEqual(t, "carton A (never arrived)", got[inCartA], "MISSING items=[] codes=[]")
		expectEqual(t, "carton B (damaged, over the limit)", got[inCartB], fmt.Sprintf("DAMAGED items=[{%s 6} {%s 4}] codes=[CARTON_OVER_LIMIT]", inGtinA, inGtinB))
		expectEqual(t, "carton X (off the manifest)", got[inCartX], "UNEXPECTED items=[] codes=[]")
	})

	t.Run("the transaction is recorded in the ledger and the projection ignores foreign ASNs", func(t *testing.T) {
		expectEqual(t, "ledger status", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'INBOUND_RECEIPT' AND subject = 'SHP-R'`), "SUCCESS")
		if tx := rig.text(t, ctx, `SELECT transaction_id FROM network_submissions WHERE kind = 'INBOUND_RECEIPT' AND subject = 'SHP-R'`); !strings.HasPrefix(tx, "tx-rcpt-") {
			t.Fatalf("transaction id = %q", tx)
		}
		expectEqual(t, "idempotency key", rig.text(t, ctx, `SELECT idempotency_key FROM network_submissions WHERE kind = 'INBOUND_RECEIPT' AND subject = 'SHP-R'`), "nf-rcpt-SHP-R")
		expectEqual(t, "applied events (the redelivered close counts once, the foreign one not at all)", rig.count(t, ctx, `SELECT count(*) FROM inbound_receipt_events`), 5)
		expectEqual(t, "projected findings", rig.count(t, ctx, `SELECT count(*) FROM inbound_receipt_findings`), 3)
	})

	t.Run("a restart reports nothing twice", func(t *testing.T) {
		mustStop(t, stop)
		polls := len(rig.inbound.pollQueries())
		stop = rig.start(t, ctx, nil)
		e2eWaitFor(t, ctx, 60*time.Second, "three more intake passes after the restart", func() bool { return len(rig.inbound.pollQueries()) >= polls+3 })
		expectEqual(t, "POST /vendor/inbound-receipts after the restart", rig.inbound.receiptPosts(), 1)
	})
	mustStop(t, stop)
}

// TestInboundReceipt_ARefusalByTheNetworkParksTheReceipt: a receipt the network
// fails for good is REJECTED, logged, and not sent again.
func TestInboundReceipt_ARefusalByTheNetworkParksTheReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startInboundRig(t, ctx)
	rig.master.declare(inGtinA, "SKU-A", "EACH", 1)
	rig.master.declare(inGtinB, "SKU-B", "EACH", 1)
	rig.inbound.failReceipts(map[string]string{"code": "ErrCartonNotOnManifest", "message": "no such carton"})
	rig.inbound.ship(shipmentTree("SHP-F"))
	stop := rig.start(t, ctx, nil)
	e2eWaitFor(t, ctx, 90*time.Second, "SHP-F is REGISTERED", func() bool { return rig.shipmentStatus(t, ctx, "SHP-F") == "REGISTERED" })
	rig.produce(t, ctx, "NF-SHP-F", closedReceiptEvents(t, "NF-SHP-F")...)

	e2eWaitFor(t, ctx, 90*time.Second, "the receipt is REJECTED", func() bool { return rig.receiptStatus(t, ctx, "NF-SHP-F") == "REJECTED" })
	if msg := rig.text(t, ctx, `SELECT last_error FROM inbound_receipts WHERE asn_number = 'NF-SHP-F'`); !strings.Contains(msg, "no such carton") {
		t.Fatalf("last_error = %q, want the network's error", msg)
	}
	posts, polls := rig.inbound.receiptPosts(), len(rig.inbound.pollQueries())
	e2eWaitFor(t, ctx, 60*time.Second, "three more passes", func() bool { return len(rig.inbound.pollQueries()) >= polls+3 })
	expectEqual(t, "POSTs after the verdict", rig.inbound.receiptPosts(), posts)
	mustStop(t, stop)
}
