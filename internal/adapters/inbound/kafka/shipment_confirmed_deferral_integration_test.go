//go:build integration

package kafka_test

import (
	"context"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The deferral half of TestShipmentConfirmedConsumer_RoundTripKafkaPostgres
// (ADR 0014 addendum D7): one order whose load departs while it is SUBMITTED
// (po-2, deferred and later confirmed) and one REJECTED (po-3, dead-lettered).

// seedDeferralOrders stores po-2 and po-3 next to the round trip's po-1.
func seedDeferralOrders(t *testing.T, ctx context.Context, orders *postgres.NetworkOrderRepo) {
	t.Helper()
	// po-2 is still SUBMITTED when
	// its load departs (the race: the shipment is recorded, not dead-lettered),
	// po-3 is REJECTED (a shipped order the network never took: dead-lettered).
	for ref, seed := range map[shared.NetworkRef]struct {
		local string
		state networkorder.State
	}{
		"po-2": {"ord-88422", networkorder.StateSubmitted},
		"po-3": {"ord-88423", networkorder.StateRejected},
	} {
		id := shared.LocalOrderId(seed.local)
		o := networkorder.Rehydrate(ref, "site-1",
			time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
			time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC),
			time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC),
			nil, seed.state, &id)
		if err := orders.Save(ctx, o); err != nil {
			t.Fatalf("seed %s: %v", ref, err)
		}
	}
}

// deferralMessages are po-2's early ShipmentConfirmed and po-3's, in that order.
func deferralMessages(t *testing.T) []kafkago.Message {
	t.Helper()
	return []kafkago.Message{
		{Key: []byte("load-9b1c"), Value: shipmentConfirmedFor(t, "evt-early", "load-9b1c", "ord-88422", 2)},
		{Key: []byte("load-5d2e"), Value: shipmentConfirmedFor(t, "evt-anomaly", "load-5d2e", "ord-88423", 0)},
	}
}

// waitForPendingShipment waits until the order carries a pending shipment, or
// stops the consumer and fails at the deadline.
func waitForPendingShipment(t *testing.T, ctx context.Context, orders *postgres.NetworkOrderRepo, ref shared.NetworkRef, deadline time.Time, stop func()) {
	t.Helper()
	for {
		o, err := orders.FindByRef(ctx, ref)
		if err != nil || o == nil {
			stop()
			t.Fatalf("FindByRef %s: %v %v", ref, o, err)
		}
		if _, ok := o.PendingShipment(); ok {
			return
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("%s carries no pending shipment within the deadline (state %v)", ref, o.State())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// readDeadLetters reads n dead letters off the DLQ topic BEFORE the consumer
// stops, so the wait is on the topic and not on a sleep. The caller closes the
// reader.
func readDeadLetters(t *testing.T, ctx context.Context, brokers []string, dlqTopic string, n int, stop func()) (*kafkago.Reader, []kafkago.Message) {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: dlqTopic, Partition: 0,
		MinBytes: 1, MaxBytes: 10e6,
	})
	var msgs []kafkago.Message
	for range n {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		m, err := reader.ReadMessage(readCtx)
		cancel()
		if err != nil {
			stop()
			_ = reader.Close()
			t.Fatalf("read DLQ message %d (the malformed one and po-3's must be dead-lettered): %v", len(msgs)+1, err)
		}
		msgs = append(msgs, m)
	}
	return reader, msgs
}

// assertDeferredShipmentSettles checks what the consumer did with po-2 and po-3
// and then plays the rest of the flow: the network settles po-2's
// acknowledgement and the confirm-pending pass confirms it, once.
func assertDeferredShipmentSettles(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orders *postgres.NetworkOrderRepo,
	confirm *usecases.ConfirmNetworkOrderShipment, dlqReader *kafkago.Reader, dlqMsgs []kafkago.Message) {
	t.Helper()
	var outboxRows int

	// po-3 (REJECTED) was a genuine anomaly: dead-lettered
	// with the confirm error, verbatim payload for replay.
	if !strings.Contains(string(dlqMsgs[1].Value), "load-5d2e") {
		t.Errorf("second DLQ payload = %s, want po-3's ShipmentConfirmed (load-5d2e)", dlqMsgs[1].Value)
	}
	var dlqErr string
	for _, h := range dlqMsgs[1].Headers {
		if h.Key == "x-dlq-error" {
			dlqErr = string(h.Value)
		}
	}
	if !strings.Contains(dlqErr, "cannot confirm shipment before acknowledgement") {
		t.Errorf("x-dlq-error = %q, want the confirm-before-acknowledge error", dlqErr)
	}
	// po-2's early shipment produced NO dead letter: nothing else is on the topic.
	extraCtx, extraCancel := context.WithTimeout(ctx, 3*time.Second)
	defer extraCancel()
	if m, err := dlqReader.ReadMessage(extraCtx); err == nil {
		t.Errorf("a third dead letter exists (%s): po-2's early shipment must be committed, not dead-lettered", m.Value)
	}

	// po-2 waits SUBMITTED with the shipment recorded, and nothing was
	// confirmed or published for it yet.
	po2, _ := orders.FindByRef(ctx, "po-2")
	if pending, ok := po2.PendingShipment(); !ok || pending.ShipmentRef != "load-9b1c" || pending.PackageCount != 2 || po2.State() != networkorder.StateSubmitted {
		t.Fatalf("po-2 = %v pending %+v ok=%v, want SUBMITTED carrying {load-9b1c 2}", po2.State(), pending, ok)
	}
	var po2Ledger int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM shipment_confirmations WHERE order_id = 'ord-88422'`).Scan(&po2Ledger); err != nil || po2Ledger != 0 {
		t.Fatalf("po-2 ledger rows = %d, %v, want 0 before it is acknowledged", po2Ledger, err)
	}

	// The network settles the acknowledgement; the confirm-pending pass
	// confirms the order once, through the real ledger and outbox.
	if err := po2.ConfirmAcknowledgement(); err != nil {
		t.Fatalf("ConfirmAcknowledgement: %v", err)
	}
	if err := orders.Save(ctx, po2); err != nil {
		t.Fatalf("save po-2 ACKNOWLEDGED: %v", err)
	}
	pass := &usecases.ConfirmPendingShipments{Orders: orders, Confirm: confirm, UnitOfWork: postgres.NewUnitOfWork(pool)}
	for range 2 { // the second pass must find nothing to do
		if _, err := pass.Execute(ctx); err != nil {
			t.Fatalf("ConfirmPendingShipments: %v", err)
		}
	}
	po2, _ = orders.FindByRef(ctx, "po-2")
	if _, ok := po2.PendingShipment(); ok || po2.State() != networkorder.StateConfirmed {
		t.Fatalf("po-2 after the pass = %v (pending cleared: %v), want CONFIRMED with the fact used", po2.State(), !ok)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM shipment_confirmations WHERE shipment_ref = 'load-9b1c' AND order_id = 'ord-88422'`).Scan(&po2Ledger); err != nil || po2Ledger != 1 {
		t.Errorf("po-2 ledger rows = %d, %v, want exactly 1", po2Ledger, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE event_type LIKE '%NetworkOrderShipmentConfirmed' AND topic = 'warehouse.network-fulfillment.events'
	`).Scan(&outboxRows); err != nil || outboxRows != 2 {
		t.Errorf("NetworkOrderShipmentConfirmed outbox rows = %d, %v, want 2 (po-1 and po-2, once each)", outboxRows, err)
	}
}

// shipmentConfirmedFor is shipmentConfirmedFixture for one load carrying one
// order, with an optional per-order package count.
func shipmentConfirmedFor(t *testing.T, id, loadId, orderId string, packageCount int) []byte {
	t.Helper()
	order := map[string]any{"order_id": orderId}
	if packageCount > 0 {
		order["package_count"] = packageCount
	}
	e := ce.New("1.0")
	e.SetID(id)
	e.SetSource("/warehouse/outbound-shipping")
	e.SetType("com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed")
	e.SetSubject(loadId)
	e.SetTime(time.Date(2026, 11, 6, 16, 2, 0, 0, time.UTC))
	if err := e.SetData("application/json", map[string]any{
		"load_id":     loadId,
		"site_id":     "SITE-1",
		"cpt_id":      "cpt-1600",
		"carrier":     "STDOUT",
		"trailer_id":  "trl-91",
		"seal_number": "SL-4471",
		"departed_at": "2026-11-06T16:01:30Z",
		"orders":      []map[string]any{order},
		"version":     6,
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
