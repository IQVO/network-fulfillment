//go:build integration

// Round trip of the ADR 0014 addendum's event path against the REAL stack
// (testcontainers): a fixture outbound-shipping ShipmentConfirmed produced
// onto a real Kafka topic, consumed by the real ShipmentConfirmedConsumer
// under its own group, confirming an ACKNOWLEDGED NetworkOrder through the
// real Postgres NetworkOrderRepo, the real UnitOfWork, the real
// transactional-outbox publisher and the real (shipment-ref, order-id)
// ShipmentConfirmationLog — with the same event produced TWICE (at-least-once
// redelivery) and a malformed message ahead of it that must land on the
// dead-letter topic without blocking the confirmation.
package kafka_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// fixedClock is the smallest ports.Clock: one instant, forever.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func TestShipmentConfirmedConsumer_RoundTripKafkaPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("nf-shipment-confirmed-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kafkaC) })
	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve brokers: %v", err)
	}

	pgC, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("networkfulfillment_shipment"),
		tcpostgres.WithUsername("networkfulfillment"),
		tcpostgres.WithPassword("networkfulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pgC) })
	url, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	// The order the load carried: ACKNOWLEDGED, its demand recorded as
	// order-management order ord-88421 — the exact join the event makes.
	orders := postgres.NewNetworkOrderRepo(pool)
	localOrderId := shared.LocalOrderId("ord-88421")
	seeded := networkorder.Rehydrate(
		"po-1", "site-1",
		time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC),
		nil, networkorder.StateAcknowledged, &localOrderId,
	)
	if err := orders.Save(ctx, seeded); err != nil {
		t.Fatalf("seed order: %v", err)
	}

	// The ONE confirm use case, wired exactly like the composition root
	// (wireConfirmShipment): real ledger, real unit of work, real outbox
	// publisher — the stub network gateway is this service's production
	// default (ADR 0009 §4).
	seq := 0
	confirm := &usecases.ConfirmNetworkOrderShipment{
		Orders:        orders,
		Gateway:       retailnetwork.NewStubGateway(nil),
		Events:        postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisher(brokers, func() string { seq++; return fmt.Sprintf("evt-itest-%d", seq) }), outboundkafka.NewAnalyticsPublisher(brokers, func() string { seq++; return fmt.Sprintf("evt-itest-%d", seq) })),
		Clock:         fixedClock{time.Date(2026, 11, 6, 16, 5, 0, 0, time.UTC)},
		UnitOfWork:    postgres.NewUnitOfWork(pool),
		Confirmations: postgres.NewShipmentConfirmationLog(pool),
	}

	topic := fmt.Sprintf("warehouse.outbound-shipping.events.itest-%d", time.Now().UnixNano())
	createTopic(t, ctx, brokers, topic)
	createTopic(t, ctx, brokers, topic+".dlq")

	// The topic's content: a malformed message first (not a CloudEvent),
	// then the fixture ShipmentConfirmed — produced TWICE, because
	// delivery is at-least-once and the second copy must change nothing.
	eventBytes := shipmentConfirmedFixture(t)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}}
	t.Cleanup(func() { _ = writer.Close() })
	msgs := []kafkago.Message{
		{Key: []byte("load-7f3a"), Value: []byte(`{"not":"a cloudevent"}`)},
		{Key: []byte("load-7f3a"), Value: eventBytes},
		{Key: []byte("load-7f3a"), Value: eventBytes},
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Run the real consumer under its own group until the work is done.
	runCtx, runCancel := context.WithCancel(ctx)
	consumer := inboundkafka.NewShipmentConfirmedConsumer(brokers, topic,
		fmt.Sprintf("nf-shipment-confirmed-itest-%d", time.Now().UnixNano()),
		orders, confirm, nil)
	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Run(runCtx) }()

	// Wait for the confirmation to land (bounded), then stop the consumer.
	deadline := time.Now().Add(60 * time.Second)
	for {
		o, err := orders.FindByRef(ctx, "po-1")
		if err != nil {
			t.Fatalf("FindByRef: %v", err)
		}
		if o != nil && o.State() == networkorder.StateConfirmed {
			break
		}
		if time.Now().After(deadline) {
			runCancel()
			t.Fatalf("order po-1 not CONFIRMED within 60s (state %v)", o.State())
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Give the consumer a moment to commit past the messages, then stop.
	time.Sleep(2 * time.Second)
	runCancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("consumer Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not stop within 30s")
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("consumer Close: %v", err)
	}

	// Exactly once, on the real stack: one ledger row, one outbox event on
	// the integration topic (the outbox fans each event out to the
	// analytics topic too — a second row there is the fan-out, not a
	// second event).
	var ledgerRows, outboxRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM shipment_confirmations`).Scan(&ledgerRows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if ledgerRows != 1 {
		t.Errorf("shipment_confirmations rows = %d, want 1 (redelivery claims nothing)", ledgerRows)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE event_type = 'NetworkOrderShipmentConfirmed' AND topic = 'warehouse.network-fulfillment.events'
	`).Scan(&outboxRows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxRows != 1 {
		t.Errorf("NetworkOrderShipmentConfirmed outbox rows = %d, want 1 (redelivery publishes nothing)", outboxRows)
	}
	var confirmedRef string
	if err := pool.QueryRow(ctx, `SELECT network_ref FROM shipment_confirmations WHERE shipment_ref = 'load-7f3a' AND order_id = 'ord-88421'`).Scan(&confirmedRef); err != nil {
		t.Fatalf("ledger row keyed (load-7f3a, ord-88421): %v", err)
	}
	if confirmedRef != "po-1" {
		t.Errorf("ledger network_ref = %q, want po-1", confirmedRef)
	}

	// The malformed message is on the dead-letter topic, committed past,
	// and did not block the confirmation behind it.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic + ".dlq", Partition: 0,
		MinBytes: 1, MaxBytes: 10e6,
	})
	defer func() { _ = dlqReader.Close() }()
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message (the malformed one must be dead-lettered): %v", err)
	}
	if string(dlqMsg.Value) != `{"not":"a cloudevent"}` {
		t.Errorf("DLQ payload = %s, want the raw malformed bytes for manual replay", dlqMsg.Value)
	}
	for _, h := range dlqMsg.Headers {
		if h.Key == "x-dlq-source-topic" && string(h.Value) != topic {
			t.Errorf("DLQ x-dlq-source-topic = %s, want %s", h.Value, topic)
		}
	}
}

// shipmentConfirmedFixture is outbound-shipping's own example event,
// byte-shaped per its merged contract (IQVO/outbound-shipping PR #2).
func shipmentConfirmedFixture(t *testing.T) []byte {
	t.Helper()
	e := ce.New("1.0")
	e.SetID("3f1d2c3b-4a5e-4b7c-8d9e-0a1b2c3d4e07")
	e.SetSource("/warehouse/outbound-shipping")
	e.SetType("com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed")
	e.SetSubject("load-7f3a")
	e.SetTime(time.Date(2026, 11, 6, 16, 2, 0, 0, time.UTC))
	if err := e.SetData("application/json", map[string]any{
		"load_id":     "load-7f3a",
		"site_id":     "SITE-1",
		"cpt_id":      "cpt-1600",
		"carrier":     "STDOUT",
		"trailer_id":  "trl-91",
		"seal_number": "SL-4471",
		"departed_at": "2026-11-06T16:01:30Z",
		"orders":      []map[string]any{{"order_id": "ord-88421"}},
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
