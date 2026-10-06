//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/analytics/report"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// TestAnalyticsProjector_AcknowledgedV1ReplayAndV2Settle_KafkaAndPostgres
// is the ADR 0016 projector acceptance test, end to end against REAL
// Kafka and Postgres (testcontainers): the topic carries
//
//   - a HISTORIC NetworkOrderAcknowledged v1 message (the exact bytes the
//     service published before the change, fixture testdata/historic-v1;
//     v1 meant "submitted") on 2026-09-23,
//   - and, produced by the real outbound AnalyticsPublisher, the new
//     lifecycle of one order on 2026-10-06: Received, Submitted and the
//     settle NetworkOrderAcknowledged v2.
//
// Projected by the real consumer into the real projection, the report
// must count one acknowledgement per day with the right latency
// (Submitted contributes nothing). Then the read model is wiped and the
// whole topic is replayed by a brand-new consumer group: the numbers must
// be identical — "replay keeps giving the same report numbers".
func TestAnalyticsProjector_AcknowledgedV1ReplayAndV2Settle_KafkaAndPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("nf-projector-v1-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kafkaC) })
	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve brokers: %v", err)
	}

	pgC, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("networkfulfillment_analytics"),
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
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	topic := fmt.Sprintf("warehouse.network-fulfillment.analytics.v2-itest-%d", time.Now().UnixNano())
	createTopic(t, ctx, brokers, topic)
	createTopic(t, ctx, brokers, topic+".dlq")

	// --- the topic's content ---
	historicV1, err := os.ReadFile(filepath.Join("testdata", "historic-v1", "NetworkOrderAcknowledged.analytics.json"))
	if err != nil {
		t.Fatalf("read historic v1 fixture: %v", err)
	}
	received := time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)
	seq := 0
	enc := outboundkafka.NewAnalyticsPublisher(nil, func() string { seq++; return fmt.Sprintf("evt-v2-%d", seq) })
	encoded, err := enc.Encode(ctx,
		shared.NetworkOrderReceived{NetworkRef: "po-2", SiteId: "site-1", LineCount: 1, At: received},
		shared.NetworkOrderSubmitted{NetworkRef: "po-2", SiteId: "site-1", LocalOrderId: "ord-2", ReceivedAt: received, At: received.Add(time.Minute)},
		shared.NetworkOrderAcknowledged{NetworkRef: "po-2", SiteId: "site-1", LocalOrderId: "ord-2", ReceivedAt: received, At: received.Add(5 * time.Minute)},
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}}
	t.Cleanup(func() { _ = writer.Close() })
	msgs := []kafkago.Message{{Key: []byte("po-1"), Value: historicV1}}
	for _, e := range encoded {
		msgs = append(msgs, kafkago.Message{Key: e.Key, Value: e.Value, Headers: e.Headers})
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("publish topic content: %v", err)
	}

	reader := analyticsstore.NewPostgresReport(pool)
	queryAll := func() map[time.Time]report.Row {
		rep, err := reader.Query(ctx, report.ReportQuery{
			From:        time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
			Granularity: report.GranularityDay,
		})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		out := map[time.Time]report.Row{}
		for _, r := range rep.Rows {
			out[r.Key.DayBucket] = r
		}
		return out
	}
	satisfied := func(rows map[time.Time]report.Row) bool {
		v1Day := rows[time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)]
		v2Day := rows[time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)]
		return v1Day.OrdersAcknowledged == 1 && v2Day.OrdersAcknowledged == 1 && v2Day.OrdersReceived == 1
	}

	runUntilProjected := func(label string) map[time.Time]report.Row {
		t.Helper()
		consumer := inboundkafka.NewAnalyticsConsumer(brokers, topic,
			inboundkafka.NewUniqueConsumerGroup("network-fulfillment-analytics-v2-itest-"+label),
			analyticsstore.NewPostgresProjection(pool), analyticsstore.NewConsumedEventsRepo(pool), nil)
		defer func() { _ = consumer.Close() }()
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- consumer.Run(runCtx) }()
		defer func() {
			stop()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("consumer did not stop")
			}
		}()

		deadline := time.Now().Add(90 * time.Second)
		for {
			rows := queryAll()
			if satisfied(rows) {
				// Give a stray duplicate/extra count a moment to show up.
				time.Sleep(2 * time.Second)
				return queryAll()
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: report never reached the expected counts; rows=%+v", label, rows)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	assertNumbers := func(label string, rows map[time.Time]report.Row) {
		t.Helper()
		v1Day := rows[time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)]
		if v1Day.OrdersAcknowledged != 1 || v1Day.AcknowledgementLatencyCount != 1 || v1Day.SumAcknowledgementLatencySeconds != 60 {
			t.Errorf("%s: historic v1 day = %+v, want 1 acknowledgement with 60s latency", label, v1Day)
		}
		v2Day := rows[time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)]
		if v2Day.OrdersReceived != 1 || v2Day.OrdersAcknowledged != 1 || v2Day.AcknowledgementLatencyCount != 1 || v2Day.SumAcknowledgementLatencySeconds != 300 {
			t.Errorf("%s: v2 day = %+v, want 1 received, 1 acknowledgement (Submitted adds none) with 300s receipt->settle latency", label, v2Day)
		}
	}

	first := runUntilProjected("first")
	assertNumbers("first pass", first)

	// Rebuild the read model from nothing and replay the whole topic with
	// a brand-new consumer group.
	for _, table := range []string{"acknowledgement_rollup", "analytics_processed_events", "analytics_consumed_events"} {
		if _, err := pool.Exec(ctx, "TRUNCATE "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	replayed := runUntilProjected("replay")
	assertNumbers("replay", replayed)
}
