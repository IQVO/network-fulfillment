//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	// pgtest boots this package's Postgres through testcontainers-go; the
	// blank import keeps that obligation visible in the package that relies
	// on it (internal/architecture's
	// TestPostgresIntegrationTestsUseTestcontainers checks exactly this).
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/analytics/report"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	"github.com/claudioed/network-fulfillment/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// analyticsMigrationsDir resolves the repository's analytics migrations
// directory relative to THIS file, so the test works no matter which
// directory go test was invoked from (run() would otherwise resolve the
// same ANALYTICS_MIGRATIONS_PATH default relative to the package dir).
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve the test source file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}

// createTopic creates one topic on the fresh broker, resolving the
// controller first (the same recipe internal/adapters/inbound/kafka's own
// integration tests use).
func createTopic(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker %s: %v", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("resolve controller: %v", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer func() { _ = controllerConn.Close() }()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

// stopWithSIGTERM drives run()'s documented stop contract (run() takes no
// context; SIGTERM is its only stop signal) by signalling the test process
// itself, retrying until run() returns.
//
// The test's own handler was registered BEFORE run() started and is NEVER
// unregistered (no signal.Stop): run()'s internal signal.Notify channel
// stays registered for the rest of the process too, so a retry still in
// flight after run() returned can never fall back to Go's default SIGTERM
// disposition and kill the test binary — the failure mode the fleet's
// product-master PR #98 first CI run hit.
func stopWithSIGTERM(t *testing.T, done <-chan error) error {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			return err
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("run() did not return after SIGTERM")
	return nil
}

// TestProjectorBootsMigratesProjectsAndStopsCleanly proves the WRITER
// composition root of the "Network Order Acknowledgement & Translation"
// data product end to end (ADR 0002 section "A separate analytics topic,
// projector, and reports binary"):
//
//   - run() boots against an EMPTY analytical Postgres (pgtest.EmptyURL)
//     and runs the analytics migrations itself — the projector owns the
//     schema (cmd/netfulfil-projector/main.go: "The projector owns the
//     analytical schema: run its migrations on start").
//   - Its only input is the fixed analytics topic (outboundkafka.
//     AnalyticsTopic) on a real Kafka (testcontainers); the full lifecycle
//     of one order — Received, Submitted, Acknowledged — is produced by
//     the repo's OWN real encoder (AnalyticsPublisher.Encode) BEFORE the
//     projector starts, proving the fresh-per-instance consumer group
//     (ADR 0002: "any projector instance in this fleet must therefore be
//     able to replay from the beginning of its topic") replays it.
//   - The documented projection answers: the acknowledgement report row
//     for that day counts 1 order received and 1 acknowledged with a
//     300s receipt->settle latency, while the Submitted event contributes
//     nothing — acknowledgements count at SETTLE time (ADR 0016), applied
//     atomically with the dedupe claim (ADR 0023).
//   - /healthz answers 200 on the admin port (runbook: liveness and
//     readiness GET /healthz for cmd/netfulfil-projector).
//   - SIGTERM drains cleanly: run() returns nil (waitForStopOrConsumerDeath
//     maps a signal to a clean stop).
func TestProjectorBootsMigratesProjectsAndStopsCleanly(t *testing.T) {
	ctx := context.Background()

	url := pgtest.EmptyURL(t)

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("nf-proj-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kafkaC) })
	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve brokers: %v", err)
	}

	// The projector consumes the FIXED analytics topic; its DLQ is derived
	// as topic+".dlq". Create both before anything produces or consumes.
	createTopic(t, ctx, brokers, outboundkafka.AnalyticsTopic)
	createTopic(t, ctx, brokers, outboundkafka.AnalyticsTopic+".dlq")

	// The topic's content, encoded by the repo's own analytics encoder:
	// one order's lifecycle on a fixed day. NetworkOrderSubmitted is on
	// the topic on purpose — it must contribute NO counter (ADR 0016).
	received := time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC)
	seq := 0
	enc := outboundkafka.NewAnalyticsPublisher(brokers, func() string {
		seq++
		return fmt.Sprintf("nf-proj-itest-evt-%d", seq)
	})
	encoded, err := enc.Encode(ctx,
		shared.NetworkOrderReceived{NetworkRef: "po-proj-it-1", SiteId: "site-it", LineCount: 1, At: received},
		shared.NetworkOrderSubmitted{
			NetworkRef: "po-proj-it-1", SiteId: "site-it", LocalOrderId: "om-proj-it-1",
			ReceivedAt: received, At: received.Add(time.Minute),
		},
		shared.NetworkOrderAcknowledged{
			NetworkRef: "po-proj-it-1", SiteId: "site-it", LocalOrderId: "om-proj-it-1",
			ReceivedAt: received, At: received.Add(5 * time.Minute),
		},
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Balancer: &kafkago.Hash{}}
	t.Cleanup(func() { _ = writer.Close() })
	msgs := make([]kafkago.Message, 0, len(encoded))
	for _, e := range encoded {
		msgs = append(msgs, kafkago.Message{Topic: e.Topic, Key: e.Key, Value: e.Value, Headers: e.Headers})
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("publish the order lifecycle: %v", err)
	}

	// run()'s documented environment, exactly the names it reads.
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("KAFKA_BROKERS", brokers[0])
	t.Setenv("ADMIN_ADDR", freeAddr(t))
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", analyticsMigrationsDir(t))
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a
	// dial to localhost:4317 at shutdown (the same trio cmd/netfulfil's
	// e2e test sets).
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	adminAddr := os.Getenv("ADMIN_ADDR")

	// The test's SIGTERM handler is registered BEFORE run() starts and is
	// never unregistered — see stopWithSIGTERM.
	term := make(chan os.Signal, 16)
	signal.Notify(term, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run() }()

	waitUntil(t, "GET /healthz = 200 on the projector's admin port", func() bool {
		resp, err := http.Get("http://" + adminAddr + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The documented projection, read back through the repo's own reader.
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	reader := analyticsstore.NewPostgresReport(pool)
	waitUntil(t, "the projected acknowledgement report for 2026-11-06", func() bool {
		rep, err := reader.Query(ctx, report.ReportQuery{
			From:        time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC),
			Granularity: report.GranularityDay,
		})
		if err != nil || len(rep.Rows) != 1 {
			return false
		}
		row := rep.Rows[0]
		return row.OrdersReceived == 1 && row.OrdersAcknowledged == 1 &&
			row.AcknowledgementLatencyCount == 1 && row.SumAcknowledgementLatencySeconds == 300
	})
	// Give a stray duplicate count a moment to show up (the same settle
	// wait internal/adapters/inbound/kafka's projector acceptance test
	// uses), then pin the numbers.
	time.Sleep(2 * time.Second)
	rep, err := reader.Query(ctx, report.ReportQuery{
		From:        time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC),
		Granularity: report.GranularityDay,
	})
	if err != nil || len(rep.Rows) != 1 {
		t.Fatalf("acknowledgement report = (err %v, %d rows), want exactly 1 row", err, len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.OrdersReceived != 1 || row.OrdersAcknowledged != 1 {
		t.Fatalf("row = %+v: want 1 received and 1 acknowledged (Submitted adds none)", row)
	}
	if row.AcknowledgementLatencyCount != 1 || row.SumAcknowledgementLatencySeconds != 300 {
		t.Fatalf("row = %+v: want one 300s receipt->settle latency", row)
	}
	if row.OrdersRejectedUntranslatableSKU != 0 || row.OrdersRejectedDomain != 0 ||
		row.AcknowledgementDeadlinesMissed != 0 || row.OrdersRejectedSubmissionFailed != 0 {
		t.Fatalf("row = %+v: no rejection counter may move for this lifecycle", row)
	}

	if err := stopWithSIGTERM(t, done); err != nil {
		t.Fatalf("a SIGTERM'd projector must drain cleanly, got %v", err)
	}
}
