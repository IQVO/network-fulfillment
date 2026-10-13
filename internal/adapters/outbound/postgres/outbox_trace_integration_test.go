//go:build integration

// Gap fix D27 (ADR 0022): the W3C trace context of the transaction that
// wrote an outbox row crosses the outbox to Kafka. Real Postgres and a real
// Kafka broker, both through testcontainers (never KAFKA_BROKERS or
// localhost).
package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what telemetry.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// startTraceKafka boots a disposable broker for one test and pre-creates
// topics so the relay's first write does not race topic auto-creation.
func startTraceKafka(t *testing.T, ctx context.Context, topics ...string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("network-fulfillment-outbox-trace-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, topic := range topics {
		if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	return brokers
}

// readMessages reads n messages from partition 0 of topic, oldest first.
func readMessages(t *testing.T, ctx context.Context, brokers []string, topic string, n int) []kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer func() { _ = reader.Close() }()
	if err := reader.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatalf("seek %s: %v", topic, err)
	}
	out := make([]kafkago.Message, 0, n)
	for len(out) < n {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s (%d of %d): %v", topic, len(out)+1, n, err)
		}
		out = append(out, msg)
	}
	return out
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// storedTraceparents returns the traceparent stored in outbox_events.headers
// of every row, in id order ("" when a row has none).
func storedTraceparents(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT headers FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan stored headers: %v", err)
		}
		var hs []struct {
			Key   string `json:"key"`
			Value []byte `json:"value"`
		}
		if err := json.Unmarshal(raw, &hs); err != nil {
			t.Fatalf("decode stored headers %s: %v", raw, err)
		}
		tp := ""
		for _, h := range hs {
			if h.Key == "traceparent" {
				tp = string(h.Value)
			}
		}
		out = append(out, tp)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	return out
}

// receiveDemandInSpan runs ReceiveNetworkDemand against real Postgres inside
// ctx (the request span) with the outbox publisher both topics share, as
// cmd/netfulfil wires it in outbox mode.
func receiveDemandInSpan(t *testing.T, ctx context.Context, pool *pgxpool.Pool, integration, analytics outboundkafka.Encoder) {
	t.Helper()
	uc := &usecases.ReceiveNetworkDemand{
		Orders:      postgres.NewNetworkOrderRepo(pool),
		Gateway:     stubGateway{},
		Planner:     &stubPlanner{feasible: true},
		Translation: passTranslation{},
		Events:      postgres.NewOutboxPublisher(pool, integration, analytics),
		Clock:       fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork:  postgres.NewUnitOfWork(pool),
	}
	if _, err := uc.Execute(ctx, demand(uniqueRef("trace"))); err != nil {
		t.Fatalf("receive demand: %v", err)
	}
}

// assertConsumerParentsOn checks a consumer extracting msg's headers with
// the global propagator lands on want, as a remote parent.
func assertConsumerParentsOn(t *testing.T, msg kafkago.Message, want trace.SpanContext) {
	t.Helper()
	consumerCtx := otel.GetTextMapPropagator().Extract(context.Background(), kafkaHeaderCarrier(msg.Headers))
	parent := trace.SpanContextFromContext(consumerCtx)
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka: a use case runs inside
// a request span; every outbox row it writes (both topics) stores that span's
// traceparent in outbox_events.headers; the relay (a later pass, no span of
// its own, a background context) sends it as a Kafka header next to the
// CloudEvents content-type; and a consumer extracting it with the global
// propagator lands on the request's trace and span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	brokers := startTraceKafka(t, ctx, outboundkafka.Topic, outboundkafka.AnalyticsTopic)
	pool := newDB(t)
	integration := outboundkafka.NewPublisher(brokers, func() string { return fmt.Sprintf("evt-%d", time.Now().UnixNano()) })
	analytics := outboundkafka.NewAnalyticsPublisher(brokers, func() string { return fmt.Sprintf("evt-%d", time.Now().UnixNano()) })
	t.Cleanup(func() { _ = integration.Close(); _ = analytics.Close() })

	reqCtx, reqSpan := tracer.Start(ctx, "poll network demand", trace.WithSpanKind(trace.SpanKindInternal))
	receiveDemandInSpan(t, reqCtx, pool, integration, analytics)
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())

	stored := storedTraceparents(t, ctx, pool)
	if len(stored) == 0 || len(stored)%2 != 0 {
		t.Fatalf("want one row per event and topic, got %d rows", len(stored))
	}
	for i, tp := range stored {
		if tp != wantTraceparent {
			t.Fatalf("outbox row %d traceparent = %q, want the request span's %q", i, tp, wantTraceparent)
		}
	}

	relay := postgres.NewOutboxRelay(pool, integration, slog.New(slog.NewTextHandler(io.Discard, nil)), postgres.WithMaxAttempts(20))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != len(stored) {
		t.Fatalf("relay: n=%d err=%v, want %d", n, err, len(stored))
	}

	for _, topic := range []string{outboundkafka.Topic, outboundkafka.AnalyticsTopic} {
		for _, msg := range readMessages(t, ctx, brokers, topic, len(stored)/2) {
			got, ok := headerValue(msg.Headers, "traceparent")
			if !ok || got != wantTraceparent {
				t.Fatalf("%s: Kafka traceparent = %q (present=%v), want %q", topic, got, ok, wantTraceparent)
			}
			if ct, _ := headerValue(msg.Headers, "content-type"); ct == "" {
				t.Fatalf("%s: content-type header lost on the outbox path: %+v", topic, msg.Headers)
			}
			assertConsumerParentsOn(t, msg, want)
		}
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged pins the other half
// of D27: a row written with no trace context (no active span, or a row that
// predates trace capture) relays with exactly its stored headers, key and
// value, and carries no traceparent a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	topic := fmt.Sprintf("warehouse.network-fulfillment.itest-no-trace-%d", time.Now().UnixNano())
	brokers := startTraceKafka(t, ctx, topic)
	pool := newDB(t)

	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (topic, event_type, key, value)
		VALUES ($1, 'com.example.Untraced', 'po-untraced', '{}')
	`, topic); err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}

	sink := outboundkafka.NewPublisher(brokers, func() string { return "unused" })
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.New(slog.NewTextHandler(io.Discard, nil)), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readMessages(t, ctx, brokers, topic, 1)[0]
	if string(msg.Key) != "po-untraced" || string(msg.Value) != `{}` {
		t.Fatalf("row relayed changed: key=%q value=%q", msg.Key, msg.Value)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("a row without stored headers must relay with none, got %+v", msg.Headers)
	}
}

// kafkaHeaderCarrier adapts kafka-go headers to the OTel text-map carrier.
type kafkaHeaderCarrier []kafkago.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	v, _ := headerValue(c, key)
	return v
}

func (c kafkaHeaderCarrier) Set(string, string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
