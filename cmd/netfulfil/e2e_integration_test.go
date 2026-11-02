//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/pathcapacitycache"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/processpathcache"
)

// The end-to-end proof of the real-wiring rule: the REAL entry point,
// run(ctx), booted in-process with only the deployment's environment against
// a real Postgres and a real Kafka (testcontainers), driven ONLY through REST
// and Kafka. Assertions are on Postgres rows and on the CloudEvents the relay
// publishes. Every Kafka read is bounded by a deadline.
//
// The one thing that is not real is order-management: it is a separate
// service, reached over REST (ORDER_MANAGEMENT_URL), so the test answers its
// two endpoints with an httptest server. The network gateway is the stub
// (NETWORK_MODE=stub), seeded from a file, exactly as a stub deployment is.

const (
	e2eNetworkRef = "po-e2e-1"
	e2eSite       = "SITE1"
)

type e2eEnv struct {
	dsn       string
	brokers   []string
	addr      string
	dir       string
	suffix    int64
	kafkaCont testcontainers.Container
	omURL     string
}

// consumedTopics are the topics this service's capability-offer caches read.
// A kafka-go group consumer that starts before its topic exists does not pick
// the topic up when it appears (real-wiring ADR, same finding as
// inbound-receiving ADR 0011), so the test creates them like the cluster
// does. The OUTPUT topics are NOT created: the relay's writer auto-creates
// them, as in the cluster.
var consumedTopics = []string{processpathcache.Topic, pathcapacitycache.Topic}

func startE2EInfra(t *testing.T, ctx context.Context) e2eEnv {
	t.Helper()
	env := startE2EContainers(t, ctx)

	// order-management stand-in: RaiseHeldOrder (POST /orders) answers a
	// promise before the deadline; ReleaseHeldOrder (POST /orders/{id}/releases)
	// answers 200. Anything else is a test failure the service would see as a
	// 404.
	om := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/orders":
			if r.Header.Get("Idempotency-Key") == "" {
				http.Error(w, `{"type":"idempotency-key-required"}`, http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"id":"om-e2e-1","promiseDate":%q,"lines":[{"status":"HELD"}]}`, time.Now().Add(12*time.Hour).UTC().Format(time.RFC3339))
		case r.Method == http.MethodPost && r.URL.Path == "/orders/om-e2e-1/releases":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(om.Close)

	dir := t.TempDir()
	seed := fmt.Sprintf(`{"demands":[{"networkRef":%q,"siteId":%q,"requiredShipBy":"+48h","lines":[{"networkLineRef":"l1","networkProductId":"NP-1","quantity":2}]}]}`, e2eNetworkRef, e2eSite)
	if err := os.WriteFile(filepath.Join(dir, "seed.json"), []byte(seed), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "products.json"), []byte(`{"products":[{"networkProductId":"NP-1","sku":"SKU-E2E"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	env.addr, env.dir, env.omURL = e2eFreeAddr(t), dir, om.URL
	return env
}

// startE2EContainers starts the real Postgres and Kafka every end-to-end test
// runs on and creates the topics the service consumes. Its result carries no
// listen address, directory or order-management URL: the callers add theirs.
func startE2EContainers(t *testing.T, ctx context.Context) e2eEnv {
	t.Helper()
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("network_fulfillment"), tcpostgres.WithUsername("nf"), tcpostgres.WithPassword("nf"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	suffix := time.Now().UnixNano()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID(fmt.Sprintf("nf-e2e-%d", suffix)))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	for _, topic := range consumedTopics {
		e2eCreateTopic(t, ctx, brokers[0], topic)
	}
	e2eWarmConsumerGroups(t, ctx, brokers)
	return e2eEnv{dsn: dsn, brokers: brokers, suffix: suffix, kafkaCont: kc}
}

// e2eWarmConsumerGroups makes a freshly started broker answer consumer-group
// requests before the service starts its capability-offer caches. The topics
// exist and have leaders by now, but the broker creates its internal group
// metadata (__consumer_offsets) and elects the coordinators lazily, on the first
// group request: until then it answers "[15] Group Coordinator Not Available",
// and the service treats a cache consumer that stops on its own as fatal (exit 1,
// by design). So throwaway groups join first, on the topics the service
// consumes, until one gets its generation: the coordinator, the join and the sync
// all work. Each attempt is a new group; a failed one is only a cold broker.
func e2eWarmConsumerGroups(t *testing.T, ctx context.Context, brokers []string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if lastErr = e2eJoinThrowawayGroup(ctx, brokers); lastErr == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("the new broker never let a throwaway consumer group join: %v", lastErr)
}

func e2eJoinThrowawayGroup(ctx context.Context, brokers []string) error {
	group, err := kafkago.NewConsumerGroup(kafkago.ConsumerGroupConfig{
		ID:               fmt.Sprintf("nf-e2e-warmup-%d", time.Now().UnixNano()),
		Brokers:          brokers,
		Topics:           consumedTopics,
		JoinGroupBackoff: 500 * time.Millisecond, // the default, 5s, outlasts a short test
	})
	if err != nil {
		return err
	}
	defer func() { _ = group.Close() }()
	joinCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = group.Next(joinCtx)
	return err
}

func e2eFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// e2eCreateTopic creates topic and waits for its leader. The broker container
// counts as ready once its start-up log line shows, which is before it
// reliably answers admin requests: a CI run had the very first CreateTopics
// reset by the peer ("connection reset by peer") and failed the test before the
// service even started. So the whole exchange is retried until the broker
// answers; a topic created by an attempt whose reply was lost is already there.
func e2eCreateTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var err error
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if err = e2eTryCreateTopic(ctx, broker, topic); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("create topic %s: %v", topic, err)
}

func e2eTryCreateTopic(ctx context.Context, broker, topic string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	err = conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	if err != nil && !errors.Is(err, kafkago.TopicAlreadyExists) {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.Host != "" {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s has no leader", topic)
}

func e2eWaitFor(t *testing.T, ctx context.Context, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

// startE2EService runs the REAL entry point in-process and returns a stop func
// that cancels it and returns run()'s exit code.
func startE2EService(t *testing.T, ctx context.Context, env e2eEnv) func() (int, error) {
	t.Helper()
	return startE2EServiceWith(t, ctx, env, nil)
}

// startE2EServiceWith is startE2EService with extra environment applied after
// the stub-mode defaults, so a test can switch the network to live, change the
// intervals or blank a variable. The variables are set with t.Setenv, so they
// are undone when the test ends.
func startE2EServiceWith(t *testing.T, ctx context.Context, env e2eEnv, overrides map[string]string) func() (int, error) {
	t.Helper()
	_, port, err := net.SplitHostPort(env.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", env.dsn)
	t.Setenv("MIGRATIONS_DATABASE_URL", "")
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(env.brokers, ","))
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("CAPABILITY_OFFER_ENABLED", "true")
	t.Setenv("PORT", port)
	t.Setenv("SITE_ID", e2eSite)
	t.Setenv("NETWORK_MODE", "stub")
	t.Setenv("NETWORK_BASE_URL", "")
	t.Setenv("NETWORK_SEED_FILE", filepath.Join(env.dir, "seed.json"))
	t.Setenv("PRODUCT_TRANSLATION_FILE", filepath.Join(env.dir, "products.json"))
	t.Setenv("ORDER_MANAGEMENT_URL", env.omURL)
	t.Setenv("INVENTORY_STORAGE_URL", env.omURL)
	t.Setenv("POLL_INTERVAL", "500ms")
	t.Setenv("SWEEP_INTERVAL", "1h")
	t.Setenv("RECOMPUTE_INTERVAL", "1h")
	t.Setenv("OUTBOX_RELAY_INTERVAL", "200ms")
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a dial
	// to localhost:4317 at shutdown.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	for k, v := range overrides {
		t.Setenv(k, v)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() { done <- run(runCtx) }()
	stopped := false
	stop := func() (int, error) {
		if stopped {
			return 0, nil
		}
		stopped = true
		cancel()
		select {
		case code := <-done:
			return code, nil
		case <-time.After(40 * time.Second):
			return -1, errors.New("run() did not return within 40s of cancellation")
		}
	}
	t.Cleanup(func() { _, _ = stop() })

	e2eWaitFor(t, ctx, 120*time.Second, "GET /readyz = 200 on real Postgres + Kafka", func() bool {
		code, _ := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "")
		return code == http.StatusOK
	})
	return stop
}

// e2eKeys numbers the fresh Idempotency-Key of every POST e2eDo sends.
var e2eKeys atomic.Int64

func (e e2eEnv) url(path string) string { return "http://" + e.addr + path }

// e2eDo issues a REST call and returns status and body. A transport error
// returns 0 (the service may still be starting). Every POST is a new
// logical submit and carries a fresh Idempotency-Key (fleet REST guideline
// section 2).
func e2eDo(t *testing.T, ctx context.Context, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost {
		req.Header.Set("Idempotency-Key", fmt.Sprintf("e2e-key-%d", e2eKeys.Add(1)))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func e2eCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// e2eEvent is one message read from an output topic.
type e2eEvent struct {
	ce.Event
	Key string
}

// e2eEventsOn reads topic from the beginning, in a loop bounded by a deadline
// (the relay auto-creates the topic on its first write, so it may not exist
// yet), until every wanted "<EventName>|<subject>" was seen. The event name
// comes from the dataschema (urn:...:<EventName>:v<N>), so a versioned type
// suffix (NetworkOrderAcknowledged is v2) does not matter.
func e2eEventsOn(t *testing.T, ctx context.Context, brokers []string, topic string, want ...string) map[string]e2eEvent {
	t.Helper()
	got := map[string]e2eEvent{}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MaxWait: 500 * time.Millisecond})
		if err := r.SetOffset(kafkago.FirstOffset); err == nil {
			readCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			for {
				m, err := r.ReadMessage(readCtx)
				if err != nil {
					break
				}
				var ev ce.Event
				if json.Unmarshal(m.Value, &ev) == nil {
					parts := strings.Split(ev.DataSchema(), ":")
					if len(parts) >= 2 {
						got[parts[len(parts)-2]+"|"+ev.Subject()] = e2eEvent{Event: ev, Key: string(m.Key)}
					}
				}
			}
			cancel()
		}
		_ = r.Close()
		missing := false
		for _, w := range want {
			if _, ok := got[w]; !ok {
				missing = true
			}
		}
		if !missing {
			return got
		}
		time.Sleep(500 * time.Millisecond)
	}
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	t.Fatalf("timed out reading %s: wanted %v, saw %v", topic, want, keys)
	return nil
}

func TestRunOnRealPostgresAndKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	env := startE2EInfra(t, ctx)
	stop := startE2EService(t, ctx, env)
	pool, err := pgxpool.New(ctx, env.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	t.Run("readyz is 200 only because Postgres and Kafka answer", func(t *testing.T) {
		code, body := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "")
		if code != http.StatusOK || !strings.Contains(body, `"ready"`) {
			t.Fatalf("readyz = %d %s", code, body)
		}
		if code, _ := e2eDo(t, ctx, http.MethodGet, env.url("/healthz"), ""); code != http.StatusOK {
			t.Fatalf("healthz = %d", code)
		}
	})

	t.Run("a seeded demand is acknowledged, a REST write confirms shipment, and the CloudEvents are published", func(t *testing.T) {
		e2eShipmentFlow(t, ctx, env, pool)
	})

	t.Run("readyz goes 503 when the broker goes away, and shutdown still completes", func(t *testing.T) {
		timeout := 10 * time.Second
		if err := env.kafkaCont.Stop(ctx, &timeout); err != nil {
			t.Fatalf("stop kafka: %v", err)
		}
		e2eWaitFor(t, ctx, 30*time.Second, "GET /readyz = 503 dependency-unavailable (kafka)", func() bool {
			code, body := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "")
			return code == http.StatusServiceUnavailable && strings.Contains(body, "dependency-unavailable") && strings.Contains(body, "kafka")
		})
		// Liveness is unaffected by a dependency.
		if code, _ := e2eDo(t, ctx, http.MethodGet, env.url("/healthz"), ""); code != http.StatusOK {
			t.Fatalf("healthz with the broker down = %d, want 200", code)
		}
		start := time.Now()
		code, err := stop()
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 35*time.Second {
			t.Fatalf("shutdown with the broker down took %s", d)
		}
		t.Logf("run() returned exit code %d after %s", code, time.Since(start))
	})
}

func e2eShipmentFlow(t *testing.T, ctx context.Context, env e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	ref := e2eNetworkRef
	confirm := env.url("/network-orders/" + ref + "/shipment-confirmation")

	// The poller picks the seeded demand up, order-management holds it, the
	// stub network takes the acknowledgement and the reconcile pass settles
	// the order ACKNOWLEDGED (all driven by the real process).
	e2eWaitFor(t, ctx, 60*time.Second, "the seeded order is ACKNOWLEDGED in Postgres", func() bool {
		return e2eCount(t, ctx, pool, `SELECT count(*) FROM network_orders WHERE network_ref = $1 AND state = 'ACKNOWLEDGED'`, ref) == 1
	})
	if code, body := e2eDo(t, ctx, http.MethodGet, env.url("/network-orders/"+ref), ""); code != http.StatusOK || !strings.Contains(body, "ACKNOWLEDGED") {
		t.Fatalf("GET network order = %d %s", code, body)
	}

	// Each call is a new submit (a fresh Idempotency-Key); the write is
	// still guarded by the domain: an unknown order is a 404 problem.
	if code, body := e2eDo(t, ctx, http.MethodPost, env.url("/network-orders/po-unknown/shipment-confirmation"), ""); code != http.StatusNotFound || !strings.Contains(body, "network-order-not-found") {
		t.Fatalf("confirm unknown order = %d %s, want 404 network-order-not-found", code, body)
	}
	if code, body := e2eDo(t, ctx, http.MethodPost, confirm, ""); code != http.StatusNoContent {
		t.Fatalf("confirm shipment = %d %s, want 204", code, body)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM network_orders WHERE network_ref = $1 AND state = 'CONFIRMED'`, ref); n != 1 {
		t.Fatalf("CONFIRMED network_orders rows = %d, want 1", n)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM network_orders WHERE network_ref = $1`, ref); n != 1 {
		t.Fatalf("network_orders rows = %d, want exactly 1", n)
	}

	// A replay is a natural no-op: 204 again, no second submission and no
	// second event.
	e2eWaitFor(t, ctx, 20*time.Second, "the outbox drains", func() bool {
		return e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	rowsBefore := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events`)
	if code, body := e2eDo(t, ctx, http.MethodPost, confirm, ""); code != http.StatusNoContent {
		t.Fatalf("replayed confirm = %d %s, want 204", code, body)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events`); n != rowsBefore {
		t.Fatalf("outbox rows after a replayed confirm = %d, want %d (no second event)", n, rowsBefore)
	}

	want := []string{"NetworkOrderReceived|" + ref, "NetworkOrderSubmitted|" + ref, "NetworkOrderAcknowledged|" + ref, "NetworkOrderShipmentConfirmed|" + ref}
	events := e2eEventsOn(t, ctx, env.brokers, outboundkafka.Topic, want...)
	sc := events["NetworkOrderShipmentConfirmed|"+ref]
	if sc.SpecVersion() != "1.0" || sc.Type() != "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderShipmentConfirmed" ||
		sc.Source() != "/warehouse/network-fulfillment" ||
		sc.DataSchema() != "urn:warehouse:network-fulfillment:events:NetworkOrderShipmentConfirmed:v1" || sc.Key != ref {
		t.Fatalf("NetworkOrderShipmentConfirmed envelope = type %q source %q schema %q key %q", sc.Type(), sc.Source(), sc.DataSchema(), sc.Key)
	}
	var data struct {
		NetworkRef string `json:"networkRef"`
		SiteID     string `json:"siteId"`
	}
	if err := json.Unmarshal(sc.Data(), &data); err != nil || data.NetworkRef != ref || data.SiteID != e2eSite {
		t.Fatalf("NetworkOrderShipmentConfirmed data = %s (%v)", sc.Data(), err)
	}

	analytics := e2eEventsOn(t, ctx, env.brokers, outboundkafka.AnalyticsTopic, "NetworkOrderShipmentConfirmed|"+ref)
	ac := analytics["NetworkOrderShipmentConfirmed|"+ref]
	if ac.SpecVersion() != "1.0" || ac.DataSchema() != "urn:warehouse:network-fulfillment:analytics:NetworkOrderShipmentConfirmed:v1" || ac.Source() != "/warehouse/network-fulfillment" {
		t.Fatalf("analytics copy = specversion %q schema %q source %q", ac.SpecVersion(), ac.DataSchema(), ac.Source())
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("%d outbox rows still unpublished", n)
	}
}
