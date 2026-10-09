//go:build integration

// Integration tests for the REST inbound adapter over the REAL stack: the
// real production handler chain (Server.Routes: CORS + otelhttp + the
// stdlib ServeMux — network-fulfillment's OLTP surface is deliberately NOT
// chi; the chi router lives in the separate reports binary) on an
// httptest.Server, the real Postgres repositories, the real UnitOfWork and
// transactional outbox, and the real inbound leg behind it: a real
// poller.Poller polling a real StubGateway into the real
// ReceiveNetworkDemand use case, wired exactly like cmd/netfulfil's
// composition root. Demand enters this context only by polling (ADR 0001
// §5: no intake endpoint exists, or could), so the "resource lifecycle"
// here is the honest one: poll -> receive -> persist -> observe over REST,
// plus the one ADR'd write endpoint (POST shipment-confirmation, ADR 0014)
// driven through its 204 / 409 / 404 paths with the repo's RFC 7807
// problem slugs.
//
// Postgres comes from testcontainers: one container per package run, one
// private database per test, migrated once. Never an external DATABASE_URL,
// never t.Skip.
package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for the
// same pattern's rationale. Never an external DATABASE_URL, never t.Skip.
const templateDB = "http_migrated_template"

var (
	sharedBaseURL string
	dbSeq         atomic.Uint64
)

// restNow is the fixed instant every wired clock in this suite returns, so
// deadlines, watermarks and published events are deterministic.
var restNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("networkfulfillment"),
		tcpostgres.WithUsername("networkfulfillment"),
		tcpostgres.WithPassword("networkfulfillment"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	sharedBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintf(os.Stderr, "resolve test file path: failed\n")
		return 1
	}
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
	if err := postgres.RunMigrations(withDB(sharedBaseURL, templateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// createDatabase creates an empty database inside the shared container.
func createDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// migratedDB hands the test a connection URL to its own private database
// cloned from the migrated template.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("nf_http_%d", dbSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), sharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return withDB(sharedBaseURL, name)
}

// restClock is the ports.Clock the wired stack accepts, pinned to restNow.
type restClock struct{ now time.Time }

func (c restClock) Now() time.Time { return c.now }

// plannerStub is order-management behind its real REST contract, exactly
// the endpoints ordermanagement.Planner calls; the REAL Planner adapter
// runs against it, so the JSON wire shapes cross a real HTTP boundary.
// Feasibility is decided by promiseDate, the same way the real service
// decides it (ADR 0001 §7).
type plannerStub struct {
	mu       sync.Mutex
	feasible bool
	nextID   int
	url      string
}

func newPlannerStub(t *testing.T, feasible bool) *plannerStub {
	t.Helper()
	stub := &plannerStub{feasible: feasible}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.nextID++
		id := fmt.Sprintf("om-%d", stub.nextID)
		feasible := stub.feasible
		stub.mu.Unlock()

		resp := map[string]any{"id": id, "lines": []any{}}
		if feasible {
			resp["promiseDate"] = restNow.Add(48 * time.Hour).Format(time.RFC3339)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("POST /orders/{id}/release", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /orders/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	stub.url = srv.URL
	return stub
}

// restAPI is the real process stack over one private migrated database:
// Postgres repo + UnitOfWork + the transactional-outbox publisher with both
// Kafka encoders, the StubGateway, the real Planner over HTTP, the real
// poller, and the real REST handler chain — served over httptest.
type restAPI struct {
	server    *httptest.Server
	pool      *pgxpool.Pool
	gateway   *network.StubGateway
	receive   *usecases.ReceiveNetworkDemand
	reconcile *usecases.ReconcileSubmittedOrders
	poller    *poller.Poller
}

func newRESTAPI(t *testing.T, feasible bool) *restAPI {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	var eventSeq atomic.Uint64
	mint := func() string { return fmt.Sprintf("evt-http-%d", eventSeq.Add(1)) }
	events := postgres.NewOutboxPublisher(pool,
		outboundkafka.NewPublisher(nil, mint),
		outboundkafka.NewAnalyticsPublisher(nil, mint),
	)
	uow := postgres.NewUnitOfWork(pool)
	gateway := network.NewStubGateway(nil)
	planner := ordermanagement.NewPlanner(newPlannerStub(t, feasible).url, nil)

	translation := memory.NewProductTranslation()
	translation.Add("NPROD-A", "SKU-A")

	clock := restClock{restNow}
	receive := &usecases.ReceiveNetworkDemand{
		Orders:      postgres.NewNetworkOrderRepo(pool),
		Gateway:     gateway,
		Planner:     planner,
		Translation: translation,
		Events:      events,
		Clock:       clock,
		UnitOfWork:  uow,
	}
	reconcile := &usecases.ReconcileSubmittedOrders{
		Orders:     postgres.NewNetworkOrderRepo(pool),
		Gateway:    gateway,
		Planner:    planner,
		Events:     events,
		Clock:      clock,
		UnitOfWork: uow,
	}
	confirm := &usecases.ConfirmNetworkOrderShipment{
		Orders:     postgres.NewNetworkOrderRepo(pool),
		Gateway:    gateway,
		Events:     events,
		Clock:      clock,
		UnitOfWork: uow,
	}

	quietLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	inbound := poller.New(gateway, receive, clock, poller.Config{
		Interval: time.Hour,
	}, quietLogger)

	api := &inboundhttp.Server{
		Orders:          postgres.NewNetworkOrderRepo(pool),
		Poller:          inbound,
		Clock:           clock,
		NetworkMode:     string(network.ModeStub),
		ConfirmShipment: confirm,
	}
	hs := httptest.NewServer(api.Routes())
	t.Cleanup(hs.Close)

	return &restAPI{
		server:    hs,
		pool:      pool,
		gateway:   gateway,
		receive:   receive,
		reconcile: reconcile,
		poller:    inbound,
	}
}

// seededDemand builds one unit of inbound demand in the network's own
// vocabulary, the shape the gateway's PollDemand returns.
func seededDemand(ref string, products ...string) contract.InboundDemand {
	lines := make([]contract.InboundLine, 0, len(products))
	for i, p := range products {
		lines = append(lines, contract.InboundLine{
			NetworkLineRef:   shared.NetworkLineRef(fmt.Sprintf("%s-%d", ref, i+1)),
			NetworkProductId: shared.NetworkProductId(p),
			Quantity:         2,
		})
	}
	return contract.InboundDemand{
		NetworkRef:     shared.NetworkRef(ref),
		SiteId:         "site-http-itest",
		RequiredShipBy: restNow.Add(72 * time.Hour),
		Lines:          lines,
	}
}

// do runs one request against the API and returns the status, headers and
// decoded JSON body.
func (a *restAPI) do(t *testing.T, method, path string) (int, http.Header, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, a.server.URL+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := a.server.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &decoded)
	}
	return resp.StatusCode, resp.Header, decoded
}

// problemSlug strips the namespace off an RFC 7807 type, leaving the slug.
func problemSlug(t *testing.T, body map[string]any) string {
	t.Helper()
	raw, _ := body["type"].(string)
	return strings.TrimPrefix(raw, "https://errors.network-fulfillment.warehouse-systems.dev/")
}

// outboxEventTypes returns every outbox row's event_type, oldest first
// (one row per event per configured topic).
func (a *restAPI) outboxEventTypes(t *testing.T) []string {
	t.Helper()
	var all string
	if err := a.pool.QueryRow(context.Background(),
		`SELECT coalesce(string_agg(event_type, ',' ORDER BY id), '') FROM outbox_events`).Scan(&all); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if all == "" {
		return nil
	}
	return strings.Split(all, ",")
}

// countRows counts rows whose event_type contains name.
func countRows(rows []string, name string) int {
	n := 0
	for _, r := range rows {
		if strings.Contains(r, name) {
			n++
		}
	}
	return n
}

// TestHTTP_PollerToReadSurfaceLifecycle drives the context's real inbound
// lifecycle end to end: seeded network demand is polled by the REAL
// poller, answered by the REAL use case stack into REAL Postgres, and then
// observed over the real REST surface — the order itself (200, both product
// vocabularies, the linked local order), the unanswered working set, and
// the inbound status whose counters prove the pass ran. An unknown ref is
// the repo's 404 problem shape.
func TestHTTP_PollerToReadSurfaceLifecycle(t *testing.T) {
	api := newRESTAPI(t, true)
	ctx := context.Background()

	// Seed the network with one translatable, feasible order and one
	// carrying a product the ACL has no mapping for: the poller must
	// deliver both answers in one clean pass.
	api.gateway.Seed(
		seededDemand("po-http-1", "NPROD-A"),
		seededDemand("po-http-2", "NPROD-MISSING"),
	)
	api.poller.PollOnce(ctx)

	// The order itself: SUBMITTED, linked to the local order, both
	// vocabularies on the line, camelCase wire shape.
	status, _, order := api.do(t, http.MethodGet, "/network-orders/po-http-1")
	if status != http.StatusOK {
		t.Fatalf("GET /network-orders/po-http-1: status %d body %v", status, order)
	}
	if order["state"] != "SUBMITTED" || order["localOrderId"] != "om-1" {
		t.Fatalf("po-http-1 = %v", order)
	}
	if order["siteId"] != "site-http-itest" || order["acknowledgementOverdue"] != false {
		t.Fatalf("po-http-1 = %v", order)
	}
	lines, _ := order["lines"].([]any)
	if len(lines) != 1 {
		t.Fatalf("po-http-1 lines = %v", order["lines"])
	}
	line, _ := lines[0].(map[string]any)
	if line["networkProductId"] != "NPROD-A" || line["sku"] != "SKU-A" || line["quantity"] != 2.0 {
		t.Fatalf("line must carry both vocabularies: %v", line)
	}

	// The untranslatable order was refused inside the window: REJECTED,
	// lineless, no local order.
	status, _, rejected := api.do(t, http.MethodGet, "/network-orders/po-http-2")
	if status != http.StatusOK || rejected["state"] != "REJECTED" {
		t.Fatalf("po-http-2 = status %d %v", status, rejected)
	}
	if _, linked := rejected["localOrderId"]; linked {
		t.Fatalf("untranslatable order must not be linked to a local order: %v", rejected)
	}

	// The unanswered working set is empty: both orders were answered.
	status, _, list := api.do(t, http.MethodGet, "/network-orders")
	if status != http.StatusOK {
		t.Fatalf("GET /network-orders: status %d", status)
	}
	if items, _ := list["networkOrders"].([]any); len(items) != 0 {
		t.Fatalf("unanswered working set = %v, want empty", list["networkOrders"])
	}

	// The inbound status reports the real poller's counters: one clean
	// pass, two received, none failed, watermark advanced.
	status, _, inbound := api.do(t, http.MethodGet, "/inbound-status")
	if status != http.StatusOK {
		t.Fatalf("GET /inbound-status: status %d", status)
	}
	if inbound["networkMode"] != "stub" || inbound["polls"] != 1.0 ||
		inbound["received"] != 2.0 || inbound["failed"] != 0.0 ||
		inbound["unanswered"] != 0.0 || inbound["overdue"] != 0.0 {
		t.Fatalf("inbound-status = %v", inbound)
	}
	if since, _ := inbound["since"].(string); since != restNow.Format(time.RFC3339) {
		t.Fatalf("inbound-status since = %v, want %s", inbound["since"], restNow.Format(time.RFC3339))
	}

	// Both orders' events were enqueued on both topics atomically: the
	// feasible order published Received + Submitted, the refused one
	// Received + Rejected — each domain event x2 (integration and
	// analytics).
	rows := api.outboxEventTypes(t)
	if n := countRows(rows, "NetworkOrderReceived"); n != 4 {
		t.Fatalf("NetworkOrderReceived rows = %d, want 4 (2 events x 2 topics); all: %v", n, rows)
	}
	if n := countRows(rows, "NetworkOrderSubmitted"); n != 2 {
		t.Fatalf("NetworkOrderSubmitted rows = %d, want 2; all: %v", n, rows)
	}
	if n := countRows(rows, "NetworkOrderRejected"); n != 2 {
		t.Fatalf("NetworkOrderRejected rows = %d, want 2; all: %v", n, rows)
	}
	if len(rows) != 8 {
		t.Fatalf("outbox rows = %v, want the eight above", rows)
	}

	// Unknown ref: the repo's 404 problem shape, not a bare status.
	status, header, problem := api.do(t, http.MethodGet, "/network-orders/po-http-nope")
	if status != http.StatusNotFound || problemSlug(t, problem) != "network-order-not-found" {
		t.Fatalf("unknown ref: status %d problem %v", status, problem)
	}
	if ct := header.Get("Content-Type"); !strings.Contains(ct, "application/problem+json") {
		t.Fatalf("unknown ref Content-Type = %q, want application/problem+json", ct)
	}
}

// TestHTTP_ShipmentConfirmationEndpoint drives the one write endpoint this
// adapter has (ADR 0014) through its whole decision table against real
// Postgres: 409 with the confirm-before-acknowledge slug while the order is
// still SUBMITTED, 204 once it settles ACKNOWLEDGED, 204 again on the
// idempotent retry, the state read back CONFIRMED, and 404 with the
// not-found slug for a ref nothing knows.
func TestHTTP_ShipmentConfirmationEndpoint(t *testing.T) {
	api := newRESTAPI(t, true)
	ctx := context.Background()
	const path = "/network-orders/po-http-confirm/shipment-confirmation"

	if _, err := api.receive.Execute(ctx, seededDemand("po-http-confirm", "NPROD-A")); err != nil {
		t.Fatalf("receive: %v", err)
	}

	// Still SUBMITTED (settlement has not run): a conflict with current
	// resource state, as the repo's problem table says.
	status, _, problem := api.do(t, http.MethodPost, path)
	if status != http.StatusConflict || problemSlug(t, problem) != "confirm-before-acknowledge" {
		t.Fatalf("confirm before acknowledge: status %d problem %v", status, problem)
	}

	// Settle the submission, then the confirmation succeeds: 204, no body.
	if _, err := api.reconcile.Execute(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	status, _, body := api.do(t, http.MethodPost, path)
	if status != http.StatusNoContent {
		t.Fatalf("confirm: status %d body %v", status, body)
	}

	// The state really moved: CONFIRMED, read back over the same surface.
	status, _, order := api.do(t, http.MethodGet, "/network-orders/po-http-confirm")
	if status != http.StatusOK || order["state"] != "CONFIRMED" {
		t.Fatalf("confirmed order = status %d %v", status, order)
	}

	// The idempotent retry is another 204 and enqueues nothing new.
	status, _, _ = api.do(t, http.MethodPost, path)
	if status != http.StatusNoContent {
		t.Fatalf("confirm (retry): status %d", status)
	}
	rows := api.outboxEventTypes(t)
	if n := countRows(rows, "NetworkOrderShipmentConfirmed"); n != 2 {
		t.Fatalf("NetworkOrderShipmentConfirmed rows = %d, want 2 (integration + analytics); all: %v", n, rows)
	}

	// A ref nothing knows: the 404 problem slug.
	status, _, problem = api.do(t, http.MethodPost, "/network-orders/po-http-nope/shipment-confirmation")
	if status != http.StatusNotFound || problemSlug(t, problem) != "network-order-not-found" {
		t.Fatalf("confirm unknown ref: status %d problem %v", status, problem)
	}
}
