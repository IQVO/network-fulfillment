//go:build integration

// Integration tests for network-fulfillment's main write use cases over the
// REAL stack: the real Postgres NetworkOrderRepo, the real UnitOfWork, and
// the real transactional-outbox publisher fanning every domain event out
// through both Kafka encoders (integration + analytics), wired exactly like
// cmd/netfulfil's composition root for a DATABASE + kafka run. The two OUT
// ports that leave this repository are real adapters too: the StubGateway
// (this service's production default, ADR 0009 §4) and the REAL
// ordermanagement.Planner over HTTP against a stub speaking
// order-management's own REST contract (ADR 0020) — so the wire shapes are
// exercised in both directions.
//
// Postgres comes from testcontainers: one container for the whole package
// (TestMain below), migrated once into a template database; each test gets
// a private clone (milliseconds). Never an external database URL, never
// t.Skip.
package usecases_test

import (
	"context"
	"encoding/json"
	"fmt"
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

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is total and tests never depend on each other's rows.
const templateDB = "usecases_migrated_template"

var (
	sharedBaseURL string
	dbSeq         atomic.Uint64
)

// wireNow is the fixed instant every wired clock in this suite returns, so
// published events and acknowledgement deadlines are deterministic.
var wireNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

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

	// Migrate a template database once; every test clones it.
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintf(os.Stderr, "resolve test file path: failed\n")
		return 1
	}
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
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

// migratedDB hands the test a connection URL to its own private database,
// cloned from the migrated template.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("nf_usecases_%d", dbSeq.Add(1))
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

// wireClock is the ports.Clock the use cases accept, pinned to wireNow.
type wireClock struct{ now time.Time }

func (c wireClock) Now() time.Time { return c.now }

// orderManagementStub is order-management behind its real REST contract —
// exactly the endpoints ordermanagement.Planner calls: POST /orders raising
// a held order, POST /orders/{id}/release, DELETE /orders/{id}. Feasibility
// is decided by promiseDate the same way the real service decides it
// (ADR 0001 §7): a promise means the deadline is makeable, a null promise
// means it is not. The REAL Planner adapter runs against it, so the JSON
// wire shapes cross a real HTTP boundary in both directions.
type orderManagementStub struct {
	mu        sync.Mutex
	feasible  bool
	nextID    int
	raised    []string
	released  []string
	cancelled []string
	url       string
}

func newOrderManagementStub(t *testing.T, feasible bool) *orderManagementStub {
	t.Helper()
	stub := &orderManagementStub{feasible: feasible}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Lines []struct {
				SKU      string `json:"sku"`
				Quantity int    `json:"quantity"`
			} `json:"lines"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		stub.mu.Lock()
		stub.nextID++
		id := fmt.Sprintf("om-%d", stub.nextID)
		stub.raised = append(stub.raised, id)
		feasible := stub.feasible
		stub.mu.Unlock()

		resp := map[string]any{"id": id, "lines": []any{}}
		if feasible {
			resp["promiseDate"] = wireNow.Add(48 * time.Hour).Format(time.RFC3339)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("POST /orders/{id}/release", func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.released = append(stub.released, r.PathValue("id"))
		stub.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.cancelled = append(stub.cancelled, r.PathValue("id"))
		stub.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	stub.url = srv.URL
	return stub
}

func (s *orderManagementStub) baseURL() string { return s.url }

func (s *orderManagementStub) raisedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.raised...)
}

func (s *orderManagementStub) releasedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.released...)
}

func (s *orderManagementStub) cancelledIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.cancelled...)
}

// wiredUsecases is the real adapter stack over one private migrated
// database: Postgres repo, UnitOfWork, the transactional-outbox publisher
// with BOTH Kafka encoders, the StubGateway, the real ordermanagement
// Planner over HTTP, and the real in-memory ProductTranslation (the ACL
// dictionary, seeded the way the curated file loads it).
type wiredUsecases struct {
	receive   *usecases.ReceiveNetworkDemand
	reconcile *usecases.ReconcileSubmittedOrders
	confirm   *usecases.ConfirmNetworkOrderShipment
	orders    *postgres.NetworkOrderRepo
	pool      *pgxpool.Pool
	gateway   *network.StubGateway
	om        *orderManagementStub
}

func newWiredUsecases(t *testing.T, feasible bool) *wiredUsecases {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	var eventSeq atomic.Uint64
	mint := func() string { return fmt.Sprintf("evt-usecases-%d", eventSeq.Add(1)) }
	events := postgres.NewOutboxPublisher(pool,
		outboundkafka.NewPublisher(nil, mint),
		outboundkafka.NewAnalyticsPublisher(nil, mint),
	)
	uow := postgres.NewUnitOfWork(pool)
	gateway := network.NewStubGateway(nil)
	om := newOrderManagementStub(t, feasible)
	planner := ordermanagement.NewPlanner(om.baseURL(), nil)

	translation := memory.NewProductTranslation()
	translation.Add("NPROD-A", "SKU-A")
	translation.Add("NPROD-B", "SKU-B")

	clock := wireClock{wireNow}
	return &wiredUsecases{
		receive: &usecases.ReceiveNetworkDemand{
			Orders:      postgres.NewNetworkOrderRepo(pool),
			Gateway:     gateway,
			Planner:     planner,
			Translation: translation,
			Events:      events,
			Clock:       clock,
			UnitOfWork:  uow,
		},
		reconcile: &usecases.ReconcileSubmittedOrders{
			Orders:     postgres.NewNetworkOrderRepo(pool),
			Gateway:    gateway,
			Planner:    planner,
			Events:     events,
			Clock:      clock,
			UnitOfWork: uow,
		},
		confirm: &usecases.ConfirmNetworkOrderShipment{
			Orders:     postgres.NewNetworkOrderRepo(pool),
			Gateway:    gateway,
			Events:     events,
			Clock:      clock,
			UnitOfWork: uow,
		},
		orders:  postgres.NewNetworkOrderRepo(pool),
		pool:    pool,
		gateway: gateway,
		om:      om,
	}
}

// itestDemand builds one unit of inbound demand in the network's own
// vocabulary, the shape PollDemand hands to ReceiveNetworkDemand.
func itestDemand(ref string, products ...string) contract.InboundDemand {
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
		SiteId:         "site-itest",
		RequiredShipBy: wireNow.Add(72 * time.Hour),
		Lines:          lines,
	}
}

// outboxEventTypes returns every outbox row's event_type, oldest first.
// Each published event appears once per configured topic (integration AND
// analytics), so a single domain event shows up as two rows.
func outboxEventTypes(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	var all string
	if err := pool.QueryRow(context.Background(),
		`SELECT coalesce(string_agg(event_type, ',' ORDER BY id), '') FROM outbox_events`).Scan(&all); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if all == "" {
		return nil
	}
	return strings.Split(all, ",")
}

// countOutboxEvents counts rows whose event_type contains name.
func countOutboxEvents(rows []string, name string) int {
	n := 0
	for _, r := range rows {
		if strings.Contains(r, name) {
			n++
		}
	}
	return n
}

// TestUsecases_ReceiveFeasibleDemandSubmitsAndPublishes drives the happy
// path through the real stack: feasible demand is translated, recorded,
// raised as a held order, SUBMITTED to the network and linked to the local
// order — with Received + Submitted each enqueued on BOTH topics inside
// the same transaction as the aggregate write.
func TestUsecases_ReceiveFeasibleDemandSubmitsAndPublishes(t *testing.T) {
	w := newWiredUsecases(t, true)
	ctx := context.Background()
	ref := shared.NetworkRef("po-usecases-feasible")

	o, err := w.receive.Execute(ctx, itestDemand(string(ref), "NPROD-A", "NPROD-B"))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %s, want SUBMITTED", o.State())
	}
	if id := o.LocalOrderId(); id == nil || string(*id) != "om-1" {
		t.Fatalf("local order id = %v, want om-1", o.LocalOrderId())
	}

	// The persisted aggregate read back through the real repo agrees.
	persisted, err := w.orders.FindByRef(ctx, ref)
	if err != nil || persisted == nil {
		t.Fatalf("FindByRef: %v %v", persisted, err)
	}
	if persisted.State() != networkorder.StateSubmitted || persisted.LocalOrderId() == nil {
		t.Fatalf("persisted = %s %v", persisted.State(), persisted.LocalOrderId())
	}

	// The network was told yes; the planner raised exactly one held order.
	if accepted, submitted := w.gateway.Acknowledgement(ref); !submitted || !accepted {
		t.Fatalf("gateway acknowledgement = submitted:%v accepted:%v, want both true", submitted, accepted)
	}
	if ids := w.om.raisedIDs(); len(ids) != 1 || ids[0] != "om-1" {
		t.Fatalf("order-management raised %v, want exactly [om-1]", ids)
	}

	rows := outboxEventTypes(t, w.pool)
	if n := countOutboxEvents(rows, "NetworkOrderReceived"); n != 2 {
		t.Fatalf("NetworkOrderReceived rows = %d, want 2 (integration + analytics); all: %v", n, rows)
	}
	if n := countOutboxEvents(rows, "NetworkOrderSubmitted"); n != 2 {
		t.Fatalf("NetworkOrderSubmitted rows = %d, want 2; all: %v", n, rows)
	}
	if len(rows) != 4 {
		t.Fatalf("outbox rows = %v, want exactly the four above", rows)
	}
}

// TestUsecases_ReceiveInfeasibleDemandRejectsAndCancelsHold proves the
// refusal path: an infeasible deadline (order-management returns no
// promise) rejects the order inside the window, cancels the hold so no
// inventory stays reserved for refused demand, and answers the network no
// — with Received + Rejected enqueued atomically.
func TestUsecases_ReceiveInfeasibleDemandRejectsAndCancelsHold(t *testing.T) {
	w := newWiredUsecases(t, false)
	ctx := context.Background()
	ref := shared.NetworkRef("po-usecases-infeasible")

	o, err := w.receive.Execute(ctx, itestDemand(string(ref), "NPROD-A"))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if o.State() != networkorder.StateRejected {
		t.Fatalf("state = %s, want REJECTED", o.State())
	}
	if accepted, submitted := w.gateway.Acknowledgement(ref); !submitted || accepted {
		t.Fatalf("gateway acknowledgement = submitted:%v accepted:%v, want submitted only", submitted, accepted)
	}

	// The hold raised for the infeasible deadline was cancelled, never released.
	if ids := w.om.cancelledIDs(); len(ids) != 1 || ids[0] != "om-1" {
		t.Fatalf("cancelled holds = %v, want [om-1]", ids)
	}
	if ids := w.om.releasedIDs(); len(ids) != 0 {
		t.Fatalf("released holds = %v, want none", ids)
	}

	rows := outboxEventTypes(t, w.pool)
	if n := countOutboxEvents(rows, "NetworkOrderReceived"); n != 2 {
		t.Fatalf("NetworkOrderReceived rows = %d, want 2; all: %v", n, rows)
	}
	if n := countOutboxEvents(rows, "NetworkOrderRejected"); n != 2 {
		t.Fatalf("NetworkOrderRejected rows = %d, want 2; all: %v", n, rows)
	}
	if len(rows) != 4 {
		t.Fatalf("outbox rows = %v, want exactly the four above", rows)
	}
}

// TestUsecases_ReceiveUntranslatableDemandRejectsLineless proves the ACL
// gap path: demand naming a product with no SKU mapping is refused inside
// the window and recorded lineless, and order-management is never even
// asked — there is nothing to hold for demand we cannot identify.
func TestUsecases_ReceiveUntranslatableDemandRejectsLineless(t *testing.T) {
	w := newWiredUsecases(t, true)
	ctx := context.Background()
	ref := shared.NetworkRef("po-usecases-untranslatable")

	o, err := w.receive.Execute(ctx, itestDemand(string(ref), "NPROD-A", "NPROD-MISSING"))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if o.State() != networkorder.StateRejected {
		t.Fatalf("state = %s, want REJECTED", o.State())
	}
	if len(o.Lines()) != 0 {
		t.Fatalf("untranslatable order must carry no lines, got %d", len(o.Lines()))
	}
	if accepted, submitted := w.gateway.Acknowledgement(ref); !submitted || accepted {
		t.Fatalf("gateway acknowledgement = submitted:%v accepted:%v, want submitted only", submitted, accepted)
	}
	if ids := w.om.raisedIDs(); len(ids) != 0 {
		t.Fatalf("order-management raised %v, want nothing for untranslatable demand", ids)
	}
	if ids := w.om.cancelledIDs(); len(ids) != 0 {
		t.Fatalf("cancelled holds = %v, want none (no hold was raised)", ids)
	}

	rows := outboxEventTypes(t, w.pool)
	if n := countOutboxEvents(rows, "NetworkOrderReceived"); n != 2 {
		t.Fatalf("NetworkOrderReceived rows = %d, want 2; all: %v", n, rows)
	}
	if n := countOutboxEvents(rows, "NetworkOrderRejected"); n != 2 {
		t.Fatalf("NetworkOrderRejected rows = %d, want 2; all: %v", n, rows)
	}
	if len(rows) != 4 {
		t.Fatalf("outbox rows = %v, want exactly the four above", rows)
	}
}

// TestUsecases_ReconcileSettlesSubmittedIntoAcknowledged walks the ADR 0001
// §5 settlement: after the network's transaction-status record reports
// SUCCESS, ReconcileSubmittedOrders moves the order SUBMITTED ->
// ACKNOWLEDGED, publishes the settled commitment (the v2 wire type), and
// releases the held order onto the floor.
func TestUsecases_ReconcileSettlesSubmittedIntoAcknowledged(t *testing.T) {
	w := newWiredUsecases(t, true)
	ctx := context.Background()
	ref := shared.NetworkRef("po-usecases-reconcile")

	if _, err := w.receive.Execute(ctx, itestDemand(string(ref), "NPROD-A")); err != nil {
		t.Fatalf("receive: %v", err)
	}

	res, err := w.reconcile.Execute(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Examined != 1 || res.Confirmed != 1 || res.Pending != 0 || res.Failed != 0 {
		t.Fatalf("reconcile result = %+v, want 1 examined / 1 confirmed", res)
	}

	persisted, err := w.orders.FindByRef(ctx, ref)
	if err != nil || persisted == nil {
		t.Fatalf("FindByRef: %v %v", persisted, err)
	}
	if persisted.State() != networkorder.StateAcknowledged {
		t.Fatalf("state = %s, want ACKNOWLEDGED", persisted.State())
	}

	if ids := w.om.releasedIDs(); len(ids) != 1 || ids[0] != "om-1" {
		t.Fatalf("released holds = %v, want [om-1]", ids)
	}

	rows := outboxEventTypes(t, w.pool)
	if n := countOutboxEvents(rows, "NetworkOrderAcknowledged"); n != 2 {
		t.Fatalf("NetworkOrderAcknowledged rows = %d, want 2; all: %v", n, rows)
	}
	if len(rows) != 6 {
		t.Fatalf("outbox rows = %v, want Received + Submitted + Acknowledged, each on both topics", rows)
	}
}

// TestUsecases_ConfirmShipmentClosesLifecycleIdempotently closes the
// aggregate lifecycle: after settlement, ConfirmNetworkOrderShipment moves
// the order to CONFIRMED and enqueues NetworkOrderShipmentConfirmed on both
// topics — exactly once, because a retried confirmation of an already
// confirmed order is a no-op, never a double submission.
func TestUsecases_ConfirmShipmentClosesLifecycleIdempotently(t *testing.T) {
	w := newWiredUsecases(t, true)
	ctx := context.Background()
	ref := shared.NetworkRef("po-usecases-confirm")

	if _, err := w.receive.Execute(ctx, itestDemand(string(ref), "NPROD-A")); err != nil {
		t.Fatalf("receive: %v", err)
	}
	if _, err := w.reconcile.Execute(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	first, err := w.confirm.Execute(ctx, ref)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if first.State() != networkorder.StateConfirmed {
		t.Fatalf("state = %s, want CONFIRMED", first.State())
	}
	if n := countOutboxEvents(outboxEventTypes(t, w.pool), "NetworkOrderShipmentConfirmed"); n != 2 {
		t.Fatalf("NetworkOrderShipmentConfirmed rows = %d, want 2 (integration + analytics)", n)
	}

	// The retried confirmation is a 200-style no-op: same aggregate, no
	// second event, no second submission to the network.
	second, err := w.confirm.Execute(ctx, ref)
	if err != nil {
		t.Fatalf("confirm (retry): %v", err)
	}
	if second.State() != networkorder.StateConfirmed {
		t.Fatalf("retry state = %s, want CONFIRMED", second.State())
	}
	rows := outboxEventTypes(t, w.pool)
	if n := countOutboxEvents(rows, "NetworkOrderShipmentConfirmed"); n != 2 {
		t.Fatalf("NetworkOrderShipmentConfirmed rows after retry = %d, want still 2; all: %v", n, rows)
	}
	if len(rows) != 8 {
		t.Fatalf("outbox rows = %v, want the 8 rows of one full lifecycle, no duplicates", rows)
	}
}
