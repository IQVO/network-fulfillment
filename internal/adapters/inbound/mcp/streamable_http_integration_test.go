//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server, driven
// by the SDK's own client (mcp.NewClient + StreamableClientTransport), with
// the REAL Postgres-backed repositories behind it — exactly the deployment
// shape cmd/mcp wires (read-only surface; every registered tool carries
// ReadOnlyHint=true). This proves the wire contract (initialize,
// tools/list, tools/call, resources/read) end-to-end, not the tool handlers
// in isolation.
//
// Postgres comes from testcontainers: one container per package run, one
// private database per test, migrated once. Never an external DATABASE_URL,
// never t.Skip.
package mcp_test

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for the
// same pattern's rationale. Never an external DATABASE_URL, never t.Skip.
const templateDB = "mcp_migrated_template"

var (
	sharedBaseURL string
	dbSeq         atomic.Uint64
)

// mcpClock is pinned so acknowledgementOverdue is deterministic: the seed's
// window (receivedAt + 24h) is comfortably in the future of clockNow.
var mcpClockNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

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
	name := fmt.Sprintf("nf_mcp_%d", dbSeq.Add(1))
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

// fixedClock is the ports.Clock the MCP adapter's Deps accept.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// reportsStub serves the netfulfil-reports REST contract the curated MCP
// tool reads through the REAL ReportsRESTClient: a day-bucketed
// acknowledgement report and a freshness figure.
type reportsStub struct {
	url  string
	rows []map[string]any
}

func newReportsStub(t *testing.T, rows []map[string]any) *reportsStub {
	t.Helper()
	stub := &reportsStub{rows: rows}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/acknowledgement", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rows": stub.rows})
	})
	mux.HandleFunc("GET /reports/acknowledgement/freshness", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"lagSeconds": 4.0})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	stub.url = srv.URL
	return stub
}

// mcpHarness wires the REAL production stack — Postgres repos (orders +
// offers), the real reports REST client, mcp.NewServer, mcp.Handler — and
// serves it over HTTP. The test drives tools/list and tools/call exactly
// like a model host would.
type mcpHarness struct {
	session *sdkmcp.ClientSession
}

// newMCPHarness seeds the read models on a fresh private database (one
// SUBMITTED order with its local order id linked, one REJECTED order, one
// capability offer) and serves the real MCP stack over Streamable HTTP.
func newMCPHarness(t *testing.T) *mcpHarness {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	orders := postgres.NewNetworkOrderRepo(pool)
	receivedAt := mcpClockNow.Add(-2 * time.Hour)

	// po-mcp-1: submitted (acknowledgement told to the network, awaiting
	// reconciliation) and linked to its local order.
	submitted, err := seededOrder("po-mcp-1", receivedAt)
	if err != nil {
		t.Fatalf("build submitted order: %v", err)
	}
	if err := submitted.Submit(); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := submitted.LinkLocalOrder("om-mcp-1"); err != nil {
		t.Fatalf("link local order: %v", err)
	}
	if err := orders.Save(ctx, submitted); err != nil {
		t.Fatalf("seed submitted order: %v", err)
	}

	// po-mcp-2: rejected as an untranslatable catalogue gap, lineless.
	rejected, err := seededUntranslatable("po-mcp-2", receivedAt)
	if err != nil {
		t.Fatalf("build rejected order: %v", err)
	}
	if err := rejected.Reject(); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if err := orders.Save(ctx, rejected); err != nil {
		t.Fatalf("seed rejected order: %v", err)
	}

	offers := postgres.NewCapabilityOfferRepo(pool)
	offer, err := capabilityoffer.New("SKU-MCP", "site-mcp", 120, 400,
		capabilityoffer.BasisThroughputConstrained, mcpClockNow)
	if err != nil {
		t.Fatalf("build capability offer: %v", err)
	}
	if err := offers.Save(ctx, offer); err != nil {
		t.Fatalf("seed capability offer: %v", err)
	}

	reports := newReportsStub(t, []map[string]any{{
		"dayBucket": "2026-10-09T00:00:00Z", "ordersReceived": 2,
		"ordersAcknowledged": 1, "ordersRejectedUntranslatableSku": 1,
	}})

	server := mcp.NewServer(mcp.Deps{
		Orders:  orders,
		Clock:   fixedClock{mcpClockNow},
		Offers:  offers,
		Reports: mcp.NewReportsRESTClient(reports.url, nil),
	})

	hs := httptest.NewServer(mcp.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{session: session}
}

// seededOrder builds a NetworkOrder in this test's own vocabulary via the
// domain constructor (one translated line).
func seededOrder(ref string, receivedAt time.Time) (*networkorder.NetworkOrder, error) {
	line, err := networkorder.NewLine("po-mcp-1-1", "NPROD-MCP", "SKU-MCP", 3)
	if err != nil {
		return nil, err
	}
	return networkorder.Receive(shared.NetworkRef(ref), "site-mcp",
		receivedAt.Add(96*time.Hour), []networkorder.Line{line}, receivedAt)
}

// seededUntranslatable builds the lineless aggregate the ACL gap path
// persists (networkorder.ReceiveUntranslatable).
func seededUntranslatable(ref string, receivedAt time.Time) (*networkorder.NetworkOrder, error) {
	return networkorder.ReceiveUntranslatable(shared.NetworkRef(ref), "site-mcp",
		receivedAt.Add(96*time.Hour), receivedAt)
}

// callTool runs tools/call and fails on a transport error; the caller
// decides whether a tool error (res.IsError) is the expected outcome.
func (h *mcpHarness) callTool(t *testing.T, name string, args map[string]any) *sdkmcp.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdkmcp.CallToolParams{
		Name: name, Arguments: args,
	})
	if err != nil {
		t.Fatalf("tools/call %s: %v", name, err)
	}
	return res
}

// structured returns the tool's structured content as a map.
func (h *mcpHarness) structured(t *testing.T, res *sdkmcp.CallToolResult) map[string]any {
	t.Helper()
	if res.IsError {
		t.Fatalf("tool error: %+v", res)
	}
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content: %+v", res)
	}
	return m
}

// TestMCP_ListToolsExposesTheContract pins the whole tool surface over the
// real transport: every registered tool answers tools/list, and each
// carries the read-only annotation a host gates on — this context's MCP
// surface is entirely read-only (ADR 0001 §5: demand arrives by polling,
// never by push).
func TestMCP_ListToolsExposesTheContract(t *testing.T) {
	h := newMCPHarness(t)

	list, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool %s must carry ReadOnlyHint=true", tool.Name)
		}
	}
	for _, want := range []string{
		"get_network_order", "list_network_orders",
		"list_capability_offers", "get_acknowledgement_report",
	} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
	if len(list.Tools) != 4 {
		t.Fatalf("tool count = %d, want 4 (tools: %v)", len(list.Tools), names)
	}
}

// TestMCP_CallToolsRoundTripThroughPostgres drives every read tool against
// the seeded data over the real Postgres repos: the single-order tool, the
// state-filtered listing (including the state enum's rejection), the
// capability-offer listing, and the curated report through the real
// reports REST client.
func TestMCP_CallToolsRoundTripThroughPostgres(t *testing.T) {
	h := newMCPHarness(t)

	// get_network_order: both vocabularies, the linked local order, the
	// SUBMITTED state.
	order := h.structured(t, h.callTool(t, "get_network_order", map[string]any{
		"networkRef": "po-mcp-1"}))
	if order["state"] != "SUBMITTED" || order["localOrderId"] != "om-mcp-1" {
		t.Fatalf("get_network_order = %v", order)
	}
	lines, _ := order["lines"].([]any)
	if len(lines) != 1 {
		t.Fatalf("get_network_order lines = %v", order["lines"])
	}
	line := lines[0].(map[string]any)
	if line["networkProductId"] != "NPROD-MCP" || line["sku"] != "SKU-MCP" {
		t.Fatalf("line must carry both vocabularies: %v", line)
	}

	// list_network_orders unfiltered: both seeded orders.
	all := h.structured(t, h.callTool(t, "list_network_orders", map[string]any{}))
	if orders := intField(asSlice(all["networkOrders"])); orders != 2 {
		t.Fatalf("list_network_orders = %v", all["networkOrders"])
	}

	// State filter: only the rejected one.
	rej := h.structured(t, h.callTool(t, "list_network_orders", map[string]any{
		"state": "REJECTED"}))
	items := asSlice(rej["networkOrders"])
	if len(items) != 1 || items[0].(map[string]any)["networkRef"] != "po-mcp-2" {
		t.Fatalf("list_network_orders state=REJECTED = %v", rej["networkOrders"])
	}

	// list_capability_offers: the throughput-constrained advertised figure.
	offers := h.structured(t, h.callTool(t, "list_capability_offers", map[string]any{}))
	offerItems := asSlice(offers["capabilityOffers"])
	if len(offerItems) != 1 {
		t.Fatalf("list_capability_offers = %v", offers["capabilityOffers"])
	}
	offer := offerItems[0].(map[string]any)
	if offer["sku"] != "SKU-MCP" || offer["advertisedQuantity"] != 120.0 ||
		offer["basis"] != "THROUGHPUT_CONSTRAINED" {
		t.Fatalf("capability offer = %v", offer)
	}

	// get_acknowledgement_report through the real REST client: the stub's
	// day bucket comes back with the counts the reports service served.
	report := h.structured(t, h.callTool(t, "get_acknowledgement_report", map[string]any{
		"from": "2026-10-09T00:00:00Z", "to": "2026-10-10T00:00:00Z"}))
	rows := asSlice(report["rows"])
	if len(rows) != 1 {
		t.Fatalf("get_acknowledgement_report rows = %v", report["rows"])
	}
	row := rows[0].(map[string]any)
	if row["ordersReceived"] != 2.0 || row["ordersAcknowledged"] != 1.0 {
		t.Fatalf("report row = %v", row)
	}
}

// TestMCP_CallToolSurfacesDomainAndInputErrorsAsToolErrors proves the
// rejection paths a host relies on: a domain not-found, an invalid state
// filter, and a missing required argument all come back as tool errors
// (res.IsError), never as transport or protocol failures.
func TestMCP_CallToolSurfacesDomainAndInputErrorsAsToolErrors(t *testing.T) {
	h := newMCPHarness(t)

	// Domain rejection: no order has this ref.
	if res := h.callTool(t, "get_network_order", map[string]any{
		"networkRef": "po-mcp-nope"}); !res.IsError {
		t.Fatal("get_network_order of an unknown ref must surface a tool error, not success")
	}

	// Invalid input: an empty networkRef fails the tool's shape check.
	if res := h.callTool(t, "get_network_order", map[string]any{
		"networkRef": ""}); !res.IsError {
		t.Fatal("get_network_order with an empty networkRef must surface a tool error")
	}

	// Invalid list query: a state outside the enum.
	if res := h.callTool(t, "list_network_orders", map[string]any{
		"state": "Nonsense"}); !res.IsError {
		t.Fatal("list_network_orders with an invalid state must surface a tool error")
	}

	// Missing required window on the curated report tool.
	if res := h.callTool(t, "get_acknowledgement_report", map[string]any{
		"from": "2026-10-09T00:00:00Z"}); !res.IsError {
		t.Fatal("get_acknowledgement_report without to must surface a tool error")
	}
}

// TestMCP_ReadResourceServesTheNetworkOrderResource proves the resource
// half of the wire contract: the network-order resource template resolves
// a seeded ref to the same DTO the tools serve, and an unknown ref is a
// protocol-level error for resources/read.
func TestMCP_ReadResourceServesTheNetworkOrderResource(t *testing.T) {
	h := newMCPHarness(t)

	res, err := h.session.ReadResource(context.Background(), &sdkmcp.ReadResourceParams{
		URI: "network-order://network-fulfillment/po-mcp-1",
	})
	if err != nil {
		t.Fatalf("resources/read: %v", err)
	}
	if len(res.Contents) != 1 || res.Contents[0].MIMEType != "application/json" {
		t.Fatalf("resource contents = %+v", res.Contents)
	}
	var order map[string]any
	if err := json.Unmarshal([]byte(res.Contents[0].Text), &order); err != nil {
		t.Fatalf("resource body is not JSON: %v", err)
	}
	if order["networkRef"] != "po-mcp-1" || order["state"] != "SUBMITTED" {
		t.Fatalf("resource body = %v", order)
	}

	// An unknown ref has no resource: the read fails at the protocol
	// level (a resource that does not exist), which is exactly how the
	// SDK surfaces it.
	if _, err := h.session.ReadResource(context.Background(), &sdkmcp.ReadResourceParams{
		URI: "network-order://network-fulfillment/po-mcp-nope",
	}); err == nil {
		t.Fatal("resources/read of an unknown ref must fail")
	}
}

// --- small helpers -----------------------------------------------------------

// asSlice coerces a JSON array field.
func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// intField returns the length of a JSON array (0 when absent).
func intField(v []any) int { return len(v) }
