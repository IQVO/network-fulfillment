//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	// pgtest boots this package's Postgres through testcontainers-go; the
	// blank import keeps that obligation visible in the package that relies
	// on it (internal/architecture's
	// TestPostgresIntegrationTestsUseTestcontainers checks exactly this).
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	"github.com/claudioed/network-fulfillment/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestMCPAgainstPostgres_ReadsWhatTheCommandSideWroteAndNeverWrites proves the
// composition this binary boots answers through its documented read tools over
// the real Postgres the command side writes: buildOrders — the same migration,
// pool and repository wiring run() performs — migrates a database itself
// (idempotently: pgtest hands it an already-migrated one, a second run must be
// a no-op), every registered tool answers exactly what the domain aggregates
// persisted, and the outbox is untouched afterwards. The tool governance (ADR
// 0002, tools.go's registerTools) registers no write tool — demand enters only
// by polling the network — so there is no write path to exercise and the
// unchanged outbox is the proof the reads never produced one.
func TestMCPAgainstPostgres_ReadsWhatTheCommandSideWroteAndNeverWrites(t *testing.T) {
	url := pgtest.URL(t) // a fresh, already migrated database
	ctx := context.Background()

	// The cmd's real composition: the same buildOrders / buildOffers / Deps
	// assembly run() performs, over the repository's migrations directory.
	orders, pool, closeOrders, err := buildOrders(ctx, url, url, "../../migrations", quietLogger())
	if err != nil {
		t.Fatalf("buildOrders: %v", err)
	}
	t.Cleanup(closeOrders)
	deps := inboundmcp.Deps{Orders: orders, Clock: system.Clock{}, Offers: buildOffers(pool)}

	seedFulfilment(t, pool)

	countOutbox := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
			t.Fatalf("count outbox_events: %v", err)
		}
		return n
	}
	before := countOutbox()

	ct, st := sdk.NewInMemoryTransports()
	ss, err := inboundmcp.NewServer(deps).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	session, err := sdk.NewClient(&sdk.Implementation{Name: "it", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	assertReadOnlyCatalogue(t, session)
	assertSeededReads(t, session)

	if after := countOutbox(); after != before {
		t.Fatalf("outbox rows %d -> %d: the MCP binary wrote an event", before, after)
	}
}

// seedFulfilment persists the state the assertions read back, the documented
// way: NetworkOrders through the domain aggregate and the Postgres repository
// the command side owns (networkorder.Receive plus the Acknowledge /
// LinkLocalOrder transitions the acknowledgement path drives), and one
// CapabilityOffer through its own aggregate constructor — the shape
// RecomputeCapabilityOffers persists (ADR 0001 section 8).
func seedFulfilment(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()

	// po-it-1: acknowledged demand linked to its order-management order —
	// the persisted correlation mapping, received 2h ago so its 24h
	// acknowledgement window (networkorder.AcknowledgementWindow) is still
	// comfortably in the future.
	acknowledged, err := networkorder.Receive(
		shared.NetworkRef("po-it-1"), shared.SiteId("SITE-IT"),
		now.Add(46*time.Hour),
		[]networkorder.Line{
			mustLine(t, "1", "NP-IT-1", "sku-it-100", 3),
			mustLine(t, "2", "NP-IT-2", "sku-it-200", 1),
		},
		now.Add(-2*time.Hour),
	)
	if err != nil {
		t.Fatalf("Receive po-it-1: %v", err)
	}
	if err := acknowledged.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge po-it-1: %v", err)
	}
	if err := acknowledged.LinkLocalOrder(shared.LocalOrderId("om-it-1")); err != nil {
		t.Fatalf("LinkLocalOrder po-it-1: %v", err)
	}
	if err := postgres.NewNetworkOrderRepo(pool).Save(ctx, acknowledged); err != nil {
		t.Fatalf("save po-it-1: %v", err)
	}

	// po-it-2: unanswered NEW demand received 1h past its acknowledgement
	// window, so the overdue flag must be true for it.
	overdue, err := networkorder.Receive(
		shared.NetworkRef("po-it-2"), shared.SiteId("SITE-IT"),
		now.Add(24*time.Hour),
		[]networkorder.Line{mustLine(t, "1", "NP-IT-1", "sku-it-100", 2)},
		now.Add(-networkorder.AcknowledgementWindow-time.Hour),
	)
	if err != nil {
		t.Fatalf("Receive po-it-2: %v", err)
	}
	if err := postgres.NewNetworkOrderRepo(pool).Save(ctx, overdue); err != nil {
		t.Fatalf("save po-it-2: %v", err)
	}

	// One advertised offer: throughput-constrained, not physical-stock-bound.
	offer, err := capabilityoffer.New(
		shared.SKU("sku-it-100"), shared.SiteId("SITE-IT"),
		7, 100, capabilityoffer.BasisThroughputConstrained,
		now.Add(-time.Minute),
	)
	if err != nil {
		t.Fatalf("capabilityoffer.New: %v", err)
	}
	if err := postgres.NewCapabilityOfferRepo(pool).Save(ctx, offer); err != nil {
		t.Fatalf("save offer: %v", err)
	}
}

// mustLine builds one order line the way demand intake does.
func mustLine(t *testing.T, ref, productId, sku string, qty int) networkorder.Line {
	t.Helper()
	l, err := networkorder.NewLine(
		shared.NetworkLineRef(ref), shared.NetworkProductId(productId), shared.SKU(sku), qty,
	)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return l
}

// assertReadOnlyCatalogue pins the tool governance the server registers: the
// documented read tools, each annotated read-only (ADR 0002's "no write
// surface"), and the curated report tool absent while REPORTS_BASE_URL is
// unset — cmd/mcp registers it only when the reports client is wired.
func assertReadOnlyCatalogue(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	res, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	want := map[string]bool{
		"get_network_order":      true,
		"list_network_orders":    true,
		"list_capability_offers": true,
	}
	got := map[string]bool{}
	for _, tool := range res.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool %q is not annotated read-only", tool.Name)
		}
		got[tool.Name] = true
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("tool %q is not registered; registered tools = %v", name, got)
		}
	}
	for name := range got {
		if !want[name] {
			t.Fatalf("unexpected tool %q registered; registered tools = %v", name, got)
		}
	}
}

// assertSeededReads reads back, through every tool, what seedFulfilment
// persisted.
func assertSeededReads(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	ctx := context.Background()
	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err != nil || res.IsError {
			t.Fatalf("%s: err=%v res=%+v", name, err, res)
		}
		return res.StructuredContent.(map[string]any)
	}
	mustFail := func(name string, args map[string]any) {
		t.Helper()
		res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err == nil && (res == nil || !res.IsError) {
			t.Fatalf("%s(%v) must fail, got err=%v res=%+v", name, args, err, res)
		}
	}

	order := call("get_network_order", map[string]any{"networkRef": "po-it-1"})
	if order["state"] != "ACKNOWLEDGED" || order["siteId"] != "SITE-IT" {
		t.Fatalf("get_network_order = %v", order)
	}
	if order["localOrderId"] != "om-it-1" {
		t.Fatalf("localOrderId = %v, want om-it-1 (the persisted correlation, never a string convention)", order["localOrderId"])
	}
	if order["acknowledgementOverdue"] != false {
		t.Fatalf("acknowledgementOverdue = %v, want false inside the window", order["acknowledgementOverdue"])
	}
	lines := order["lines"].([]any)
	if len(lines) != 2 {
		t.Fatalf("get_network_order lines = %v", lines)
	}
	first := lines[0].(map[string]any)
	if first["networkProductId"] != "NP-IT-1" || first["sku"] != "sku-it-100" || first["quantity"] != 3.0 {
		t.Fatalf("line 1 = %v: both vocabularies (network product id and resolved SKU) must come back", first)
	}

	overdue := call("get_network_order", map[string]any{"networkRef": "po-it-2"})
	if overdue["state"] != "NEW" || overdue["acknowledgementOverdue"] != true {
		t.Fatalf("po-it-2 = %v: want NEW demand past its acknowledgement window", overdue)
	}
	if overdue["localOrderId"] != nil {
		t.Fatalf("po-it-2 localOrderId = %v, want none before acknowledgement", overdue["localOrderId"])
	}

	if all := call("list_network_orders", map[string]any{})["networkOrders"].([]any); len(all) != 2 {
		t.Fatalf("list_network_orders = %v, want both seeded orders", all)
	}
	newOnly := call("list_network_orders", map[string]any{"state": "NEW"})["networkOrders"].([]any)
	if len(newOnly) != 1 || newOnly[0].(map[string]any)["networkRef"] != "po-it-2" {
		t.Fatalf("list_network_orders state=NEW = %v", newOnly)
	}
	if confirmed := call("list_network_orders", map[string]any{"state": "CONFIRMED"})["networkOrders"].([]any); len(confirmed) != 0 {
		t.Fatalf("list_network_orders state=CONFIRMED = %v, want none", confirmed)
	}
	// An unknown state is refused rather than silently matching nothing.
	mustFail("list_network_orders", map[string]any{"state": "BOGUS"})
	// A reference that was never received is a failed read, not an empty order.
	mustFail("get_network_order", map[string]any{"networkRef": "po-it-ghost"})

	offers := call("list_capability_offers", map[string]any{})["capabilityOffers"].([]any)
	if len(offers) != 1 {
		t.Fatalf("list_capability_offers = %v", offers)
	}
	offer := offers[0].(map[string]any)
	if offer["sku"] != "sku-it-100" || offer["siteId"] != "SITE-IT" ||
		offer["advertisedQuantity"] != 7.0 || offer["basis"] != "THROUGHPUT_CONSTRAINED" {
		t.Fatalf("offer = %v", offer)
	}
}
