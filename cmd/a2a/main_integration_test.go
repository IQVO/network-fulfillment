//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/jackc/pgx/v5/pgxpool"

	// pgtest boots this package's Postgres through testcontainers-go; the
	// blank import keeps that obligation visible in the package that relies
	// on it (internal/architecture's
	// TestPostgresIntegrationTestsUseTestcontainers checks exactly this).
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"

	inbounda2a "github.com/claudioed/network-fulfillment/internal/adapters/inbound/a2a"
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

// wireEnvelope is a TEST-LOCAL copy of the answer contract (the adapter's
// envelope): if the inbound adapter drifts, this suite fails instead of
// silently following it — the same discipline internal/adapters/inbound/a2a's
// own suite applies.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		NetworkDemand *struct {
			Count  int `json:"count"`
			Orders []struct {
				NetworkRef             string  `json:"networkRef"`
				SiteId                 string  `json:"siteId"`
				State                  string  `json:"state"`
				LocalOrderId           *string `json:"localOrderId"`
				AcknowledgementOverdue bool    `json:"acknowledgementOverdue"`
				Lines                  []struct {
					NetworkProductId string `json:"networkProductId"`
					SKU              string `json:"sku"`
					Quantity         int    `json:"quantity"`
				} `json:"lines"`
			} `json:"orders"`
		} `json:"networkDemand"`
		AdvertisedAvailability *struct {
			Count  int `json:"count"`
			Offers []struct {
				SKU                string `json:"sku"`
				SiteId             string `json:"siteId"`
				AdvertisedQuantity int    `json:"advertisedQuantity"`
				Basis              string `json:"basis"`
			} `json:"offers"`
		} `json:"advertisedAvailability"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	} `json:"problem"`
}

// TestA2AAgainstPostgres_ReadsWhatTheCommandSideWroteAndNeverWrites proves the
// composition this binary boots answers over the real JSON-RPC router on the
// real Postgres the command side writes: buildOrders — the same migration,
// pool and repository wiring run() performs — migrates a database itself
// (idempotently: pgtest hands it an already-migrated one), both documented
// skills answer exactly what the domain aggregates persisted, the Agent Card
// is served at the well-known path with exactly those two skills (ADR 0030),
// and the outbox is untouched afterwards: the agent is read-only, so there is
// nothing for a relay to publish from this binary.
func TestA2AAgainstPostgres_ReadsWhatTheCommandSideWroteAndNeverWrites(t *testing.T) {
	url := pgtest.URL(t) // a fresh, already migrated database
	ctx := context.Background()

	// The cmd's real composition: the same buildOrders / buildOffers / Deps
	// assembly run() performs, over the repository's migrations directory.
	orders, pool, closeOrders, err := buildOrders(ctx, url, url, "../../migrations", quietLogger())
	if err != nil {
		t.Fatalf("buildOrders: %v", err)
	}
	t.Cleanup(closeOrders)
	deps := inbounda2a.Deps{Orders: orders, Offers: buildOffers(pool), Clock: system.Clock{}}

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

	// The cmd's real router (healthz, the well-known Agent Card, the
	// JSON-RPC endpoint at both / and /a2a) around the real executor.
	handler := newRouter(
		inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: "http://placeholder.invalid/", Version: "it"}),
		"network-fulfillment-a2a",
	)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client, err := a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/", Version: "it"}))
	if err != nil {
		t.Fatal(err)
	}

	assertAgentCardServed(t, srv.URL)
	assertSeededReads(t, client)

	if after := countOutbox(); after != before {
		t.Fatalf("outbox rows %d -> %d: the A2A binary wrote an event", before, after)
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

// assertAgentCardServed proves the router really serves the Agent Card at the
// well-known path, advertising exactly the two curated read skills (ADR 0030).
func assertAgentCardServed(t *testing.T, srvURL string) {
	t.Helper()
	res, err := http.Get(srvURL + a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("fetch agent card: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("agent card status = %d", res.StatusCode)
	}
	var card struct {
		Name   string `json:"name"`
		Skills []struct {
			ID string `json:"id"`
		} `json:"skills"`
	}
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatalf("decode agent card: %v", err)
	}
	if card.Name != "network-fulfillment" {
		t.Fatalf("agent card name = %q", card.Name)
	}
	ids := map[string]bool{}
	for _, s := range card.Skills {
		ids[s.ID] = true
	}
	if !ids["network_demand_status"] || !ids["advertised_availability"] || len(ids) != 2 {
		t.Fatalf("agent card skills = %v, want exactly network_demand_status and advertised_availability", ids)
	}
}

// callSkill sends one skill request and returns the decoded envelope plus the
// reply's one-line text summary.
func callSkill(t *testing.T, client *a2aclient.Client, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("%s: %v", skill, err)
	}
	reply, ok := resp.(*a2a.Message)
	if !ok || len(reply.Parts) < 2 {
		t.Fatalf("%s: reply = %T, want a message with a data and a text part", skill, resp)
	}
	data, ok := reply.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("%s: first reply part is %T, want data", skill, reply.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("%s: reply is not the skill envelope: %v (%s)", skill, err, raw)
	}
	text, _ := reply.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

// assertSeededReads reads back, through both skills, what seedFulfilment
// persisted.
func assertSeededReads(t *testing.T, client *a2aclient.Client) {
	t.Helper()

	env, summary := callSkill(t, client, "network_demand_status", map[string]any{"networkRef": "po-it-1"})
	if env.Problem != nil || env.Data.NetworkDemand == nil || env.Data.NetworkDemand.Count != 1 {
		t.Fatalf("network_demand_status po-it-1 = %+v", env)
	}
	order := env.Data.NetworkDemand.Orders[0]
	if order.State != "ACKNOWLEDGED" || order.SiteId != "SITE-IT" {
		t.Fatalf("po-it-1 = %+v", order)
	}
	if order.LocalOrderId == nil || *order.LocalOrderId != "om-it-1" {
		t.Fatalf("po-it-1 localOrderId = %v, want om-it-1 (the persisted correlation)", order.LocalOrderId)
	}
	if order.AcknowledgementOverdue {
		t.Fatalf("po-it-1 is overdue inside its window: %+v", order)
	}
	if len(order.Lines) != 2 ||
		order.Lines[0].NetworkProductId != "NP-IT-1" || order.Lines[0].SKU != "sku-it-100" || order.Lines[0].Quantity != 3 {
		t.Fatalf("po-it-1 lines = %+v: both vocabularies must come back", order.Lines)
	}
	if summary == "" {
		t.Fatal("the reply's text part must carry the one-line summary")
	}

	env, _ = callSkill(t, client, "network_demand_status", map[string]any{"state": "NEW"})
	if env.Problem != nil || env.Data.NetworkDemand.Count != 1 {
		t.Fatalf("network_demand_status state=NEW = %+v", env)
	}
	if newOrder := env.Data.NetworkDemand.Orders[0]; newOrder.NetworkRef != "po-it-2" || !newOrder.AcknowledgementOverdue {
		t.Fatalf("state=NEW = %+v, want po-it-2 past its window", newOrder)
	}

	env, _ = callSkill(t, client, "network_demand_status", map[string]any{"state": "CONFIRMED"})
	if env.Problem != nil || env.Data.NetworkDemand.Count != 0 {
		t.Fatalf("state=CONFIRMED = %+v, want none", env)
	}

	// An unknown reference is a 404 problem answer, never an empty order
	// (the same classification the REST adapter applies).
	env, _ = callSkill(t, client, "network_demand_status", map[string]any{"networkRef": "po-it-ghost"})
	if env.Problem == nil || env.Problem.Status != 404 {
		t.Fatalf("po-it-ghost = %+v, want a 404 problem", env)
	}

	// networkRef and state are mutually exclusive (the card's contract).
	env, _ = callSkill(t, client, "network_demand_status", map[string]any{"networkRef": "po-it-1", "state": "NEW"})
	if env.Problem == nil || env.Problem.Status != 400 {
		t.Fatalf("networkRef+state = %+v, want a 400 problem", env)
	}

	// A skill the card does not offer is refused, never guessed.
	env, _ = callSkill(t, client, "no_such_skill", map[string]any{})
	if env.Problem == nil || env.Problem.Status != 400 {
		t.Fatalf("no_such_skill = %+v, want a 400 problem", env)
	}

	env, _ = callSkill(t, client, "advertised_availability", map[string]any{})
	if env.Problem != nil || env.Data.AdvertisedAvailability == nil || env.Data.AdvertisedAvailability.Count != 1 {
		t.Fatalf("advertised_availability = %+v", env)
	}
	offer := env.Data.AdvertisedAvailability.Offers[0]
	if offer.SKU != "sku-it-100" || offer.SiteId != "SITE-IT" ||
		offer.AdvertisedQuantity != 7 || offer.Basis != "THROUGHPUT_CONSTRAINED" {
		t.Fatalf("offer = %+v", offer)
	}

	// The sku/siteId narrowing filters, it never fabricates.
	env, _ = callSkill(t, client, "advertised_availability", map[string]any{"siteId": "SITE-OTHER"})
	if env.Problem != nil || env.Data.AdvertisedAvailability.Count != 0 {
		t.Fatalf("siteId=SITE-OTHER = %+v, want no offers", env)
	}
}
