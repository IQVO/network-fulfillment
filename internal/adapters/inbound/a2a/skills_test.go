package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var base = time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// harness builds Deps over in-memory repos, seeding directly through the
// domain constructors (network orders only arrive by polling, so there is no
// use case to seed through).
type harness struct {
	t      *testing.T
	orders *memory.NetworkOrderRepo
	offers *memory.CapabilityOfferRepo
	deps   Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	orders := memory.NewNetworkOrderRepo()
	offers := memory.NewCapabilityOfferRepo()
	return &harness{t: t, orders: orders, offers: offers, deps: Deps{Orders: orders, Offers: offers, Clock: fixedClock{now: base}}}
}

func (h *harness) seedOrder(ref string, receivedAt time.Time, mutate func(*networkorder.NetworkOrder)) {
	h.t.Helper()
	line, err := networkorder.NewLine("1", "ASIN-1", "sku-1", 3)
	if err != nil {
		h.t.Fatalf("NewLine: %v", err)
	}
	o, err := networkorder.Receive(shared.NetworkRef(ref), "site-1", receivedAt.Add(48*time.Hour), []networkorder.Line{line}, receivedAt)
	if err != nil {
		h.t.Fatalf("Receive: %v", err)
	}
	if mutate != nil {
		mutate(o)
	}
	if err := h.orders.Save(context.Background(), o); err != nil {
		h.t.Fatalf("Save: %v", err)
	}
}

func (h *harness) seedOffer(sku, site string, qty int, basis capabilityoffer.Basis) {
	h.t.Helper()
	o, err := capabilityoffer.New(shared.SKU(sku), shared.SiteId(site), qty, qty+10, basis, base)
	if err != nil {
		h.t.Fatalf("capabilityoffer.New: %v", err)
	}
	if err := h.offers.Save(context.Background(), o); err != nil {
		h.t.Fatalf("Save offer: %v", err)
	}
}

// seedStandard seeds four orders: po-1 NEW, po-2 NEW and overdue, po-3
// SUBMITTED (with its held order), po-4 REJECTED.
func (h *harness) seedStandard() {
	h.seedOrder("po-1", base.Add(-time.Hour), nil)
	h.seedOrder("po-2", base.Add(-25*time.Hour), nil)
	h.seedOrder("po-3", base.Add(-2*time.Hour), func(o *networkorder.NetworkOrder) {
		if err := o.RecordHold("lo-3"); err != nil {
			h.t.Fatalf("RecordHold: %v", err)
		}
		if err := o.Submit(); err != nil {
			h.t.Fatalf("Submit: %v", err)
		}
	})
	h.seedOrder("po-4", base.Add(-3*time.Hour), func(o *networkorder.NetworkOrder) {
		if err := o.RecordHold("lo-4"); err != nil {
			h.t.Fatalf("RecordHold: %v", err)
		}
		if err := o.Reject(); err != nil {
			h.t.Fatalf("Reject: %v", err)
		}
	})
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
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
				ComputedAt         string `json:"computedAt"`
			} `json:"offers"`
		} `json:"advertisedAvailability"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct{ client *a2aclient.Client }

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope and the
// one-line text summary of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+"/"+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
}

func TestWireNetworkDemandStatusOneOrderByRef(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillNetworkDemandStatus, map[string]any{"networkRef": "po-3"})
	d := env.Data.NetworkDemand
	if env.Problem != nil || d == nil || d.Count != 1 || len(d.Orders) != 1 {
		t.Fatalf("envelope = %+v", env)
	}
	o := d.Orders[0]
	if o.NetworkRef != "po-3" || o.State != "SUBMITTED" || o.SiteId != "site-1" || o.AcknowledgementOverdue {
		t.Fatalf("order = %+v", o)
	}
	if o.LocalOrderId == nil || *o.LocalOrderId != "lo-3" {
		t.Fatalf("localOrderId = %v, want lo-3 once SUBMITTED", o.LocalOrderId)
	}
	if text != "po-3: SUBMITTED, 1 line(s)" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireNetworkDemandStatusCarriesBothVocabularies(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	env, _ := newWire(t, h.deps).skill(t, skillNetworkDemandStatus, map[string]any{"networkRef": "po-1"})
	d := env.Data.NetworkDemand
	if d == nil || len(d.Orders) != 1 {
		t.Fatalf("envelope = %+v", env)
	}
	lines := d.Orders[0].Lines
	if len(lines) != 1 || lines[0].SKU != "sku-1" || lines[0].NetworkProductId != "ASIN-1" || lines[0].Quantity != 3 {
		t.Fatalf("lines = %+v, want both vocabularies", lines)
	}
}

// A rejected order keeps its hold id internally (the hold sweep needs it) but
// the published contract shows localOrderId only for a committed order, so the
// agent must not expose it - the same rule as REST and MCP.
func TestWireRejectedOrderDoesNotExposeItsHold(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	env, _ := newWire(t, h.deps).skill(t, skillNetworkDemandStatus, map[string]any{"networkRef": "po-4"})
	d := env.Data.NetworkDemand
	if d == nil || d.Orders[0].State != "REJECTED" || d.Orders[0].LocalOrderId != nil {
		t.Fatalf("a REJECTED order must not expose localOrderId: %+v", env)
	}
}

func TestWireNetworkDemandStatusReportsOverdueWithTheDomainPredicate(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillNetworkDemandStatus, map[string]any{"networkRef": "po-2"})
	if d := env.Data.NetworkDemand; d == nil || !d.Orders[0].AcknowledgementOverdue {
		t.Fatalf("po-2 was received 25h ago: want acknowledgementOverdue, got %+v", env)
	}
	if text != "po-2: NEW, 1 line(s), acknowledgement overdue" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireNetworkDemandStatusListsAndFiltersByState(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillNetworkDemandStatus, map[string]any{})
	d := env.Data.NetworkDemand
	if d == nil || d.Count != 4 || d.Orders[0].NetworkRef != "po-1" || d.Orders[3].NetworkRef != "po-4" {
		t.Fatalf("every order, sorted by ref, expected: %+v", d)
	}
	if text != "4 network orders, 1 past the acknowledgement deadline" {
		t.Fatalf("summary = %q", text)
	}

	env, text = w.skill(t, skillNetworkDemandStatus, map[string]any{"state": "NEW"})
	d = env.Data.NetworkDemand
	if d == nil || d.Count != 2 || d.Orders[0].NetworkRef != "po-1" || d.Orders[1].NetworkRef != "po-2" {
		t.Fatalf("NEW orders expected: %+v", d)
	}
	if text != "2 NEW network orders, 1 past the acknowledgement deadline" {
		t.Fatalf("summary = %q", text)
	}

	env, _ = w.skill(t, skillNetworkDemandStatus, map[string]any{"state": "CONFIRMED"})
	if d := env.Data.NetworkDemand; env.Problem != nil || d == nil || d.Count != 0 || d.Orders == nil {
		t.Fatalf("a state with no orders is an empty list, not a problem: %+v", env)
	}
}

func (h *harness) seedThreeOffers() {
	h.seedOffer("sku-2", "site-1", 3, capabilityoffer.BasisThroughputConstrained)
	h.seedOffer("sku-1", "site-1", 7, capabilityoffer.BasisPhysical)
	h.seedOffer("sku-1", "site-2", 5, capabilityoffer.BasisPhysical)
}

func TestWireAdvertisedAvailabilityListsEveryOfferSorted(t *testing.T) {
	h := newHarness(t)
	h.seedThreeOffers()
	env, text := newWire(t, h.deps).skill(t, skillAdvertisedAvailability, map[string]any{})
	a := env.Data.AdvertisedAvailability
	if env.Problem != nil || a == nil || a.Count != 3 {
		t.Fatalf("envelope = %+v", env)
	}
	if a.Offers[0].SKU != "sku-1" || a.Offers[0].SiteId != "site-1" || a.Offers[0].AdvertisedQuantity != 7 || a.Offers[0].Basis != "PHYSICAL" || a.Offers[0].ComputedAt != "2026-09-23T08:00:00Z" {
		t.Fatalf("first offer = %+v, want sku-1/site-1 7 PHYSICAL (sorted by sku, site)", a.Offers[0])
	}
	if a.Offers[2].SKU != "sku-2" || a.Offers[2].Basis != "THROUGHPUT_CONSTRAINED" {
		t.Fatalf("last offer = %+v", a.Offers[2])
	}
	if text != "3 advertised offer(s)" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireAdvertisedAvailabilityFilters(t *testing.T) {
	h := newHarness(t)
	h.seedThreeOffers()
	w := newWire(t, h.deps)

	env, _ := w.skill(t, skillAdvertisedAvailability, map[string]any{"sku": "sku-1", "siteId": "site-2"})
	if a := env.Data.AdvertisedAvailability; a == nil || a.Count != 1 || a.Offers[0].AdvertisedQuantity != 5 {
		t.Fatalf("sku+site filter: %+v", env.Data.AdvertisedAvailability)
	}
	env, _ = w.skill(t, skillAdvertisedAvailability, map[string]any{"sku": "ghost"})
	if a := env.Data.AdvertisedAvailability; env.Problem != nil || a == nil || a.Count != 0 || a.Offers == nil {
		t.Fatalf("a filter matching nothing is an empty list, not a problem: %+v", env)
	}
}

func TestWireUnknownNetworkRefIsAnsweredAs404NotRaised(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	env, text := w.skill(t, skillNetworkDemandStatus, map[string]any{"networkRef": "ghost"})
	wantProblem(t, env, 404, "network-order-not-found")
	if !strings.Contains(env.Problem.Detail, `"ghost"`) || !strings.Contains(text, "404") {
		t.Fatalf("detail = %q, text = %q", env.Problem.Detail, text)
	}
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	cases := map[string]struct {
		skill string
		args  map[string]any
	}{
		"ref and state together":      {skillNetworkDemandStatus, map[string]any{"networkRef": "po-1", "state": "NEW"}},
		"empty ref":                   {skillNetworkDemandStatus, map[string]any{"networkRef": ""}},
		"blank ref":                   {skillNetworkDemandStatus, map[string]any{"networkRef": "   "}},
		"unknown state":               {skillNetworkDemandStatus, map[string]any{"state": "SHIPPED"}},
		"empty state":                 {skillNetworkDemandStatus, map[string]any{"state": ""}},
		"typo'd argument":             {skillNetworkDemandStatus, map[string]any{"stat": "NEW"}},
		"unknown argument":            {skillNetworkDemandStatus, map[string]any{"force": true}},
		"wrong type":                  {skillNetworkDemandStatus, map[string]any{"networkRef": 42}},
		"availability: empty sku":     {skillAdvertisedAvailability, map[string]any{"sku": ""}},
		"availability: blank site":    {skillAdvertisedAvailability, map[string]any{"siteId": "  "}},
		"availability: typo'd name":   {skillAdvertisedAvailability, map[string]any{"skuu": "sku-1"}},
		"availability: network vocab": {skillAdvertisedAvailability, map[string]any{"networkProductId": "ASIN-1"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 400, "invalid-argument")
		})
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	w := newWire(t, h.deps)

	env, _ := w.call(t, a2a.NewTextPart("not json at all"))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillNetworkDemandStatus, "arguments": map[string]any{}, "extra": 1}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"network_demand_status","arguments":{}} {"trailing":true}`))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.skill(t, "confirm_shipment", map[string]any{"networkRef": "po-1"})
	wantProblem(t, env, 400, "unknown-skill")

	// A text part holding the same JSON is accepted (documented contract).
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"network_demand_status","arguments":{"networkRef":"po-1"}}`))
	if env.Problem != nil || env.Data.NetworkDemand == nil {
		t.Fatalf("a text part holding the skill JSON must be accepted, got %+v", env)
	}
	// Absent arguments are the empty object.
	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillNetworkDemandStatus}))
	if env.Problem != nil || env.Data.NetworkDemand == nil || env.Data.NetworkDemand.Count != 4 {
		t.Fatalf("absent arguments must list every order, got %+v", env)
	}
}

// failingOrders and failingOffers answer every read with an error, to prove a
// store failure is answered in-band as a 500 that leaks nothing.
type failingOrders struct {
	ports.NetworkOrderRepo
	err error
}

func (f failingOrders) FindByRef(context.Context, shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	return nil, f.err
}

func (f failingOrders) ListAll(context.Context) ([]*networkorder.NetworkOrder, error) {
	return nil, f.err
}

type failingOffers struct {
	ports.CapabilityOfferRepo
	err error
}

func (f failingOffers) ListAll(context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	return nil, f.err
}

func TestWireStoreFailuresAreAnsweredAs500WithoutLeakingTheCause(t *testing.T) {
	secret := errors.New("pq: password authentication failed for user netfulfil at 10.0.0.7")
	w := newWire(t, Deps{Orders: failingOrders{err: secret}, Offers: failingOffers{err: secret}, Clock: fixedClock{now: base}})

	for _, call := range []struct {
		skill string
		args  map[string]any
	}{
		{skillNetworkDemandStatus, map[string]any{"networkRef": "po-1"}},
		{skillNetworkDemandStatus, map[string]any{}},
		{skillAdvertisedAvailability, map[string]any{}},
	} {
		env, text := w.skill(t, call.skill, call.args)
		wantProblem(t, env, 500, "internal-error")
		for _, s := range []string{env.Problem.Detail, env.Problem.Title, text} {
			if strings.Contains(s, "password") || strings.Contains(s, "10.0.0.7") {
				t.Fatalf("an answer leaks the store error: %q", s)
			}
		}
	}
}

func TestWireUnwiredPortsAreAnswered503NotAnEmptyList(t *testing.T) {
	w := newWire(t, Deps{})
	env, _ := w.skill(t, skillNetworkDemandStatus, map[string]any{})
	wantProblem(t, env, 503, "dependency-unavailable")
	env, _ = w.skill(t, skillAdvertisedAvailability, map[string]any{})
	wantProblem(t, env, 503, "dependency-unavailable")
}

// TestCardIsServedAndDialable proves discovery end to end: the card handler
// serves the card at the well-known path and a client built from the FETCHED
// card reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	h := newHarness(t)
	h.seedStandard()
	mux := http.NewServeMux()
	mux.Handle("/", Handler(NewExecutor(h.deps)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(Config{AgentURL: srv.URL + "/", Version: "test"})))

	res, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("card fetch = %v / %v", res, err)
	}
	defer func() { _ = res.Body.Close() }()
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatal(err)
	}
	env, _ := (&wire{client: client}).skill(t, skillNetworkDemandStatus, map[string]any{"networkRef": "po-1"})
	if env.Data.NetworkDemand == nil {
		t.Fatalf("a client built from the served card could not call the agent: %+v", env)
	}
}
