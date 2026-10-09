// Package main_test hosts the godog (Cucumber for Go) acceptance suite. It
// drives the REAL HTTP routers over HTTP — the OLTP server's Routes() and the
// chi router of the netfulfil-reports deployable, wired the way
// cmd/netfulfil and cmd/netfulfil-reports wire them in production, but over
// the in-memory outbound adapters — so every scenario in features/*.feature
// is a true black-box test of the REST API.
//
// Demand never enters this context over HTTP (it is POLLED from the network,
// ADR 0001 §5), so scenarios that need orders seed the real StubGateway and
// run the real poller, exactly as the service does.
package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// TestFeatures runs every Gherkin feature under features/ against freshly
// wired servers.
//
// Scenarios tagged @known-bug describe the CORRECT behaviour from
// apis/openapi.yaml for a defect the suite found; they are excluded from the
// run until the defect is fixed (the tag is then deleted), so the suite never
// encodes the bug as expected behaviour. See docs/adr/0018.
func TestFeatures(t *testing.T) {
	// BDD_TAGS overrides the tag filter, e.g. BDD_TAGS=@known-bug to run only
	// the scenarios that describe a defect and watch them fail.
	tags := os.Getenv("BDD_TAGS")
	if tags == "" {
		tags = "~@known-bug"
	}
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Tags:     tags,
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}

// world is the per-scenario state: every collaborator freshly built over
// in-memory adapters, the servers started on demand, and whatever the last
// HTTP call returned.
type world struct {
	clock     *fixedClock
	orders    *memory.NetworkOrderRepo
	offers    *memory.CapabilityOfferRepo
	dict      *memory.ProductTranslation
	gateway   *recordingGateway
	planner   *fakePlanner
	bus       *eventBus
	store     *analyticsstore.MemoryStore
	readiness *inboundhttp.Readiness
	metrics   *telemetry.CircuitBreakerMetrics
	inventory *fakeInventory
	pathCap   *fakePathCap
	capacity  *fakeCapacity
	consumer  *inboundkafka.AnalyticsConsumer

	receive       *usecases.ReceiveNetworkDemand
	reconcile     *usecases.ReconcileSubmittedOrders
	sweep         *usecases.SweepAcknowledgementDeadlines
	rejectOverdue *usecases.RejectOverdueOrders
	recompute     *usecases.RecomputeCapabilityOffers
	poller        *poller.Poller

	// offersEnabled mirrors CAPABILITY_OFFER_ENABLED: when false the route is
	// not registered at all.
	offersEnabled bool
	api           *httptest.Server
	reports       *httptest.Server
	client        *http.Client

	demands map[string]contract.InboundDemand
	err     error

	status  int
	body    []byte
	headers http.Header
}

// reset builds the composition root the way cmd/netfulfil does, but over the
// in-memory adapters, a fixed clock and the in-process doubles.
func (w *world) reset() {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	w.clock = newFixedClock(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))
	w.orders = memory.NewNetworkOrderRepo()
	w.offers = memory.NewCapabilityOfferRepo()
	w.dict = memory.NewProductTranslation()
	w.gateway = newRecordingGateway(network.NewStubGateway(logger))
	w.planner = newFakePlanner()
	w.readiness = &inboundhttp.Readiness{}
	w.metrics = telemetry.NewCircuitBreakerMetrics()
	w.inventory = newFakeInventory()
	w.pathCap = &fakePathCap{}
	w.capacity = &fakeCapacity{units: map[string]int{}}

	w.store = analyticsstore.NewMemoryStore()
	w.store.Now = w.clock.Now
	w.consumer = &inboundkafka.AnalyticsConsumer{Projection: w.store, Processed: seenEvents{}}
	w.bus = newEventBus(w.consumer)

	w.receive = &usecases.ReceiveNetworkDemand{
		Orders: w.orders, Gateway: w.gateway, Planner: w.planner,
		Translation: w.dict, Events: w.bus, Clock: w.clock,
	}
	w.reconcile = &usecases.ReconcileSubmittedOrders{
		Orders: w.orders, Gateway: w.gateway, Planner: w.planner, Events: w.bus, Clock: w.clock,
	}
	w.sweep = &usecases.SweepAcknowledgementDeadlines{Orders: w.orders, Events: w.bus, Clock: w.clock}
	w.rejectOverdue = &usecases.RejectOverdueOrders{
		Orders: w.orders, Planner: w.planner, Events: w.bus, Clock: w.clock,
	}
	w.recompute = &usecases.RecomputeCapabilityOffers{
		Translation: w.dict, Inventory: w.inventory, PathCap: w.pathCap, Capacity: w.capacity,
		Offers: w.offers, Clock: w.clock, Logger: logger, SiteId: "site-1",
	}
	w.poller = poller.New(w.gateway, w.receive, w.clock, poller.Config{Interval: time.Hour}, logger)

	w.offersEnabled = true
	w.demands = map[string]contract.InboundDemand{}
	w.err = nil
	w.status, w.body, w.headers = 0, nil, nil
	// Redirects are NOT followed: a 301 from the router is itself behaviour
	// the scenarios may assert on.
	w.client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// apiServer starts the OLTP REST server on first use, so a scenario can
// decide whether capability offers are enabled before it comes up.
func (w *world) apiServer() *httptest.Server {
	if w.api == nil {
		s := &inboundhttp.Server{
			Orders:          w.orders,
			Poller:          w.poller,
			Clock:           w.clock,
			NetworkMode:     string(network.ModeStub),
			Readiness:       w.readiness,
			MetricsRegistry: w.metrics.Registry,
			ConfirmShipment: &usecases.ConfirmNetworkOrderShipment{
				Orders: w.orders, Gateway: w.gateway, Events: w.bus, Clock: w.clock,
			},
		}
		if w.offersEnabled {
			s.Offers = w.offers
		}
		w.api = httptest.NewServer(s.Routes())
	}
	return w.api
}

// reportsServer starts the netfulfil-reports chi router over the read model
// the production analytics consumer has been projecting into.
func (w *world) reportsServer() *httptest.Server {
	if w.reports == nil {
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		handlers := &inboundhttp.ReportsHandlers{Store: w.store}
		w.reports = httptest.NewServer(inboundhttp.NewReportsRouter(handlers, logger))
	}
	return w.reports
}

func (w *world) stop() {
	for _, s := range []*httptest.Server{w.api, w.reports} {
		if s != nil {
			s.Close()
		}
	}
	w.api, w.reports = nil, nil
}

// do issues a real net/http request. No Authorization header is ever sent:
// this context's endpoints are unauthenticated by fleet rule.
func (w *world) do(ctx context.Context, srv *httptest.Server, method, path string) (int, []byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, method, srv.URL+path, http.NoBody)
	if err != nil {
		return 0, nil, nil, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, body, resp.Header, nil
}

// record issues a request and remembers it as the response the Then steps
// assert against.
func (w *world) record(ctx context.Context, srv *httptest.Server, method, path string) error {
	status, body, headers, err := w.do(ctx, srv, method, path)
	if err != nil {
		return err
	}
	w.status, w.body, w.headers = status, body, headers
	return nil
}

func (w *world) decode(dest any) error {
	if err := json.Unmarshal(w.body, dest); err != nil {
		return fmt.Errorf("response body is not valid JSON (%w): %s", err, string(w.body))
	}
	return nil
}

// ------------------------------------------------------- generic steps ----

func (w *world) iGET(ctx context.Context, path string) error {
	return w.record(ctx, w.apiServer(), http.MethodGet, path)
}

func (w *world) iGETFromReports(ctx context.Context, path string) error {
	return w.record(ctx, w.reportsServer(), http.MethodGet, path)
}

func (w *world) theResponseStatusIs(expected int) error {
	if w.status != expected {
		return fmt.Errorf("expected status %d, got %d: %s", expected, w.status, string(w.body))
	}
	return nil
}

// theProblemDetailTypeIs asserts the RFC 7807 body: correct content type, and
// a "type" URI whose last segment identifies the error category.
func (w *world) theProblemDetailTypeIs(slug string) error {
	if ct := w.headers.Get("Content-Type"); ct != "application/problem+json" {
		return fmt.Errorf("expected Content-Type application/problem+json, got %q", ct)
	}
	var problem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	}
	if err := w.decode(&problem); err != nil {
		return err
	}
	if got := problem.Type[strings.LastIndex(problem.Type, "/")+1:]; got != slug {
		return fmt.Errorf("expected problem type %q, got %q (from %q)", slug, got, problem.Type)
	}
	if problem.Title == "" {
		return fmt.Errorf("expected the problem document to carry a title")
	}
	if problem.Status != w.status {
		return fmt.Errorf("problem body status %d does not match HTTP status %d", problem.Status, w.status)
	}
	return nil
}

func (w *world) theResponseIsNotAnAuthenticationChallenge() error {
	if w.status == http.StatusUnauthorized || w.status == http.StatusForbidden {
		return fmt.Errorf("expected an unauthenticated endpoint, got %d", w.status)
	}
	if challenge := w.headers.Get("WWW-Authenticate"); challenge != "" {
		return fmt.Errorf("expected no WWW-Authenticate challenge, got %q", challenge)
	}
	return nil
}

func (w *world) theBodyContains(fragment string) error {
	if !strings.Contains(string(w.body), fragment) {
		return fmt.Errorf("expected the body to contain %s, got %s", fragment, string(w.body))
	}
	return nil
}

func (w *world) timePasses(n int, unit string) error {
	unitDuration := map[string]time.Duration{"second": time.Second, "minute": time.Minute, "hour": time.Hour}[unit]
	w.clock.Advance(time.Duration(n) * unitDuration)
	return nil
}

// InitializeScenario registers the step definitions and gives every scenario
// its own wiring and its own in-memory state.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.reset()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.stop()
		return ctx, nil
	})

	sc.Step(`^I GET "([^"]*)"$`, w.iGET)
	sc.Step(`^I GET "([^"]*)" without credentials$`, w.iGET)
	sc.Step(`^I GET "([^"]*)" on the reports service$`, w.iGETFromReports)
	sc.Step(`^the response status is (\d+)$`, w.theResponseStatusIs)
	sc.Step(`^the problem detail type is "([^"]*)"$`, w.theProblemDetailTypeIs)
	sc.Step(`^the response is not an authentication challenge$`, w.theResponseIsNotAnAuthenticationChallenge)
	sc.Step(`^the response body contains (.+)$`, w.theBodyContains)
	sc.Step(`^(\d+) (second|minute|hour)s? (?:pass|passes)$`, w.timePasses)

	w.registerOrderSteps(sc)
	w.registerLifecycleSteps(sc)
	w.registerOpsSteps(sc)
	w.registerEventSteps(sc)
	w.registerOfferSteps(sc)
	w.registerReportSteps(sc)
}
