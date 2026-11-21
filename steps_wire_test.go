package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The held-order wire steps (features/held_order_wire_rules.feature). They
// drive the REAL order-management planner — the production adapter of
// ports.FulfillmentPlanner — against an in-process double of its REST API, so
// the bytes this context puts on the wire are pinned: the site map of ADR 0035
// in front of ADR 0029's site-code rule, the hold and ship-complete flags, the
// network's deadline handed over unchanged, the SKU-sorted lines that make a
// retried raise byte-identical under the same Idempotency-Key, and the refusal
// classes of ADR 0037 the adapter reads for the two keyed routes.

// bddWirePromise is the departure the double commits to in its default
// answer; a fixed instant the verdict steps assert against.
var bddWirePromise = time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)

// bddWireOrder is the local order id the double answers every raise with.
const bddWireOrder = "ord-0217"

type bddWireRequest struct {
	method string
	path   string
	key    string
	body   []byte
}

// bddWireAnswer is one scripted answer; an empty problem means the answer
// carries no problem document at all.
type bddWireAnswer struct {
	status  int
	problem string
}

type bddWireWorld struct {
	srv      *httptest.Server
	planner  *ordermanagement.Planner
	mu       sync.Mutex
	requests []bddWireRequest
	scripted []bddWireAnswer
	promise  bool
	siteMap  map[shared.SiteId]shared.SiteId

	deadline   time.Time
	lastRaise  contract.HeldOrderRequest
	lastResult contract.HeldOrderResult
	lastErr    error
}

func (x *bddWireWorld) serve(rw http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	x.mu.Lock()
	x.requests = append(x.requests, bddWireRequest{
		method: r.Method, path: r.URL.Path,
		key: r.Header.Get("Idempotency-Key"), body: body,
	})
	ans := bddWireAnswer{status: http.StatusCreated}
	if n := len(x.scripted); n > 0 {
		ans, x.scripted = x.scripted[0], x.scripted[1:]
	}
	promise := x.promise
	x.mu.Unlock()

	if ans.status >= 400 {
		rw.WriteHeader(ans.status)
		if ans.problem != "" {
			_, _ = fmt.Fprintf(rw, `{"type":"https://order-management/problems/%s","detail":"suite double"}`, ans.problem)
			return
		}
		_, _ = io.WriteString(rw, `not a problem document`)
		return
	}
	rw.WriteHeader(ans.status)
	if promise {
		_, _ = fmt.Fprintf(rw, `{"id":%q,"promiseDate":%q}`, bddWireOrder, bddWirePromise.Format(time.RFC3339))
		return
	}
	_, _ = fmt.Fprintf(rw, `{"id":%q}`, bddWireOrder)
}

// wire returns the scenario's planner world, built on first use.
func (w *world) wire() *bddWireWorld {
	if w.wireW != nil {
		return w.wireW
	}
	x := &bddWireWorld{promise: true, siteMap: map[shared.SiteId]shared.SiteId{}}
	x.srv = httptest.NewServer(http.HandlerFunc(x.serve))
	w.wireW = x
	return x
}

// wireRaise raises a held order through the real planner and records the
// outcome for the assertion steps; the deadline is always 20 hours out.
func (w *world) wireRaise(req contract.HeldOrderRequest) error {
	x := w.wire()
	x.deadline = w.clock.Now().Add(20 * time.Hour)
	req.RequiredShipBy = x.deadline
	x.lastRaise = req
	x.planner = ordermanagement.NewPlanner(x.srv.URL, x.srv.Client(), ordermanagement.WithSiteMap(x.siteMap))
	x.lastResult, x.lastErr = x.planner.RaiseHeldOrder(context.Background(), req)
	return nil
}

// wireRaises returns every POST /orders the double saw, in order.
func (x *bddWireWorld) wireRaises() []bddWireRequest {
	x.mu.Lock()
	defer x.mu.Unlock()
	var out []bddWireRequest
	for _, r := range x.requests {
		if r.method == http.MethodPost && r.path == "/orders" {
			out = append(out, r)
		}
	}
	return out
}

func (x *bddWireWorld) lastWireRaise() (bddWireRequest, error) {
	raises := x.wireRaises()
	if len(raises) == 0 {
		return bddWireRequest{}, fmt.Errorf("order-management was never asked to raise a held order")
	}
	return raises[len(raises)-1], nil
}

// bddRaiseBody is the wire shape of POST /orders this context sends.
type bddRaiseBody struct {
	Lines []struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	} `json:"lines"`
	ReleaseOnAllocation  bool      `json:"releaseOnAllocation"`
	AllowPartialShipment bool      `json:"allowPartialShipment"`
	RequiredShipBy       time.Time `json:"requiredShipBy"`
}

func wireBodyOf(r bddWireRequest, out any) error {
	if err := json.Unmarshal(r.body, out); err != nil {
		return fmt.Errorf("the raise body is not JSON: %w", err)
	}
	return nil
}

func (w *world) theSiteMapTranslates(network, warehouse string) error {
	x := w.wire()
	x.siteMap[shared.SiteId(network)] = shared.SiteId(warehouse)
	return nil
}

func (w *world) omAnswersRaiseWith(kind string) error {
	w.wire().promise = kind == "a promise"
	return nil
}

func (w *world) omAnswersNextRaiseWithProblem(status int, slug string) error {
	x := w.wire()
	x.mu.Lock()
	x.scripted = append(x.scripted, bddWireAnswer{status: status, problem: slug})
	x.mu.Unlock()
	return nil
}

func (w *world) omAnswersNextRaiseWithNoProblem(status int) error {
	x := w.wire()
	x.mu.Lock()
	x.scripted = append(x.scripted, bddWireAnswer{status: status})
	x.mu.Unlock()
	return nil
}

func (w *world) raiseHeldOrder1(ref, site string, qty int, sku string) error {
	return w.wireRaise(contract.HeldOrderRequest{
		NetworkRef: shared.NetworkRef(ref), SiteId: shared.SiteId(site),
		Lines: map[shared.SKU]int{shared.SKU(sku): qty},
	})
}

func (w *world) raiseHeldOrder2(ref, site string, q1 int, sku1 string, q2 int, sku2 string) error {
	return w.wireRaise(contract.HeldOrderRequest{
		NetworkRef: shared.NetworkRef(ref), SiteId: shared.SiteId(site),
		Lines: map[shared.SKU]int{shared.SKU(sku1): q1, shared.SKU(sku2): q2},
	})
}

func (w *world) raiseHeldOrder4(ref, site string, q1 int, sku1 string, q2 int, sku2 string, q3 int, sku3 string, q4 int, sku4 string) error {
	return w.wireRaise(contract.HeldOrderRequest{
		NetworkRef: shared.NetworkRef(ref), SiteId: shared.SiteId(site),
		Lines: map[shared.SKU]int{
			shared.SKU(sku1): q1, shared.SKU(sku2): q2, shared.SKU(sku3): q3, shared.SKU(sku4): q4,
		},
	})
}

func (w *world) theRaiseIsRetried() error {
	x := w.wire()
	if x.planner == nil {
		return fmt.Errorf("no raise to retry")
	}
	x.lastResult, x.lastErr = x.planner.RaiseHeldOrder(context.Background(), x.lastRaise)
	return nil
}

func (w *world) theRaiseCarriedKey(key string) error {
	r, err := w.wire().lastWireRaise()
	if err != nil {
		return err
	}
	if r.key != key {
		return fmt.Errorf("the raise carried Idempotency-Key %q, want %q", r.key, key)
	}
	return nil
}

func (w *world) bothRaisesCarriedKey(key string) error {
	raises := w.wire().wireRaises()
	if len(raises) < 2 {
		return fmt.Errorf("order-management saw %d raises, want at least 2", len(raises))
	}
	a, b := raises[len(raises)-2], raises[len(raises)-1]
	if a.key != key || b.key != key {
		return fmt.Errorf("the raises carried keys %q and %q, want %q on both", a.key, b.key, key)
	}
	return nil
}

func (w *world) theRaiseCarriedSite(site string) error {
	r, err := w.wire().lastWireRaise()
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := wireBodyOf(r, &fields); err != nil {
		return err
	}
	raw, ok := fields["fulfillmentSiteId"]
	if !ok {
		return fmt.Errorf("the raise omitted fulfillmentSiteId, want %q: %s", site, r.body)
	}
	var got string
	if err := json.Unmarshal(raw, &got); err != nil || got != site {
		return fmt.Errorf("the raise carried fulfillmentSiteId %s, want %q", raw, site)
	}
	return nil
}

func (w *world) theRaiseOmittedSite() error {
	r, err := w.wire().lastWireRaise()
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := wireBodyOf(r, &fields); err != nil {
		return err
	}
	if raw, ok := fields["fulfillmentSiteId"]; ok {
		return fmt.Errorf("the raise carried fulfillmentSiteId %s, want the key absent", raw)
	}
	return nil
}

func (w *world) theRaiseWasHeldAndShipComplete() error {
	r, err := w.wire().lastWireRaise()
	if err != nil {
		return err
	}
	var body bddRaiseBody
	if err := wireBodyOf(r, &body); err != nil {
		return err
	}
	if body.ReleaseOnAllocation || body.AllowPartialShipment {
		return fmt.Errorf("the raise was releaseOnAllocation=%v allowPartialShipment=%v, want both false: %s",
			body.ReleaseOnAllocation, body.AllowPartialShipment, r.body)
	}
	return nil
}

func (w *world) theRaiseCarriedTheDeadline() error {
	x := w.wire()
	r, err := x.lastWireRaise()
	if err != nil {
		return err
	}
	var body bddRaiseBody
	if err := wireBodyOf(r, &body); err != nil {
		return err
	}
	if !body.RequiredShipBy.Equal(x.deadline) {
		return fmt.Errorf("the raise carried requiredShipBy %s, want the network's %s", body.RequiredShipBy, x.deadline)
	}
	return nil
}

func (w *world) theRaiseCarriedLinesInOrder(sku1, sku2, sku3, sku4 string) error {
	r, err := w.wire().lastWireRaise()
	if err != nil {
		return err
	}
	var body bddRaiseBody
	if err := wireBodyOf(r, &body); err != nil {
		return err
	}
	want := []string{sku1, sku2, sku3, sku4}
	if len(body.Lines) != len(want) {
		return fmt.Errorf("the raise carried %d lines, want %d", len(body.Lines), len(want))
	}
	for i, l := range body.Lines {
		if l.SKU != want[i] {
			return fmt.Errorf("line %d of the raise is %s, want %s (sorted by SKU)", i+1, l.SKU, want[i])
		}
	}
	return nil
}

func (w *world) theRetriedRaiseWasByteIdentical() error {
	raises := w.wire().wireRaises()
	if len(raises) < 2 {
		return fmt.Errorf("order-management saw %d raises, want at least 2", len(raises))
	}
	a, b := raises[len(raises)-2], raises[len(raises)-1]
	if a.key != b.key {
		return fmt.Errorf("the raises carried keys %q and %q", a.key, b.key)
	}
	if !bytes.Equal(a.body, b.body) {
		return fmt.Errorf("the retried raise differed: %s vs %s", a.body, b.body)
	}
	return nil
}

func (w *world) theVerdictWasFeasible() error {
	x := w.wire()
	if x.lastErr != nil {
		return fmt.Errorf("the raise failed: %w", x.lastErr)
	}
	if !x.lastResult.Feasible || x.lastResult.LocalOrderId != bddWireOrder || !x.lastResult.PromisedCutoff.Equal(bddWirePromise) {
		return fmt.Errorf("the verdict was %+v, want feasible with %s and the promise %s",
			x.lastResult, bddWireOrder, bddWirePromise)
	}
	return nil
}

func (w *world) theVerdictWasInfeasible(local string) error {
	x := w.wire()
	if x.lastErr != nil {
		return fmt.Errorf("no promise is a verdict, not an error: %w", x.lastErr)
	}
	if x.lastResult.Feasible || string(x.lastResult.LocalOrderId) != local {
		return fmt.Errorf("the verdict was %+v, want infeasible keeping %s", x.lastResult, local)
	}
	return nil
}

func (w *world) theRaiseWasRefusedRetryably() error {
	var refused *ordermanagement.RefusalError
	if !errors.As(w.wire().lastErr, &refused) {
		return fmt.Errorf("the raise failed with %w, want a classified refusal", w.wire().lastErr)
	}
	if !refused.Released {
		return fmt.Errorf("the refusal %q was final, want released", refused.Problem)
	}
	return nil
}

func (w *world) theRaiseWasRefusedFinally() error {
	var refused *ordermanagement.RefusalError
	if !errors.As(w.wire().lastErr, &refused) {
		return fmt.Errorf("the raise failed with %w, want a classified refusal", w.wire().lastErr)
	}
	if refused.Released {
		return fmt.Errorf("the refusal %q was released, want final", refused.Problem)
	}
	return nil
}

func (w *world) theRaiseFailedAsAPlainError() error {
	x := w.wire()
	var refused *ordermanagement.RefusalError
	if x.lastErr == nil {
		return fmt.Errorf("the raise succeeded, want a plain error")
	}
	if errors.As(x.lastErr, &refused) {
		return fmt.Errorf("the failure %w is a refusal, want a plain error", x.lastErr)
	}
	return nil
}

func (w *world) noRequestReachedOM() error {
	x := w.wire()
	x.mu.Lock()
	defer x.mu.Unlock()
	if n := len(x.requests); n != 0 {
		return fmt.Errorf("order-management was asked %d times, want never", n)
	}
	return nil
}

func (w *world) theHeldOrderIsReleased(id string) error {
	x := w.wire()
	if x.planner == nil {
		return fmt.Errorf("no raise happened before the release")
	}
	x.lastErr = x.planner.ReleaseHeldOrder(context.Background(), shared.LocalOrderId(id))
	if x.lastErr != nil {
		return fmt.Errorf("the release failed: %w", x.lastErr)
	}
	return nil
}

func (w *world) theReleaseWasKeyed(key string) error {
	x := w.wire()
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.requests) == 0 {
		return fmt.Errorf("order-management was never asked to release")
	}
	r := x.requests[len(x.requests)-1]
	if r.key != key {
		return fmt.Errorf("the release carried Idempotency-Key %q, want %q", r.key, key)
	}
	return nil
}

func (w *world) theReleaseCalled(path string) error {
	x := w.wire()
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.requests) == 0 {
		return fmt.Errorf("order-management was never asked to release")
	}
	r := x.requests[len(x.requests)-1]
	if r.method != http.MethodPost || r.path != path {
		return fmt.Errorf("the release was %s %s, want POST %s", r.method, r.path, path)
	}
	return nil
}

func (w *world) registerHeldOrderWireSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the site map translates "([^"]*)" to "([^"]*)"$`, w.theSiteMapTranslates)
	sc.Step(`^order-management answers the raise with (a promise|no promise)$`, w.omAnswersRaiseWith)
	sc.Step(`^order-management answers the next raise with (\d+) and the problem "([^"]*)"$`, w.omAnswersNextRaiseWithProblem)
	sc.Step(`^order-management answers the next raise with (\d+) and no problem document$`, w.omAnswersNextRaiseWithNoProblem)
	sc.Step(`^a held order is raised for "([^"]*)" at site "([^"]*)" shipping (\d+) of "([^"]*)"$`, w.raiseHeldOrder1)
	sc.Step(`^a held order is raised for "([^"]*)" at site "([^"]*)" shipping (\d+) of "([^"]*)" and (\d+) of "([^"]*)"$`, w.raiseHeldOrder2)
	sc.Step(`^a held order is raised for "([^"]*)" at site "([^"]*)" shipping (\d+) of "([^"]*)" and (\d+) of "([^"]*)" and (\d+) of "([^"]*)" and (\d+) of "([^"]*)"$`, w.raiseHeldOrder4)
	sc.Step(`^the raise is retried$`, w.theRaiseIsRetried)
	sc.Step(`^the raise carried the Idempotency-Key "([^"]*)"$`, w.theRaiseCarriedKey)
	sc.Step(`^both raises carried the Idempotency-Key "([^"]*)"$`, w.bothRaisesCarriedKey)
	sc.Step(`^the raise carried fulfillmentSiteId "([^"]*)"$`, w.theRaiseCarriedSite)
	sc.Step(`^the raise omitted fulfillmentSiteId$`, w.theRaiseOmittedSite)
	sc.Step(`^the raise was for a held, ship-complete order$`, w.theRaiseWasHeldAndShipComplete)
	sc.Step(`^the raise carried the network's deadline unchanged$`, w.theRaiseCarriedTheDeadline)
	sc.Step(`^the raise carried the lines in SKU order "([^"]*)", "([^"]*)", "([^"]*)", "([^"]*)"$`, w.theRaiseCarriedLinesInOrder)
	sc.Step(`^the retried raise was byte-identical under the same key$`, w.theRetriedRaiseWasByteIdentical)
	sc.Step(`^the verdict was feasible$`, w.theVerdictWasFeasible)
	sc.Step(`^the verdict was infeasible, keeping the local order "([^"]*)"$`, w.theVerdictWasInfeasible)
	sc.Step(`^the raise was refused in a way a retry can fix$`, w.theRaiseWasRefusedRetryably)
	sc.Step(`^the raise was refused with a final verdict$`, w.theRaiseWasRefusedFinally)
	sc.Step(`^the raise failed as a plain error, not a refusal$`, w.theRaiseFailedAsAPlainError)
	sc.Step(`^no request reached order-management$`, w.noRequestReachedOM)
	sc.Step(`^the held order "([^"]*)" is released$`, w.theHeldOrderIsReleased)
	sc.Step(`^the release was keyed "([^"]*)"$`, w.theReleaseWasKeyed)
	sc.Step(`^the release called "([^"]*)"$`, w.theReleaseCalled)
}
