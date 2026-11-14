package main_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of the documentation-derived scenarios (features/*_rules.feature).
// They reuse the suite's vocabulary and add only what the documented rules
// need: per-hold answers from order-management, a release that fails, a
// transaction-status lookup that fails, fixtures for the states the docs name
// (an order stuck NEW with its hold open, an untranslatable order recorded but
// never answered), and assertions on event payloads.
//
// The doubles decorate the SAME use cases the rest of the suite drives: the
// Before hook below swaps the Planner/Gateway fields after world.reset built
// them, so existing steps keep working against the same state.

// ---- doubles ----------------------------------------------------------------

// scriptedPlanner is order-management with a script per hold: the answer to the
// cancel of ONE hold, and whether the release fails. A hold with no script falls
// through to the suite's fakePlanner (global cancelErr, success otherwise).
type scriptedPlanner struct {
	*fakePlanner

	smu             sync.Mutex
	holdAnswers     map[shared.LocalOrderId]error
	releaseErr      error
	releaseFails    map[shared.LocalOrderId]error
	releaseAttempts int
}

func (p *scriptedPlanner) ReleaseHeldOrder(ctx context.Context, id shared.LocalOrderId) error {
	p.smu.Lock()
	p.releaseAttempts++
	err := p.releaseErr
	if hold, ok := p.releaseFails[id]; ok {
		err = hold
	}
	p.smu.Unlock()
	if err != nil {
		return err
	}
	return p.fakePlanner.ReleaseHeldOrder(ctx, id)
}

func (p *scriptedPlanner) CancelHeldOrder(ctx context.Context, id shared.LocalOrderId) error {
	p.smu.Lock()
	err, scripted := p.holdAnswers[id]
	p.smu.Unlock()
	if !scripted {
		return p.fakePlanner.CancelHeldOrder(ctx, id)
	}
	p.mu.Lock()
	p.cancelAttempts = append(p.cancelAttempts, id)
	p.mu.Unlock()
	return err
}

// scriptedGateway is the network whose transaction-status lookup can fail for
// one submission while every other call reaches the real stub.
type scriptedGateway struct {
	ports.NetworkGateway

	smu        sync.Mutex
	lookupDown map[shared.NetworkRef]bool
}

func (g *scriptedGateway) SubmissionStatus(ctx context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	g.smu.Lock()
	down := g.lookupDown[ref]
	g.smu.Unlock()
	if down {
		return contract.SubmissionPending, errors.New("the network's transaction-status lookup failed")
	}
	return g.NetworkGateway.SubmissionStatus(ctx, ref)
}

// tallyHoldMetrics is ports.OrphanHoldMetrics: one count per outcome.
type tallyHoldMetrics struct {
	mu     sync.Mutex
	counts map[contract.HoldSweepOutcome]int
}

func (m *tallyHoldMetrics) Swept(_ context.Context, outcome contract.HoldSweepOutcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[outcome]++
}

// cutoffCapacity is wes-work-planning's remaining capacity keyed by BOTH the
// path and the exact cutoff instant, as the real cache is: a figure observed for
// one cutoff says nothing about another.
type cutoffCapacity struct {
	units map[string]map[int64]int
}

func (c *cutoffCapacity) RemainingCapacity(pathID string, cutoffAt time.Time) (int, bool) {
	units, ok := c.units[pathID][cutoffAt.UnixNano()]
	return units, ok
}

// ---- the per-scenario state --------------------------------------------------

type rulesWorld struct {
	w *world

	planner     *scriptedPlanner
	gateway     *scriptedGateway
	holdMetrics *tallyHoldMetrics
	capacity    *cutoffCapacity
	sweepTally  usecases.SweepOrphanedHoldsResult
}

func (r *rulesWorld) reset() {
	w := r.w
	r.planner = &scriptedPlanner{
		fakePlanner:  w.planner,
		holdAnswers:  map[shared.LocalOrderId]error{},
		releaseFails: map[shared.LocalOrderId]error{},
	}
	r.gateway = &scriptedGateway{NetworkGateway: w.gateway, lookupDown: map[shared.NetworkRef]bool{}}
	r.holdMetrics = &tallyHoldMetrics{counts: map[contract.HoldSweepOutcome]int{}}
	r.capacity = &cutoffCapacity{units: map[string]map[int64]int{}}
	r.sweepTally = usecases.SweepOrphanedHoldsResult{}

	w.receive.Planner = r.planner
	w.reconcile.Planner = r.planner
	w.reconcile.Gateway = r.gateway
	w.rejectOverdue.Planner = r.planner
	w.sweepHolds.Planner = r.planner
	w.sweepHolds.Metrics = r.holdMetrics
}

// ---- Given: order-management and the network ----------------------------------

func (r *rulesWorld) holdOf(ctx context.Context, ref string) (shared.LocalOrderId, error) {
	o, err := r.w.orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil || o == nil {
		return "", fmt.Errorf("network order %q not found: %w", ref, err)
	}
	if o.LocalOrderId() == nil {
		return "", fmt.Errorf("network order %q has no recorded hold", ref)
	}
	return *o.LocalOrderId(), nil
}

// holdAnswersCancel scripts order-management's answer to the cancel of ONE hold,
// translated to the sentinel the real Planner adapter produces (404 -> not
// found, 409/422 -> already released, anything else an unclassified failure).
func (r *rulesWorld) holdAnswersCancel(ctx context.Context, ref string, status int) error {
	id, err := r.holdOf(ctx, ref)
	if err != nil {
		return err
	}
	r.planner.smu.Lock()
	defer r.planner.smu.Unlock()
	switch status {
	case 404:
		r.planner.holdAnswers[id] = fmt.Errorf("order-management DELETE: status 404: %w", contract.ErrHeldOrderNotFound)
	case 409, 422:
		r.planner.holdAnswers[id] = fmt.Errorf("order-management DELETE: status %d: %w", status, contract.ErrHeldOrderAlreadyReleased)
	case 500, 502, 503:
		r.planner.holdAnswers[id] = fmt.Errorf("order-management DELETE: status %d", status)
	default:
		return fmt.Errorf("the scenario can only script 404, 409, 422, 500, 502 or 503, got %d", status)
	}
	return nil
}

func (r *rulesWorld) holdCancelsAgain(ctx context.Context, ref string) error {
	id, err := r.holdOf(ctx, ref)
	if err != nil {
		return err
	}
	r.planner.smu.Lock()
	defer r.planner.smu.Unlock()
	delete(r.planner.holdAnswers, id)
	return nil
}

func (r *rulesWorld) releaseFails() error {
	r.planner.smu.Lock()
	defer r.planner.smu.Unlock()
	r.planner.releaseErr = errors.New("order-management POST releases: status 503")
	return nil
}

func (r *rulesWorld) holdReleaseFails(ctx context.Context, ref string) error {
	id, err := r.holdOf(ctx, ref)
	if err != nil {
		return err
	}
	r.planner.smu.Lock()
	defer r.planner.smu.Unlock()
	r.planner.releaseFails[id] = errors.New("order-management POST releases: status 503")
	return nil
}

func (r *rulesWorld) releaseAttemptsAre(expected int) error {
	r.planner.smu.Lock()
	defer r.planner.smu.Unlock()
	if r.planner.releaseAttempts != expected {
		return fmt.Errorf("expected order-management to be asked to release %d time(s), was asked %d time(s)", expected, r.planner.releaseAttempts)
	}
	return nil
}

func (r *rulesWorld) distinctHeldOrders(expected int) error {
	r.w.planner.mu.Lock()
	defer r.w.planner.mu.Unlock()
	if got := len(r.w.planner.ids); got != expected {
		return fmt.Errorf("expected order-management to know %d distinct held order(s), it knows %d: %v", expected, got, r.w.planner.ids)
	}
	return nil
}

func (r *rulesWorld) statusLookupFails(ref string) error {
	r.gateway.smu.Lock()
	defer r.gateway.smu.Unlock()
	r.gateway.lookupDown[shared.NetworkRef(ref)] = true
	return nil
}

func (r *rulesWorld) statusLookupWorks(ref string) error {
	r.gateway.smu.Lock()
	defer r.gateway.smu.Unlock()
	delete(r.gateway.lookupDown, shared.NetworkRef(ref))
	return nil
}

func (r *rulesWorld) networkSettles(ref string) error {
	delete(r.w.gateway.unsettled, shared.NetworkRef(ref))
	return nil
}

// ---- Given: fixtures the documentation names -----------------------------------

// stuckNewWithOpenHold builds the state ADR 0024 exists for: the demand was
// refused, but the cancel failed, so the order is still NEW and names a hold
// order-management is still holding. The setup's own cancel attempt is not
// counted: the scenario asserts on what happens AFTER the fixture.
func (r *rulesWorld) stuckNewWithOpenHold(ctx context.Context, ref string) error {
	w := r.w
	d := contract.InboundDemand{
		NetworkRef:     shared.NetworkRef(ref),
		SiteId:         "site-1",
		RequiredShipBy: w.clock.Now().Add(48 * time.Hour),
		Lines:          []contract.InboundLine{{NetworkLineRef: "1", NetworkProductId: "ASIN-1", Quantity: 2}},
	}
	w.demands[ref] = d

	w.planner.feasible, w.planner.cancelErr = false, errPlannerDown
	_, err := w.receive.Execute(ctx, d)
	w.planner.feasible, w.planner.cancelErr = true, nil
	if err == nil {
		return fmt.Errorf("expected the receipt to fail while order-management cannot cancel the refused hold")
	}
	w.planner.cancelAttempts = nil

	o, err := w.orders.FindByRef(ctx, d.NetworkRef)
	if err != nil || o == nil {
		return fmt.Errorf("fixture order %q was not recorded: %w", ref, err)
	}
	if o.State() != networkorder.StateNew || !o.HoldOpen() {
		return fmt.Errorf("fixture order %q should be NEW with an open hold, is %s (open=%t)", ref, o.State(), o.HoldOpen())
	}
	return nil
}

// untranslatableRecorded is the crash window of ReceiveNetworkDemand: the
// lineless order was saved, the process stopped before the refusal. The network
// still lists the demand (it names a product nobody mapped).
func (r *rulesWorld) untranslatableRecorded(ctx context.Context, ref string) error {
	w := r.w
	now := w.clock.Now()
	o, err := networkorder.ReceiveUntranslatable(shared.NetworkRef(ref), "site-1", now.Add(48*time.Hour), now)
	if err != nil {
		return err
	}
	if err := w.orders.Save(ctx, o); err != nil {
		return err
	}
	w.seedDemand(ref, "site-1", 48, []contract.InboundLine{
		{NetworkLineRef: "1", NetworkProductId: "ASIN-GHOST", Quantity: 2},
	})
	return nil
}

// rejectedWithOpenHold is a REJECTED order whose hold was never settled, the
// shape the canvas allows ("a NEW and a REJECTED order can both carry an open
// hold"). No flow of today produces it (every reject frees the hold first), so
// it is built through the aggregate and saved.
func (r *rulesWorld) rejectedWithOpenHold(ctx context.Context, ref string) error {
	w := r.w
	now := w.clock.Now()
	line, err := networkorder.NewLine("1", "ASIN-1", "sku-1", 2)
	if err != nil {
		return err
	}
	o, err := networkorder.Receive(shared.NetworkRef(ref), "site-1", now.Add(48*time.Hour), []networkorder.Line{line}, now)
	if err != nil {
		return err
	}
	if err := o.RecordHold(shared.LocalOrderId("ord-legacy-" + ref)); err != nil {
		return err
	}
	if err := o.Reject(); err != nil {
		return err
	}
	return w.orders.Save(ctx, o)
}

// ---- When ----------------------------------------------------------------------

func (r *rulesWorld) sweepRunsAndKeepsTally(ctx context.Context) error {
	res, err := r.w.sweepHolds.Execute(ctx)
	r.sweepTally = res
	return err
}

// ---- Then ----------------------------------------------------------------------

func (r *rulesWorld) theSweepTallyIs(examined, cancelled, released, failed int) error {
	want := usecases.SweepOrphanedHoldsResult{Examined: examined, Cancelled: cancelled, AlreadyReleased: released, Failed: failed}
	if r.sweepTally != want {
		return fmt.Errorf("expected the sweep to report %+v, it reported %+v", want, r.sweepTally)
	}
	return nil
}

func (r *rulesWorld) theSweepMetricCounted(expected int, outcome string) error {
	r.holdMetrics.mu.Lock()
	defer r.holdMetrics.mu.Unlock()
	if got := r.holdMetrics.counts[contract.HoldSweepOutcome(outcome)]; got != expected {
		return fmt.Errorf("expected the %q outcome to be counted %d time(s), counted %d (all: %v)", outcome, expected, got, r.holdMetrics.counts)
	}
	return nil
}

// lastEventData decodes the data of the newest events-stream message of this
// name for this order.
func (r *rulesWorld) lastEventData(name, ref string) (map[string]any, error) {
	msgs, err := r.w.decoded("events")
	if err != nil {
		return nil, err
	}
	var found map[string]any
	for _, m := range msgs {
		if domainEventName(m.event.Type()) != name || m.event.Subject() != ref {
			continue
		}
		var data map[string]any
		if err := m.event.DataAs(&data); err != nil {
			return nil, err
		}
		found = data
	}
	if found == nil {
		return nil, fmt.Errorf("no %s event was published for %q", name, ref)
	}
	return found, nil
}

func (r *rulesWorld) eventCarries(name, ref, field, want string) error {
	data, err := r.lastEventData(name, ref)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(data[field]); got != want {
		return fmt.Errorf("expected the %s event for %q to carry %s=%q, it carries %q (data %v)", name, ref, field, want, got, data)
	}
	return nil
}

func (r *rulesWorld) eventNamesLocalOrder(ctx context.Context, name, ref string) error {
	id, err := r.holdOf(ctx, ref)
	if err != nil {
		return err
	}
	return r.eventCarries(name, ref, "localOrderId", string(id))
}

func (r *rulesWorld) theOrderHasNoLines() error {
	o, err := r.w.order()
	if err != nil {
		return err
	}
	if len(o.Lines) != 0 {
		return fmt.Errorf("expected no lines, got %+v", o.Lines)
	}
	return nil
}

// ---- recompute: capacity per cutoff ---------------------------------------------

// capacityForCutoff observes a path's remaining capacity for ONE cutoff instant
// (the cutoff that many minutes from now) and makes the recompute pass read
// capacity per instant instead of per path.
func (r *rulesWorld) capacityForCutoff(pathID string, units, minutes int) error {
	at := r.w.clock.Now().Add(time.Duration(minutes) * time.Minute)
	if r.capacity.units[pathID] == nil {
		r.capacity.units[pathID] = map[int64]int{}
	}
	r.capacity.units[pathID][at.UnixNano()] = units
	r.w.recompute.Capacity = r.capacity
	return nil
}

func (r *rulesWorld) pathOvercommitted(pathID string, units int) error {
	r.w.capacity.units[pathID] = -units
	return nil
}

func (w *world) registerRuleSteps(sc *godog.ScenarioContext) {
	r := &rulesWorld{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		r.reset()
		return ctx, nil
	})

	sc.Step(`^order-management answers the cancel of the hold of "([^"]*)" with status (\d+)$`, r.holdAnswersCancel)
	sc.Step(`^order-management cancels the hold of "([^"]*)" again$`, r.holdCancelsAgain)
	sc.Step(`^order-management fails to release held orders$`, r.releaseFails)
	sc.Step(`^order-management fails to release the held order of "([^"]*)"$`, r.holdReleaseFails)
	sc.Step(`^order-management was asked to release (\d+) times? in total$`, r.releaseAttemptsAre)
	sc.Step(`^order-management knows (\d+) distinct held orders?$`, r.distinctHeldOrders)
	sc.Step(`^the network cannot be asked about the submission of "([^"]*)"$`, r.statusLookupFails)
	sc.Step(`^the network can be asked about the submission of "([^"]*)" again$`, r.statusLookupWorks)
	sc.Step(`^the network settles the submission for "([^"]*)"$`, r.networkSettles)

	sc.Step(`^a network order "([^"]*)" is stuck NEW with its hold open$`, r.stuckNewWithOpenHold)
	sc.Step(`^an untranslatable network order "([^"]*)" was recorded but never answered$`, r.untranslatableRecorded)
	sc.Step(`^a network order "([^"]*)" was rejected before its hold was settled$`, r.rejectedWithOpenHold)

	sc.Step(`^the orphaned-hold sweep runs and its tally is kept$`, r.sweepRunsAndKeepsTally)
	sc.Step(`^the sweep tally is (\d+) examined, (\d+) cancelled, (\d+) already released and (\d+) failed$`, r.theSweepTallyIs)
	sc.Step(`^the sweep metric counted (\d+) "([^"]*)"$`, r.theSweepMetricCounted)

	sc.Step(`^the "([^"]*)" event for "([^"]*)" carries (\w+) "([^"]*)"$`, r.eventCarries)
	sc.Step(`^the "([^"]*)" event for "([^"]*)" carries (\w+) (\d+)$`, func(name, ref, field string, want int) error {
		return r.eventCarries(name, ref, field, fmt.Sprint(want))
	})
	sc.Step(`^the "([^"]*)" event for "([^"]*)" names the local order of "[^"]*"$`, r.eventNamesLocalOrder)
	sc.Step(`^the network order has no lines$`, r.theOrderHasNoLines)

	sc.Step(`^path "([^"]*)" has (\d+) units of remaining capacity for the cutoff in (\d+) minutes$`, r.capacityForCutoff)
	sc.Step(`^path "([^"]*)" is over-committed by (\d+) units$`, r.pathOvercommitted)
}
