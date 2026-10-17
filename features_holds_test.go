package main_test

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of features/orphaned_holds.feature (ADR 0024): order-management's
// answer to the cancel is driven by the scenario, and the sweep is the real
// SweepOrphanedHolds use case over the in-memory repository.

func (w *world) orderManagementFailsToCancel() error {
	w.planner.cancelErr = errPlannerDown
	return nil
}

func (w *world) orderManagementCancelsAgain() error {
	w.planner.cancelErr = nil
	return nil
}

// orderManagementAnswersTheCancel maps the HTTP status order-management
// answers to the sentinel the Planner adapter produces for it.
func (w *world) orderManagementAnswersTheCancel(status int) error {
	switch status {
	case 404:
		w.planner.cancelErr = fmt.Errorf("order-management DELETE: status 404: %w", contract.ErrHeldOrderNotFound)
	case 409:
		w.planner.cancelErr = fmt.Errorf("order-management DELETE: status 409: %w", contract.ErrHeldOrderAlreadyReleased)
	default:
		return fmt.Errorf("the scenario can only drive 404 or 409, got %d", status)
	}
	return nil
}

func (w *world) theOrphanedHoldSweepRuns(ctx context.Context) error {
	_, err := w.sweepHolds.Execute(ctx)
	return err
}

func (w *world) theHoldIsOpen(ctx context.Context, ref string) error {
	o, err := w.orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil || o == nil {
		return fmt.Errorf("network order %q not found (%v)", ref, err)
	}
	if !o.HoldOpen() {
		return fmt.Errorf("expected the hold of %q to be open, it is settled as %q", ref, o.HoldOutcome())
	}
	return nil
}

func (w *world) theHoldIsSettledAs(ctx context.Context, ref, outcome string) error {
	o, err := w.orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil || o == nil {
		return fmt.Errorf("network order %q not found (%v)", ref, err)
	}
	if string(o.HoldOutcome()) != outcome {
		return fmt.Errorf("expected the hold of %q settled as %q, got %q", ref, outcome, o.HoldOutcome())
	}
	return nil
}

func (w *world) heldOrderCancelAttempts(expected int) error {
	if got := len(w.planner.cancelAttempts); got != expected {
		return fmt.Errorf("expected order-management to be asked to cancel %d time(s), was asked %d time(s)", expected, got)
	}
	return nil
}

func (w *world) registerHoldSteps(sc *godog.ScenarioContext) {
	sc.Step(`^order-management fails to cancel held orders$`, w.orderManagementFailsToCancel)
	sc.Step(`^order-management cancels held orders again$`, w.orderManagementCancelsAgain)
	sc.Step(`^order-management answers the cancel with status (\d+)$`, w.orderManagementAnswersTheCancel)
	sc.Step(`^the orphaned-hold sweep runs$`, w.theOrphanedHoldSweepRuns)
	sc.Step(`^the hold of network order "([^"]*)" is open$`, w.theHoldIsOpen)
	sc.Step(`^the hold of network order "([^"]*)" is settled as "([^"]*)"$`, w.theHoldIsSettledAs)
	sc.Step(`^order-management was asked to cancel (\d+) times? in total$`, w.heldOrderCancelAttempts)
}
