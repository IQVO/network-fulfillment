package main_test

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ---- Given: the collaborators' behaviour --------------------------------------

func (w *world) orderManagementCannotMeetTheShipBy() error {
	w.planner.feasible = false
	return nil
}

func (w *world) orderManagementIsUnavailable() error {
	w.planner.unavailable = true
	return nil
}

func (w *world) orderManagementIsAvailableAgain() error {
	w.planner.unavailable = false
	return nil
}

func (w *world) theNetworkWillRefuseTheSubmission(ref string) error {
	w.gateway.refused[shared.NetworkRef(ref)] = true
	return nil
}

func (w *world) theNetworkHasNotSettledTheSubmission(ref string) error {
	w.gateway.unsettled[shared.NetworkRef(ref)] = true
	return nil
}

func (w *world) theNetworkWillFailShipmentConfirmations() error {
	w.gateway.failShipment = true
	return nil
}

func (w *world) theNetworkAcceptsShipmentConfirmationsAgain() error {
	w.gateway.failShipment = false
	return nil
}

// ---- When: the scheduled jobs -------------------------------------------------

func (w *world) reconciliationRuns(ctx context.Context) error {
	_, err := w.reconcile.Execute(ctx)
	return err
}

func (w *world) theDeadlineSweepRuns(ctx context.Context) error {
	_, err := w.sweep.Execute(ctx)
	return err
}

func (w *world) overdueOrdersAreRejected(ctx context.Context) error {
	_, err := w.rejectOverdue.Execute(ctx)
	return err
}

// ---- Then: what the network was told ------------------------------------------

func (w *world) networkWasToldTimes(ref string, expected int) error {
	if got := len(w.gateway.acks[shared.NetworkRef(ref)]); got != expected {
		return fmt.Errorf("expected the network to be told %d time(s) about %q, was told %d time(s)", expected, ref, got)
	}
	return nil
}

func (w *world) networkWasToldAnswer(ref string, accepted bool) error {
	answers := w.gateway.acks[shared.NetworkRef(ref)]
	if len(answers) == 0 {
		return fmt.Errorf("the network was never told anything about %q", ref)
	}
	if got := answers[len(answers)-1]; got != accepted {
		return fmt.Errorf("expected the network to be told accepted=%t for %q, was told accepted=%t", accepted, ref, got)
	}
	return nil
}

func (w *world) shipmentConfirmationsSent(expected int, ref string) error {
	if got := w.gateway.shipments[shared.NetworkRef(ref)]; got != expected {
		return fmt.Errorf("expected %d shipment confirmation(s) accepted by the network for %q, got %d", expected, ref, got)
	}
	return nil
}

func (w *world) shipmentConfirmationAttempts(expected int, ref string) error {
	if got := w.gateway.shipmentAttempts[shared.NetworkRef(ref)]; got != expected {
		return fmt.Errorf("expected %d shipment confirmation attempt(s) for %q, got %d", expected, ref, got)
	}
	return nil
}

// ---- Then: what order-management was asked / told -----------------------------

func (w *world) heldOrdersRaised(expected int) error {
	if got := len(w.planner.raised); got != expected {
		return fmt.Errorf("expected %d held order(s) raised in order-management, got %d", expected, got)
	}
	return nil
}

func (w *world) heldOrdersReleased(expected int) error {
	if got := len(w.planner.released); got != expected {
		return fmt.Errorf("expected %d held order(s) released, got %d", expected, got)
	}
	return nil
}

func (w *world) heldOrdersCancelled(expected int) error {
	if got := len(w.planner.cancelled); got != expected {
		return fmt.Errorf("expected %d held order(s) cancelled, got %d", expected, got)
	}
	return nil
}

// theHeldOrderRequestAsks asserts order-management was asked in OUR
// vocabulary only: SKUs and quantities, never a network identifier
// (ADR 0001 §2).
func (w *world) theHeldOrderRequestAsks(qty int, sku string) error {
	if len(w.planner.raised) == 0 {
		return fmt.Errorf("no held order was raised in order-management")
	}
	req := w.planner.raised[len(w.planner.raised)-1]
	if got := req.Lines[shared.SKU(sku)]; got != qty {
		return fmt.Errorf("expected the held order to ask for %d of %q, got %v", qty, sku, req.Lines)
	}
	return nil
}

func (w *world) theHeldOrderRequestCarriesOnlySKUs(count int) error {
	if len(w.planner.raised) == 0 {
		return fmt.Errorf("no held order was raised in order-management")
	}
	if got := len(w.planner.raised[len(w.planner.raised)-1].Lines); got != count {
		return fmt.Errorf("expected the held order to carry %d SKU(s), got %d", count, got)
	}
	return nil
}

func (w *world) theHeldOrderRequestCarriesTheShipBy(ref string) error {
	if len(w.planner.raised) == 0 {
		return fmt.Errorf("no held order was raised in order-management")
	}
	req := w.planner.raised[len(w.planner.raised)-1]
	if want := w.demands[ref].RequiredShipBy; !req.RequiredShipBy.Equal(want) {
		return fmt.Errorf("expected order-management to be asked about the network's deadline %s unchanged, got %s", want, req.RequiredShipBy)
	}
	return nil
}

func (w *world) theHeldOrderWasReleasedNotCancelled() error {
	if len(w.planner.released) != 1 || len(w.planner.cancelled) != 0 {
		return fmt.Errorf("expected the held order released and not cancelled, got released=%v cancelled=%v", w.planner.released, w.planner.cancelled)
	}
	return nil
}

func (w *world) registerLifecycleSteps(sc *godog.ScenarioContext) {
	sc.Step(`^order-management cannot meet the required ship-by$`, w.orderManagementCannotMeetTheShipBy)
	sc.Step(`^order-management is unavailable$`, w.orderManagementIsUnavailable)
	sc.Step(`^order-management is available again$`, w.orderManagementIsAvailableAgain)
	sc.Step(`^the network will refuse the submission for "([^"]*)"$`, w.theNetworkWillRefuseTheSubmission)
	sc.Step(`^the network has not yet settled the submission for "([^"]*)"$`, w.theNetworkHasNotSettledTheSubmission)
	sc.Step(`^the network will fail shipment confirmations$`, w.theNetworkWillFailShipmentConfirmations)
	sc.Step(`^the network accepts shipment confirmations again$`, w.theNetworkAcceptsShipmentConfirmationsAgain)

	sc.Step(`^the acknowledgement reconciliation runs$`, w.reconciliationRuns)
	sc.Step(`^the acknowledgement deadline sweep runs$`, w.theDeadlineSweepRuns)
	sc.Step(`^overdue orders are rejected$`, w.overdueOrdersAreRejected)

	sc.Step(`^the network was told about "([^"]*)" exactly (\d+) times?$`, w.networkWasToldTimes)
	sc.Step(`^the network was told "([^"]*)" is accepted$`, func(ref string) error { return w.networkWasToldAnswer(ref, true) })
	sc.Step(`^the network was told "([^"]*)" is rejected$`, func(ref string) error { return w.networkWasToldAnswer(ref, false) })
	sc.Step(`^the network accepted (\d+) shipment confirmations? for "([^"]*)"$`, w.shipmentConfirmationsSent)
	sc.Step(`^the network was asked to confirm shipment of (\d+) times? for "([^"]*)"$`, w.shipmentConfirmationAttempts)

	sc.Step(`^order-management holds (\d+) raised orders?$`, w.heldOrdersRaised)
	sc.Step(`^order-management had (\d+) held orders? released$`, w.heldOrdersReleased)
	sc.Step(`^order-management had (\d+) held orders? cancelled$`, w.heldOrdersCancelled)
	sc.Step(`^the held order asks order-management for (\d+) units? of "([^"]*)"$`, w.theHeldOrderRequestAsks)
	sc.Step(`^the held order names (\d+) SKUs?$`, w.theHeldOrderRequestCarriesOnlySKUs)
	sc.Step(`^the held order carries the required ship-by of "([^"]*)" unchanged$`, w.theHeldOrderRequestCarriesTheShipBy)
	sc.Step(`^the held order was released and not cancelled$`, w.theHeldOrderWasReleasedNotCancelled)
}
