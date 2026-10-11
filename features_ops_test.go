package main_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ---- operations: readiness, metrics, the read-only surface --------------------

func (w *world) theServiceBeginsGracefulShutdown() error {
	w.readiness.SetNotReady()
	return nil
}

func (w *world) theBreakerIsClosed(dependency string) error {
	w.metrics.SetState(dependency, 0)
	return nil
}

func (w *world) iSendTo(ctx context.Context, method, path string) error {
	return w.record(ctx, w.apiServer(), method, path)
}

// theRequestIsRefusedAsReadOnly: this context has no HTTP intake (ADR 0001
// §5) — demand is polled, never pushed — so any write other than the one
// ADR 0014 endpoint must be refused, not silently accepted.
func (w *world) theRequestIsRefusedAsReadOnly() error {
	if w.status != http.StatusMethodNotAllowed && w.status != http.StatusNotFound {
		return fmt.Errorf("expected the write to be refused (405/404), got %d: %s", w.status, string(w.body))
	}
	return nil
}

func (w *world) theResponseBodyIsEmpty() error {
	if len(w.body) != 0 {
		return fmt.Errorf("expected an empty body, got %q", string(w.body))
	}
	return nil
}

func (w *world) theResponseStatusIsNotSuccess() error {
	if w.status >= 200 && w.status < 300 {
		return fmt.Errorf("expected a non-success status, got %d", w.status)
	}
	return nil
}

func (w *world) theInboundStatusWatermarkIs(expected string) error {
	var s inboundStatusBody
	if err := w.decode(&s); err != nil {
		return err
	}
	if s.Since == nil || *s.Since != expected {
		return fmt.Errorf("expected the watermark to be %s, got %v", expected, s.Since)
	}
	return nil
}

// ---- planner / fixtures ---------------------------------------------------------

func (w *world) orderManagementCanMeetTheShipBy() error {
	w.planner.feasible = true
	return nil
}

func (w *world) theHeldOrderIsForSite(site string) error {
	if len(w.planner.raised) == 0 {
		return fmt.Errorf("no held order was raised in order-management")
	}
	if got := w.planner.raised[len(w.planner.raised)-1].SiteId; string(got) != site {
		return fmt.Errorf("expected the held order for site %q, got %q", site, got)
	}
	return nil
}

// aNetworkOrderRejectedBecauseOf produces an order refused for one of the
// four documented reasons (shared.RejectionReason), through the real use
// cases, so scenarios about the report do not repeat the setup four ways.
func (w *world) aNetworkOrderRejectedBecauseOf(ctx context.Context, ref, reason string) error {
	switch reason {
	case "UNTRANSLATABLE_SKU":
		d := contract.InboundDemand{
			NetworkRef:     shared.NetworkRef(ref),
			SiteId:         "site-1",
			RequiredShipBy: w.clock.Now().Add(48 * time.Hour),
			Lines:          []contract.InboundLine{{NetworkLineRef: "1", NetworkProductId: "ASIN-NOT-IN-DICTIONARY", Quantity: 1}},
		}
		w.demands[ref] = d
		_, err := w.receive.Execute(ctx, d)
		return err
	case "INFEASIBLE_DEADLINE":
		return w.aNetworkOrderInState(ctx, ref, "REJECTED")
	case "ACKNOWLEDGEMENT_DEADLINE_MISSED":
		if err := w.aNetworkOrderInState(ctx, ref, "NEW"); err != nil {
			return err
		}
		w.clock.Advance(25 * time.Hour)
		_, err := w.rejectOverdue.Execute(ctx)
		return err
	case "SUBMISSION_FAILED":
		w.gateway.refused[shared.NetworkRef(ref)] = true
		if err := w.aNetworkOrderInState(ctx, ref, "SUBMITTED"); err != nil {
			return err
		}
		_, err := w.reconcile.Execute(ctx)
		return err
	default:
		return fmt.Errorf("unknown rejection reason %q", reason)
	}
}

func (w *world) registerOpsSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the service begins graceful shutdown$`, w.theServiceBeginsGracefulShutdown)
	sc.Step(`^the "([^"]*)" circuit breaker is closed$`, w.theBreakerIsClosed)
	sc.Step(`^I send "(POST|PUT|PATCH|DELETE)" to "([^"]*)"$`, w.iSendTo)
	sc.Step(`^the request is refused because the endpoint is read-only$`, w.theRequestIsRefusedAsReadOnly)
	sc.Step(`^the response body is empty$`, w.theResponseBodyIsEmpty)
	sc.Step(`^the response status is not a success$`, w.theResponseStatusIsNotSuccess)
	sc.Step(`^the inbound status watermark is "([^"]*)"$`, w.theInboundStatusWatermarkIs)

	sc.Step(`^order-management can meet the required ship-by$`, w.orderManagementCanMeetTheShipBy)
	sc.Step(`^the held order is for site "([^"]*)"$`, w.theHeldOrderIsForSite)
	sc.Step(`^a network order "([^"]*)" rejected because of "([^"]*)"$`, w.aNetworkOrderRejectedBecauseOf)
}
