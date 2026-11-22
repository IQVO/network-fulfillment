package main_test

import (
	"context"
	"fmt"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// The refusal-class steps (features/refusal_class_rules.feature). They pin the
// ASN-registration half of the fleet idempotency rule of 2026-11-18 (ADR 0037,
// Decision 3): a refusal inbound-receiving RELEASES committed nothing and freed
// the Idempotency-Key, so the same ASN is registered again under the SAME key
// and may pass; the loop is bounded by MaxReleasedRefusalAttempts (default 30,
// about a day of backoff) and ends REJECTED as a final refusal does. A final
// refusal is a verdict at once. A transport failure was never a verdict and is
// never bounded. The double answers with contract.AsnRefusedError exactly as
// the real adapter classifies the callee's problem slugs
// (inboundreceiving/refusal_classes.go holds that table).

// receivingRefusesReleased makes the double refuse the ASN with a refusal the
// callee releases (replay class RETRYABLE: unknown-sku, concurrent-modification,
// duplicate-sscc...): nothing was committed and a retry may pass.
func (w *world) receivingRefusesReleased(asn, slug string) error {
	w.inbound().receiving.refused[asn] = bddRefusal{slug: slug, released: true}
	return nil
}

// receivingRefusesFinal makes the double refuse the ASN with a stored verdict
// (replay class FINAL: receipt-closed, asn-terminal, an unknown slug or no
// problem document): a retry is answered the same, so it is never made.
func (w *world) receivingRefusesFinal(asn, slug string) error {
	w.inbound().receiving.refused[asn] = bddRefusal{slug: slug}
	return nil
}

func (w *world) receivingStopsRefusing(asn string) error {
	delete(w.inbound().receiving.refused, asn)
	return nil
}

// receivingIsUnreachable makes every RegisterAsn fail as transport: a plain
// error, never a refusal, so no attempt bound applies to it (ADR 0037 D3).
func (w *world) receivingIsUnreachable() error {
	w.inbound().receiving.down = true
	return nil
}

// intakeGivesUpAfter pins the released-refusal attempt bound for the scenario.
func (w *world) intakeGivesUpAfter(n int) error {
	w.inbound().intake.MaxReleasedRefusalAttempts = n
	return nil
}

// everyRefusalWasForTheSameAsn: the ASN number derives from the shipment
// reference and IS the Idempotency-Key, so every retry must carry the same one.
func (w *world) everyRefusalWasForTheSameAsn(asn string) error {
	refused := w.inbound().receiving.refusedAsns
	if len(refused) == 0 {
		return fmt.Errorf("inbound-receiving was never refused an ASN")
	}
	for _, got := range refused {
		if got != asn {
			return fmt.Errorf("a refusal was for ASN %s, want every one for %s (%v)", got, asn, refused)
		}
	}
	return nil
}

// intakeRetriesEveryMinute runs one pass per minute, like the worker's ticker,
// until the shipment leaves PENDING or the budget of hours is spent.
func (w *world) intakeRetriesEveryMinute(ref string, hours int) error {
	iw := w.inbound()
	for i := 0; i < hours*60; i++ {
		w.clock.Advance(time.Minute)
		if _, err := iw.intake.Execute(context.Background()); err != nil {
			return err
		}
		if row := iw.repo.rows[ref]; row != nil && row.status != contract.InboundPending {
			return nil
		}
	}
	return nil
}

func (w *world) registerRefusalClassSteps(sc *godog.ScenarioContext) {
	sc.Step(`^inbound-receiving refuses ASN "([^"]*)" with a released "([^"]*)"$`, w.receivingRefusesReleased)
	sc.Step(`^inbound-receiving refuses ASN "([^"]*)" with a final "([^"]*)"$`, w.receivingRefusesFinal)
	sc.Step(`^inbound-receiving stops refusing ASN "([^"]*)"$`, w.receivingStopsRefusing)
	sc.Step(`^inbound-receiving is unreachable$`, w.receivingIsUnreachable)
	sc.Step(`^the intake gives up after (\d+) released refusals$`, w.intakeGivesUpAfter)
	sc.Step(`^every refusal was for the same ASN "([^"]*)"$`, w.everyRefusalWasForTheSameAsn)
	sc.Step(`^the intake keeps retrying shipment "([^"]*)" for (\d+) hours in one-minute passes$`, w.intakeRetriesEveryMinute)
}
