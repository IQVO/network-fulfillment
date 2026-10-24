package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// spyGateway wraps the stub and records, in order, what the use cases asked of
// the network, so the ADR 0026 port contract (reason, package count, shipped-at,
// re-submission) is asserted on the call itself.
type spyGateway struct {
	*retailnetwork.StubGateway

	acks      []contract.Acknowledgement
	labelReqs []int
	shipped   []time.Time
	calls     []string

	labelErr error
}

func newSpyGateway() *spyGateway {
	return &spyGateway{StubGateway: retailnetwork.NewStubGateway(nil)}
}

func (g *spyGateway) SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, answer contract.Acknowledgement) error {
	g.calls = append(g.calls, "ack")
	g.acks = append(g.acks, answer)
	return g.StubGateway.SubmitAcknowledgement(ctx, ref, answer)
}

func (g *spyGateway) RequestLabel(ctx context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error) {
	g.calls = append(g.calls, "label")
	g.labelReqs = append(g.labelReqs, packageCount)
	if g.labelErr != nil {
		return nil, g.labelErr
	}
	return g.StubGateway.RequestLabel(ctx, ref, packageCount)
}

func (g *spyGateway) SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef, shippedAt time.Time) error {
	g.calls = append(g.calls, "ship")
	g.shipped = append(g.shipped, shippedAt)
	return g.StubGateway.SubmitShipmentConfirmation(ctx, ref, shippedAt)
}

func TestReceive_TheNetworksAcknowledgeByReplacesTheLocalWindow(t *testing.T) {
	cases := []struct {
		name      string
		ackBy     time.Time
		translate shared.NetworkProductId
		want      time.Time
	}{
		{"network deadline, translatable", now().Add(90 * time.Minute), "ASIN-1", now().Add(90 * time.Minute)},
		{"no network deadline, translatable", time.Time{}, "ASIN-1", now().Add(networkorder.AcknowledgementWindow)},
		{"network deadline, untranslatable", now().Add(45 * time.Minute), "ASIN-UNKNOWN", now().Add(45 * time.Minute)},
		{"no network deadline, untranslatable", time.Time{}, "ASIN-UNKNOWN", now().Add(networkorder.AcknowledgementWindow)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(true)
			d := demand("po-1", tc.translate)
			d.AcknowledgeBy = tc.ackBy

			o, err := f.receive().Execute(context.Background(), d)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if !o.AcknowledgeBy().Equal(tc.want) {
				t.Fatalf("acknowledgeBy = %v, want %v", o.AcknowledgeBy(), tc.want)
			}
		})
	}
}

func TestReceive_AnswersTheNetworkWithOurReason(t *testing.T) {
	cases := []struct {
		name     string
		feasible bool
		product  shared.NetworkProductId
		accepted bool
		reason   shared.RejectionReason
	}{
		{"feasible is accepted without a reason", true, "ASIN-1", true, ""},
		{"infeasible deadline", false, "ASIN-1", false, shared.RejectionReasonInfeasibleDeadline},
		{"untranslatable product", true, "ASIN-UNKNOWN", false, shared.RejectionReasonUntranslatableSKU},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(tc.feasible)
			spy := newSpyGateway()
			uc := f.receive()
			uc.Gateway = spy

			if _, err := uc.Execute(context.Background(), demand("po-1", tc.product)); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if len(spy.acks) != 1 {
				t.Fatalf("acknowledgements = %v, want exactly one", spy.acks)
			}
			if got := spy.acks[0]; got.Accepted != tc.accepted || got.Reason != tc.reason {
				t.Fatalf("answer = %+v, want accepted=%v reason=%q", got, tc.accepted, tc.reason)
			}
		})
	}
}

// A redelivered purchase order is answered once (features/network_order_intake):
// the second delivery of a SUBMITTED order sends nothing to the network.
func TestReceive_ARedeliveredSubmittedOrderIsNotAnsweredAgain(t *testing.T) {
	f := newFixture(true)
	spy := newSpyGateway()
	uc := f.receive()
	uc.Gateway = spy

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	raises := len(f.planner.calls)

	o, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1"))
	if err != nil {
		t.Fatalf("second delivery: %v", err)
	}
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED", o.State())
	}
	if len(spy.acks) != 1 {
		t.Fatalf("acknowledgements = %v, want exactly one", spy.acks)
	}
	if len(f.planner.calls) != raises {
		t.Fatalf("planner calls = %v, want unchanged", f.planner.calls)
	}
}

// notSentGateway reports that the answer never reached the network.
type notSentGateway struct {
	*spyGateway
	status contract.SubmissionStatusValue
}

func (g *notSentGateway) SubmissionStatus(context.Context, shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	return g.status, nil
}

func TestReconcile_ASubmittedOrderWhoseAnswerWasNeverSentIsAnsweredAgain(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	gw := &notSentGateway{spyGateway: newSpyGateway(), status: contract.SubmissionNotSent}
	uc := f.reconcile()
	uc.Gateway = gw

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Resent != 1 || res.Confirmed != 0 || res.Failed != 0 || res.Pending != 0 {
		t.Fatalf("result = %+v, want exactly one Resent", res)
	}
	if len(gw.acks) != 1 || !gw.acks[0].Accepted || gw.acks[0].Reason != "" {
		t.Fatalf("acknowledgements = %+v, want one plain accept", gw.acks)
	}
	o, _ := f.orders.FindByRef(context.Background(), "po-1")
	if o.State() != networkorder.StateSubmitted {
		t.Fatalf("state = %v, want SUBMITTED until the network settles the resent answer", o.State())
	}
	if got := f.planner.calls; len(got) != 1 {
		t.Fatalf("planner calls = %v, want only the original raise (nothing released yet)", got)
	}
}

func TestReconcile_AFailedResendIsRetriedOnTheNextPass(t *testing.T) {
	f := newFixture(true)
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	uc := f.reconcile()
	uc.Gateway = &failingResendGateway{notSentGateway: &notSentGateway{spyGateway: newSpyGateway(), status: contract.SubmissionNotSent}, err: errBoom}

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v (one failed resend must not fail the pass)", err)
	}
	if res.Resent != 0 || res.Examined != 1 {
		t.Fatalf("result = %+v, want the order examined and not counted as resent", res)
	}
}

type failingResendGateway struct {
	*notSentGateway
	err error
}

func (g *failingResendGateway) SubmitAcknowledgement(context.Context, shared.NetworkRef, contract.Acknowledgement) error {
	return g.err
}

func TestConfirmShipment_AsksForTheLabelBeforeItRecordsConfirmed(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")
	spy := newSpyGateway()
	pub := &recordingPublisher{}
	uc := f.confirmShipment()
	uc.Gateway = spy
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), "po-1"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got, want := spy.calls, []string{"label", "ship"}; !equal(got, want) {
		t.Fatalf("gateway calls = %v, want %v", got, want)
	}
	if len(spy.labelReqs) != 1 || spy.labelReqs[0] != 1 {
		t.Fatalf("label requests = %v, want one request for one package", spy.labelReqs)
	}
	if len(spy.shipped) != 1 || !spy.shipped[0].Equal(now()) {
		t.Fatalf("shippedAt = %v, want the use case's clock %v", spy.shipped, now())
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
}

func TestConfirmShipment_ALabelFailureLeavesTheOrderAcknowledgedAndRetryable(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")
	spy := newSpyGateway()
	spy.labelErr = errBoom
	pub := &recordingPublisher{}
	uc := f.confirmShipment()
	uc.Gateway = spy
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), "po-1"); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	saved, err := f.orders.FindByRef(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if saved.State() != networkorder.StateAcknowledged {
		t.Fatalf("state = %v, want ACKNOWLEDGED: nothing may be recorded for a shipment the network cannot confirm", saved.State())
	}
	if len(pub.events) != 0 {
		t.Fatalf("events = %d, want none", len(pub.events))
	}
	if got, want := spy.calls, []string{"label"}; !equal(got, want) {
		t.Fatalf("gateway calls = %v, want %v (no confirmation without a label)", got, want)
	}

	spy.labelErr = nil
	if _, err := uc.Execute(context.Background(), "po-1"); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestConfirmShipment_ARefusedOrderNeverCostsANetworkCall(t *testing.T) {
	f := newFixture(false) // infeasible: REJECTED, never acknowledged
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	spy := newSpyGateway()
	uc := f.confirmShipment()
	uc.Gateway = spy

	if _, err := uc.Execute(context.Background(), "po-1"); !errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge) {
		t.Fatalf("err = %v, want ErrConfirmBeforeAcknowledge", err)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("gateway calls = %v, want none", spy.calls)
	}
}
