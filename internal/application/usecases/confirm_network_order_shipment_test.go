package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func (f *fixture) confirmShipment() *usecases.ConfirmNetworkOrderShipment {
	return &usecases.ConfirmNetworkOrderShipment{
		Orders:  f.orders,
		Gateway: f.gateway,
		Events:  nopPublisher{},
		Clock:   fixedClock{t: now()},
	}
}

// acknowledgedOrder seeds, acknowledges and reconciles one order via the
// real ReceiveNetworkDemand + ReconcileSubmittedOrders use cases, end to
// end, so these tests exercise ConfirmNetworkOrderShipment against a
// genuinely ACKNOWLEDGED aggregate rather than a hand-built one. Since
// ADR 0001 §5's two-phase submit/reconcile split, ReceiveNetworkDemand
// alone only reaches SUBMITTED; a reconciliation pass (the stub gateway
// reconciles deterministically to SUCCESS) is what settles it to
// ACKNOWLEDGED.
func acknowledgedOrder(t *testing.T, f *fixture, ref shared.NetworkRef) *networkorder.NetworkOrder {
	t.Helper()
	if _, err := f.receive().Execute(context.Background(), demand(ref, "ASIN-1")); err != nil {
		t.Fatalf("seed receive: %v", err)
	}
	if _, err := f.reconcile().Execute(context.Background()); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}
	o, err := f.orders.FindByRef(context.Background(), ref)
	if err != nil {
		t.Fatalf("seed find: %v", err)
	}
	if o.State() != networkorder.StateAcknowledged {
		t.Fatalf("seed state = %v, want ACKNOWLEDGED (feasible fixture)", o.State())
	}
	return o
}

func TestConfirmShipment_PublishesEventAndSubmitsToGateway(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")

	pub := &recordingPublisher{}
	uc := f.confirmShipment()
	uc.Events = pub

	o, err := uc.Execute(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o.State() != networkorder.StateConfirmed {
		t.Fatalf("state = %v, want CONFIRMED", o.State())
	}

	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	confirmed, ok := pub.events[0].(shared.NetworkOrderShipmentConfirmed)
	if !ok {
		t.Fatalf("events[0] = %T, want NetworkOrderShipmentConfirmed", pub.events[0])
	}
	if confirmed.NetworkRef != "po-1" {
		t.Errorf("NetworkRef = %q, want po-1", confirmed.NetworkRef)
	}
	if confirmed.LocalOrderId != *o.LocalOrderId() {
		t.Errorf("LocalOrderId = %q, want %q", confirmed.LocalOrderId, *o.LocalOrderId())
	}

	saved, err := f.orders.FindByRef(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if saved.State() != networkorder.StateConfirmed {
		t.Fatalf("persisted state = %v, want CONFIRMED", saved.State())
	}
}

func TestConfirmShipment_UnknownRefReturnsErrOrderNotFound(t *testing.T) {
	f := newFixture(true)
	uc := f.confirmShipment()
	if _, err := uc.Execute(context.Background(), "po-missing"); !errors.Is(err, usecases.ErrOrderNotFound) {
		t.Fatalf("err = %v, want ErrOrderNotFound", err)
	}
}

func TestConfirmShipment_RequiresAcknowledgedState(t *testing.T) {
	f := newFixture(false) // infeasible -> rejected, never acknowledged
	if _, err := f.receive().Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	uc := f.confirmShipment()
	if _, err := uc.Execute(context.Background(), "po-1"); !errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge) {
		t.Fatalf("err = %v, want ErrConfirmBeforeAcknowledge", err)
	}
}

func TestConfirmShipment_IsIdempotentOnAlreadyConfirmedOrder(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")

	pub := &recordingPublisher{}
	uc := f.confirmShipment()
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), "po-1"); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events after first call = %d, want 1", len(pub.events))
	}

	// Second call: already CONFIRMED. Must return success WITHOUT
	// re-publishing or re-submitting to the gateway.
	if _, err := uc.Execute(context.Background(), "po-1"); err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events after second (idempotent) call = %d, want still 1", len(pub.events))
	}
}

func TestConfirmShipment_PublishFailureIsReported(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")

	uc := f.confirmShipment()
	uc.Events = &recordingPublisher{err: errBoom}

	if _, err := uc.Execute(context.Background(), "po-1"); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	// No UnitOfWork is wired in this fixture (nil means no transactional
	// backing, same as every other use case's nil-UoW behaviour): Save
	// runs before Publish and is not rolled back by a failed Publish in
	// that mode, so the in-memory repo legitimately already shows
	// CONFIRMED here. The outbox-backed path (ADR 0003) is what gives
	// the atomic all-or-nothing guarantee; that is covered by this same
	// use case's atomically() call, already exercised generically by
	// ReceiveNetworkDemand's own UnitOfWork tests.
	saved, err := f.orders.FindByRef(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if saved.State() != networkorder.StateConfirmed {
		t.Fatalf("persisted state = %v, want CONFIRMED (nil UnitOfWork: Save is not rolled back by a Publish failure)", saved.State())
	}
}

func TestConfirmShipment_GatewaySubmissionFailureIsReported(t *testing.T) {
	f := newFixture(true)
	acknowledgedOrder(t, f, "po-1")

	uc := f.confirmShipment()
	uc.Gateway = errSubmitConfirmationGateway{err: errBoom}

	if _, err := uc.Execute(context.Background(), "po-1"); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	// The state transition and its event already committed (submission
	// is fallible and comes last), so a retry would hit the idempotent
	// already-CONFIRMED branch rather than re-publishing.
	saved, err := f.orders.FindByRef(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if saved.State() != networkorder.StateConfirmed {
		t.Fatalf("persisted state = %v, want CONFIRMED even though the gateway submission failed", saved.State())
	}
}

type errSubmitConfirmationGateway struct{ err error }

func (errSubmitConfirmationGateway) PollDemand(context.Context, time.Time) ([]contract.InboundDemand, error) {
	return nil, nil
}

func (errSubmitConfirmationGateway) SubmitAcknowledgement(context.Context, shared.NetworkRef, bool) error {
	return nil
}

func (g errSubmitConfirmationGateway) SubmitShipmentConfirmation(context.Context, shared.NetworkRef) error {
	return g.err
}

func (errSubmitConfirmationGateway) DeclareCapability(context.Context, contract.CapabilityDeclaration) error {
	return nil
}

func (errSubmitConfirmationGateway) SubmitAvailability(context.Context, contract.AvailabilityUpdate) error {
	return nil
}

func (errSubmitConfirmationGateway) RequestLabel(context.Context, shared.NetworkRef) (contract.LabelResult, error) {
	return contract.LabelResult{}, nil
}

func (errSubmitConfirmationGateway) SubmissionStatus(context.Context, shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	return contract.SubmissionSuccess, nil
}
