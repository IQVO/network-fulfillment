package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// This file closes the remaining error-propagation branches in
// ReceiveNetworkDemand and SweepAcknowledgementDeadlines that the
// existing suites (receive_network_demand_test.go, events_test.go,
// failure_paths_test.go) do not reach: a non-ErrUnknownProduct
// translation failure, a domain-construction failure on an otherwise
// valid demand, the untranslatable path's own publish/construction
// failures, and the reject()/acknowledge() gateway-submit and
// second-publish failures that only fire once the FIRST publish
// (publishReceived) has already succeeded.

var errOtherTranslation = errors.New("translation backend unavailable")

// erroringTranslation always fails with a non-ErrUnknownProduct error,
// exercising Execute's "some other translation failure" branch (the
// ErrUnknownProduct case is the one everything else already tests).
type erroringTranslation struct{}

func (erroringTranslation) ToSKU(context.Context, shared.NetworkProductId) (shared.SKU, error) {
	return "", errOtherTranslation
}

func TestReceive_NonUnknownProductTranslationFailureAborts(t *testing.T) {
	f := newFixture(true)
	uc := f.receive()
	uc.Translation = erroringTranslation{}

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errOtherTranslation) {
		t.Fatalf("err = %v, want errOtherTranslation", err)
	}
	// This is a genuine failure, not a business rejection: no answer
	// may reach the network and no hold may be raised.
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}
	if _, submitted := f.gateway.Acknowledgement("po-1"); submitted {
		t.Fatal("no acknowledgement may be submitted on a translation backend failure")
	}
}

func TestReceive_ZeroLineDemandFailsDomainConstruction(t *testing.T) {
	f := newFixture(true)

	// No lines at all: translate() succeeds trivially (nothing to
	// translate), but networkorder.Receive rejects an empty order —
	// distinct from the untranslatable path, which always has at
	// least the demand's own line count recorded as zero.
	empty := contract.InboundDemand{
		NetworkRef:     "po-empty",
		SiteId:         "site-1",
		RequiredShipBy: now().Add(48 * time.Hour),
		Lines:          nil,
	}
	if _, err := f.receive().Execute(context.Background(), empty); !errors.Is(err, shared.ErrNoLines) {
		t.Fatalf("err = %v, want ErrNoLines", err)
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}
}

func TestReceive_NonPositiveQuantityFailsLineConstruction(t *testing.T) {
	f := newFixture(true)

	// A mapped product (translation succeeds) but an invalid quantity:
	// this exercises NewLine's OWN validation failure inside translate,
	// distinct from an unmapped product.
	bad := contract.InboundDemand{
		NetworkRef:     "po-1",
		SiteId:         "site-1",
		RequiredShipBy: now().Add(48 * time.Hour),
		Lines: []contract.InboundLine{
			{NetworkLineRef: "a", NetworkProductId: "ASIN-1", Quantity: 0},
		},
	}
	if _, err := f.receive().Execute(context.Background(), bad); !errors.Is(err, shared.ErrNonPositiveQuantity) {
		t.Fatalf("err = %v, want ErrNonPositiveQuantity", err)
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none", f.planner.calls)
	}
}

func TestReceive_UntranslatableWithEmptyRefFailsDomainConstruction(t *testing.T) {
	f := newFixture(true)

	// An empty NetworkRef on otherwise-untranslatable demand: the
	// translation failure is detected first, then rejectUntranslatable
	// itself fails to construct the order — a distinct failure from
	// every other untranslatable-path test, which all use a real ref.
	bad := contract.InboundDemand{
		NetworkRef:     "",
		SiteId:         "site-1",
		RequiredShipBy: now().Add(48 * time.Hour),
		Lines: []contract.InboundLine{
			{NetworkLineRef: "a", NetworkProductId: "ASIN-UNKNOWN", Quantity: 1},
		},
	}
	if _, err := f.receive().Execute(context.Background(), bad); !errors.Is(err, shared.ErrEmptyNetworkRef) {
		t.Fatalf("err = %v, want ErrEmptyNetworkRef", err)
	}
}

// selectiveFailPublisher succeeds on its first N calls and fails on
// every call after that, letting a test isolate a SECOND publish call
// (reject's or acknowledge's own event) while allowing the FIRST
// publish (publishReceived) to succeed — something a publisher that
// always fails cannot do, since Execute would never get past the
// first call.
type selectiveFailPublisher struct {
	succeedCalls int
	calls        int
	err          error
}

func (p *selectiveFailPublisher) Publish(context.Context, any) error {
	p.calls++
	if p.calls <= p.succeedCalls {
		return nil
	}
	return p.err
}

func TestReceive_RejectOwnPublishFailsAfterReceivedPublishSucceeds(t *testing.T) {
	f := newFixture(false) // infeasible -> reject path
	uc := f.receive()
	pub := &selectiveFailPublisher{succeedCalls: 1, err: errBoom}
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	if pub.calls != 2 {
		t.Fatalf("publish calls = %d, want 2 (Received succeeded, Rejected failed)", pub.calls)
	}
	// The rejection was already committed to the repo and the network
	// before the publish failed — that ordering is the whole point of
	// publish being the LAST step.
	accepted, submitted := f.gateway.Acknowledgement("po-1")
	if !submitted || accepted {
		t.Fatalf("acknowledgement submitted=%v accepted=%v, want true/false even though the rejection publish failed", submitted, accepted)
	}
}

func TestReceive_AcknowledgeOwnPublishFailsAfterReceivedPublishSucceeds(t *testing.T) {
	f := newFixture(true) // feasible -> acknowledge path
	uc := f.receive()
	pub := &selectiveFailPublisher{succeedCalls: 1, err: errBoom}
	uc.Events = pub

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	if pub.calls != 2 {
		t.Fatalf("publish calls = %d, want 2 (Received succeeded, Acknowledged failed)", pub.calls)
	}
	// Release already ran: it precedes the publish on purpose, and a
	// publish failure must not be mistaken for grounds to have skipped
	// it.
	if got := f.planner.calls; !equal(got, []string{"raise", "release"}) {
		t.Fatalf("planner calls = %v, want [raise release] even though the acknowledgement publish failed", got)
	}
}

// errSubmitGateway forces SubmitAcknowledgement to fail while otherwise
// delegating nothing (this use case never calls
// SubmitShipmentConfirmation), letting a test isolate reject's and
// acknowledge's own gateway-submission failure branch.
type errSubmitGateway struct{ err error }

func (errSubmitGateway) PollDemand(context.Context, time.Time) ([]contract.InboundDemand, error) {
	return nil, nil
}

func (g errSubmitGateway) SubmitAcknowledgement(context.Context, shared.NetworkRef, bool) error {
	return g.err
}

func (errSubmitGateway) SubmitShipmentConfirmation(context.Context, shared.NetworkRef) error {
	return nil
}

func TestReceive_RejectGatewaySubmissionFailureAborts(t *testing.T) {
	f := newFixture(false) // infeasible -> reject path
	uc := f.receive()
	uc.Gateway = errSubmitGateway{err: errBoom}

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	// The local hold was already cancelled and the rejection already
	// saved — submission is fallible and comes after both, so a retry
	// of the same poll can safely resubmit without redoing either.
	if got := f.planner.calls; !equal(got, []string{"raise", "cancel"}) {
		t.Fatalf("planner calls = %v, want [raise cancel] even though the network submission failed", got)
	}
	saved, err := f.orders.FindByRef(context.Background(), "po-1")
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if saved.State() != networkorder.StateRejected {
		t.Fatalf("state = %v, want REJECTED even though the network submission failed", saved.State())
	}
}

func TestReceive_AcknowledgeGatewaySubmissionFailureAborts(t *testing.T) {
	f := newFixture(true) // feasible -> acknowledge path
	uc := f.receive()
	uc.Gateway = errSubmitGateway{err: errBoom}

	if _, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	// Release must NOT have run: submission is fallible and precedes
	// release, the irreversible step, on purpose.
	if got := f.planner.calls; !equal(got, []string{"raise"}) {
		t.Fatalf("planner calls = %v, want [raise] — release must not run when the network submission failed", got)
	}
}

// unansweredCancellingRepo wraps the real in-memory repo, delegating
// ListUnanswered to it (so the sweep sees genuine unanswered orders)
// while letting a test force Save to fail — isolating
// SweepAcknowledgementDeadlines.missOne's OWN Save failure from its
// CancelHeldOrder failure (already covered by
// TestSweep_OneFailingOrderDoesNotAbortThePass) and from
// ListUnanswered's failure (TestSweep_RepositoryFailureIsReported).
type unansweredCancellingRepo struct {
	*memory.NetworkOrderRepo
	saveErr error
}

func (r *unansweredCancellingRepo) Save(ctx context.Context, o *networkorder.NetworkOrder) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.NetworkOrderRepo.Save(ctx, o)
}

func TestSweep_MissOneSaveFailureIsSkippedNotFatalToThePass(t *testing.T) {
	f := newFixture(true)
	repo := &unansweredCancellingRepo{NetworkOrderRepo: f.orders, saveErr: errBoom}

	stuck := networkorder.Rehydrate("po-old", "site-1", now().Add(48*time.Hour),
		now().Add(-time.Hour), now().Add(-25*time.Hour),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, nil)
	if err := f.orders.Save(context.Background(), stuck); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: repo, Planner: f.planner, Events: nopPublisher{},
		Clock: fixedClock{t: now()},
	}
	res, err := sweep.Execute(context.Background())
	if err != nil {
		t.Fatalf("sweep must not abort the whole pass on a single Save failure: %v", err)
	}
	if res.Examined != 1 || res.Missed != 0 {
		t.Fatalf("res = %+v, want Examined=1 Missed=0 — a Save failure must not count as a hit and must not fail the pass", res)
	}
}

// TestSweep_UnansweredButNotYetOverdueOrderIsSkipped exercises the
// sweep's "continue" branch for an order that IS in ListUnanswered
// (state NEW) but has not yet crossed its acknowledgement deadline —
// distinct from every existing sweep test, which either uses an
// ACKNOWLEDGED order (absent from ListUnanswered entirely, so this
// branch is never reached) or an overdue one.
func TestSweep_UnansweredButNotYetOverdueOrderIsSkipped(t *testing.T) {
	f := newFixture(true)

	// Received a minute ago: still unanswered, but nowhere near its
	// 24h deadline.
	fresh := networkorder.Rehydrate("po-fresh", "site-1", now().Add(48*time.Hour),
		now().Add(24*time.Hour), now().Add(-time.Minute),
		[]networkorder.Line{mustLine(t)}, networkorder.StateNew, nil)
	if err := f.orders.Save(context.Background(), fresh); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	sweep := &usecases.SweepAcknowledgementDeadlines{
		Orders: f.orders, Planner: f.planner, Events: nopPublisher{},
		Clock: fixedClock{t: now()},
	}
	res, err := sweep.Execute(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Examined != 1 {
		t.Fatalf("examined = %d, want 1 — the order IS in the unanswered set", res.Examined)
	}
	if res.Missed != 0 {
		t.Fatalf("missed = %d, want 0 — its deadline has not passed", res.Missed)
	}
	if len(f.planner.calls) != 0 {
		t.Fatalf("planner calls = %v, want none — a not-yet-overdue order must not be touched", f.planner.calls)
	}
}
