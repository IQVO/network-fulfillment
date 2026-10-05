package poller_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/poller"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var errBoom = errors.New("boom")

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stepClock advances by a fixed step on every read, so each poll gets a
// distinguishable timestamp and the watermark's movement is observable.
type stepClock struct {
	t    time.Time
	step time.Duration
}

func (c *stepClock) Now() time.Time {
	c.t = c.t.Add(c.step)
	return c.t
}

// fakeGateway hands out one scripted batch per poll and records the
// watermark it was asked with.
//
// duringCall simulates a slow network call by advancing the clock while
// the gateway is "in flight". That is what makes the before/after
// distinction observable: with the read taken afterwards, the watermark
// jumps past demand that arrived during the call itself.
type fakeGateway struct {
	batches    [][]contract.InboundDemand
	errs       []error
	calls      int
	sinces     []time.Time
	duringCall func()
}

func (g *fakeGateway) PollDemand(_ context.Context, since time.Time) ([]contract.InboundDemand, error) {
	g.sinces = append(g.sinces, since)
	if g.duringCall != nil {
		g.duringCall()
	}
	i := g.calls
	g.calls++
	if i < len(g.errs) && g.errs[i] != nil {
		return nil, g.errs[i]
	}
	if i < len(g.batches) {
		return g.batches[i], nil
	}
	return nil, nil
}

func (g *fakeGateway) SubmitAcknowledgement(context.Context, shared.NetworkRef, bool) error {
	return nil
}
func (g *fakeGateway) SubmitAvailability(context.Context, contract.AvailabilityUpdate) error {
	return nil
}
func (g *fakeGateway) DeclareCapability(context.Context, contract.CapabilityDeclaration) error {
	return nil
}
func (g *fakeGateway) RequestLabel(context.Context, shared.NetworkRef) (contract.LabelResult, error) {
	return contract.LabelResult{}, nil
}
func (g *fakeGateway) SubmissionStatus(context.Context, shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	return contract.SubmissionSuccess, nil
}
func (g *fakeGateway) SubmitShipmentConfirmation(context.Context, shared.NetworkRef) error {
	return nil
}

// fakeReceiver records what it was handed and can fail on one ref.
type fakeReceiver struct {
	seen   []shared.NetworkRef
	failOn shared.NetworkRef
}

func (r *fakeReceiver) Execute(_ context.Context, d contract.InboundDemand) (*networkorder.NetworkOrder, error) {
	r.seen = append(r.seen, d.NetworkRef)
	if r.failOn != "" && d.NetworkRef == r.failOn {
		return nil, errBoom
	}
	// The aggregate is irrelevant to the poller, which is the point of
	// the local Receiver interface — returning nil proves the poller
	// never dereferences it.
	return nil, nil
}

func demand(ref string) contract.InboundDemand {
	return contract.InboundDemand{
		NetworkRef:     shared.NetworkRef(ref),
		SiteId:         "site-1",
		RequiredShipBy: time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC),
		Lines: []contract.InboundLine{
			{NetworkLineRef: "1", NetworkProductId: "ASIN-1", Quantity: 1},
		},
	}
}

func newPoller(g *fakeGateway, r *fakeReceiver, clock *stepClock) *poller.Poller {
	return poller.New(g, r, clock, poller.Config{Interval: time.Hour}, discardLogger())
}

func TestPollOnce_HandsEveryDemandToTheReceiver(t *testing.T) {
	g := &fakeGateway{batches: [][]contract.InboundDemand{{demand("po-1"), demand("po-2")}}}
	r := &fakeReceiver{}
	p := newPoller(g, r, &stepClock{t: time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC), step: time.Minute})

	p.PollOnce(context.Background())

	if len(r.seen) != 2 || r.seen[0] != "po-1" || r.seen[1] != "po-2" {
		t.Fatalf("receiver saw %v, want [po-1 po-2]", r.seen)
	}
	if s := p.Stats(); s.Received != 2 || s.Failed != 0 || s.Polls != 1 {
		t.Fatalf("stats = %+v, want 1 poll / 2 received / 0 failed", s)
	}
}

// The cold start must ask for everything outstanding. An order we have
// never seen is exactly the one whose deadline is closest.
func TestPollOnce_ColdStartAsksFromTheZeroWatermark(t *testing.T) {
	g := &fakeGateway{}
	p := newPoller(g, &fakeReceiver{}, &stepClock{t: time.Now(), step: time.Minute})

	p.PollOnce(context.Background())

	if len(g.sinces) != 1 {
		t.Fatalf("gateway polled %d times, want 1", len(g.sinces))
	}
	if !g.sinces[0].IsZero() {
		t.Fatalf("cold start asked from %v, want the zero time", g.sinces[0])
	}
}

// The watermark must be read BEFORE the gateway call, not after, or
// demand that arrived during the call itself is skipped forever.
//
// The gateway advances the clock mid-call to represent a slow request.
// A watermark read afterwards would land past that instant, so any
// demand the network accepted while we waited would never be fetched.
func TestPollOnce_WatermarkIsTakenBeforeTheCallNotAfter(t *testing.T) {
	start := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	clock := &stepClock{t: start, step: time.Minute}
	g := &fakeGateway{}
	// Three extra clock reads while the call is in flight: 3 minutes of
	// simulated network latency during which demand could arrive.
	g.duringCall = func() { clock.Now(); clock.Now(); clock.Now() }
	p := newPoller(g, &fakeReceiver{}, clock)

	p.PollOnce(context.Background())

	// Read before the call = the first tick after start. Read after =
	// start + 4 minutes, which skips the whole in-flight window.
	wantBefore := start.Add(time.Minute)
	got := p.Stats().Since
	if !got.Equal(wantBefore) {
		t.Fatalf("watermark = %v, want the instant the poll BEGAN (%v); a later value skips demand that arrived during the call",
			got, wantBefore)
	}

	p.PollOnce(context.Background())
	if second := p.Stats().Since; !second.After(got) {
		t.Fatalf("watermark did not advance on a clean second pass: %v then %v", got, second)
	}
	if g.sinces[1] != got {
		t.Fatalf("second poll asked from %v, want the first pass's watermark %v", g.sinces[1], got)
	}
}

// The rule the whole adapter exists to get right: a failure must leave
// the watermark alone so the demand is re-fetched. Advancing past an
// order whose 24h clock is running loses it silently.
func TestPollOnce_FailureDoesNotAdvanceTheWatermark(t *testing.T) {
	g := &fakeGateway{batches: [][]contract.InboundDemand{
		{demand("po-ok"), demand("po-bad")},
	}}
	r := &fakeReceiver{failOn: "po-bad"}
	p := newPoller(g, r, &stepClock{t: time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC), step: time.Minute})

	p.PollOnce(context.Background())

	if s := p.Stats(); !s.Since.IsZero() {
		t.Fatalf("watermark advanced to %v despite a failure; that demand can never be re-fetched", s.Since)
	}
	if s := p.Stats(); s.Failed != 1 || s.Received != 1 {
		t.Fatalf("stats = %+v, want 1 received / 1 failed", s)
	}
}

// One bad order must not abort the pass: the others are real demand with
// their own clocks running.
func TestPollOnce_OneFailureDoesNotStopTheBatch(t *testing.T) {
	g := &fakeGateway{batches: [][]contract.InboundDemand{
		{demand("po-1"), demand("po-bad"), demand("po-3")},
	}}
	r := &fakeReceiver{failOn: "po-bad"}
	p := newPoller(g, r, &stepClock{t: time.Now(), step: time.Minute})

	p.PollOnce(context.Background())

	if len(r.seen) != 3 {
		t.Fatalf("receiver saw %v, want all three units attempted", r.seen)
	}
	if r.seen[2] != "po-3" {
		t.Fatalf("the unit after the failure was not attempted: %v", r.seen)
	}
}

// A gateway error is the ticker's problem, not a crash — and it must not
// move the watermark either.
func TestPollOnce_GatewayErrorIsSurvivedAndWatermarkHeld(t *testing.T) {
	g := &fakeGateway{errs: []error{errBoom}}
	r := &fakeReceiver{}
	p := newPoller(g, r, &stepClock{t: time.Now(), step: time.Minute})

	p.PollOnce(context.Background())

	if len(r.seen) != 0 {
		t.Fatalf("receiver was called despite a gateway error: %v", r.seen)
	}
	if s := p.Stats(); !s.Since.IsZero() {
		t.Fatalf("watermark advanced to %v after a gateway error", s.Since)
	}
	if s := p.Stats(); s.Polls != 1 {
		t.Fatalf("polls = %d, want the attempt counted", s.Polls)
	}
}

// An empty poll is a successful poll: nothing outstanding means the
// watermark should move, or a quiet period makes us re-ask for an
// ever-growing window.
func TestPollOnce_EmptyBatchStillAdvancesTheWatermark(t *testing.T) {
	g := &fakeGateway{batches: [][]contract.InboundDemand{{}}}
	p := newPoller(g, &fakeReceiver{}, &stepClock{t: time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC), step: time.Minute})

	p.PollOnce(context.Background())

	if s := p.Stats(); s.Since.IsZero() {
		t.Fatal("an empty poll left the watermark at zero; the window would grow forever")
	}
}

// A cancelled context must stop the batch rather than start work it
// cannot finish. The remaining demand is safe because the watermark
// stays put.
func TestPollOnce_CancelledContextAbandonsTheBatch(t *testing.T) {
	g := &fakeGateway{batches: [][]contract.InboundDemand{
		{demand("po-1"), demand("po-2"), demand("po-3")},
	}}
	r := &fakeReceiver{}
	p := newPoller(g, r, &stepClock{t: time.Now(), step: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.PollOnce(ctx)

	if len(r.seen) != 0 {
		t.Fatalf("an already-cancelled context still processed %v", r.seen)
	}
	if s := p.Stats(); !s.Since.IsZero() {
		t.Fatalf("watermark advanced to %v on an abandoned pass", s.Since)
	}
}

// Run must poll immediately, not sleep out the first interval: a restart
// is when we are most likely to be behind, and the acknowledgement clock
// ran while we were down.
func TestRun_PollsImmediatelyWithoutWaitingForTheFirstTick(t *testing.T) {
	g := &fakeGateway{batches: [][]contract.InboundDemand{{demand("po-1")}}}
	r := &fakeReceiver{}
	// An interval far longer than this test will live: anything observed
	// can only have come from the immediate poll.
	p := poller.New(g, r, &stepClock{t: time.Now(), step: time.Minute},
		poller.Config{Interval: time.Hour}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	deadline := time.After(2 * time.Second)
	for p.Stats().Polls == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("Run did not poll before its first tick; a restart would add our own latency to the network's deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if len(r.seen) != 1 || r.seen[0] != "po-1" {
		t.Fatalf("immediate poll delivered %v, want [po-1]", r.seen)
	}
}

func TestRun_StopsOnContextCancellation(t *testing.T) {
	p := poller.New(&fakeGateway{}, &fakeReceiver{}, &stepClock{t: time.Now(), step: time.Minute},
		poller.Config{Interval: 10 * time.Millisecond}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// Config.Since is the resume point: a caller that knows where it left
// off must not be forced back into a cold start.
func TestNew_HonoursAnInitialWatermark(t *testing.T) {
	since := time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC)
	g := &fakeGateway{}
	p := poller.New(g, &fakeReceiver{}, &stepClock{t: time.Now(), step: time.Minute},
		poller.Config{Interval: time.Hour, Since: since}, discardLogger())

	p.PollOnce(context.Background())

	if g.sinces[0] != since {
		t.Fatalf("first poll asked from %v, want the configured %v", g.sinces[0], since)
	}
}
