package main_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/network"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The doubles in this file stand in for the things this bounded context
// reaches OUT to. The suite never makes a REST or MCP call to a sibling
// context: order-management, inventory-storage, process-path-management and
// wes-work-planning are all represented by the in-process doubles below, and
// the external retail network is represented by the real StubGateway.

// ------------------------------------------------------------------ clock --

// fixedClock is the scenario's only source of time. Scenarios move it with
// "N minutes pass" instead of sleeping.
type fixedClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFixedClock(t time.Time) *fixedClock { return &fixedClock{t: t} }

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ---------------------------------------------------------------- planner --

var errPlannerDown = errors.New("order-management is unavailable")

// fakePlanner is order-management, seen through ports.FulfillmentPlanner.
// Feasibility is order-management's decision (ADR 0001 §7), so the double
// simply answers what the scenario told it to.
type fakePlanner struct {
	mu          sync.Mutex
	feasible    bool
	unavailable bool
	seq         int
	raised      []contract.HeldOrderRequest
	released    []shared.LocalOrderId
	cancelled   []shared.LocalOrderId
}

func newFakePlanner() *fakePlanner { return &fakePlanner{feasible: true} }

func (p *fakePlanner) RaiseHeldOrder(_ context.Context, req contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unavailable {
		return contract.HeldOrderResult{}, errPlannerDown
	}
	p.seq++
	p.raised = append(p.raised, req)
	return contract.HeldOrderResult{
		LocalOrderId:   shared.LocalOrderId(fmt.Sprintf("ord-%04d", p.seq)),
		Feasible:       p.feasible,
		PromisedCutoff: req.RequiredShipBy,
	}, nil
}

func (p *fakePlanner) ReleaseHeldOrder(_ context.Context, id shared.LocalOrderId) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = append(p.released, id)
	return nil
}

func (p *fakePlanner) CancelHeldOrder(_ context.Context, id shared.LocalOrderId) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelled = append(p.cancelled, id)
	return nil
}

// ---------------------------------------------------------------- gateway --

var errNetworkRefused = errors.New("the network refused the shipment confirmation")

// recordingGateway wraps the REAL StubGateway (the production NETWORK_MODE=stub
// adapter) and records what this context told the network, so scenarios can
// assert on the outbound leg without reaching into adapter internals.
type recordingGateway struct {
	*network.StubGateway

	mu               sync.Mutex
	acks             map[shared.NetworkRef][]bool
	shipmentAttempts map[shared.NetworkRef]int
	shipments        map[shared.NetworkRef]int
	failShipment     bool
	refused          map[shared.NetworkRef]bool
	unsettled        map[shared.NetworkRef]bool
}

func newRecordingGateway(stub *network.StubGateway) *recordingGateway {
	return &recordingGateway{
		StubGateway:      stub,
		acks:             map[shared.NetworkRef][]bool{},
		shipmentAttempts: map[shared.NetworkRef]int{},
		shipments:        map[shared.NetworkRef]int{},
		refused:          map[shared.NetworkRef]bool{},
		unsettled:        map[shared.NetworkRef]bool{},
	}
}

func (g *recordingGateway) SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, accepted bool) error {
	g.mu.Lock()
	g.acks[ref] = append(g.acks[ref], accepted)
	g.mu.Unlock()
	return g.StubGateway.SubmitAcknowledgement(ctx, ref, accepted)
}

func (g *recordingGateway) SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef) error {
	g.mu.Lock()
	g.shipmentAttempts[ref]++
	fail := g.failShipment
	if !fail {
		g.shipments[ref]++
	}
	g.mu.Unlock()
	if fail {
		return errNetworkRefused
	}
	return g.StubGateway.SubmitShipmentConfirmation(ctx, ref)
}

// SubmissionStatus is the network's transaction-status record. By default the
// stub settles instantly; a scenario can make the network refuse a submission
// or leave it unsettled.
func (g *recordingGateway) SubmissionStatus(ctx context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	g.mu.Lock()
	refused, unsettled := g.refused[ref], g.unsettled[ref]
	g.mu.Unlock()
	switch {
	case refused:
		return contract.SubmissionFailure, nil
	case unsettled:
		return contract.SubmissionPending, nil
	default:
		return g.StubGateway.SubmissionStatus(ctx, ref)
	}
}

// ------------------------------------------------------ capability inputs --

var errInventoryDown = errors.New("inventory-storage is unreachable")

// fakeInventory is inventory-storage's usable-inventory read model.
type fakeInventory struct {
	usable  map[shared.SKU]int
	failing map[shared.SKU]bool
}

func newFakeInventory() *fakeInventory {
	return &fakeInventory{usable: map[shared.SKU]int{}, failing: map[shared.SKU]bool{}}
}

func (i *fakeInventory) UsableQuantity(_ context.Context, sku shared.SKU) (int, error) {
	if i.failing[sku] {
		return 0, errInventoryDown
	}
	return i.usable[sku], nil
}

// fakePathCap is process-path-management's fulfillment-capability cache: the
// next CPT cutoff and the paths eligible for it.
type fakePathCap struct {
	scheduled bool
	cutoffIn  time.Duration
	paths     []contract.EligiblePath
}

func (p *fakePathCap) NextCutoff(_ shared.SiteId, after time.Time) (contract.NextCutoff, bool) {
	if !p.scheduled {
		return contract.NextCutoff{}, false
	}
	return contract.NextCutoff{CutoffAt: after.Add(p.cutoffIn), Paths: p.paths}, true
}

// fakeCapacity is wes-work-planning's remaining-admission-capacity figure per
// path. A path with no entry is "never observed" (known=false).
type fakeCapacity struct{ units map[string]int }

func (c *fakeCapacity) RemainingCapacity(pathID string, _ time.Time) (int, bool) {
	units, ok := c.units[pathID]
	return units, ok
}

// -------------------------------------------------------------- event bus --

// recordingWriter is the Kafka writer the real publishers are given: it keeps
// every message instead of sending it to a broker.
type recordingWriter struct {
	stream string
	bus    *eventBus
}

func (r recordingWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	for _, m := range msgs {
		r.bus.messages = append(r.bus.messages, publishedMessage{stream: r.stream, msg: m})
	}
	return nil
}

type publishedMessage struct {
	stream string
	msg    kafkago.Message
}

// seenEvents is the analytics consumer's idempotency gate (ProcessedEvents).
type seenEvents map[string]struct{}

func (s seenEvents) MarkProcessed(_ context.Context, id string) (bool, error) {
	if _, dup := s[id]; dup {
		return false, nil
	}
	s[id] = struct{}{}
	return true, nil
}

// eventBus is the ports.EventPublisher every use case publishes to. It runs
// the PRODUCTION encoders (the integration Publisher and the AnalyticsPublisher,
// both emitting CloudEvents 1.0) into recording writers, then hands each
// analytics message to the PRODUCTION analytics consumer, which projects it
// into the report read model exactly as netfulfil-projector would.
type eventBus struct {
	messages    []publishedMessage
	seq         int
	integration *outboundkafka.Publisher
	analytics   *outboundkafka.AnalyticsPublisher
	consumer    *inboundkafka.AnalyticsConsumer
}

func newEventBus(consumer *inboundkafka.AnalyticsConsumer) *eventBus {
	b := &eventBus{consumer: consumer}
	b.integration = &outboundkafka.Publisher{Writer: recordingWriter{stream: "events", bus: b}, NewId: b.newID}
	b.analytics = &outboundkafka.AnalyticsPublisher{Writer: recordingWriter{stream: "analytics", bus: b}, NewId: b.newID}
	return b
}

// newID mints a UUID-shaped CloudEvents id, unique per occurrence.
func (b *eventBus) newID() string {
	b.seq++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", b.seq)
}

func (b *eventBus) Publish(ctx context.Context, event any) error {
	if err := b.integration.Publish(ctx, event); err != nil {
		return err
	}
	before := len(b.messages)
	if err := b.analytics.Publish(ctx, event); err != nil {
		return err
	}
	for _, m := range b.messages[before:] {
		if err := b.consumer.HandleMessage(ctx, m.msg.Value); err != nil {
			return err
		}
	}
	return nil
}
