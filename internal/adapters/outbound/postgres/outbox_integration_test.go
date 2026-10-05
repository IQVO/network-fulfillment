//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// recordingSink is an OutboxRelay Sink fake that records every encoded
// message it is asked to send, optionally failing on one EventType so a
// test can prove the relay stops at (and retries) exactly that row.
type recordingSink struct {
	sent   []outboundkafka.Encoded
	failOn string // EventType to fail on, "" for never
	failed map[string]bool
	err    error
}

func (s *recordingSink) Send(_ context.Context, encoded ...outboundkafka.Encoded) error {
	for _, enc := range encoded {
		if s.failOn != "" && enc.EventType == s.failOn && !s.failed[enc.EventType] {
			if s.failed == nil {
				s.failed = map[string]bool{}
			}
			s.failed[enc.EventType] = true
			return s.err
		}
		s.sent = append(s.sent, enc)
	}
	return nil
}

// receivedType is the full CloudEvents type the outbox persists as
// event_type for a NetworkOrderReceived row.
const receivedType = "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived"

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func networkOrderReceive(t *testing.T, ref shared.NetworkRef, now time.Time) *networkorder.NetworkOrder {
	t.Helper()
	o, err := networkorder.Receive(ref, "site-1", now.Add(48*time.Hour),
		[]networkorder.Line{mustLine(t, "1", "ASIN-AAA", "sku-aaa", 1)}, now)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return o
}

// TestOutbox_ReceiveNetworkDemand_CommitsAggregateAndBothTopicsTogether
// is the whole point of the outbox: after the use case runs, the
// aggregate row AND one unpublished outbox row per topic must exist —
// never one without the other.
func TestOutbox_ReceiveNetworkDemand_CommitsAggregateAndBothTopicsTogether(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	repo := postgres.NewNetworkOrderRepo(pool)
	integration := outboundkafka.NewPublisher(nil, func() string { return "evt-1" })
	analytics := outboundkafka.NewAnalyticsPublisher(nil, func() string { return "evt-1" })
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)

	uc := &usecases.ReceiveNetworkDemand{
		Orders:      repo,
		Gateway:     stubGateway{},
		Planner:     &stubPlanner{feasible: true},
		Translation: passTranslation{},
		Events:      pub,
		Clock:       fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork:  uow,
	}

	ref := uniqueRef("outbox")
	if _, err := uc.Execute(ctx, demand(ref)); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	found, err := repo.FindByRef(ctx, ref)
	if err != nil || found == nil {
		t.Fatalf("expected the order persisted, got %v err=%v", found, err)
	}
	// Received + Acknowledged, each fanned out to both topics = 4 rows.
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 4 {
		t.Fatalf("expected 4 unpublished outbox rows (2 events x 2 topics), got %d", got)
	}
	if got := countOutbox(t, pool, "topic = '"+outboundkafka.Topic+"'"); got != 2 {
		t.Fatalf("expected 2 integration-topic rows, got %d", got)
	}
	if got := countOutbox(t, pool, "topic = '"+outboundkafka.AnalyticsTopic+"'"); got != 2 {
		t.Fatalf("expected 2 analytics-topic rows, got %d", got)
	}
}

// failingEncoder always errors, letting a test provoke an outbox insert
// failure without needing a database-level constraint violation.
type failingEncoder struct{ err error }

func (e failingEncoder) Encode(context.Context, ...shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return nil, e.err
}

// TestOutbox_PublishFailure_RollsBackAggregate is the outbox's core
// guarantee: if the event cannot be enqueued, the aggregate change must
// not survive either.
func TestOutbox_PublishFailure_RollsBackAggregate(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	repo := postgres.NewNetworkOrderRepo(pool)
	pub := postgres.NewOutboxPublisher(pool, failingEncoder{err: errors.New("encode failed")})
	uow := postgres.NewUnitOfWork(pool)

	uc := &usecases.ReceiveNetworkDemand{
		Orders:      repo,
		Gateway:     stubGateway{},
		Planner:     &stubPlanner{feasible: true},
		Translation: passTranslation{},
		Events:      pub,
		Clock:       fixedClock{t: time.Now().UTC()},
		UnitOfWork:  uow,
	}

	ref := uniqueRef("outbox-fail")
	if _, err := uc.Execute(ctx, demand(ref)); err == nil {
		t.Fatal("expected the encode failure to fail the publish")
	}

	found, err := repo.FindByRef(ctx, ref)
	if err != nil {
		t.Fatalf("FindByRef: %v", err)
	}
	if found != nil {
		t.Fatal("aggregate row survived a failed publish: the unit of work did not roll back")
	}
	if got := countOutbox(t, pool, "event_type = '"+receivedType+"'"); got != 0 {
		t.Fatalf("expected no outbox rows for the failed publish, got %d", got)
	}
}

// TestOutboxRelay_PublishesInOrderAndMarksRows exercises the relay end
// to end: it must publish unpublished rows oldest-first, mark every one
// published, and a second pass must be a no-op.
func TestOutboxRelay_PublishesInOrderAndMarksRows(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := postgres.NewNetworkOrderRepo(pool)
	integration := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	pub := postgres.NewOutboxPublisher(pool, integration)
	uow := postgres.NewUnitOfWork(pool)

	for i, ref := range []shared.NetworkRef{uniqueRef("relay-a"), uniqueRef("relay-b"), uniqueRef("relay-c")} {
		o := networkOrderReceive(t, ref, now.Add(time.Duration(i)*time.Second))
		if err := uow.Execute(ctx, func(ctx context.Context) error {
			if err := repo.Save(ctx, o); err != nil {
				return err
			}
			return pub.Publish(ctx, shared.NetworkOrderReceived{
				NetworkRef: o.NetworkRef(), SiteId: o.SiteId(),
				RequiredShipBy: o.RequiredShipBy(), AcknowledgeBy: o.AcknowledgeBy(),
				LineCount: 1, At: now,
			})
		}); err != nil {
			t.Fatalf("setup publish %d: %v", i, err)
		}
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n != 3 || len(sink.sent) != 3 {
		t.Fatalf("expected 3 published, got n=%d sent=%d", n, len(sink.sent))
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}
	// The relay forwards the CloudEvent exactly as persisted: the id
	// minted at enqueue time, the full type, and the content-type header.
	for _, enc := range sink.sent {
		ev, err := cloudevents.Decode(enc.Value)
		if err != nil {
			t.Fatalf("relayed value is not a CloudEvent: %v", err)
		}
		if ev.ID() != "evt" || ev.Type() != receivedType || ev.Subject() == "" {
			t.Fatalf("relayed event attributes = id %q type %q subject %q", ev.ID(), ev.Type(), ev.Subject())
		}
		if len(enc.Headers) != 1 || enc.Headers[0].Key != "content-type" || string(enc.Headers[0].Value) != cloudevents.MediaType {
			t.Fatalf("relayed headers = %+v, want the CloudEvents content-type header", enc.Headers)
		}
	}
	// A second pass finds nothing and republishes nothing.
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 3 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

// TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater proves the
// relay preserves per-aggregate ordering across a broker outage: it
// stops at the failing row, leaves later rows pending, records the
// attempt, and drains the rest once the sink recovers.
func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := postgres.NewNetworkOrderRepo(pool)
	integration := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	pub := postgres.NewOutboxPublisher(pool, integration)
	uow := postgres.NewUnitOfWork(pool)

	refs := []shared.NetworkRef{uniqueRef("a1"), uniqueRef("b2"), uniqueRef("c3")}
	for i, ref := range refs {
		o := networkOrderReceive(t, ref, now.Add(time.Duration(i)*time.Second))
		if err := uow.Execute(ctx, func(ctx context.Context) error {
			if err := repo.Save(ctx, o); err != nil {
				return err
			}
			return pub.Publish(ctx, shared.NetworkOrderReceived{
				NetworkRef: o.NetworkRef(), SiteId: o.SiteId(),
				RequiredShipBy: o.RequiredShipBy(), AcknowledgeBy: o.AcknowledgeBy(),
				LineCount: 1, At: now,
			})
		}); err != nil {
			t.Fatalf("setup %d: %v", i, err)
		}
	}

	sink := &recordingSink{failOn: receivedType, err: errors.New("broker down")}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithBatchSize(1))
	// With batch size 1, one row is claimed per pass; the first pass's
	// row IS the failing one (oldest first), so it fails immediately.
	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 0 {
		t.Fatalf("expected nothing published on the failing pass, got n=%d", n)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 3 {
		t.Fatalf("expected all 3 rows still pending, got %d", got)
	}
	var attempts int
	var lastErr string
	if err := pool.QueryRow(ctx, `
		SELECT attempts, coalesce(last_error,'') FROM outbox_events
		WHERE event_type = $1 ORDER BY id LIMIT 1
	`, receivedType).Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read first row: %v", err)
	}
	if attempts != 1 || lastErr == "" {
		t.Fatalf("expected the failed row to record the attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}

	// Broker recovers: subsequent passes drain the rest, in order.
	sink.failOn = ""
	for i := 0; i < 3; i++ {
		if _, err := relay.RelayOnce(ctx); err != nil {
			t.Fatalf("recovery pass %d: %v", i, err)
		}
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
}

// --- fixtures local to this file: minimal fakes for the application
// layer's out ports, kept here rather than imported from the usecases
// test package so this integration suite has no dependency on that
// package's internal test helpers. ---

type stubGateway struct{}

func (stubGateway) PollDemand(context.Context, time.Time) ([]contract.InboundDemand, error) {
	return nil, nil
}
func (stubGateway) SubmitAcknowledgement(context.Context, shared.NetworkRef, bool) error {
	return nil
}
func (stubGateway) SubmitAvailability(context.Context, contract.AvailabilityUpdate) error {
	return nil
}
func (stubGateway) DeclareCapability(context.Context, contract.CapabilityDeclaration) error {
	return nil
}
func (stubGateway) RequestLabel(context.Context, shared.NetworkRef) (contract.LabelResult, error) {
	return contract.LabelResult{}, nil
}
func (stubGateway) SubmissionStatus(context.Context, shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	return contract.SubmissionSuccess, nil
}
func (stubGateway) SubmitShipmentConfirmation(context.Context, shared.NetworkRef) error {
	return nil
}

type stubPlanner struct{ feasible bool }

func (p *stubPlanner) RaiseHeldOrder(context.Context, contract.HeldOrderRequest) (contract.HeldOrderResult, error) {
	return contract.HeldOrderResult{LocalOrderId: "ord-1", Feasible: p.feasible}, nil
}
func (p *stubPlanner) ReleaseHeldOrder(context.Context, shared.LocalOrderId) error { return nil }
func (p *stubPlanner) CancelHeldOrder(context.Context, shared.LocalOrderId) error  { return nil }

type passTranslation struct{}

func (passTranslation) ToSKU(_ context.Context, id shared.NetworkProductId) (shared.SKU, error) {
	return shared.SKU("sku-" + string(id)), nil
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func demand(ref shared.NetworkRef) contract.InboundDemand {
	return contract.InboundDemand{
		NetworkRef:     ref,
		SiteId:         "site-1",
		RequiredShipBy: time.Now().UTC().Add(48 * time.Hour),
		Lines: []contract.InboundLine{
			{NetworkLineRef: "a", NetworkProductId: "ASIN-1", Quantity: 1},
		},
	}
}
