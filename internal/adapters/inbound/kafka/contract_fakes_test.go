package kafka

// In-package copies of the projection/processed fakes from
// analytics_consumer_test.go (package kafka_test): the contract tests
// reflect and drive this package's own AnalyticsConsumer, so they must be
// in-package (package kafka) and cannot see the _test package's helpers.
// The two test packages are compiled separately; duplicate names are legal.

import (
	"context"
	"time"
)

// contractCall captures one projection-store method invocation.
type contractCall struct {
	method  string
	eventId string
	at      time.Time
	latency float64
	reason  string
}

// contractFakeProjection records the calls the consumer makes so a test can
// assert the envelope was routed to the right method.
type contractFakeProjection struct {
	calls []contractCall
}

func (f *contractFakeProjection) ApplyNetworkOrderReceived(_ context.Context, eventId string, at time.Time) error {
	f.calls = append(f.calls, contractCall{method: "received", eventId: eventId, at: at})
	return nil
}

func (f *contractFakeProjection) ApplyNetworkOrderAcknowledged(_ context.Context, eventId string, at time.Time, latencySeconds float64) error {
	f.calls = append(f.calls, contractCall{method: "acknowledged", eventId: eventId, at: at, latency: latencySeconds})
	return nil
}

func (f *contractFakeProjection) ApplyNetworkOrderRejected(_ context.Context, eventId string, at time.Time, reason string) error {
	f.calls = append(f.calls, contractCall{method: "rejected", eventId: eventId, at: at, reason: reason})
	return nil
}

// contractFakeProcessed is an in-memory ProcessedEvents.
type contractFakeProcessed struct {
	seen map[string]bool
}

func newContractFakeProcessed() *contractFakeProcessed {
	return &contractFakeProcessed{seen: map[string]bool{}}
}

func (p *contractFakeProcessed) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if p.seen[eventId] {
		return false, nil
	}
	p.seen[eventId] = true
	return true, nil
}
