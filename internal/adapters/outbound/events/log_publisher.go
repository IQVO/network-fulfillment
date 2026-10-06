// Package events provides outbound EventPublisher implementations. The
// interface is intentionally the shape a Kafka producer would satisfy
// (Publish(ctx, event) error), so a broker-backed publisher (see
// ../kafka) can be dropped in later without touching the application
// layer.
package events

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/eventwire"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// LogPublisher publishes domain events by logging them as JSON. It is the
// default outbound EventPublisher (EVENT_PUBLISHER unset), used for local
// development and by the unit/BDD suite so neither needs a broker.
type LogPublisher struct {
	logger *slog.Logger
}

// NewLogPublisher builds a LogPublisher writing to logger. A nil logger
// falls back to slog.Default().
func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogPublisher{logger: logger}
}

// Publish logs event as JSON. A shared.DomainEvent is logged in its
// adapter-owned wire shape (eventwire.Payload), the same as on Kafka.
func (p *LogPublisher) Publish(ctx context.Context, event any) error {
	if de, ok := event.(shared.DomainEvent); ok {
		wire, err := eventwire.Payload(de)
		if err != nil {
			return err
		}
		event = wire
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	p.logger.InfoContext(ctx, "integration event", "event", json.RawMessage(payload))
	return nil
}
