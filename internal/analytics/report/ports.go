package report

import (
	"context"
	"time"
)

// ReportStore is the read side of the acknowledgement-report data product:
// the reader process queries it to serve reports.
type ReportStore interface {
	// Query returns the report rows matching q.
	Query(ctx context.Context, q ReportQuery) (AcknowledgementReport, error)
	// FreshnessLag reports how far the read model lags real time: the age
	// of the most recently applied event.
	FreshnessLag(ctx context.Context) (time.Duration, error)
}

// ProjectionStore is the write side of the acknowledgement-report data
// product: the projector process applies each consumed event to it. Every
// Apply* method is idempotent on eventId — applying the same eventId twice
// records the effect once, so the at-least-once Kafka stream can be
// projected exactly once.
type ProjectionStore interface {
	// ApplyNetworkOrderReceived records an order intake in at's day bucket.
	ApplyNetworkOrderReceived(ctx context.Context, eventId string, at time.Time) error
	// ApplyNetworkOrderAcknowledged records a commitment, contributing
	// latencySeconds (the gap between receipt and acknowledgement) to the
	// bucket's running average.
	ApplyNetworkOrderAcknowledged(ctx context.Context, eventId string, at time.Time, latencySeconds float64) error
	// ApplyNetworkOrderRejected records a refusal, bucketed by reason:
	// "UNTRANSLATABLE_SKU", "INFEASIBLE_DEADLINE", or
	// "ACKNOWLEDGEMENT_DEADLINE_MISSED". A reason outside this set is
	// recorded as a no-op counter-wise (there is nothing else to bucket
	// it into) but the event id is still claimed, so it is never retried
	// forever.
	ApplyNetworkOrderRejected(ctx context.Context, eventId string, at time.Time, reason string) error
}
