// Package analyticsstore provides the outbound adapters that persist and
// serve network-fulfillment's "Network Order Acknowledgement &
// Translation" read model: an in-memory implementation (MemoryStore) for
// tests and local runs, and Postgres implementations (a writer projection
// and a read-only reader) for deployment. All satisfy the
// report.ProjectionStore and/or report.ReportStore ports.
package analyticsstore

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// MemoryStore is an in-memory implementation of both report.ProjectionStore
// (write) and report.ReportStore (read), backed by a map. It is idempotent
// per eventId via a seen-set, so a duplicate delivery is a no-op. It is
// safe for concurrent use.
type MemoryStore struct {
	// Now supplies the current time for FreshnessLag; defaults to
	// time.Now when nil so lag is deterministic under test.
	Now func() time.Time

	mu   sync.Mutex
	seen map[string]struct{}
	rows map[report.RowKey]*report.Row
	// latest is the OccurredAt of the most recently applied event, used
	// to compute FreshnessLag.
	latest time.Time
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		seen: map[string]struct{}{},
		rows: map[report.RowKey]*report.Row{},
	}
}

func dayBucket(t time.Time) time.Time { return t.UTC().Truncate(24 * time.Hour) }

// firstApply marks eventId as seen and reports whether this is the first
// time (so the caller should apply the effect) or a duplicate (skip). It
// also advances the freshness watermark. The caller must hold s.mu.
func (s *MemoryStore) firstApply(eventId string, at time.Time) bool {
	if _, dup := s.seen[eventId]; dup {
		return false
	}
	s.seen[eventId] = struct{}{}
	if at.After(s.latest) {
		s.latest = at
	}
	return true
}

func (s *MemoryStore) row(at time.Time) *report.Row {
	k := report.RowKey{DayBucket: dayBucket(at)}
	r, ok := s.rows[k]
	if !ok {
		r = &report.Row{Key: k}
		s.rows[k] = r
	}
	return r
}

// mutate is the shared body of every counter-only Apply* method: dedupe,
// then apply fn to the day-bucket row.
func (s *MemoryStore) mutate(eventId string, at time.Time, fn func(*report.Row)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.firstApply(eventId, at) {
		return nil
	}
	fn(s.row(at))
	return nil
}

// ApplyNetworkOrderReceived records an order intake. Idempotent on eventId.
func (s *MemoryStore) ApplyNetworkOrderReceived(_ context.Context, eventId string, at time.Time) error {
	return s.mutate(eventId, at, func(r *report.Row) { r.OrdersReceived++ })
}

// ApplyNetworkOrderAcknowledged records a commitment, contributing
// latencySeconds to the bucket's running average. Idempotent on eventId.
func (s *MemoryStore) ApplyNetworkOrderAcknowledged(_ context.Context, eventId string, at time.Time, latencySeconds float64) error {
	return s.mutate(eventId, at, func(r *report.Row) {
		r.OrdersAcknowledged++
		r.SumAcknowledgementLatencySeconds += latencySeconds
		r.AcknowledgementLatencyCount++
	})
}

// ApplyNetworkOrderRejected records a refusal, bucketed by reason.
// Idempotent on eventId.
func (s *MemoryStore) ApplyNetworkOrderRejected(_ context.Context, eventId string, at time.Time, reason string) error {
	return s.mutate(eventId, at, func(r *report.Row) {
		switch reason {
		case "UNTRANSLATABLE_SKU":
			r.OrdersRejectedUntranslatableSKU++
		case "INFEASIBLE_DEADLINE":
			r.OrdersRejectedDomain++
		case "ACKNOWLEDGEMENT_DEADLINE_MISSED":
			r.AcknowledgementDeadlinesMissed++
		}
	})
}

// Query returns the rows matching q. From is inclusive, To is exclusive,
// both compared against a row's DayBucket.
func (s *MemoryStore) Query(_ context.Context, q report.ReportQuery) (report.AcknowledgementReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := report.AcknowledgementReport{}
	for k, r := range s.rows {
		if k.DayBucket.Before(q.From) || !k.DayBucket.Before(q.To) {
			continue
		}
		row := *r
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

// FreshnessLag returns how far the read model lags real time: now minus
// the OccurredAt of the most recently applied event. Zero when nothing has
// been applied yet, and never negative (a future-dated event clamps to
// zero).
func (s *MemoryStore) FreshnessLag(_ context.Context) (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latest.IsZero() {
		return 0, nil
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	lag := now.Sub(s.latest)
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

// Compile-time assertions that MemoryStore satisfies both ports.
var (
	_ report.ProjectionStore = (*MemoryStore)(nil)
	_ report.ReportStore     = (*MemoryStore)(nil)
)
