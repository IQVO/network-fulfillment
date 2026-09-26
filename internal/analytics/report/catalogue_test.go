package report_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// fakeStore is an in-memory implementation of both report ports used to
// exercise report derivation from a synthetic event sequence. It is a test
// double local to this package: the production stores live in the
// analyticsstore outbound adapter.
type fakeStore struct {
	seen map[string]bool
	rows map[report.RowKey]*report.Row
}

func newFakeStore() *fakeStore {
	return &fakeStore{seen: map[string]bool{}, rows: map[report.RowKey]*report.Row{}}
}

func dayBucket(t time.Time) time.Time { return t.UTC().Truncate(24 * time.Hour) }

func (s *fakeStore) dup(eventId string) bool {
	if s.seen[eventId] {
		return true
	}
	s.seen[eventId] = true
	return false
}

func (s *fakeStore) row(at time.Time) *report.Row {
	k := report.RowKey{DayBucket: dayBucket(at)}
	r, ok := s.rows[k]
	if !ok {
		r = &report.Row{Key: k}
		s.rows[k] = r
	}
	return r
}

func (s *fakeStore) ApplyNetworkOrderReceived(_ context.Context, eventId string, at time.Time) error {
	if s.dup(eventId) {
		return nil
	}
	s.row(at).OrdersReceived++
	return nil
}

func (s *fakeStore) ApplyNetworkOrderAcknowledged(_ context.Context, eventId string, at time.Time, latencySeconds float64) error {
	if s.dup(eventId) {
		return nil
	}
	r := s.row(at)
	r.OrdersAcknowledged++
	r.SumAcknowledgementLatencySeconds += latencySeconds
	r.AcknowledgementLatencyCount++
	return nil
}

func (s *fakeStore) ApplyNetworkOrderRejected(_ context.Context, eventId string, at time.Time, reason string) error {
	if s.dup(eventId) {
		return nil
	}
	r := s.row(at)
	switch reason {
	case "UNTRANSLATABLE_SKU":
		r.OrdersRejectedUntranslatableSKU++
	case "INFEASIBLE_DEADLINE":
		r.OrdersRejectedDomain++
	case "ACKNOWLEDGEMENT_DEADLINE_MISSED":
		r.AcknowledgementDeadlinesMissed++
	}
	return nil
}

func (s *fakeStore) Query(_ context.Context, q report.ReportQuery) (report.AcknowledgementReport, error) {
	out := report.AcknowledgementReport{}
	for k, r := range s.rows {
		if k.DayBucket.Before(q.From) || !k.DayBucket.Before(q.To) {
			continue
		}
		out.Rows = append(out.Rows, *r)
	}
	return out, nil
}

func (s *fakeStore) FreshnessLag(_ context.Context) (time.Duration, error) { return 0, nil }

func TestAcknowledgementReport_DerivesFromEventSequence(t *testing.T) {
	base := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	s := newFakeStore()
	ctx := context.Background()

	// One day: 4 received, 1 acknowledged (latency 3600s), 1 rejected
	// untranslatable, 1 rejected domain, 1 acknowledgement deadline missed.
	must(t, s.ApplyNetworkOrderReceived(ctx, "r1", base))
	must(t, s.ApplyNetworkOrderReceived(ctx, "r2", base))
	must(t, s.ApplyNetworkOrderReceived(ctx, "r3", base))
	must(t, s.ApplyNetworkOrderReceived(ctx, "r4", base))
	must(t, s.ApplyNetworkOrderAcknowledged(ctx, "a1", base, 3600))
	must(t, s.ApplyNetworkOrderRejected(ctx, "j1", base, "UNTRANSLATABLE_SKU"))
	must(t, s.ApplyNetworkOrderRejected(ctx, "j2", base, "INFEASIBLE_DEADLINE"))
	must(t, s.ApplyNetworkOrderRejected(ctx, "j3", base, "ACKNOWLEDGEMENT_DEADLINE_MISSED"))

	rep, err := s.Query(ctx, report.ReportQuery{
		From: base.Add(-24 * time.Hour), To: base.Add(24 * time.Hour),
		Granularity: report.GranularityDay,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	row := findRow(rep, report.RowKey{DayBucket: dayBucket(base)})
	if row == nil {
		t.Fatal("no row for the day bucket")
	}
	if row.OrdersReceived != 4 {
		t.Errorf("OrdersReceived = %d, want 4", row.OrdersReceived)
	}
	if row.OrdersAcknowledged != 1 {
		t.Errorf("OrdersAcknowledged = %d, want 1", row.OrdersAcknowledged)
	}
	if row.OrdersRejectedUntranslatableSKU != 1 {
		t.Errorf("OrdersRejectedUntranslatableSKU = %d, want 1", row.OrdersRejectedUntranslatableSKU)
	}
	if row.OrdersRejectedDomain != 1 {
		t.Errorf("OrdersRejectedDomain = %d, want 1", row.OrdersRejectedDomain)
	}
	if row.AcknowledgementDeadlinesMissed != 1 {
		t.Errorf("AcknowledgementDeadlinesMissed = %d, want 1", row.AcknowledgementDeadlinesMissed)
	}
	if got := row.AvgAcknowledgementLatencySeconds(); got != 3600 {
		t.Errorf("AvgAcknowledgementLatencySeconds() = %v, want 3600", got)
	}
}

func TestAcknowledgementReport_FiltersAndIdempotency(t *testing.T) {
	base := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()

	tests := []struct {
		name  string
		query report.ReportQuery
		want  int
	}{
		{"no filter", report.ReportQuery{From: base.Add(-24 * time.Hour), To: base.Add(24 * time.Hour), Granularity: report.GranularityDay}, 1},
		{"window excludes all", report.ReportQuery{From: base.Add(30 * 24 * time.Hour), To: base.Add(60 * 24 * time.Hour), Granularity: report.GranularityDay}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newFakeStore()
			// Apply the same event twice with the same eventId -> counts once.
			must(t, s.ApplyNetworkOrderReceived(ctx, "dup", base))
			must(t, s.ApplyNetworkOrderReceived(ctx, "dup", base))

			rep, err := s.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(rep.Rows) != tt.want {
				t.Errorf("rows = %d, want %d", len(rep.Rows), tt.want)
			}
			if tt.name == "no filter" {
				row := findRow(rep, report.RowKey{DayBucket: dayBucket(base)})
				if row == nil || row.OrdersReceived != 1 {
					t.Errorf("dedupe failed: row = %v", row)
				}
			}
		})
	}
}

// TestRow_AvgAcknowledgementLatencySeconds_ZeroWhenNothingAcknowledged
// covers the pure method directly: an empty/zero bucket must report zero,
// never NaN from a 0/0 division.
func TestRow_AvgAcknowledgementLatencySeconds_ZeroWhenNothingAcknowledged(t *testing.T) {
	var r report.Row
	if got := r.AvgAcknowledgementLatencySeconds(); got != 0 {
		t.Errorf("AvgAcknowledgementLatencySeconds() on empty row = %v, want 0", got)
	}
}

// TestRow_AvgAcknowledgementLatencySeconds_Averages covers more than one
// contributing acknowledgement: the average, not the sum, is reported.
func TestRow_AvgAcknowledgementLatencySeconds_Averages(t *testing.T) {
	r := report.Row{SumAcknowledgementLatencySeconds: 300, AcknowledgementLatencyCount: 3}
	if got := r.AvgAcknowledgementLatencySeconds(); got != 100 {
		t.Errorf("AvgAcknowledgementLatencySeconds() = %v, want 100", got)
	}
}

func findRow(rep report.AcknowledgementReport, k report.RowKey) *report.Row {
	for i := range rep.Rows {
		if rep.Rows[i].Key == k {
			return &rep.Rows[i]
		}
	}
	return nil
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
}
