package analyticsstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

func TestMemoryStore_ProjectsAndDedupes(t *testing.T) {
	base := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	s := analyticsstore.NewMemoryStore()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	must(s.ApplyNetworkOrderReceived(ctx, "e1", base))
	must(s.ApplyNetworkOrderReceived(ctx, "e2", base))
	must(s.ApplyNetworkOrderAcknowledged(ctx, "e3", base, 60))
	// duplicate event id -> counts once
	must(s.ApplyNetworkOrderAcknowledged(ctx, "e3", base, 60))
	must(s.ApplyNetworkOrderRejected(ctx, "e4", base, "UNTRANSLATABLE_SKU"))
	must(s.ApplyNetworkOrderRejected(ctx, "e5", base, "INFEASIBLE_DEADLINE"))
	must(s.ApplyNetworkOrderRejected(ctx, "e6", base, "ACKNOWLEDGEMENT_DEADLINE_MISSED"))

	rep, err := s.Query(ctx, report.ReportQuery{
		From:        base.Add(-24 * time.Hour),
		To:          base.Add(24 * time.Hour),
		Granularity: report.GranularityDay,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.OrdersReceived != 2 {
		t.Errorf("OrdersReceived = %d, want 2", row.OrdersReceived)
	}
	if row.OrdersAcknowledged != 1 {
		t.Errorf("OrdersAcknowledged = %d, want 1 (deduped)", row.OrdersAcknowledged)
	}
	if row.AvgAcknowledgementLatencySeconds() != 60 {
		t.Errorf("AvgAcknowledgementLatencySeconds() = %v, want 60", row.AvgAcknowledgementLatencySeconds())
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
}

func TestMemoryStore_FreshnessLag(t *testing.T) {
	ctx := context.Background()

	t.Run("empty store is zero lag", func(t *testing.T) {
		s := analyticsstore.NewMemoryStore()
		lag, err := s.FreshnessLag(ctx)
		if err != nil {
			t.Fatalf("FreshnessLag: %v", err)
		}
		if lag != 0 {
			t.Errorf("lag = %v, want 0", lag)
		}
	})

	t.Run("lag from latest applied event", func(t *testing.T) {
		latest := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
		now := latest.Add(90 * time.Second)
		s := analyticsstore.NewMemoryStore()
		s.Now = func() time.Time { return now }
		if err := s.ApplyNetworkOrderReceived(ctx, "e1", latest); err != nil {
			t.Fatalf("apply: %v", err)
		}
		lag, err := s.FreshnessLag(ctx)
		if err != nil {
			t.Fatalf("FreshnessLag: %v", err)
		}
		if lag != 90*time.Second {
			t.Errorf("lag = %v, want 90s", lag)
		}
	})

	t.Run("future-dated event clamps to zero", func(t *testing.T) {
		latest := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
		now := latest.Add(-90 * time.Second)
		s := analyticsstore.NewMemoryStore()
		s.Now = func() time.Time { return now }
		if err := s.ApplyNetworkOrderReceived(ctx, "e1", latest); err != nil {
			t.Fatalf("apply: %v", err)
		}
		lag, err := s.FreshnessLag(ctx)
		if err != nil {
			t.Fatalf("FreshnessLag: %v", err)
		}
		if lag != 0 {
			t.Errorf("lag = %v, want 0 (clamped)", lag)
		}
	})
}

func TestMemoryStore_QueryFiltersWindow(t *testing.T) {
	base := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	s := analyticsstore.NewMemoryStore()
	if err := s.ApplyNetworkOrderReceived(ctx, "e1", base); err != nil {
		t.Fatalf("apply: %v", err)
	}

	rep, err := s.Query(ctx, report.ReportQuery{
		From: base.Add(30 * 24 * time.Hour), To: base.Add(60 * 24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 (window excludes the bucket)", len(rep.Rows))
	}
}
