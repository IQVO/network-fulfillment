package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// ADR 0027 §8: a counter by outcome and a revision gauge by site.
func TestNodeProfileMetrics_CountsEachOutcomeAndTracksTheRevisionPerSite(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m := NewNodeProfileMetrics(provider.Meter("test"))
	ctx := context.Background()
	m.Declared(ctx, "FC-1", contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: 1})
	m.Declared(ctx, "FC-1", contract.ProfileResult{Outcome: contract.ProfileUnchanged, Revision: 1})
	m.Declared(ctx, "FC-1", contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: 2})
	m.Declared(ctx, "FC-2", contract.ProfileResult{Outcome: contract.ProfileSkippedNoNode})
	m.Declared(ctx, "FC-3", contract.ProfileResult{Outcome: contract.ProfileSkippedIncomplete})

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	counts, revisions := map[string]int64{}, map[string]int64{}
	units := map[string]string{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			units[md.Name] = md.Unit
			switch data := md.Data.(type) {
			case metricdata.Sum[int64]:
				if !data.IsMonotonic {
					t.Fatal("the declarations instrument must be a counter")
				}
				for _, dp := range data.DataPoints {
					v, _ := dp.Attributes.Value(attribute.Key("outcome"))
					counts[v.AsString()] = dp.Value
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					v, _ := dp.Attributes.Value(attribute.Key("site"))
					revisions[v.AsString()] = dp.Value
				}
			}
		}
	}

	wantCounts := map[string]int64{"confirmed": 2, "unchanged": 1, "skipped_no_node": 1, "skipped_incomplete": 1}
	if len(counts) != len(wantCounts) {
		t.Fatalf("outcomes = %v, want %v", counts, wantCounts)
	}
	for k, v := range wantCounts {
		if counts[k] != v {
			t.Errorf("outcome %q = %d, want %d (all: %v)", k, counts[k], v, counts)
		}
	}
	if len(revisions) != 1 || revisions["FC-1"] != 2 {
		t.Errorf("revision gauge = %v, want only FC-1 at 2 (a skipped site has no revision to report)", revisions)
	}
	if units["network_fulfillment.node_profile.declarations"] != "{declaration}" || units["network_fulfillment.node_profile.revision"] != "{revision}" {
		t.Errorf("units = %v", units)
	}
}

func TestNodeProfileMetrics_ANilMeterUsesTheGlobalProviderWithoutFailing(t *testing.T) {
	m := NewNodeProfileMetrics(nil)
	m.Declared(context.Background(), "FC-1", contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: 1})
}
