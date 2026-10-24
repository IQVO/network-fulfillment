package telemetry

import (
	"context"
	"maps"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// collected is what a manual reader saw: the counter's data points by outcome, the
// gauge's by site, and each instrument's unit.
type collected struct {
	counts, revisions map[string]int64
	units             map[string]string
	monotonic         bool
}

func collect(ctx context.Context, t *testing.T, reader sdkmetric.Reader) collected {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := collected{counts: map[string]int64{}, revisions: map[string]int64{}, units: map[string]string{}}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			out.units[md.Name] = md.Unit
			switch data := md.Data.(type) {
			case metricdata.Sum[int64]:
				out.monotonic = data.IsMonotonic
				for _, dp := range data.DataPoints {
					out.counts[attrValue(dp.Attributes, "outcome")] = dp.Value
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					out.revisions[attrValue(dp.Attributes, "site")] = dp.Value
				}
			}
		}
	}
	return out
}

func attrValue(set attribute.Set, key string) string {
	v, _ := set.Value(attribute.Key(key))
	return v.AsString()
}

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

	got := collect(ctx, t, reader)

	wantCounts := map[string]int64{"confirmed": 2, "unchanged": 1, "skipped_no_node": 1, "skipped_incomplete": 1}
	if !maps.Equal(got.counts, wantCounts) {
		t.Errorf("outcomes = %v, want %v", got.counts, wantCounts)
	}
	if !got.monotonic {
		t.Error("the declarations instrument must be a counter")
	}
	// A skipped site has no revision to report.
	if want := map[string]int64{"FC-1": 2}; !maps.Equal(got.revisions, want) {
		t.Errorf("revision gauge = %v, want %v", got.revisions, want)
	}
	wantUnits := map[string]string{
		"network_fulfillment.node_profile.declarations": "{declaration}",
		"network_fulfillment.node_profile.revision":     "{revision}",
	}
	if !maps.Equal(got.units, wantUnits) {
		t.Errorf("units = %v, want %v", got.units, wantUnits)
	}
}

func TestNodeProfileMetrics_ANilMeterUsesTheGlobalProviderWithoutFailing(t *testing.T) {
	m := NewNodeProfileMetrics(nil)
	m.Declared(context.Background(), "FC-1", contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: 1})
}
