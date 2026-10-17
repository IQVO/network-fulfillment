package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// ADR 0024 §7: network_fulfillment.orphaned_holds.swept, unit {hold}, one
// data point per outcome.
func TestOrphanHoldMetrics_CountsEachOutcomeSeparately(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m := NewOrphanHoldMetrics(provider.Meter("test"))
	ctx := context.Background()
	m.Swept(ctx, contract.HoldSweepCancelled)
	m.Swept(ctx, contract.HoldSweepCancelled)
	m.Swept(ctx, contract.HoldSweepAlreadyReleased)
	m.Swept(ctx, contract.HoldSweepFailed)

	got, unit := collectSweptByOutcome(ctx, t, reader)
	want := map[string]int64{"cancelled": 2, "already_released": 1, "failed": 1}
	if len(got) != len(want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("outcome %q = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}
	if unit != "{hold}" {
		t.Fatalf("unit = %q, want {hold}", unit)
	}
}

// collectSweptByOutcome reads the swept counter from the reader: its data
// points keyed by the `outcome` attribute, and its unit. It fails the test when
// the instrument is not a monotonic int64 sum.
func collectSweptByOutcome(ctx context.Context, t *testing.T, reader sdkmetric.Reader) (map[string]int64, string) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := map[string]int64{}
	var unit string
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "network_fulfillment.orphaned_holds.swept" {
				continue
			}
			unit = md.Unit
			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("data is %T, want a Sum[int64] counter", md.Data)
			}
			if !sum.IsMonotonic {
				t.Fatal("the instrument must be monotonic (a counter)")
			}
			for _, dp := range sum.DataPoints {
				v, _ := dp.Attributes.Value(attribute.Key("outcome"))
				got[v.AsString()] = dp.Value
			}
		}
	}
	return got, unit
}
