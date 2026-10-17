package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// orphanHoldInstrument is the Tier 2 business counter of the orphaned-hold
// sweep (ADR 0024 §7; the naming and unit follow wes-work-planning ADR 0013,
// adopted here by ADR 0020). One data point per outcome.
const orphanHoldInstrument = "network_fulfillment.orphaned_holds.swept"

// OrphanHoldMetrics implements ports.OrphanHoldMetrics on an OTel counter.
type OrphanHoldMetrics struct {
	swept metric.Int64Counter
}

// NewOrphanHoldMetrics builds the counter on meter. A nil meter uses the global
// MeterProvider that Setup installs; before Setup (or in a test that never
// calls it) that is OTel's no-op provider, so the sweep counts nothing instead
// of failing. Instrument creation can only fail on an invalid name, which the
// constant is not, so there is no error to propagate: a counter that cannot be
// built degrades to a no-op rather than stopping a hold from being freed.
func NewOrphanHoldMetrics(meter metric.Meter) *OrphanHoldMetrics {
	if meter == nil {
		meter = otel.Meter("network-fulfillment")
	}
	c, err := meter.Int64Counter(orphanHoldInstrument,
		metric.WithUnit("{hold}"),
		metric.WithDescription("Orphaned order-management holds the sweep acted on, by outcome (cancelled, already_released, failed)."),
	)
	if err != nil {
		c, _ = noop.NewMeterProvider().Meter("network-fulfillment").Int64Counter(orphanHoldInstrument)
	}
	return &OrphanHoldMetrics{swept: c}
}

// Swept records one hold's outcome.
func (m *OrphanHoldMetrics) Swept(ctx context.Context, outcome contract.HoldSweepOutcome) {
	m.swept.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", string(outcome))))
}
