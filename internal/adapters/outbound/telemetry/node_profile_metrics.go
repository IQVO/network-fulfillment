package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The node profile instruments (ADR 0027 §8; naming and units follow
// wes-work-planning ADR 0013, adopted here by ADR 0020).
const (
	// nodeProfileDeclarations counts one data point per outcome of a site's
	// declaration attempt.
	nodeProfileDeclarations = "network_fulfillment.node_profile.declarations"
	// nodeProfileRevision is the revision this service last worked on, per site.
	nodeProfileRevision = "network_fulfillment.node_profile.revision"
)

// NodeProfileMetrics implements ports.ProfileMetrics on an OTel counter and gauge.
type NodeProfileMetrics struct {
	declarations metric.Int64Counter
	revision     metric.Int64Gauge
}

// NewNodeProfileMetrics builds the instruments on meter. A nil meter uses the
// global MeterProvider that Setup installs (OTel's no-op one before Setup), so a
// declaration is never stopped by telemetry: an instrument that cannot be built
// degrades to a no-op.
func NewNodeProfileMetrics(meter metric.Meter) *NodeProfileMetrics {
	if meter == nil {
		meter = otel.Meter("network-fulfillment")
	}
	fallback := noop.NewMeterProvider().Meter("network-fulfillment")
	declarations, err := meter.Int64Counter(nodeProfileDeclarations,
		metric.WithUnit("{declaration}"),
		metric.WithDescription("Node profile declaration attempts by outcome (declared, confirmed, pending, unchanged, failed, skipped_no_node, skipped_no_schedule, skipped_incomplete)."),
	)
	if err != nil {
		declarations, _ = fallback.Int64Counter(nodeProfileDeclarations)
	}
	revision, err := meter.Int64Gauge(nodeProfileRevision,
		metric.WithUnit("{revision}"),
		metric.WithDescription("The node profile revision this service last declared or settled, per site."),
	)
	if err != nil {
		revision, _ = fallback.Int64Gauge(nodeProfileRevision)
	}
	return &NodeProfileMetrics{declarations: declarations, revision: revision}
}

// Declared records one site's attempt. The revision gauge is set only when the
// attempt concerned a revision.
func (m *NodeProfileMetrics) Declared(ctx context.Context, site shared.SiteId, result contract.ProfileResult) {
	m.declarations.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", string(result.Outcome))))
	if result.Revision > 0 {
		m.revision.Record(ctx, int64(result.Revision), metric.WithAttributes(attribute.String("site", string(site))))
	}
}
