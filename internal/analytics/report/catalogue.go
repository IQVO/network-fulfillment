// Package report holds network-fulfillment's "Network Order Acknowledgement
// & Translation" read model: the shapes of the analytical report the data
// product serves, the query that selects it, and the outbound ports the
// writer and reader adapters implement. It is a read-model region that
// depends on nothing else in this module — the OLTP domain and application
// layers must not import it, and it must not import them (mirrors
// facility-layout's ADR-0010 and process-path-management's ADR 0007).
//
// Like process-path-management's catalogue, a NetworkOrder has no spatial
// scope dimension worth grouping by for this report (SiteId exists but
// this v1 report answers a network-wide operational question — is the
// acknowledgement pipeline healthy — not a per-site one), so rows are
// bucketed by DAY alone.
package report

import "time"

// Granularity is the time-bucket resolution a report is rolled up to.
type Granularity string

const (
	// GranularityDay rolls rows up into UTC day buckets (midnight UTC).
	GranularityDay Granularity = "day"
)

// RowKey identifies a single report row: the UTC day bucket it aggregates.
type RowKey struct {
	DayBucket time.Time
}

// Row is one aggregated day-bucket row of the Network Order Acknowledgement
// & Translation report.
type Row struct {
	Key RowKey

	// OrdersReceived is the number of NetworkOrderReceived events: every
	// order intake, answered or not (yet).
	OrdersReceived int
	// OrdersAcknowledged is the number of NetworkOrderAcknowledged events:
	// demand this context committed to fulfilling in full.
	OrdersAcknowledged int
	// OrdersRejectedUntranslatableSKU is the number of NetworkOrderRejected
	// events whose Reason is UNTRANSLATABLE_SKU — a catalogue gap: the
	// Anti-Corruption Layer had no mapping for a product the network sent.
	OrdersRejectedUntranslatableSKU int
	// OrdersRejectedDomain is the number of NetworkOrderRejected events
	// whose Reason is INFEASIBLE_DEADLINE — a genuine domain refusal:
	// order-management could not promise the required ship-by date.
	OrdersRejectedDomain int
	// AcknowledgementDeadlinesMissed is the number of NetworkOrderRejected
	// events whose Reason is ACKNOWLEDGEMENT_DEADLINE_MISSED — an
	// operational failure distinct from either rejection above: the 24h
	// window closed with no answer ever sent.
	AcknowledgementDeadlinesMissed int

	// sumAcknowledgementLatencySeconds and acknowledgementLatencyCount
	// back AvgAcknowledgementLatencySeconds. Kept as a running sum/count
	// rather than a stored average so idempotent re-application (or a
	// rebuild by replay) never has to "un-average" a prior value — each
	// contributing NetworkOrderAcknowledged event adds its own latency
	// once, and the average is only ever computed at read time.
	SumAcknowledgementLatencySeconds float64
	AcknowledgementLatencyCount      int
}

// AvgAcknowledgementLatencySeconds is the mean gap between a
// NetworkOrderReceived and its NetworkOrderAcknowledged, in seconds, for
// orders acknowledged in this bucket. Zero (not NaN) when nothing was
// acknowledged in the bucket.
func (r Row) AvgAcknowledgementLatencySeconds() float64 {
	if r.AcknowledgementLatencyCount == 0 {
		return 0
	}
	return r.SumAcknowledgementLatencySeconds / float64(r.AcknowledgementLatencyCount)
}

// AcknowledgementReport is the full result of a report query: the matching
// rows.
type AcknowledgementReport struct {
	Rows []Row
}

// ReportQuery selects and filters the rows a report covers. From is
// inclusive and To is exclusive, both compared against a row's DayBucket.
type ReportQuery struct {
	From        time.Time
	To          time.Time
	Granularity Granularity
}
