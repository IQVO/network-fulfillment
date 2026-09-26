package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"log/slog"

	"github.com/claudioed/network-fulfillment/internal/analytics/report"
)

// ReportsHandlers is the inbound HTTP adapter for network-fulfillment's
// "Network Order Acknowledgement & Translation" data product's READER. It
// depends only on the read-model port (report.ReportStore); it never
// touches the OLTP use cases, the domain, or the writer.
type ReportsHandlers struct {
	Store report.ReportStore
}

// acknowledgementRowDTO is the wire shape of one report row. It is a
// dedicated DTO so the read-model struct (report.Row) never leaks onto
// the API.
type acknowledgementRowDTO struct {
	DayBucket                        string  `json:"dayBucket"`
	OrdersReceived                   int     `json:"ordersReceived"`
	OrdersAcknowledged               int     `json:"ordersAcknowledged"`
	OrdersRejectedUntranslatableSKU  int     `json:"ordersRejectedUntranslatableSku"`
	OrdersRejectedDomain             int     `json:"ordersRejectedDomain"`
	AcknowledgementDeadlinesMissed   int     `json:"acknowledgementDeadlinesMissed"`
	AvgAcknowledgementLatencySeconds float64 `json:"avgAcknowledgementLatencySeconds"`
}

// acknowledgementReportDTO is the wire shape of the acknowledgement &
// translation report response.
type acknowledgementReportDTO struct {
	Rows []acknowledgementRowDTO `json:"rows"`
}

// freshnessDTO is the wire shape of the freshness-lag response, matching
// every sibling data product's convention exactly.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetAcknowledgementReport serves GET /reports/acknowledgement. from and
// to (RFC3339) are required; granularity is optional and, today, must be
// "day" (the only granularity this report supports).
func (h *ReportsHandlers) GetAcknowledgementReport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}

	granularity := report.GranularityDay
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityDay) {
			writeReportBadRequest(w, r, "granularity must be 'day'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toAcknowledgementReportDTO(rep))
}

func toAcknowledgementReportDTO(rep report.AcknowledgementReport) acknowledgementReportDTO {
	dto := acknowledgementReportDTO{Rows: make([]acknowledgementRowDTO, 0, len(rep.Rows))}
	for _, row := range rep.Rows {
		dto.Rows = append(dto.Rows, acknowledgementRowDTO{
			DayBucket:                        row.Key.DayBucket.UTC().Format(time.RFC3339),
			OrdersReceived:                   row.OrdersReceived,
			OrdersAcknowledged:               row.OrdersAcknowledged,
			OrdersRejectedUntranslatableSKU:  row.OrdersRejectedUntranslatableSKU,
			OrdersRejectedDomain:             row.OrdersRejectedDomain,
			AcknowledgementDeadlinesMissed:   row.AcknowledgementDeadlinesMissed,
			AvgAcknowledgementLatencySeconds: row.AvgAcknowledgementLatencySeconds(),
		})
	}
	return dto
}

// GetAcknowledgementReportFreshness serves GET
// /reports/acknowledgement/freshness.
func (h *ReportsHandlers) GetAcknowledgementReportFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetReportsHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetReportsHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400
// and returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400,
// reusing the service-wide problem writer.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest,
		problemInfo{"invalid-report-query", "The report query is malformed or missing a required parameter"},
		detail, r.URL.Path)
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError,
		problemInfo{"report-store-error", "The report could not be served"},
		err.Error(), r.URL.Path)
}

// NewReportsRouter builds the chi router for the netfulfil-reports reader
// service. A nil logger falls back to slog.Default(). The router is
// trace-free, consistent with the rest of the analytics pipeline
// (network-fulfillment has no OTel package for the analytics processes).
//
// This is a SEPARATE router from the OLTP Server.Routes() stdlib mux
// above: the reports binary is its own composition root
// (cmd/netfulfil-reports) and never shares a process with the OLTP
// server.
func NewReportsRouter(h *ReportsHandlers, logger *slog.Logger) *chi.Mux {
	if logger == nil {
		logger = slog.Default()
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.GetReportsHealthz)

	r.Get("/reports/acknowledgement", h.GetAcknowledgementReport)
	r.Get("/reports/acknowledgement/freshness", h.GetAcknowledgementReportFreshness)

	return r
}
