package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- reports REST views (tool + client boundary) ---------------------------

// AcknowledgementRowView is one row of the acknowledgement & translation
// report as the MCP tool returns it and the reports REST client decodes
// it. Field tags match the reports service's JSON so the same struct
// round-trips both ways.
type AcknowledgementRowView struct {
	DayBucket                        string  `json:"dayBucket"`
	OrdersReceived                   int     `json:"ordersReceived"`
	OrdersAcknowledged               int     `json:"ordersAcknowledged"`
	OrdersRejectedUntranslatableSKU  int     `json:"ordersRejectedUntranslatableSku"`
	OrdersRejectedDomain             int     `json:"ordersRejectedDomain"`
	AcknowledgementDeadlinesMissed   int     `json:"acknowledgementDeadlinesMissed"`
	AvgAcknowledgementLatencySeconds float64 `json:"avgAcknowledgementLatencySeconds"`
}

// AcknowledgementReportView is the acknowledgement & translation report
// body.
type AcknowledgementReportView struct {
	Rows []AcknowledgementRowView `json:"rows"`
}

// FreshnessView is the freshness-lag body.
type FreshnessView struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// AcknowledgementReportQuery is the filter set passed to the reports REST
// client.
type AcknowledgementReportQuery struct {
	From        string
	To          string
	Granularity string
}

// ReportsClient is the narrow port the MCP report tool depends on: a
// client of the netfulfil-reports REST service. It is an interface so the
// tool can be unit-tested with a fake, and so the curated tool never
// talks to the analytical database directly — it goes through the
// reports REST surface, preserving the single read path.
type ReportsClient interface {
	GetAcknowledgementReport(ctx context.Context, q AcknowledgementReportQuery) (AcknowledgementReportView, error)
	GetFreshness(ctx context.Context) (FreshnessView, error)
}

// --- reports REST client -----------------------------------------------------

// ReportsRESTClient is the HTTP implementation of ReportsClient. Base URL
// and the *http.Client are injected so the composition root controls the
// target and timeouts, and tests can point it at an httptest server.
type ReportsRESTClient struct {
	baseURL string
	http    *http.Client
}

// NewReportsRESTClient constructs a ReportsRESTClient for the reports
// service at baseURL. A nil httpClient falls back to a client with a
// sane timeout.
func NewReportsRESTClient(baseURL string, httpClient *http.Client) *ReportsRESTClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &ReportsRESTClient{baseURL: baseURL, http: httpClient}
}

// GetAcknowledgementReport calls GET /reports/acknowledgement with q as
// the query string.
func (c *ReportsRESTClient) GetAcknowledgementReport(ctx context.Context, q AcknowledgementReportQuery) (AcknowledgementReportView, error) {
	vals := url.Values{}
	vals.Set("from", q.From)
	vals.Set("to", q.To)
	if q.Granularity != "" {
		vals.Set("granularity", q.Granularity)
	}
	var out AcknowledgementReportView
	if err := c.getJSON(ctx, "/reports/acknowledgement?"+vals.Encode(), &out); err != nil {
		return AcknowledgementReportView{}, err
	}
	return out, nil
}

// GetFreshness calls GET /reports/acknowledgement/freshness.
func (c *ReportsRESTClient) GetFreshness(ctx context.Context) (FreshnessView, error) {
	var out FreshnessView
	if err := c.getJSON(ctx, "/reports/acknowledgement/freshness", &out); err != nil {
		return FreshnessView{}, err
	}
	return out, nil
}

// getJSON performs a GET against baseURL+path and decodes a 2xx JSON body
// into out. A non-2xx response is an error.
func (c *ReportsRESTClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("reports client: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("reports client: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reports client: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("reports client: decode: %w", err)
	}
	return nil
}

// Compile-time assertion that ReportsRESTClient satisfies the port.
var _ ReportsClient = (*ReportsRESTClient)(nil)

// --- get_acknowledgement_report tool ----------------------------------------

// AcknowledgementReportToolInput is the tool's argument set (untrusted,
// from a model).
type AcknowledgementReportToolInput struct {
	From        string `json:"from" jsonschema:"start of the window, inclusive, RFC3339 (required)"`
	To          string `json:"to" jsonschema:"end of the window, exclusive, RFC3339 (required)"`
	Granularity string `json:"granularity,omitempty" jsonschema:"time bucket granularity; only 'day' is supported"`
}

// getAcknowledgementReport is the tool handler: it validates the required
// window, delegates to the reports REST client, and returns the report
// view.
func (d Deps) getAcknowledgementReport(ctx context.Context, in AcknowledgementReportToolInput) (AcknowledgementReportView, error) {
	return GetAcknowledgementReportForTest(ctx, d.Reports, in)
}

// GetAcknowledgementReportForTest is the tool's pure logic, factored out
// so it can be unit-tested with a fake ReportsClient independent of the
// MCP server wiring. It validates from/to and forwards the filters.
func GetAcknowledgementReportForTest(ctx context.Context, client ReportsClient, in AcknowledgementReportToolInput) (AcknowledgementReportView, error) {
	if client == nil {
		return AcknowledgementReportView{}, fmt.Errorf("reports client not configured")
	}
	if in.From == "" || in.To == "" {
		return AcknowledgementReportView{}, fmt.Errorf("from and to are required (RFC3339)")
	}
	return client.GetAcknowledgementReport(ctx, AcknowledgementReportQuery{
		From:        in.From,
		To:          in.To,
		Granularity: in.Granularity,
	})
}

// registerReportTool adds the curated read-only acknowledgement-report
// tool. It is registered only when a reports client is configured
// (Deps.Reports != nil), so an MCP deployment without the reports service
// simply does not expose it.
func (d Deps) registerReportTool(server *mcp.Server) {
	if d.Reports == nil {
		return
	}
	readOnly := true
	addTool(server, &mcp.Tool{
		Name:        "get_acknowledgement_report",
		Description: "Return the network-fulfillment 'Network Order Acknowledgement & Translation' report for a time window, bucketed by day: orders received, orders acknowledged, orders rejected (split by untranslatable-SKU catalogue gap vs. genuine domain refusal), acknowledgement deadlines missed, and the average acknowledgement latency in seconds. Reads via the netfulfil-reports REST service, never the analytical database directly.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getAcknowledgementReport)
}
