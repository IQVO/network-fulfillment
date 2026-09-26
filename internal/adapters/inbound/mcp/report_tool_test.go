package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	inboundmcp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/mcp"
)

func TestReportsRESTClient_GetAcknowledgementReport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/reports/acknowledgement" {
			t.Errorf("path = %s, want /reports/acknowledgement", r.URL.Path)
		}
		if got := r.URL.Query().Get("from"); got != "2026-09-01T00:00:00Z" {
			t.Errorf("from = %s", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(inboundmcp.AcknowledgementReportView{
			Rows: []inboundmcp.AcknowledgementRowView{
				{DayBucket: "2026-09-01", OrdersReceived: 10, OrdersAcknowledged: 8, OrdersRejectedUntranslatableSKU: 1, OrdersRejectedDomain: 1, AcknowledgementDeadlinesMissed: 0, AvgAcknowledgementLatencySeconds: 3600},
			},
		})
	}))
	defer srv.Close()

	client := inboundmcp.NewReportsRESTClient(srv.URL, nil)
	out, err := client.GetAcknowledgementReport(context.Background(), inboundmcp.AcknowledgementReportQuery{
		From: "2026-09-01T00:00:00Z",
		To:   "2026-09-02T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("GetAcknowledgementReport: %v", err)
	}
	if len(out.Rows) != 1 || out.Rows[0].OrdersReceived != 10 {
		t.Errorf("rows = %+v", out.Rows)
	}
}

func TestReportsRESTClient_GetFreshness(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/reports/acknowledgement/freshness" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(inboundmcp.FreshnessView{LagSeconds: 4.5})
	}))
	defer srv.Close()

	client := inboundmcp.NewReportsRESTClient(srv.URL, nil)
	out, err := client.GetFreshness(context.Background())
	if err != nil {
		t.Fatalf("GetFreshness: %v", err)
	}
	if out.LagSeconds != 4.5 {
		t.Errorf("lagSeconds = %v, want 4.5", out.LagSeconds)
	}
}

func TestReportsRESTClient_NonOKStatusIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := inboundmcp.NewReportsRESTClient(srv.URL, nil)
	if _, err := client.GetFreshness(context.Background()); err == nil {
		t.Fatal("expected an error for a 500 response")
	}
}

// fakeReportsClient is a hand-rolled ReportsClient double for testing the
// tool's pure validation logic without an HTTP round trip.
type fakeReportsClient struct {
	view inboundmcp.AcknowledgementReportView
	err  error
}

func (f fakeReportsClient) GetAcknowledgementReport(context.Context, inboundmcp.AcknowledgementReportQuery) (inboundmcp.AcknowledgementReportView, error) {
	return f.view, f.err
}

func (f fakeReportsClient) GetFreshness(context.Context) (inboundmcp.FreshnessView, error) {
	return inboundmcp.FreshnessView{}, f.err
}

func TestGetAcknowledgementReportForTest_RequiresFromAndTo(t *testing.T) {
	client := fakeReportsClient{}
	if _, err := inboundmcp.GetAcknowledgementReportForTest(context.Background(), client, inboundmcp.AcknowledgementReportToolInput{}); err == nil {
		t.Fatal("expected an error when from/to are missing")
	}
}

func TestGetAcknowledgementReportForTest_NilClientIsAnError(t *testing.T) {
	if _, err := inboundmcp.GetAcknowledgementReportForTest(context.Background(), nil, inboundmcp.AcknowledgementReportToolInput{From: "2026-09-01T00:00:00Z", To: "2026-09-02T00:00:00Z"}); err == nil {
		t.Fatal("expected an error for a nil reports client")
	}
}

func TestGetAcknowledgementReportForTest_DelegatesToClient(t *testing.T) {
	client := fakeReportsClient{view: inboundmcp.AcknowledgementReportView{
		Rows: []inboundmcp.AcknowledgementRowView{{DayBucket: "2026-09-01", OrdersReceived: 5}},
	}}
	out, err := inboundmcp.GetAcknowledgementReportForTest(context.Background(), client, inboundmcp.AcknowledgementReportToolInput{
		From: "2026-09-01T00:00:00Z",
		To:   "2026-09-02T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("GetAcknowledgementReportForTest: %v", err)
	}
	if len(out.Rows) != 1 || out.Rows[0].OrdersReceived != 5 {
		t.Errorf("rows = %+v", out.Rows)
	}
}
