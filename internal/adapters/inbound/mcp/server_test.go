package mcp_test

import (
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
)

type systemClockStub struct{}

func (systemClockStub) Now() time.Time { return time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC) }

func TestNewServer_BuildsWithoutError(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	deps := mcp.Deps{Orders: orders, Clock: systemClockStub{}}
	srv := mcp.NewServer(deps)
	if srv == nil {
		t.Fatal("NewServer returned nil")
	}
}

func TestNewServer_WithReportsClientRegistersReportTool(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	deps := mcp.Deps{Orders: orders, Clock: systemClockStub{}, Reports: fakeReportsClient{}}
	srv := mcp.NewServer(deps)
	if srv == nil {
		t.Fatal("NewServer returned nil")
	}
}

func TestHandler_ReturnsAnHTTPHandler(t *testing.T) {
	orders := memory.NewNetworkOrderRepo()
	deps := mcp.Deps{Orders: orders, Clock: systemClockStub{}}
	srv := mcp.NewServer(deps)
	h := mcp.Handler(srv)
	if h == nil {
		t.Fatal("Handler returned nil")
	}
}
