package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inbounda2a "github.com/claudioed/network-fulfillment/internal/adapters/inbound/a2a"
)

const testAgentURL = "http://network-fulfillment-a2a.warehouse-systems.svc.cluster.local:8093/"

// newTestRouter builds the router exactly as run() does: the real executor
// (empty deps are fine - no skill that touches a store is invoked here) and the
// real Agent Card.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(inbounda2a.Deps{})), card, "network-fulfillment-a2a")
}

func TestRouter_HealthzServesOK(t *testing.T) {
	h := newTestRouter(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != `{"status":"ok"}` {
		t.Fatalf("GET /healthz body = %q, want {\"status\":\"ok\"}", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("GET /healthz Content-Type = %q, want application/json", ct)
	}
}

func TestRouter_ServesTheAgentCardAtTheWellKnownPath(t *testing.T) {
	h := newTestRouter(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET agent card: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{`"name":"network-fulfillment"`, testAgentURL, `"network_demand_status"`, `"advertised_availability"`} {
		if !strings.Contains(body, want) {
			t.Errorf("agent card lacks %s: %s", want, body)
		}
	}
}

// TestRouter_BothMountsReachTheJSONRPCHandler pins the fleet's
// <CTX>_A2A_ENDPOINT convention: the endpoint answers at "/" and at "/a2a".
func TestRouter_BothMountsReachTheJSONRPCHandler(t *testing.T) {
	h := newTestRouter(t)
	for _, path := range []string{"/", "/a2a"} {
		t.Run(path, func(t *testing.T) {
			// An unknown skill is answered in-band before any store is read, so
			// a JSON-RPC result proves the request reached the SDK handler at
			// this mount point.
			body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"data":{"skill":"launch_rocket","arguments":{}}}]}}}`
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("A2A-Version", "1.0")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			raw, _ := io.ReadAll(rec.Body)
			if rec.Code == http.StatusNotFound || !strings.Contains(string(raw), `"jsonrpc"`) {
				t.Fatalf("POST %s: %d %q does not look like a JSON-RPC reply", path, rec.Code, raw)
			}
			if !strings.Contains(string(raw), "unknown-skill") {
				t.Fatalf("POST %s: reply %q lacks the in-band unknown-skill problem", path, raw)
			}
		})
	}
}
