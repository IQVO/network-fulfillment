// Package mcp is network-fulfillment's inbound Model Context Protocol
// adapter: a read-only surface over this context's own read use cases,
// following the exact structural template facility-layout's
// internal/adapters/inbound/mcp uses (server.go, tools.go, report_tool.go,
// resources.go, prompts.go), adapted to this repo's domain.
//
// Unlike facility-layout, network-fulfillment has no per-tool-call OTel
// tracing wrapper: this adapter's process (cmd/mcp) does call
// telemetry.Setup and wire otelchi HTTP RED metrics on its chi router
// (ADR 0020), but those instrument the HTTP transport layer, not
// individual MCP tool invocations — there is no tracing span per tool
// call here, unlike facility-layout's addTool.
package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer builds the MCP server for this bounded context with every
// read tool, the network-order resource, and the answer_network_demand
// prompt registered.
//
// network-fulfillment's REST surface is read-only (ADR 0001 §5: demand
// arrives by polling, never by push), and so is its MCP surface: every
// registered tool is a read tool.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "network-fulfillment-mcp", Version: "1.0.0"},
		&mcp.ServerOptions{
			Instructions: "Read-only access to network-fulfillment: demand received from the external retail network and the answer this context gave it. Get one order by its network reference, list orders (optionally filtered by state), or read the Network Order Acknowledgement & Translation analytics report. Start with the answer_network_demand prompt.",
		},
	)

	deps.registerTools(server)
	deps.registerResources(server)
	deps.registerPrompts(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server. There is
// no authentication layer in front of it: this deployable is reached only
// from inside the cluster, and the fleet's static-bearer auth rollout was
// removed fleet-wide.
func Handler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
}
