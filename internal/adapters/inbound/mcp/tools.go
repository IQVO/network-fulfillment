package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Deps is everything the MCP tools need, injected by the composition
// root. network-fulfillment's REST surface reads its repository
// DIRECTLY rather than through a per-read use case (see
// internal/adapters/inbound/http/server.go's own Server: "every handler
// here is a pure projection of stored state with no orchestration"), and
// this adapter follows the SAME convention so the two read surfaces
// cannot drift onto two different query paths.
//
// This context is read-only to the rest of the system (ADR 0001 §5), so
// every dependency here is a read dependency. There is no write use case
// and no write tool.
type Deps struct {
	// Orders is the same ports.NetworkOrderRepo the HTTP adapter reads,
	// backing get_network_order and list_network_orders.
	Orders ports.NetworkOrderRepo
	// Clock supplies "now" for AcknowledgementOverdue, matching the
	// domain predicate the sweep itself uses (never a local comparison
	// that could drift from it).
	Clock ports.Clock
	// Reports is the client of the netfulfil-reports REST service,
	// backing the curated get_acknowledgement_report tool. When nil,
	// that tool is not registered (an MCP deployment without the
	// reports service).
	Reports ReportsClient
}

// --- get_network_order ----------------------------------------------------

type getNetworkOrderInput struct {
	NetworkRef string `json:"networkRef" jsonschema:"the network's own reference for the demand, e.g. po-4711"`
}

func (d Deps) getNetworkOrder(ctx context.Context, in getNetworkOrderInput) (networkOrderDTO, error) {
	if in.NetworkRef == "" {
		return networkOrderDTO{}, fmt.Errorf("networkRef is required")
	}
	o, err := d.Orders.FindByRef(ctx, shared.NetworkRef(in.NetworkRef))
	if err != nil {
		return networkOrderDTO{}, err
	}
	if o == nil {
		return networkOrderDTO{}, usecases.ErrOrderNotFound
	}
	return toNetworkOrderDTO(o, d.Clock.Now()), nil
}

// --- list_network_orders ---------------------------------------------------

type listNetworkOrdersInput struct {
	// State optionally filters the listing. Empty lists every order
	// regardless of state. A value that is not one of NEW, ACKNOWLEDGED,
	// REJECTED, CONFIRMED is rejected rather than silently matching
	// nothing.
	State string `json:"state,omitempty" jsonschema:"optional state filter: NEW, ACKNOWLEDGED, REJECTED, or CONFIRMED; omit to list every order"`
}

type listNetworkOrdersOutput struct {
	NetworkOrders []networkOrderDTO `json:"networkOrders"`
}

func (d Deps) listNetworkOrders(ctx context.Context, in listNetworkOrdersInput) (listNetworkOrdersOutput, error) {
	var state networkorder.State
	if in.State != "" {
		state = networkorder.State(in.State)
		switch state {
		case networkorder.StateNew, networkorder.StateAcknowledged, networkorder.StateRejected, networkorder.StateConfirmed:
		default:
			return listNetworkOrdersOutput{}, fmt.Errorf("unknown state %q: want one of NEW, ACKNOWLEDGED, REJECTED, CONFIRMED", in.State)
		}
	}

	orders, err := d.Orders.ListAll(ctx)
	if err != nil {
		return listNetworkOrdersOutput{}, err
	}

	now := d.Clock.Now()
	out := listNetworkOrdersOutput{NetworkOrders: make([]networkOrderDTO, 0, len(orders))}
	for _, o := range orders {
		if state != "" && o.State() != state {
			continue
		}
		out.NetworkOrders = append(out.NetworkOrders, toNetworkOrderDTO(o, now))
	}
	return out, nil
}

// --- registration -----------------------------------------------------------

// registerTools adds every tool to the server.
//
// network-fulfillment is a read-only Anti-Corruption Layer: every
// registered tool is a read tool (ReadOnlyHint: true). No write tool is
// registered — demand enters this context only by polling the network
// (ADR 0001 §5), never through this surface.
func (d Deps) registerTools(server *mcp.Server) {
	readOnly := true

	addTool(server, &mcp.Tool{
		Name:        "get_network_order",
		Description: "Get one network order by its network reference (the external network's own purchase-order number, e.g. po-4711). Returns both vocabularies for each line — the network's product id and the SKU this platform resolved it to — plus the current state, the acknowledgement deadline, and whether that deadline has already passed unanswered. Never carries customer PII: ship-to name/address/phone are out of scope for this context.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getNetworkOrder)

	addTool(server, &mcp.Tool{
		Name:        "list_network_orders",
		Description: "List network orders, optionally filtered by state (NEW, ACKNOWLEDGED, REJECTED, or CONFIRMED). Omit state to list every order. NEW is the unanswered working set the acknowledgement sweep acts on; use this to see how much demand is currently awaiting an answer, or to audit what has already been acknowledged, rejected, or confirmed shipped.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.listNetworkOrders)

	// Curated read-only data-product tool, registered only when the
	// reports client is configured.
	d.registerReportTool(server)
}

// addTool registers one tool. network-fulfillment has no OTel package
// (unlike facility-layout's addTool, which wraps every call in a trace
// span), so this is a direct registration with no cross-cutting
// instrumentation to add.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		var zero Out
		out, err := handle(ctx, in)
		if err != nil {
			return nil, zero, err
		}
		return nil, out, nil
	})
}
