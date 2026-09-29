package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// answerNetworkDemandSOP is the operational standard-operating-procedure
// the answer_network_demand prompt hands to the model.
const answerNetworkDemandSOP = `You are inspecting network-fulfillment's record of demand received from an external retail network and the answers this platform gave it. Use only the MCP tools; never assume state. This context is READ-ONLY — demand enters it only by polling the network, never through this surface.

Procedure:
1. To answer "what happened to purchase order X", call get_network_order with that network reference (networkRef). It returns the order's state (NEW/ACKNOWLEDGED/REJECTED/CONFIRMED), both vocabularies for each line (the network's product id and the SKU this platform resolved it to), the acknowledgement deadline, and whether that deadline has already passed unanswered (acknowledgementOverdue).
2. To answer "how much demand is currently unanswered" or "what has this context decided lately", call list_network_orders, optionally with state set to NEW (the unanswered working set the acknowledgement sweep acts on), ACKNOWLEDGED, REJECTED, or CONFIRMED. Omit state to see everything.
3. To answer a trend question — "how many orders are we rejecting for untranslatable products this week", "is our average acknowledgement latency creeping up" — call get_acknowledgement_report with a from/to RFC3339 window. It returns day-bucketed counts of orders received, acknowledged, and rejected (split into untranslatable-SKU catalogue gaps vs. genuine domain refusals vs. missed deadlines), plus average acknowledgement latency in seconds.

Interpretation:
- acknowledgementOverdue=true on an order in NEW means its 24-hour window has already closed with no answer ever sent — this is the sweep's own definition of overdue, not a local approximation.
- A rejection with reason UNTRANSLATABLE_SKU is a catalogue gap (no SKU mapping existed for a product the network sent); INFEASIBLE_DEADLINE is a genuine capacity refusal from order-management's promise policy; ACKNOWLEDGEMENT_DEADLINE_MISSED is an operational failure (the window closed unanswered).
- This surface never carries customer PII (ship-to name, address, phone): those live in a different bounded context entirely and are out of scope for every tool here.

Done means: you have named the specific network reference(s), state(s), or report window that answers the question, each justified from tool output. Do not attempt to change anything; this service exposes no write tool.`

// registerPrompts adds the workflow prompts (operational SOPs).
func (d Deps) registerPrompts(server *mcp.Server) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "answer_network_demand",
		Description: "Standard operating procedure for inspecting network demand and this context's answers to it — one order, the unanswered/answered working sets, or a day-bucketed acknowledgement trend — using the read tools.",
	}, func(ctx context.Context, _ *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{
			Description: "How to inspect network demand: one order by reference, orders filtered by state, or the acknowledgement & translation trend report — read-only.",
			Messages: []*mcp.PromptMessage{{
				Role:    "user",
				Content: &mcp.TextContent{Text: answerNetworkDemandSOP},
			}},
		}, nil
	})
}
