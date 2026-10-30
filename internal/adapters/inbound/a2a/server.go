// Package a2a is network-fulfillment's inbound Agent2Agent adapter: a third
// driving adapter beside REST and MCP, exposing this bounded context to the
// fleet's agent ecosystem. It is built on the official A2A Go SDK
// (a2aproject/a2a-go/v2) and speaks JSON-RPC 2.0 over HTTP, with the Agent
// Card served at the well-known path.
//
// Per ADR 0030, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state. It is not a data-sync channel -
// facts between contexts still travel exclusively as CloudEvents on Kafka
// (ADR 0008).
//
// This agent is READ-ONLY, exactly like the MCP adapter (ADR 0002): the one
// write this context has, shipment confirmation (ADR 0014), is REST only, and
// demand enters only by polling the network (ADR 0001 section 5). Delegation
// must never widen what the context exposes, so no write skill exists and Deps
// holds no use case, publisher or unit of work.
//
// Like the MCP adapter, this package depends inward on the application layer
// (ports, usecases) and the domain only - never on an outbound adapter. The
// composition root (cmd/a2a) wires concrete repositories into Deps. Domain
// structs never leak across the message boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the same read ports the MCP adapter reads (this context's REST
// and MCP reads are projections of stored state with no use case in between,
// see internal/adapters/inbound/http/server.go), so the three surfaces cannot
// drift onto different query paths. Every dependency is a read dependency; the
// architecture test TestA2AAdapterStaysReadOnly keeps it that way.
type Deps struct {
	// Orders backs network_demand_status.
	Orders ports.NetworkOrderRepo
	// Offers backs advertised_availability (ADR 0001 section 8). When nil
	// the skill answers a 503 problem instead of an empty list that would
	// read as "nothing is advertised".
	Offers ports.CapabilityOfferRepo
	// Clock supplies "now" for AcknowledgementOverdue, matching the domain
	// predicate the sweep itself uses.
	Clock ports.Clock
}

// Card builds this agent's Agent Card (ADR 0030). The surface is two curated,
// goal-shaped READ skills - not one skill per endpoint, the same curation
// discipline ADR 0002 applied to MCP tools. The card declares no security
// schemes on purpose: this surface is unauthenticated by the same fleet-wide
// decision as REST and MCP; schemes are added here, not in a redesign, if that
// decision is ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "network-fulfillment",
		Description: "Network Fulfillment agent for the Warehouse Execution System: the anti-corruption layer between the fleet and the external retail fulfillment network. Read-only. Answers where the network's demand stands (one order by its network reference, or the orders in a state, with both vocabularies per line and whether the acknowledgement deadline has passed unanswered) and what availability this context would advertise to the network (the throughput-constrained advertised quantity per SKU and site). It never changes state: shipment confirmation is REST only and demand enters only by polling the network. No customer PII exists in any answer.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/network-fulfillment",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillNetworkDemandStatus,
				Name:        "Where the network's demand stands",
				Description: "Read network orders. With networkRef (the network's own purchase-order reference, e.g. po-4711): that one order, or a not-found problem. With state (NEW, SUBMITTED, ACKNOWLEDGED, REJECTED or CONFIRMED): every order in that state; with neither: every order. NEW is the unanswered working set the acknowledgement sweep acts on, SUBMITTED an acknowledgement told to the network but not yet reconciled. networkRef and state are mutually exclusive. Each order carries both vocabularies per line (the network's product id and the resolved SKU), its state, the acknowledgement deadline and whether that deadline has passed unanswered. Never carries ship-to PII.",
				Tags:        []string{"network-fulfillment", "orders", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"network_demand_status","arguments":{"networkRef":"po-4711"}}`,
					`{"skill":"network_demand_status","arguments":{"state":"NEW"}}`,
				},
			},
			{
				ID:          skillAdvertisedAvailability,
				Name:        "Availability this context advertises to the network",
				Description: "Read the currently stored CapabilityOffers: the throughput-constrained advertised quantity this context would tell the network it can ship, per SKU and site, and whether that figure is limited by physical stock (PHYSICAL) or by the process path's remaining admission capacity before its next cutoff (THROUGHPUT_CONSTRAINED). Optionally narrow by sku and/or siteId. The offer is persisted and recomputed by the OLTP service; this skill reads the latest snapshot and computes nothing.",
				Tags:        []string{"network-fulfillment", "capability", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"advertised_availability","arguments":{}}`,
					`{"skill":"advertised_availability","arguments":{"sku":"sku-1","siteId":"site-1"}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are synchronous reads, so there is no
// long-running Task lifecycle to track. The response carries the caller's
// contextId (and taskId when the client referenced one) so a delegating saga
// can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler - the only transport this
// agent ships (ADR 0030), mirroring the MCP adapter's "Streamable HTTP only"
// minimalism.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
