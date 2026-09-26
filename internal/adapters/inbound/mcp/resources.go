package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// networkOrderURIScheme is the scheme+authority prefix of the
// network-order resource URI. A concrete resource URI is
// networkOrderURIScheme + "<networkRef>".
const networkOrderURIScheme = "network-order://network-fulfillment/"

// registerResources adds the read-model resource: what this context
// recorded for one unit of network demand, and what it told the network
// about it, addressed by the network's own reference.
func (d Deps) registerResources(server *mcp.Server) {
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: networkOrderURIScheme + "{networkRef}",
		Name:        "network order",
		Description: "One network order — what we recorded for a unit of demand and the answer we gave it — addressed by the network's own reference, e.g. network-order://network-fulfillment/po-4711.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri := req.Params.URI
		ref, ok := strings.CutPrefix(uri, networkOrderURIScheme)
		if !ok || ref == "" {
			return nil, fmt.Errorf("resource %q is not a valid network-order URI", uri)
		}
		o, err := d.Orders.FindByRef(ctx, shared.NetworkRef(ref))
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, fmt.Errorf("no network order for ref %q", ref)
		}
		body, err := json.Marshal(toNetworkOrderDTO(o, d.Clock.Now()))
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      uri,
				MIMEType: "application/json",
				Text:     string(body),
			}},
		}, nil
	})
}
