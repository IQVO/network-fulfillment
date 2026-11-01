package retailnetwork

import (
	"context"
	"fmt"
)

// The Vendor API route the gateway registers nodes on.
var pathNodes = []string{"vendor", "nodes"}

// convergeNodes takes every configured site one step closer to having a node.
// It never blocks the poll: a failure is logged and the next tick tries again.
func (g *LiveGateway) convergeNodes(ctx context.Context) {
	if len(g.sites) == 0 {
		return
	}
	nodes, err := g.client.listNodes(ctx)
	if err != nil {
		g.logger.Warn("retail-network: listing the vendor's nodes failed; node convergence skipped this tick", "err", err)
		return
	}
	listed := make(map[string]string, len(nodes))
	for _, n := range nodes {
		listed[n.VendorNodeCode] = n.NodeID
	}
	for _, site := range g.sites {
		if err := g.convergeSite(ctx, site, listed); err != nil {
			g.logger.Warn("retail-network: node convergence incomplete; the next tick retries", "site", site, "err", err)
		}
	}
}

func (g *LiveGateway) convergeSite(ctx context.Context, site string, listed map[string]string) error {
	if nodeID, ok := listed[site]; ok {
		return g.store.SaveNode(ctx, site, nodeID)
	}
	if _, mapped, err := g.store.NodeIDForSite(ctx, site); err != nil || mapped {
		return err
	}
	def, ok := g.defs[site]
	if !ok {
		g.logger.Warn("retail-network: site has no node on the network and no node definition to register one from", "site", site)
		return nil
	}
	return g.registerNode(ctx, def)
}

// registerNode registers def's site as a vendor node. The ledger row, with the
// exact body, is written before the call; the same key and body are what any
// retry sends, so the network never creates the node twice.
func (g *LiveGateway) registerNode(ctx context.Context, def NodeDefinition) error {
	body, err := buildNodeRegistrationBody(g.client.vendorID, def)
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindNodeRegistration, Subject: def.SiteCode, IdempotencyKey: idempotencyKey("node", def.SiteCode), Body: body,
	})
	if err != nil {
		return err
	}
	if row, err = g.send(ctx, row, "registerVendorNode", pathNodes); err != nil {
		return err
	}
	return g.settleRegistration(ctx, row)
}

// settleRegistration reads the registration's transaction and, once it
// succeeded, stores the nodeId it carries. A PROCESSING transaction is left for
// the next tick.
func (g *LiveGateway) settleRegistration(ctx context.Context, row Submission) error {
	if row.TransactionID == "" {
		return nil
	}
	tx, err := g.client.getTransaction(ctx, row.TransactionID)
	if err != nil {
		return err
	}
	if tx.Status == txFailure {
		errs := joinErrors(tx.Errors)
		if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusFailure, errs); err != nil {
			return err
		}
		return fmt.Errorf("retail-network: the network refused the registration of node %s: %s", row.Subject, errs)
	}
	if tx.Status != txSuccess {
		return nil
	}
	if tx.NodeID == "" {
		return fmt.Errorf("retail-network: the registration of node %s succeeded without a nodeId", row.Subject)
	}
	if err := g.store.SaveNode(ctx, row.Subject, tx.NodeID); err != nil {
		return err
	}
	return g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusSuccess, "")
}
