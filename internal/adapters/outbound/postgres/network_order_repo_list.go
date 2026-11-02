package postgres

import (
	"context"

	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// listOrders runs a network_ref-only query and loads every referenced order
// through FindByRef. It is the one page-scan shape behind all List* methods,
// which differ only in their WHERE/ORDER BY and their candidate predicate.
//
// Refs are collected and the rows closed before any load, because each load
// runs its own queries on the same pool and holding these rows open across
// them can exhaust a small pool under concurrency. A ref whose order vanished
// between the two queries is skipped, not an error: a concurrent answer or
// settle simply removes the candidate the sweep would have skipped anyway.
// keep is then consulted for each order that did load.
func (r *NetworkOrderRepo) listOrders(ctx context.Context, query string, keep func(*networkorder.NetworkOrder) bool) ([]*networkorder.NetworkOrder, error) {
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var refs []shared.NetworkRef
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			rows.Close()
			return nil, err
		}
		refs = append(refs, shared.NetworkRef(ref))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]*networkorder.NetworkOrder, 0, len(refs))
	for _, ref := range refs {
		o, err := r.FindByRef(ctx, ref)
		if err != nil {
			return nil, err
		}
		if o == nil || !keep(o) {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

func every(*networkorder.NetworkOrder) bool { return true }

// ListUnanswered returns every order still in NEW.
//
// It queries state directly rather than filtering on the deadline, so the
// sweep — not the repository — decides what "overdue" means. Pushing the
// `acknowledge_by < now` comparison into SQL would move a domain rule
// (AcknowledgementOverdue) into the adapter and give the sweep a second,
// untested definition of the same thing.
func (r *NetworkOrderRepo) ListUnanswered(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	return r.listOrders(ctx, `
		SELECT network_ref
		FROM network_orders
		WHERE state = 'NEW'
		ORDER BY acknowledge_by
	`, every)
}

// ListSubmitted returns every order currently SUBMITTED — accepted and
// told to the network, but not yet reconciled against its
// transaction-status record. ReconcileSubmittedOrders is the one caller.
func (r *NetworkOrderRepo) ListSubmitted(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	return r.listOrders(ctx, `
		SELECT network_ref
		FROM network_orders
		WHERE state = 'SUBMITTED'
		ORDER BY received_at
	`, every)
}

// ListAll returns every order regardless of state, for the read-only MCP
// list_network_orders tool. Ordered by received_at so pagination (were
// it ever added) would be stable; today the whole set is returned.
func (r *NetworkOrderRepo) ListAll(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	return r.listOrders(ctx, `
		SELECT network_ref
		FROM network_orders
		ORDER BY received_at
	`, every)
}

// ListOpenHolds returns the candidates of the orphaned-hold sweep (ADR 0024
// §3): every order with a recorded hold that has no outcome yet, in NEW or
// REJECTED. Like ListUnanswered it narrows by state and never by the
// deadline: whether acknowledgeBy plus the grace has passed is the domain's
// rule (NetworkOrder.HoldOrphaned), not a second definition in SQL.
// Oldest acknowledgement deadline first, so a pass frees the longest-leaked
// hold first. Served by the partial index network_orders_open_holds.
func (r *NetworkOrderRepo) ListOpenHolds(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	return r.listOrders(ctx, `
		SELECT network_ref
		FROM network_orders
		WHERE local_order_id IS NOT NULL
		  AND hold_outcome IS NULL
		  AND state IN ('NEW', 'REJECTED')
		ORDER BY acknowledge_by, network_ref
	`, (*networkorder.NetworkOrder).HoldOpen)
}
