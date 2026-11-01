package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ListUnansweredPage returns one keyset page of the unanswered working set,
// ordered by (acknowledge_by, network_ref): rows strictly after `after`
// (Sort = acknowledgeBy in RFC 3339 nanoseconds, ID = networkRef), at most
// limit rows.
func (r *NetworkOrderRepo) ListUnansweredPage(ctx context.Context, after *contract.Keyset, limit int) ([]*networkorder.NetworkOrder, error) {
	sql := `SELECT network_ref FROM network_orders WHERE state = 'NEW'
		ORDER BY acknowledge_by, network_ref LIMIT $1`
	args := []any{limit}
	if after != nil {
		at, err := time.Parse(time.RFC3339Nano, after.Sort)
		if err != nil {
			return nil, fmt.Errorf("postgres: page position: %w", err)
		}
		sql = `SELECT network_ref FROM network_orders WHERE state = 'NEW'
			AND (acknowledge_by, network_ref) > ($2, $3)
			ORDER BY acknowledge_by, network_ref LIMIT $1`
		args = append(args, at, after.ID)
	}
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	// Collected before loading, like ListUnanswered: each load runs its own
	// queries on the same pool.
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
		if o != nil {
			out = append(out, o)
		}
	}
	return out, nil
}
