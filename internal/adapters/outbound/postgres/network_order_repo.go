package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// NetworkOrderRepo is a pgxpool-backed ports.NetworkOrderRepo. The
// NetworkOrder aggregate spans two tables (network_orders +
// network_order_lines) and is always written in a single transaction: a
// line set that disagreed with its order's state would be demand we
// cannot correctly answer, which is the one thing this context exists to
// get right.
type NetworkOrderRepo struct {
	pool *pgxpool.Pool
}

// NewNetworkOrderRepo constructs a NetworkOrderRepo over pool.
func NewNetworkOrderRepo(pool *pgxpool.Pool) *NetworkOrderRepo {
	return &NetworkOrderRepo{pool: pool}
}

// Save upserts the aggregate.
//
// Only the mutable facts are updated on conflict. network_ref, site_id,
// required_ship_by, acknowledge_by and received_at are fixed at intake:
// the network set them and we may not move them. In particular
// acknowledge_by must never be rewritten by a later save, or every
// re-save would silently extend the SLA clock the sweep is measuring.
func (r *NetworkOrderRepo) Save(ctx context.Context, o *networkorder.NetworkOrder) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var localOrderId *string
	if id := o.LocalOrderId(); id != nil {
		s := string(*id)
		localOrderId = &s
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO network_orders (
			network_ref, site_id, required_ship_by, acknowledge_by,
			state, local_order_id, received_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (network_ref) DO UPDATE SET
			state = EXCLUDED.state,
			local_order_id = EXCLUDED.local_order_id
	`,
		string(o.NetworkRef()), string(o.SiteId()), o.RequiredShipBy(), o.AcknowledgeBy(),
		string(o.State()), localOrderId, o.ReceivedAt(),
	); err != nil {
		return err
	}

	// Lines are immutable once received — the network sends demand, not
	// amendments — so an upsert on the composite key is enough and there
	// is deliberately no delete-then-reinsert here. Rewriting the line
	// set on every save would risk losing a line whose translation has
	// since been removed from the dictionary, changing what we already
	// acknowledged.
	for _, l := range o.Lines() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO network_order_lines (
				network_ref, network_line_ref, network_product_id, sku, quantity
			)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (network_ref, network_line_ref) DO NOTHING
		`,
			string(o.NetworkRef()), string(l.NetworkLineRef()),
			string(l.NetworkProductId()), string(l.SKU()), l.Quantity(),
		); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// FindByRef returns (nil, nil) when no order has this ref — "not found"
// is the application's concern, not the repository's.
func (r *NetworkOrderRepo) FindByRef(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT site_id, required_ship_by, acknowledge_by, state, local_order_id, received_at
		FROM network_orders WHERE network_ref = $1
	`, string(ref))

	o, err := r.scanOrder(ctx, ref, row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// ListUnanswered returns every order still in NEW.
//
// It queries state directly rather than filtering on the deadline, so the
// sweep — not the repository — decides what "overdue" means. Pushing the
// `acknowledge_by < now` comparison into SQL would move a domain rule
// (AcknowledgementOverdue) into the adapter and give the sweep a second,
// untested definition of the same thing.
func (r *NetworkOrderRepo) ListUnanswered(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT network_ref
		FROM network_orders
		WHERE state = 'NEW'
		ORDER BY acknowledge_by
	`)
	if err != nil {
		return nil, err
	}

	// Collected before loading, because each load runs its own queries on
	// the same pool and holding these rows open across them can exhaust a
	// small pool under concurrency.
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
		// A concurrent answer between the two queries is not an error:
		// the order is simply no longer unanswered, and the sweep would
		// have skipped it anyway.
		if o == nil {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

// ListAll returns every order regardless of state, for the read-only MCP
// list_network_orders tool. Ordered by received_at so pagination (were
// it ever added) would be stable; today the whole set is returned.
func (r *NetworkOrderRepo) ListAll(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT network_ref
		FROM network_orders
		ORDER BY received_at
	`)
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
		if o == nil {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

// scanOrder rehydrates one aggregate from an order row plus its lines.
func (r *NetworkOrderRepo) scanOrder(ctx context.Context, ref shared.NetworkRef, row pgx.Row) (*networkorder.NetworkOrder, error) {
	var (
		siteId          string
		requiredShipBy  time.Time
		acknowledgeBy   time.Time
		state           string
		localOrderIdRaw *string
		receivedAt      time.Time
	)
	if err := row.Scan(&siteId, &requiredShipBy, &acknowledgeBy, &state, &localOrderIdRaw, &receivedAt); err != nil {
		return nil, err
	}

	lines, err := r.findLines(ctx, ref)
	if err != nil {
		return nil, err
	}

	var localOrderId *shared.LocalOrderId
	if localOrderIdRaw != nil {
		id := shared.LocalOrderId(*localOrderIdRaw)
		localOrderId = &id
	}

	return networkorder.Rehydrate(
		ref,
		shared.SiteId(siteId),
		requiredShipBy, acknowledgeBy, receivedAt,
		lines,
		networkorder.State(state),
		localOrderId,
	), nil
}

// findLines loads an order's lines.
//
// Returning an EMPTY slice for an order with no lines is correct and
// load-bearing, not a degenerate case: an untranslatable order
// (ReceiveUntranslatable) is lineless by construction and exists only to
// carry our refusal. This is also why the order row is read on its own
// rather than joined to its lines — a join would make that legitimate
// order indistinguishable from one that does not exist.
func (r *NetworkOrderRepo) findLines(ctx context.Context, ref shared.NetworkRef) ([]networkorder.Line, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT network_line_ref, network_product_id, sku, quantity
		FROM network_order_lines WHERE network_ref = $1
		ORDER BY network_line_ref
	`, string(ref))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lines []networkorder.Line
	for rows.Next() {
		var (
			lineRef   string
			productId string
			sku       string
			quantity  int
		)
		if err := rows.Scan(&lineRef, &productId, &sku, &quantity); err != nil {
			return nil, err
		}
		// Built through the domain constructor rather than by struct
		// literal: Line's fields are unexported, and going through
		// NewLine means a row that somehow violates an invariant surfaces
		// here instead of becoming an aggregate that cannot be trusted.
		l, err := networkorder.NewLine(
			shared.NetworkLineRef(lineRef),
			shared.NetworkProductId(productId),
			shared.SKU(sku),
			quantity,
		)
		if err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}
