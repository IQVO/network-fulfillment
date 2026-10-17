package postgres

import (
	"context"
	"errors"
	"fmt"
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
//
// Save opens its OWN transaction when called standalone, but JOINS an
// outer one when ctx already carries one (beginOrJoin) — the latter is
// what happens inside UnitOfWork.Execute, so the order row and its
// outbox row(s) commit together (transactional outbox).
func (r *NetworkOrderRepo) Save(ctx context.Context, o *networkorder.NetworkOrder) error {
	tx, commit, rollback, err := beginOrJoin(ctx, r.pool)
	if err != nil {
		return err
	}
	defer func() { _ = rollback(ctx) }()

	var localOrderId *string
	if id := o.LocalOrderId(); id != nil {
		s := string(*id)
		localOrderId = &s
	}

	// The hold outcome and its instant travel together (a CHECK in migration
	// 0005 enforces it); both are NULL while the hold is open.
	var holdOutcome *string
	var holdSettledAt *time.Time
	if outcome := o.HoldOutcome(); outcome != "" {
		s := string(outcome)
		at := o.HoldSettledAt()
		holdOutcome = &s
		holdSettledAt = &at
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO network_orders (
			network_ref, site_id, required_ship_by, acknowledge_by,
			state, local_order_id, received_at, hold_outcome, hold_settled_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (network_ref) DO UPDATE SET
			state = EXCLUDED.state,
			local_order_id = EXCLUDED.local_order_id,
			hold_outcome = EXCLUDED.hold_outcome,
			hold_settled_at = EXCLUDED.hold_settled_at
	`,
		string(o.NetworkRef()), string(o.SiteId()), o.RequiredShipBy(), o.AcknowledgeBy(),
		string(o.State()), localOrderId, o.ReceivedAt(), holdOutcome, holdSettledAt,
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

	return commit(ctx)
}

// FindByRef returns (nil, nil) when no order has this ref — "not found"
// is the application's concern, not the repository's.
func (r *NetworkOrderRepo) FindByRef(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT site_id, required_ship_by, acknowledge_by, state, local_order_id, received_at,
		       hold_outcome, hold_settled_at
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

// ListSubmitted returns every order currently SUBMITTED — accepted and
// told to the network, but not yet reconciled against its
// transaction-status record. ReconcileSubmittedOrders is the one caller.
func (r *NetworkOrderRepo) ListSubmitted(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT network_ref
		FROM network_orders
		WHERE state = 'SUBMITTED'
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

// ListOpenHolds returns the candidates of the orphaned-hold sweep (ADR 0024
// §3): every order with a recorded hold that has no outcome yet, in NEW or
// REJECTED. Like ListUnanswered it narrows by state and never by the
// deadline: whether acknowledgeBy plus the grace has passed is the domain's
// rule (NetworkOrder.HoldOrphaned), not a second definition in SQL.
// Oldest acknowledgement deadline first, so a pass frees the longest-leaked
// hold first. Served by the partial index network_orders_open_holds.
func (r *NetworkOrderRepo) ListOpenHolds(ctx context.Context) ([]*networkorder.NetworkOrder, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT network_ref
		FROM network_orders
		WHERE local_order_id IS NOT NULL
		  AND hold_outcome IS NULL
		  AND state IN ('NEW', 'REJECTED')
		ORDER BY acknowledge_by, network_ref
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
		// Settled or answered between the two queries: no longer a candidate.
		if o == nil || !o.HoldOpen() {
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
		stateRaw        string
		localOrderIdRaw *string
		receivedAt      time.Time
		holdOutcomeRaw  *string
		holdSettledAt   *time.Time
	)
	if err := row.Scan(&siteId, &requiredShipBy, &acknowledgeBy, &stateRaw, &localOrderIdRaw, &receivedAt,
		&holdOutcomeRaw, &holdSettledAt); err != nil {
		return nil, err
	}

	state, err := networkorder.ParseState(stateRaw)
	if err != nil {
		return nil, fmt.Errorf("rehydrate network order %q: %w", ref, err)
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

	o := networkorder.Rehydrate(
		ref,
		shared.SiteId(siteId),
		requiredShipBy, acknowledgeBy, receivedAt,
		lines,
		state,
		localOrderId,
	)
	if holdOutcomeRaw != nil {
		// Validated like the state: a value outside the vocabulary is a row
		// this code did not write, and must not become an aggregate.
		outcome, err := networkorder.ParseHoldOutcome(*holdOutcomeRaw)
		if err != nil {
			return nil, fmt.Errorf("rehydrate network order %q: %w", ref, err)
		}
		var settledAt time.Time
		if holdSettledAt != nil {
			settledAt = *holdSettledAt
		}
		o.RestoreHold(outcome, settledAt)
	}
	return o, nil
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
