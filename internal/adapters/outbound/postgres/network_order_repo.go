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

	if err := r.writeOrder(ctx, tx, o); err != nil {
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

// writeOrder performs the order row's conditional upsert and leaves the
// aggregate carrying the row's new version (RETURNING): after a
// successful Save the instance and the row agree, so a use case that
// saves the same aggregate twice — record the hold, then the answer —
// cannot trip over its own earlier write.
func (r *NetworkOrderRepo) writeOrder(ctx context.Context, tx pgx.Tx, o *networkorder.NetworkOrder) error {
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

	// The row is written at the NEXT version ($10 + 1) on both branches:
	// the update takes the row from the loaded version ($10) to loaded+1,
	// and an insert starts the row at loaded+1 — so the version RETURNed
	// always equals the aggregate's version once RestoreVersion applies it.
	// An insert that stored $10 itself would leave the row one behind the
	// in-memory aggregate, and the SAME instance's next Save would trip
	// the guard as if a concurrent writer had moved the row.
	//
	// The guard binds only aggregates that have seen a row ($11 false):
	// a writer that loaded the row can be stale, and zero rows — the
	// missing RETURNING row — tells it so. A freshly received aggregate
	// has never read storage; it is an intake write, applied exactly as
	// the pre-version upsert applied it, and the arrival-fixed columns
	// are absent from the SET list so a re-delivery still cannot move
	// the clock the first arrival started.
	var rowVersion int
	err := tx.QueryRow(ctx, `
		INSERT INTO network_orders (
			network_ref, site_id, required_ship_by, acknowledge_by,
			state, local_order_id, received_at, hold_outcome, hold_settled_at,
			aggregate_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 + 1)
		ON CONFLICT (network_ref) DO UPDATE SET
			state = EXCLUDED.state,
			local_order_id = EXCLUDED.local_order_id,
			hold_outcome = EXCLUDED.hold_outcome,
			hold_settled_at = EXCLUDED.hold_settled_at,
			aggregate_version = network_orders.aggregate_version + 1
		WHERE network_orders.aggregate_version = $10 OR NOT $11
		RETURNING aggregate_version
	`,
		string(o.NetworkRef()), string(o.SiteId()), o.RequiredShipBy(), o.AcknowledgeBy(),
		string(o.State()), localOrderId, o.ReceivedAt(), holdOutcome, holdSettledAt,
		o.Version(), o.LoadedFromStorage(),
	).Scan(&rowVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		// The WHERE rejected the update: the stored row moved on to a
		// newer version between our load and this write — a lost-update
		// window the caller closes by reloading and retrying (or, on
		// the REST surface, answering 409).
		return fmt.Errorf("save network order %q: %w", o.NetworkRef(), networkorder.ErrVersionConflict)
	}
	if err != nil {
		return err
	}
	o.RestoreVersion(rowVersion)
	return nil
}

// FindByRef returns (nil, nil) when no order has this ref — "not found"
// is the application's concern, not the repository's.
func (r *NetworkOrderRepo) FindByRef(ctx context.Context, ref shared.NetworkRef) (*networkorder.NetworkOrder, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT site_id, required_ship_by, acknowledge_by, state, local_order_id, received_at,
		       hold_outcome, hold_settled_at, aggregate_version
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

// FindByLocalOrderId returns the NetworkOrder whose demand became this
// order-management OrderId — the persisted correlation ADR 0014 named, now
// the join key of outbound-shipping's ShipmentConfirmed. (nil, nil) when
// no order of ours carries the id: a local order raised from customer
// demand is not this context's business. The link is one-to-one in
// practice (one raised local order per NetworkOrder, one NetworkOrder per
// order-management order created from network demand); the deterministic
// order-by makes the improbable duplicate impossible to observe as
// flip-flop reads.
func (r *NetworkOrderRepo) FindByLocalOrderId(ctx context.Context, localOrderId shared.LocalOrderId) (*networkorder.NetworkOrder, error) {
	var ref string
	err := r.pool.QueryRow(ctx, `
		SELECT network_ref FROM network_orders WHERE local_order_id = $1
		ORDER BY network_ref
		LIMIT 1
	`, string(localOrderId)).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Rehydrate through FindByRef so the full aggregate (lines, hold,
	// version) comes back exactly as every other reader sees it.
	return r.FindByRef(ctx, shared.NetworkRef(ref))
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
		version         int
	)
	if err := row.Scan(&siteId, &requiredShipBy, &acknowledgeBy, &stateRaw, &localOrderIdRaw, &receivedAt,
		&holdOutcomeRaw, &holdSettledAt, &version); err != nil {
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
	o.RestoreVersion(version)
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
