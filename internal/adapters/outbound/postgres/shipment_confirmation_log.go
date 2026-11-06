package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ShipmentConfirmationLog is a pgxpool-backed
// ports.ShipmentConfirmationLog: one row per (shipment reference, order id)
// pair the network was already told confirms that order (ADR 0014
// addendum). The table is written ONLY inside ConfirmNetworkOrderShipment's
// atomic scope, so a claimed row and a CONFIRMED order row are always
// committed together.
type ShipmentConfirmationLog struct {
	pool *pgxpool.Pool
}

// NewShipmentConfirmationLog constructs a ShipmentConfirmationLog over pool.
func NewShipmentConfirmationLog(pool *pgxpool.Pool) *ShipmentConfirmationLog {
	return &ShipmentConfirmationLog{pool: pool}
}

// Exists reports whether the pair was already confirmed.
func (l *ShipmentConfirmationLog) Exists(ctx context.Context, shipmentRef string, orderId string) (bool, error) {
	var exists bool
	err := l.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM shipment_confirmations
			WHERE shipment_ref = $1 AND order_id = $2
		)
	`, shipmentRef, orderId).Scan(&exists)
	return exists, err
}

// Record claims the pair. JOINS the caller's unit of work when ctx already
// carries one (beginOrJoin): inside ConfirmNetworkOrderShipment the claim
// commits with the CONFIRMED save and the outbox write. A duplicate claim
// is a no-op, not an error — the aggregate's version guard is the
// concurrency backstop, and the early Exists check is the fast no-op path.
func (l *ShipmentConfirmationLog) Record(ctx context.Context, shipmentRef string, orderId string, networkRef shared.NetworkRef, confirmedAt time.Time) error {
	tx, commit, rollback, err := beginOrJoin(ctx, l.pool)
	if err != nil {
		return err
	}
	defer func() { _ = rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO shipment_confirmations (
			shipment_ref, order_id, network_ref, confirmed_at
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (shipment_ref, order_id) DO NOTHING
	`, shipmentRef, orderId, string(networkRef), confirmedAt); err != nil {
		return err
	}
	return commit(ctx)
}
