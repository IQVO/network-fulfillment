package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// InboundShipmentRepo is the Postgres ports.InboundShipmentRepo and
// ports.GtinCache (migration 0008, ADR 0028): the shipment <-> ASN number <->
// status table, the poll cursor per site and the GTIN -> SKU cache.
type InboundShipmentRepo struct {
	pool *pgxpool.Pool
}

var (
	_ ports.InboundShipmentRepo = (*InboundShipmentRepo)(nil)
	_ ports.GtinCache           = (*InboundShipmentRepo)(nil)
)

// NewInboundShipmentRepo builds the repo over pool.
func NewInboundShipmentRepo(pool *pgxpool.Pool) *InboundShipmentRepo {
	return &InboundShipmentRepo{pool: pool}
}

// Record stores s as PENDING; a shipment already recorded is untouched.
func (r *InboundShipmentRepo) Record(ctx context.Context, s contract.InboundShipment, asnNumber string, now time.Time) (bool, error) {
	payload, err := json.Marshal(s)
	if err != nil {
		return false, err
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO inbound_shipments (shipment_ref, asn_number, site_id, owner_ref, shipped_at, status, next_attempt_at, payload)
		VALUES ($1, $2, $3, $4, $5, 'PENDING', $6, $7)
		ON CONFLICT (shipment_ref) DO NOTHING`,
		s.Ref, asnNumber, string(s.SiteId), s.OwnerRef, s.ShippedAt, now, payload)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Due returns the PENDING shipments that may be attempted now, oldest first.
func (r *InboundShipmentRepo) Due(ctx context.Context, now time.Time, limit int) ([]contract.InboundRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT asn_number, attempts, payload FROM inbound_shipments
		WHERE status = 'PENDING' AND next_attempt_at <= $1
		ORDER BY shipped_at, shipment_ref LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contract.InboundRecord
	for rows.Next() {
		var rec contract.InboundRecord
		var payload []byte
		if err := rows.Scan(&rec.AsnNumber, &rec.Attempts, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &rec.Shipment); err != nil {
			return nil, fmt.Errorf("inbound shipment %s: undecodable payload: %w", rec.AsnNumber, err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MarkRegistered closes the intake of a shipment.
func (r *InboundShipmentRepo) MarkRegistered(ctx context.Context, ref string, now time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE inbound_shipments SET status = 'REGISTERED', registered_at = $2, last_error = '', updated_at = now()
		WHERE shipment_ref = $1 AND status = 'PENDING'`, ref, now)
	return err
}

// MarkRetry keeps the shipment PENDING with the error and the next attempt.
func (r *InboundShipmentRepo) MarkRetry(ctx context.Context, ref, lastError string, next time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE inbound_shipments SET attempts = attempts + 1, last_error = $2, next_attempt_at = $3, updated_at = now()
		WHERE shipment_ref = $1 AND status = 'PENDING'`, ref, lastError, next)
	return err
}

// MarkRejected parks the shipment.
func (r *InboundShipmentRepo) MarkRejected(ctx context.Context, ref, lastError string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE inbound_shipments SET status = 'REJECTED', attempts = attempts + 1, last_error = $2, updated_at = now()
		WHERE shipment_ref = $1 AND status = 'PENDING'`, ref, lastError)
	return err
}

// Cursor returns the site's poll cursor, zero when there is none.
func (r *InboundShipmentRepo) Cursor(ctx context.Context, site shared.SiteId) (time.Time, error) {
	var at time.Time
	err := r.pool.QueryRow(ctx, `SELECT shipped_after FROM inbound_poll_cursors WHERE site_id = $1`, string(site)).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	return at.UTC(), err
}

// AdvanceCursor moves the cursor forward only.
func (r *InboundShipmentRepo) AdvanceCursor(ctx context.Context, site shared.SiteId, to time.Time) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO inbound_poll_cursors (site_id, shipped_after) VALUES ($1, $2)
		ON CONFLICT (site_id) DO UPDATE SET shipped_after = GREATEST(inbound_poll_cursors.shipped_after, EXCLUDED.shipped_after), updated_at = now()`,
		string(site), to)
	return err
}

// CachedGtin returns the stored answer for gtin.
func (r *InboundShipmentRepo) CachedGtin(ctx context.Context, gtin string) (contract.GtinResolution, bool, error) {
	res := contract.GtinResolution{GTIN: gtin}
	err := r.pool.QueryRow(ctx, `SELECT sku, level, units_per_pack, active FROM gtin_sku_cache WHERE gtin = $1`, gtin).
		Scan(&res.SKU, &res.Level, &res.UnitsPerPack, &res.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return contract.GtinResolution{}, false, nil
	}
	return res, err == nil, err
}

// CacheGtin stores an answer; a GTIN identifies exactly one (sku, level), so only
// the activity flag can change on a later answer.
func (r *InboundShipmentRepo) CacheGtin(ctx context.Context, res contract.GtinResolution) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO gtin_sku_cache (gtin, sku, level, units_per_pack, active) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (gtin) DO UPDATE SET active = EXCLUDED.active, resolved_at = now()`,
		res.GTIN, res.SKU, res.Level, max(res.UnitsPerPack, 1), res.Active)
	return err
}
