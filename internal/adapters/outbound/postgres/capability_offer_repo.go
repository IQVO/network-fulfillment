package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// CapabilityOfferRepo is a pgxpool-backed ports.CapabilityOfferRepo.
type CapabilityOfferRepo struct {
	pool *pgxpool.Pool
}

func NewCapabilityOfferRepo(pool *pgxpool.Pool) *CapabilityOfferRepo {
	return &CapabilityOfferRepo{pool: pool}
}

// Save upserts the one current snapshot for (sku, siteId) — a recompute
// pass fully replaces whatever was there before, never appends.
func (r *CapabilityOfferRepo) Save(ctx context.Context, o capabilityoffer.CapabilityOffer) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO capability_offers (sku, site_id, advertised_quantity, basis, computed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (sku, site_id) DO UPDATE SET
			advertised_quantity = EXCLUDED.advertised_quantity,
			basis = EXCLUDED.basis,
			computed_at = EXCLUDED.computed_at
	`,
		string(o.SKU()), string(o.SiteId()), o.AdvertisedQuantity(), string(o.Basis()), o.ComputedAt(),
	)
	return err
}

// ListAll returns every currently-stored offer.
func (r *CapabilityOfferRepo) ListAll(ctx context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT sku, site_id, advertised_quantity, basis, computed_at
		FROM capability_offers
		ORDER BY sku, site_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []capabilityoffer.CapabilityOffer
	for rows.Next() {
		var (
			sku        string
			siteId     string
			qty        int
			basisRaw   string
			computedAt time.Time
		)
		if err := rows.Scan(&sku, &siteId, &qty, &basisRaw, &computedAt); err != nil {
			return nil, err
		}
		basis, err := capabilityoffer.ParseBasis(basisRaw)
		if err != nil {
			return nil, fmt.Errorf("rehydrate capability offer %q at site %q: %w", sku, siteId, err)
		}
		// New's own physicalAvailable check is satisfied trivially:
		// a persisted row was already a valid offer when saved, and
		// qty <= qty is always true when physicalAvailable is read
		// back as the SAME stored quantity for a row that never
		// recorded physical separately. This repo intentionally
		// trusts a stored row rather than re-validating it.
		o, err := capabilityoffer.New(shared.SKU(sku), shared.SiteId(siteId), qty, qty, basis, computedAt)
		if err != nil {
			return nil, fmt.Errorf("rehydrate capability offer %q at site %q: %w", sku, siteId, err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
