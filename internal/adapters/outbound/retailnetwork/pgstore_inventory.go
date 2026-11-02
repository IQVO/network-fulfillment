package retailnetwork

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *PGStore) InventoryFeedState(ctx context.Context, siteCode string, part int) (InventoryFeedState, bool, error) {
	st := InventoryFeedState{SiteCode: siteCode, Part: part}
	err := s.pool.QueryRow(ctx, `
		SELECT subject, fingerprint, as_of
		FROM network_inventory_feeds WHERE site_code = $1 AND part = $2`, siteCode, part).
		Scan(&st.Subject, &st.Fingerprint, &st.AsOf)
	if errors.Is(err, pgx.ErrNoRows) {
		return InventoryFeedState{}, false, nil
	}
	if err != nil {
		return InventoryFeedState{}, false, err
	}
	st.AsOf = st.AsOf.UTC()
	return st, true, nil
}

// SaveInventoryFeedState is one guarded upsert: a part never goes back to an
// earlier AsOf, so a slow writer cannot overwrite what a newer pass recorded.
func (s *PGStore) SaveInventoryFeedState(ctx context.Context, st InventoryFeedState) error {
	if st.Part < 1 || st.Subject == "" || st.Fingerprint == "" || st.AsOf.IsZero() {
		return fmt.Errorf("retailnetwork: an inventory feed state needs a part of at least 1, a subject, a fingerprint and an asOf (got part %d)", st.Part)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO network_inventory_feeds (site_code, part, subject, fingerprint, as_of)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (site_code, part) DO UPDATE
		SET subject = EXCLUDED.subject, fingerprint = EXCLUDED.fingerprint, as_of = EXCLUDED.as_of, updated_at = now()
		WHERE network_inventory_feeds.as_of <= EXCLUDED.as_of`,
		st.SiteCode, st.Part, st.Subject, st.Fingerprint, st.AsOf.UTC())
	return mapForeignKeyViolation(err)
}

func (s *PGStore) RejectedListings(ctx context.Context, siteCode string) ([]RejectedListing, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT listing_id, reason, rejected_at
		FROM network_rejected_listings WHERE site_code = $1 ORDER BY listing_id`, siteCode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RejectedListing
	for rows.Next() {
		var l RejectedListing
		if err := rows.Scan(&l.ListingID, &l.Reason, &l.RejectedAt); err != nil {
			return nil, err
		}
		l.RejectedAt = l.RejectedAt.UTC()
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *PGStore) RejectListing(ctx context.Context, siteCode string, l RejectedListing) error {
	if l.ListingID == "" {
		return errors.New("retailnetwork: a rejected listing needs its id")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO network_rejected_listings (site_code, listing_id, reason, rejected_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (site_code, listing_id) DO UPDATE
		SET reason = EXCLUDED.reason, rejected_at = EXCLUDED.rejected_at`,
		siteCode, l.ListingID, l.Reason, l.RejectedAt.UTC())
	return mapForeignKeyViolation(err)
}
