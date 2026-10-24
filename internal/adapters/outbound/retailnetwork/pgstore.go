package retailnetwork

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// PGStore is the Postgres implementation of Store (migration 0006).
type PGStore struct {
	pool   *pgxpool.Pool
	newRef func() shared.NetworkRef
}

var _ Store = (*PGStore)(nil)

// NewPGStore builds a store on pool. The NetworkRefs it mints are opaque random
// ids: never derived from the network's purchase order number (ADR 0026).
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool, newRef: func() shared.NetworkRef {
		return shared.NetworkRef("NR-" + uuid.NewString())
	}}
}

func (s *PGStore) NodeIDForSite(ctx context.Context, siteCode string) (string, bool, error) {
	return lookup(ctx, s.pool, `SELECT node_id FROM network_nodes WHERE site_code = $1`, siteCode)
}

func (s *PGStore) SiteForNode(ctx context.Context, nodeID string) (string, bool, error) {
	return lookup(ctx, s.pool, `SELECT site_code FROM network_nodes WHERE node_id = $1`, nodeID)
}

func lookup(ctx context.Context, pool *pgxpool.Pool, query string, arg any) (string, bool, error) {
	var out string
	err := pool.QueryRow(ctx, query, arg).Scan(&out)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return out, true, nil
}

func (s *PGStore) SaveNode(ctx context.Context, siteCode, nodeID string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO network_nodes (site_code, node_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, siteCode, nodeID)
	if err != nil {
		return err
	}
	// ON CONFLICT DO NOTHING hides a clash: read it back.
	got, ok, err := s.NodeIDForSite(ctx, siteCode)
	if err != nil {
		return err
	}
	if !ok || got != nodeID {
		return fmt.Errorf("%w: site %q is mapped to %q, not %q", ErrMappingConflict, siteCode, got, nodeID)
	}
	return nil
}

func (s *PGStore) SavePurchaseOrder(ctx context.Context, po PurchaseOrder) (shared.NetworkRef, error) {
	var ref shared.NetworkRef
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO network_purchase_orders (network_ref, po_number, vendor_id, node_id, required_ship_by, ack_by)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (po_number) DO NOTHING`,
			string(s.newRef()), po.PoNumber, po.VendorID, po.NodeID, po.RequiredShipBy, po.AckBy); err != nil {
			return err
		}
		var minted string
		if err := tx.QueryRow(ctx, `SELECT network_ref FROM network_purchase_orders WHERE po_number = $1`, po.PoNumber).Scan(&minted); err != nil {
			return err
		}
		ref = shared.NetworkRef(minted)
		for _, l := range po.Lines {
			if _, err := tx.Exec(ctx, `
				INSERT INTO network_purchase_order_lines (network_ref, line_no, network_line_ref, listing_id, network_product_id, quantity)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (network_ref, line_no) DO NOTHING`,
				minted, l.LineNo, string(l.NetworkLineRef), l.ListingID, string(l.NetworkProductID), l.Quantity); err != nil {
				return err
			}
		}
		return nil
	})
	return ref, err
}

func (s *PGStore) PurchaseOrder(ctx context.Context, ref shared.NetworkRef) (PurchaseOrder, bool, error) {
	po := PurchaseOrder{Ref: ref}
	err := s.pool.QueryRow(ctx, `
		SELECT po_number, vendor_id, node_id, required_ship_by, ack_by
		FROM network_purchase_orders WHERE network_ref = $1`, string(ref)).
		Scan(&po.PoNumber, &po.VendorID, &po.NodeID, &po.RequiredShipBy, &po.AckBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return PurchaseOrder{}, false, nil
	}
	if err != nil {
		return PurchaseOrder{}, false, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT line_no, network_line_ref, listing_id, network_product_id, quantity
		FROM network_purchase_order_lines WHERE network_ref = $1 ORDER BY line_no`, string(ref))
	if err != nil {
		return PurchaseOrder{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var l PurchaseOrderLine
		var lineRef, productID string
		if err := rows.Scan(&l.LineNo, &lineRef, &l.ListingID, &productID, &l.Quantity); err != nil {
			return PurchaseOrder{}, false, err
		}
		l.NetworkLineRef, l.NetworkProductID = shared.NetworkLineRef(lineRef), shared.NetworkProductId(productID)
		po.Lines = append(po.Lines, l)
	}
	return po, true, rows.Err()
}

const submissionColumns = `kind, subject, idempotency_key, request_body, COALESCE(transaction_id, ''), status, errors`

func scanSubmission(row pgx.Row) (Submission, error) {
	var sub Submission
	var kind, status string
	if err := row.Scan(&kind, &sub.Subject, &sub.IdempotencyKey, &sub.Body, &sub.TransactionID, &status, &sub.Errors); err != nil {
		return Submission{}, err
	}
	sub.Kind, sub.Status = SubmissionKind(kind), SubmissionStatus(status)
	return sub, nil
}

func (s *PGStore) Submission(ctx context.Context, kind SubmissionKind, subject string) (Submission, bool, error) {
	sub, err := scanSubmission(s.pool.QueryRow(ctx,
		`SELECT `+submissionColumns+` FROM network_submissions WHERE kind = $1 AND subject = $2`, string(kind), subject))
	if errors.Is(err, pgx.ErrNoRows) {
		return Submission{}, false, nil
	}
	if err != nil {
		return Submission{}, false, err
	}
	return sub, true, nil
}

func (s *PGStore) BeginSubmission(ctx context.Context, sub Submission) (Submission, error) {
	if len(sub.IdempotencyKey) == 0 || len(sub.IdempotencyKey) > maxIdempotencyKeyLength {
		return Submission{}, fmt.Errorf("retailnetwork: idempotency key must be 1 to %d characters, got %d", maxIdempotencyKeyLength, len(sub.IdempotencyKey))
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO network_submissions (kind, subject, idempotency_key, request_body, status)
		VALUES ($1, $2, $3, $4, 'SENDING')
		ON CONFLICT (kind, subject) DO NOTHING`,
		string(sub.Kind), sub.Subject, sub.IdempotencyKey, sub.Body); err != nil {
		return Submission{}, err
	}
	stored, ok, err := s.Submission(ctx, sub.Kind, sub.Subject)
	if err != nil {
		return Submission{}, err
	}
	if !ok {
		return Submission{}, fmt.Errorf("retailnetwork: ledger row (%s, %s) vanished after insert", sub.Kind, sub.Subject)
	}
	return stored, nil
}

func (s *PGStore) RecordTransaction(ctx context.Context, kind SubmissionKind, subject, transactionID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE network_submissions
		SET transaction_id = $3,
		    status = CASE WHEN status = 'SENDING' THEN 'PROCESSING' ELSE status END,
		    updated_at = now()
		WHERE kind = $1 AND subject = $2 AND transaction_id IS NULL`,
		string(kind), subject, transactionID)
	return err
}

func (s *PGStore) RecordOutcome(ctx context.Context, kind SubmissionKind, subject string, status SubmissionStatus, errs string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE network_submissions
		SET status = $3, errors = $4, updated_at = now()
		WHERE kind = $1 AND subject = $2 AND status NOT IN ('SUCCESS', 'FAILURE')`,
		string(kind), subject, string(status), errs)
	return err
}

func (s *PGStore) SaveLabels(ctx context.Context, ref shared.NetworkRef, labels []Label) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, l := range labels {
			if _, err := tx.Exec(ctx, `
				INSERT INTO network_labels (network_ref, package_no, label_ref, tracking_number, carrier)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (network_ref, package_no) DO NOTHING`,
				string(ref), l.PackageNo, l.LabelRef, l.TrackingNumber, l.Carrier); err != nil {
				return mapUniqueViolation(err)
			}
		}
		return nil
	})
}

// mapUniqueViolation turns a tracking-number clash into ErrMappingConflict.
func mapUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s", ErrMappingConflict, pgErr.ConstraintName)
	}
	return err
}

func (s *PGStore) Labels(ctx context.Context, ref shared.NetworkRef) ([]Label, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT package_no, label_ref, tracking_number, carrier
		FROM network_labels WHERE network_ref = $1 ORDER BY package_no`, string(ref))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Label
	for rows.Next() {
		var l Label
		if err := rows.Scan(&l.PackageNo, &l.LabelRef, &l.TrackingNumber, &l.Carrier); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PackageNo < out[j].PackageNo })
	return out, nil
}
