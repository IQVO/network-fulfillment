package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// ReceiptProjectionRepo is the Postgres ports.ReceiptProjection (migration 0009,
// ADR 0028): the projection of inbound-receiving's receipt events for the ASNs
// this service registered, and the queue of closed receipts to report.
type ReceiptProjectionRepo struct {
	pool *pgxpool.Pool
}

var _ ports.ReceiptProjection = (*ReceiptProjectionRepo)(nil)

// NewReceiptProjectionRepo builds the repo over pool.
func NewReceiptProjectionRepo(pool *pgxpool.Pool) *ReceiptProjectionRepo {
	return &ReceiptProjectionRepo{pool: pool}
}

// apply runs write in one transaction that first checks the ASN is ours and
// claims the event id. An event about another ASN, or one already applied, is a
// committed no-op.
func (r *ReceiptProjectionRepo) apply(ctx context.Context, eventID, asnNumber string, write func(tx pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var ours bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM inbound_shipments WHERE asn_number = $1)`, asnNumber).Scan(&ours); err != nil {
		return err
	}
	if !ours {
		return nil
	}
	tag, err := tx.Exec(ctx, `INSERT INTO inbound_receipt_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		if err := write(tx); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RecordCartonReceived stores an accepted carton; the first acceptance wins.
func (r *ReceiptProjectionRepo) RecordCartonReceived(ctx context.Context, eventID string, f contract.ReceivedCartonFact) error {
	contents, err := json.Marshal(f.Contents)
	if err != nil {
		return err
	}
	key := f.SSCC
	if key == "" {
		key = f.LPN
	}
	return r.apply(ctx, eventID, f.AsnNumber, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO inbound_receipt_cartons (asn_number, carton_key, sscc, receipt_id, contents, received_at)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (asn_number, carton_key) DO NOTHING`,
			f.AsnNumber, key, f.SSCC, f.ReceiptID, contents, f.ReceivedAt)
		return err
	})
}

// RecordFinding stores one carton discrepancy.
func (r *ReceiptProjectionRepo) RecordFinding(ctx context.Context, eventID string, f contract.CartonFinding) error {
	return r.apply(ctx, eventID, f.AsnNumber, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO inbound_receipt_findings (event_id, asn_number, receipt_id, sscc, kind, expected, actual, raised_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			eventID, f.AsnNumber, f.ReceiptID, f.SSCC, f.Kind, f.Expected, f.Actual, f.At)
		return err
	})
}

// RecordClosed queues the receipt to be reported; the first close wins.
func (r *ReceiptProjectionRepo) RecordClosed(ctx context.Context, eventID string, f contract.ReceiptClosedFact, now time.Time) error {
	return r.apply(ctx, eventID, f.AsnNumber, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO inbound_receipts (asn_number, receipt_id, closed_at, next_attempt_at)
			VALUES ($1, $2, $3, $4) ON CONFLICT (asn_number) DO NOTHING`,
			f.AsnNumber, f.ReceiptID, f.ClosedAt, now)
		return err
	})
}

// DueReceipts returns the closed receipts that may be reported now, with the
// shipment they belong to, the cartons the dock accepted and the findings.
func (r *ReceiptProjectionRepo) DueReceipts(ctx context.Context, now time.Time, limit int) ([]contract.ReceiptFacts, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT r.asn_number, r.receipt_id, r.closed_at, r.attempts, s.payload
		FROM inbound_receipts r JOIN inbound_shipments s ON s.asn_number = r.asn_number
		WHERE r.status = 'PENDING' AND r.next_attempt_at <= $1
		ORDER BY r.closed_at, r.asn_number LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	type head struct {
		asn   string
		facts contract.ReceiptFacts
	}
	var heads []head
	for rows.Next() {
		var h head
		var payload []byte
		if err := rows.Scan(&h.asn, &h.facts.ReceiptID, &h.facts.ClosedAt, &h.facts.Attempts, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal(payload, &h.facts.Shipment); err != nil {
			rows.Close()
			return nil, fmt.Errorf("inbound receipt %s: undecodable shipment: %w", h.asn, err)
		}
		heads = append(heads, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]contract.ReceiptFacts, 0, len(heads))
	for _, h := range heads {
		if h.facts.Cartons, err = r.cartons(ctx, h.asn); err != nil {
			return nil, err
		}
		if h.facts.Findings, err = r.findings(ctx, h.asn); err != nil {
			return nil, err
		}
		out = append(out, h.facts)
	}
	return out, nil
}

func (r *ReceiptProjectionRepo) cartons(ctx context.Context, asn string) ([]contract.ReceivedCartonFact, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT carton_key, sscc, receipt_id, contents, received_at FROM inbound_receipt_cartons
		WHERE asn_number = $1 ORDER BY received_at, carton_key`, asn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contract.ReceivedCartonFact
	for rows.Next() {
		f := contract.ReceivedCartonFact{AsnNumber: asn}
		var key string
		var contents []byte
		if err := rows.Scan(&key, &f.SSCC, &f.ReceiptID, &contents, &f.ReceivedAt); err != nil {
			return nil, err
		}
		if f.SSCC == "" {
			f.LPN = key
		}
		if err := json.Unmarshal(contents, &f.Contents); err != nil {
			return nil, fmt.Errorf("inbound receipt carton %s/%s: undecodable contents: %w", asn, key, err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (r *ReceiptProjectionRepo) findings(ctx context.Context, asn string) ([]contract.CartonFinding, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT receipt_id, sscc, kind, expected, actual, raised_at FROM inbound_receipt_findings
		WHERE asn_number = $1 ORDER BY raised_at, event_id`, asn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contract.CartonFinding
	for rows.Next() {
		f := contract.CartonFinding{AsnNumber: asn}
		if err := rows.Scan(&f.ReceiptID, &f.SSCC, &f.Kind, &f.Expected, &f.Actual, &f.At); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// MarkReceiptSent closes the report: the network has the receipt.
func (r *ReceiptProjectionRepo) MarkReceiptSent(ctx context.Context, asn string, now time.Time) error {
	return r.exec(ctx, `UPDATE inbound_receipts SET status = 'SENT', sent_at = $2, last_error = '', updated_at = now()
		WHERE asn_number = $1 AND status = 'PENDING'`, asn, now)
}

// MarkReceiptRetry keeps the receipt PENDING with the error and the next attempt.
func (r *ReceiptProjectionRepo) MarkReceiptRetry(ctx context.Context, asn, lastError string, next time.Time) error {
	return r.exec(ctx, `UPDATE inbound_receipts SET attempts = attempts + 1, last_error = $2, next_attempt_at = $3, updated_at = now()
		WHERE asn_number = $1 AND status = 'PENDING'`, asn, lastError, next)
}

// MarkReceiptRejected parks a receipt the network refused for good.
func (r *ReceiptProjectionRepo) MarkReceiptRejected(ctx context.Context, asn, lastError string) error {
	return r.exec(ctx, `UPDATE inbound_receipts SET status = 'REJECTED', attempts = attempts + 1, last_error = $2, updated_at = now()
		WHERE asn_number = $1 AND status = 'PENDING'`, asn, lastError)
}

func (r *ReceiptProjectionRepo) exec(ctx context.Context, sql string, args ...any) error {
	_, err := r.pool.Exec(ctx, sql, args...)
	return err
}
