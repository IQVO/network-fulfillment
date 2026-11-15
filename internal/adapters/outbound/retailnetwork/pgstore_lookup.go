package retailnetwork

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var _ ports.PurchaseOrderIndex = (*PGStore)(nil)

// FindNetworkRefByPONumber returns the NetworkRef minted for a purchase order
// number (ADR 0036): the reverse of PurchaseOrder, which goes from the ref to the
// number. The UNIQUE (po_number) constraint of migration 0006 is the index, and
// a number the adapter has never recorded (every number, in NETWORK_MODE=stub)
// is (_, false, nil).
func (s *PGStore) FindNetworkRefByPONumber(ctx context.Context, poNumber string) (shared.NetworkRef, bool, error) {
	var ref string
	err := s.pool.QueryRow(ctx, `SELECT network_ref FROM network_purchase_orders WHERE po_number = $1`, poNumber).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return shared.NetworkRef(ref), true, nil
}
