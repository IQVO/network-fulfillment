//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// The schema's CHECK constraints would reject a bad value at write time,
// so each test drops the one under test to simulate a corrupt row (a
// manual edit, a restore from another schema version, a future enum
// value read by an older binary). Each test owns its own database via
// newDB, so dropping a constraint cannot leak into another test.

func TestNetworkOrderRepo_FindByRef_RejectsUnknownPersistedState(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `ALTER TABLE network_orders DROP CONSTRAINT network_orders_state_check`); err != nil {
		t.Fatalf("drop state check: %v", err)
	}
	ref := uniqueRef("badstate")
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `
		INSERT INTO network_orders (network_ref, site_id, required_ship_by, acknowledge_by, state, received_at)
		VALUES ($1, 'site-1', $2, $3, 'BOGUS', $4)
	`, string(ref), now.Add(48*time.Hour), now.Add(24*time.Hour), now); err != nil {
		t.Fatalf("insert corrupt row: %v", err)
	}

	got, err := repo.FindByRef(ctx, ref)
	if !errors.Is(err, networkorder.ErrUnknownState) {
		t.Fatalf("FindByRef err = %v, want ErrUnknownState", err)
	}
	if got != nil {
		t.Fatalf("FindByRef returned an aggregate %+v for a corrupt row", got)
	}
	if !strings.Contains(err.Error(), "rehydrate network order") || !strings.Contains(err.Error(), string(ref)) {
		t.Fatalf("err = %q, want rehydrate context naming the ref", err)
	}
}

func TestNetworkOrderRepo_Lists_SurfaceUnknownPersistedState(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewNetworkOrderRepo(pool)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `ALTER TABLE network_orders DROP CONSTRAINT network_orders_state_check`); err != nil {
		t.Fatalf("drop state check: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `
		INSERT INTO network_orders (network_ref, site_id, required_ship_by, acknowledge_by, state, received_at)
		VALUES ($1, 'site-1', $2, $3, 'BOGUS', $4)
	`, string(uniqueRef("badlist")), now.Add(48*time.Hour), now.Add(24*time.Hour), now); err != nil {
		t.Fatalf("insert corrupt row: %v", err)
	}

	if _, err := repo.ListAll(ctx); !errors.Is(err, networkorder.ErrUnknownState) {
		t.Fatalf("ListAll err = %v, want ErrUnknownState", err)
	}
}

func TestCapabilityOfferRepo_ListAll_RejectsUnknownPersistedBasis(t *testing.T) {
	pool := newDB(t)
	repo := postgres.NewCapabilityOfferRepo(pool)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `ALTER TABLE capability_offers DROP CONSTRAINT capability_offers_basis_check`); err != nil {
		t.Fatalf("drop basis check: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO capability_offers (sku, site_id, advertised_quantity, basis, computed_at)
		VALUES ('sku-bad', 'site-1', 5, 'BOGUS', now())
	`); err != nil {
		t.Fatalf("insert corrupt row: %v", err)
	}

	out, err := repo.ListAll(ctx)
	if !errors.Is(err, capabilityoffer.ErrUnknownBasis) {
		t.Fatalf("ListAll err = %v, want ErrUnknownBasis", err)
	}
	if out != nil {
		t.Fatalf("ListAll returned %v for a corrupt row, want nil", out)
	}
	if !strings.Contains(err.Error(), "rehydrate capability offer") || !strings.Contains(err.Error(), "sku-bad") {
		t.Fatalf("err = %q, want rehydrate context naming the sku", err)
	}
}
