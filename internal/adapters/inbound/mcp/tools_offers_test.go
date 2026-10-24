package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/memory"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// This file covers the three handlers/DTOs the rest of the suite does not
// reach: listCapabilityOffers, toCapabilityOfferDTO, and the
// getAcknowledgementReport tool wrapper. It lives in package mcp (like
// tools_test.go) because those entry points are unexported.

// stubReportsClient records the last query it was asked and answers a fixed
// view. (The mcp_test package has its own fakeReportsClient; this one is
// package-local.)
type stubReportsClient struct {
	queries []AcknowledgementReportQuery
	err     error
}

func (c *stubReportsClient) GetAcknowledgementReport(_ context.Context, q AcknowledgementReportQuery) (AcknowledgementReportView, error) {
	c.queries = append(c.queries, q)
	if c.err != nil {
		return AcknowledgementReportView{}, c.err
	}
	return AcknowledgementReportView{Rows: []AcknowledgementRowView{{DayBucket: "2026-10-08"}}}, nil
}

func (c *stubReportsClient) GetFreshness(context.Context) (FreshnessView, error) {
	return FreshnessView{}, nil
}

// failingOffersRepo is a CapabilityOfferRepo whose ListAll always fails, to
// prove the tool propagates the store error rather than answering an empty
// list.
type failingOffersRepo struct{ err error }

func (r failingOffersRepo) Save(context.Context, capabilityoffer.CapabilityOffer) error { return nil }

func (r failingOffersRepo) ListAll(context.Context) ([]capabilityoffer.CapabilityOffer, error) {
	return nil, r.err
}

func mustOffer(t *testing.T, sku string, qty int, basis capabilityoffer.Basis, computedAt time.Time) capabilityoffer.CapabilityOffer {
	t.Helper()
	o, err := capabilityoffer.New(shared.SKU(sku), "site-1", qty, qty+10, basis, computedAt)
	if err != nil {
		t.Fatalf("capabilityoffer.New: %v", err)
	}
	return o
}

func TestListCapabilityOffers_MapsEveryOfferField(t *testing.T) {
	repo := memory.NewCapabilityOfferRepo()
	computedAt := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for _, o := range []capabilityoffer.CapabilityOffer{
		mustOffer(t, "sku-1", 7, capabilityoffer.BasisPhysical, computedAt),
		mustOffer(t, "sku-2", 3, capabilityoffer.BasisThroughputConstrained, computedAt),
	} {
		if err := repo.Save(context.Background(), o); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	d := Deps{Offers: repo, Clock: fixedClock{now: base}}

	out, err := d.listCapabilityOffers(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("listCapabilityOffers: %v", err)
	}
	if len(out.CapabilityOffers) != 2 {
		t.Fatalf("offers = %d, want 2", len(out.CapabilityOffers))
	}
	bySKU := map[string]capabilityOfferDTO{}
	for _, dto := range out.CapabilityOffers {
		bySKU[dto.SKU] = dto
	}
	physical, ok := bySKU["sku-1"]
	if !ok {
		t.Fatalf("sku-1 missing from %v", out.CapabilityOffers)
	}
	if physical.SiteId != "site-1" || physical.AdvertisedQuantity != 7 || physical.Basis != "PHYSICAL" {
		t.Errorf("sku-1 DTO = %+v", physical)
	}
	if physical.ComputedAt != "2026-10-08T12:00:00Z" {
		t.Errorf("computedAt = %q, want RFC3339 UTC", physical.ComputedAt)
	}
	if tc := bySKU["sku-2"]; tc.Basis != "THROUGHPUT_CONSTRAINED" {
		t.Errorf("sku-2 basis = %q, want THROUGHPUT_CONSTRAINED", tc.Basis)
	}
}

func TestListCapabilityOffers_EmptyRepoYieldsEmptyArray(t *testing.T) {
	d := Deps{Offers: memory.NewCapabilityOfferRepo(), Clock: fixedClock{now: base}}
	out, err := d.listCapabilityOffers(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("listCapabilityOffers: %v", err)
	}
	if out.CapabilityOffers == nil || len(out.CapabilityOffers) != 0 {
		t.Errorf("offers = %#v, want a non-nil empty array (an empty JSON list, not null)", out.CapabilityOffers)
	}
}

func TestListCapabilityOffers_StoreErrorPropagates(t *testing.T) {
	boom := errors.New("analytics store unavailable")
	d := Deps{Offers: failingOffersRepo{err: boom}, Clock: fixedClock{now: base}}
	if _, err := d.listCapabilityOffers(context.Background(), struct{}{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the store error propagated", err)
	}
}

func TestToCapabilityOfferDTO_FormatsComputedAtAsUTC(t *testing.T) {
	// 14:00 in UTC+02:00 is 12:00 UTC: the DTO must normalise, not echo.
	local := time.Date(2026, 10, 8, 14, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	dto := toCapabilityOfferDTO(mustOffer(t, "sku-1", 5, capabilityoffer.BasisPhysical, local))
	if dto.ComputedAt != "2026-10-08T12:00:00Z" {
		t.Errorf("computedAt = %q, want 2026-10-08T12:00:00Z", dto.ComputedAt)
	}
}

func TestGetAcknowledgementReport_DelegatesAndForwardsFilters(t *testing.T) {
	client := &stubReportsClient{}
	d := Deps{Reports: client}

	in := AcknowledgementReportToolInput{From: "2026-10-08T00:00:00Z", To: "2026-10-09T00:00:00Z"}
	out, err := d.getAcknowledgementReport(context.Background(), in)
	if err != nil {
		t.Fatalf("getAcknowledgementReport: %v", err)
	}
	if len(client.queries) != 1 {
		t.Fatalf("queries = %d, want 1", len(client.queries))
	}
	if client.queries[0].From != "2026-10-08T00:00:00Z" || client.queries[0].To != "2026-10-09T00:00:00Z" {
		t.Errorf("forwarded query = %+v", client.queries[0])
	}
	if len(out.Rows) != 1 || out.Rows[0].DayBucket != "2026-10-08" {
		t.Errorf("view = %+v, want the client's rows untouched", out)
	}
}

func TestGetAcknowledgementReport_ClientErrorPropagates(t *testing.T) {
	boom := errors.New("reports service down")
	d := Deps{Reports: &stubReportsClient{err: boom}}
	if _, err := d.getAcknowledgementReport(context.Background(), AcknowledgementReportToolInput{From: "a", To: "b"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the client error propagated", err)
	}
}

func TestGetAcknowledgementReport_UnconfiguredClientIsAnError(t *testing.T) {
	d := Deps{} // Reports nil: the tool is not even registered in production
	if _, err := d.getAcknowledgementReport(context.Background(), AcknowledgementReportToolInput{From: "a", To: "b"}); err == nil {
		t.Fatal("expected an error when no reports client is configured")
	}
}
