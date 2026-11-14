package inboundreceiving

// In-package copies of the httptest client factory and ASN fixture from
// client_test.go (package inboundreceiving_test): the spec contract tests
// reflect this package's unexported registerAsnDTO, so they must be
// in-package and cannot see the _test package's helpers.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func contractAsn() contract.AsnRegistration {
	return contract.AsnRegistration{
		AsnNumber: "NF-SHP-1", SupplierRef: "S-100", OwnerRef: "S-100",
		Lines: []contract.AsnLine{{LineNo: 1, SKU: "SKU-A", ExpectedQty: 12}},
		Cartons: []contract.AsnCarton{{
			SSCC: "006141410000000012", ParentSSCC: "006141410000000036", Kind: contract.CartonHomogeneousCase, GrossWeightG: 4200,
			Dims:     &contract.CartonDims{LengthMm: 400, WidthMm: 300, HeightMm: 200},
			Contents: []contract.AsnCartonContent{{LineNo: 1, SKU: "SKU-A", GTIN: "00614141000036", Qty: 12, Lot: "L-77", Expiry: "2027-03-01"}},
		}},
	}
}

func contractClientFor(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	rest, err := fleetrest.New(fleetrest.Config{Dependency: "inbound-receiving", BaseURL: srv.URL, Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return New(rest)
}
