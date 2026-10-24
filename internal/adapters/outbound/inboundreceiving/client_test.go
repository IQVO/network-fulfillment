package inboundreceiving_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/inboundreceiving"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func asn() contract.AsnRegistration {
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

func clientFor(t *testing.T, h http.HandlerFunc) *inboundreceiving.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	rest, err := fleetrest.New(fleetrest.Config{Dependency: "inbound-receiving", BaseURL: srv.URL, Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return inboundreceiving.New(rest)
}

func problem(w http.ResponseWriter, status int, slug, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "https://errors.inbound-receiving.warehouse-systems.dev/" + slug, "title": slug, "status": status, "detail": detail})
}

func TestRegisterAsn_PostsTheExactBodyWithADeterministicKey(t *testing.T) {
	var gotBody []byte
	var gotKey, gotPath string
	c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotKey, gotPath = r.Header.Get("Idempotency-Key"), r.URL.Path
		w.WriteHeader(http.StatusCreated)
	})
	if err := c.RegisterAsn(context.Background(), asn()); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/asns" || gotKey != "nf-asn-NF-SHP-1" {
		t.Errorf("path=%s key=%s", gotPath, gotKey)
	}
	want, _ := inboundreceiving.Body(asn())
	if string(gotBody) != string(want) {
		t.Errorf("body differs:\n%s\n%s", gotBody, want)
	}
	var dto map[string]any
	_ = json.Unmarshal(gotBody, &dto)
	cartons := dto["cartons"].([]any)[0].(map[string]any)
	if dto["ownerRef"] != "S-100" || cartons["parentSscc"] != "006141410000000036" || cartons["kind"] != "HOMOGENEOUS_CASE" ||
		cartons["dims"].(map[string]any)["lengthMm"] != float64(400) || cartons["contents"].([]any)[0].(map[string]any)["qty"] != float64(12) {
		t.Errorf("body = %s", gotBody)
	}
}

func TestRegisterAsn_AnAsnThatAlreadyExistsIsSuccess(t *testing.T) {
	c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) {
		problem(w, http.StatusConflict, "asn-already-exists", "exists")
	})
	if err := c.RegisterAsn(context.Background(), asn()); err != nil {
		t.Fatalf("a replay must be a no-op, got %v", err)
	}
}

func TestRegisterAsn_Refusals(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		slug      string
		retryable bool
		refused   bool
	}{
		{"unknown sku waits", http.StatusUnprocessableEntity, "unknown-sku", true, true},
		{"duplicate sscc is a verdict", http.StatusUnprocessableEntity, "duplicate-sscc", false, true},
		{"a bad request is a verdict", http.StatusBadRequest, "invalid-request", false, true},
		{"another conflict is an error to retry", http.StatusConflict, "concurrent-update", false, false},
		{"a 404 is an error to retry", http.StatusNotFound, "not-found", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) { problem(w, tc.status, tc.slug, "detail text") })
			err := c.RegisterAsn(context.Background(), asn())
			var refused *contract.AsnRefusedError
			if got := errors.As(err, &refused); got != tc.refused {
				t.Fatalf("err = %v, refused=%v want %v", err, got, tc.refused)
			}
			if tc.refused && (refused.Retryable != tc.retryable || refused.Problem != tc.slug || !strings.Contains(refused.Detail, "detail text")) {
				t.Errorf("refusal = %+v", refused)
			}
			if err == nil {
				t.Error("a refusal is an error")
			}
		})
	}
}

func TestRegisterAsn_ATransportFailureIsAnErrorNotAVerdict(t *testing.T) {
	c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })
	err := c.RegisterAsn(context.Background(), asn())
	var refused *contract.AsnRefusedError
	if err == nil || errors.As(err, &refused) {
		t.Fatalf("err = %v", err)
	}
}
