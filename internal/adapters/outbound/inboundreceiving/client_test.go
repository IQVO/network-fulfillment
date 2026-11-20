package inboundreceiving_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	}{
		// RETRYABLE at inbound-receiving (ADR 0027): released, the same key may pass.
		{"unknown sku waits: the product projection lags", http.StatusUnprocessableEntity, "unknown-sku", true},
		{"concurrent-modification is the slug inbound-receiving answers", http.StatusConflict, "concurrent-modification", true},
		{"duplicate sscc is released: cancelling the other ASN frees it", http.StatusUnprocessableEntity, "duplicate-sscc", true},
		{"idempotency-key-reused is never stored", http.StatusConflict, "idempotency-key-reused", true},
		{"a 404 the callee releases", http.StatusNotFound, "asn-not-found", true},
		{"a 409 state not reached yet", http.StatusConflict, "asn-in-progress", true},
		// FINAL at inbound-receiving, or never able to pass from here: terminal.
		{"a bad carton is a verdict", http.StatusBadRequest, "invalid-carton", false},
		{"a bad sscc is a verdict", http.StatusUnprocessableEntity, "invalid-sscc", false},
		{"a terminal ASN is a verdict, not an error to loop on", http.StatusConflict, "asn-terminal", false},
		{"a closed receipt is a verdict", http.StatusConflict, "receipt-closed", false},
		{"our own body is the same on every attempt: malformed-request cannot pass", http.StatusBadRequest, "malformed-request", false},
		{"the slug this repo used to look for is not one inbound-receiving answers", http.StatusConflict, "concurrent-update", false},
		{"an unknown slug is terminal", http.StatusUnprocessableEntity, "a-slug-nobody-declared", false},
		{"an unknown 404 slug is terminal", http.StatusNotFound, "not-found", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) { problem(w, tc.status, tc.slug, "detail text") })
			err := c.RegisterAsn(context.Background(), asn())
			var refused *contract.AsnRefusedError
			if !errors.As(err, &refused) {
				t.Fatalf("err = %v, want a refusal", err)
			}
			if refused.Retryable != tc.retryable || refused.Problem != tc.slug || !strings.Contains(refused.Detail, "detail text") {
				t.Errorf("refusal = %+v, want retryable=%v problem=%s", refused, tc.retryable, tc.slug)
			}
		})
	}
}

// A 4xx the table reads (400, 404, 409, 422) without a problem document has no
// slug, and an unknown refusal never loops: terminal.
func TestRegisterAsn_AClientErrorWithoutAProblemDocumentIsTerminal(t *testing.T) {
	bodies := map[string]string{"empty": "", "html": "<html>nope</html>", "json without a type": `{"detail":"x"}`}
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity} {
		for name, body := range bodies {
			t.Run(http.StatusText(status)+" "+name, func(t *testing.T) {
				c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status); _, _ = io.WriteString(w, body) })
				err := c.RegisterAsn(context.Background(), asn())
				var refused *contract.AsnRefusedError
				if !errors.As(err, &refused) || refused.Retryable || refused.Problem != "" {
					t.Fatalf("err = %v (%+v), want a terminal refusal without a slug", err, refused)
				}
				if !strings.Contains(err.Error(), strconv.Itoa(status)) {
					t.Errorf("the operator must see the status in %q", err)
				}
			})
		}
	}
}

// Every other answer stays an error the next pass retries: it was never a verdict.
func TestRegisterAsn_OtherAnswersAreErrorsNotVerdicts(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) {
				problem(w, status, "unknown-sku", "looks like a released slug")
			})
			err := c.RegisterAsn(context.Background(), asn())
			var refused *contract.AsnRefusedError
			if err == nil || errors.As(err, &refused) {
				t.Fatalf("status %d: err = %v, want a plain error", status, err)
			}
		})
	}
}

// The point of the change: a released refusal is retried under the SAME key
// with the SAME body, and the later 201 completes it.
func TestRegisterAsn_AReleasedRefusalIsRetriedUnderTheSameKeyAndPasses(t *testing.T) {
	var keys, bodies []string
	calls := 0
	c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		keys, bodies = append(keys, r.Header.Get("Idempotency-Key")), append(bodies, string(b))
		calls++
		if calls == 1 {
			problem(w, http.StatusUnprocessableEntity, "unknown-sku", "SKU-A is not known yet")
			return
		}
		w.WriteHeader(http.StatusCreated)
	})
	err := c.RegisterAsn(context.Background(), asn())
	var refused *contract.AsnRefusedError
	if !errors.As(err, &refused) || !refused.Retryable {
		t.Fatalf("first attempt: %v, want a released refusal", err)
	}
	if err := c.RegisterAsn(context.Background(), asn()); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	if len(keys) != 2 || keys[0] != "nf-asn-NF-SHP-1" || keys[0] != keys[1] || bodies[0] != bodies[1] {
		t.Errorf("keys = %v, bodies equal = %v: a retry must carry the same key and the same body", keys, bodies[0] == bodies[1])
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
