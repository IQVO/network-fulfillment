package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/network-fulfillment/internal/adapters/inbound/http"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// TestEveryMappedErrorHasItsOwnProblemType is the structural version of
// the guard in server_test.go, and the one that would have caught
// order-management's shipped defect at the source.
//
// There, ErrOrderNotHeld was added to the STATUS table and not to the
// problem-type table, so a deliberate 409 was delivered with
// type=internal-error and the title "An unexpected internal error
// occurred". Two tables, one updated. The status looked right, so every
// status-only assertion passed.
//
// This drives each error THROUGH the real handler chain (the only way to
// reach the unexported mapping tables from an external test package) and
// asserts that anything not mapped to a 500 also has a problem type of
// its own.
func TestEveryMappedErrorHasItsOwnProblemType(t *testing.T) {
	// Every error the adapter claims to map. Adding one to errors.go
	// without adding it here leaves the pairing unverified, which is why
	// the list is explicit rather than derived.
	mapped := []error{
		usecases.ErrOrderNotFound,
		shared.ErrUnknownProduct,
		shared.ErrEmptyNetworkRef,
		shared.ErrEmptyNetworkLineRef,
		shared.ErrEmptyNetworkProductId,
		shared.ErrNonPositiveQuantity,
		shared.ErrNoLines,
		networkorder.ErrConfirmBeforeAcknowledge,
	}

	for _, err := range mapped {
		t.Run(err.Error(), func(t *testing.T) {
			srv := &inboundhttp.Server{
				Orders:      failingRepo{err: err},
				Poller:      fakeStats{},
				Clock:       fixedClock{t: now()},
				NetworkMode: "stub",
			}
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/network-orders/po-1", nil))

			var body problemBody
			if decodeErr := json.Unmarshal(rec.Body.Bytes(), &body); decodeErr != nil {
				t.Fatalf("decode problem: %v (body: %s)", decodeErr, rec.Body.String())
			}

			// A 500 legitimately uses internal-error; everything else
			// must carry a slug naming the actual rule.
			if body.Status == http.StatusInternalServerError {
				return
			}
			if strings.HasSuffix(body.Type, "/internal-error") {
				t.Fatalf("%v maps to %d but fell through to the internal-error problem type; "+
					"it is in statusFor's table and missing from problemFor's",
					err, body.Status)
			}
			if strings.Contains(body.Title, "unexpected internal error") {
				t.Fatalf("%v maps to %d but is titled as an internal error: %q",
					err, body.Status, body.Title)
			}
			if body.Detail == "" {
				t.Fatalf("%v produced no detail; the caller cannot tell what happened", err)
			}
		})
	}
}

// An error the tables have never heard of must be a 500 with the
// internal-error type — the honest answer, and the one case where that
// slug is correct.
func TestUnmappedErrorIs500WithInternalErrorType(t *testing.T) {
	srv := &inboundhttp.Server{
		Orders:      failingRepo{err: errBoom},
		Poller:      fakeStats{},
		Clock:       fixedClock{t: now()},
		NetworkMode: "stub",
	}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/network-orders/po-1", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var body problemBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.HasSuffix(body.Type, "/internal-error") {
		t.Fatalf("problem.type = %q, want the internal-error slug for a genuinely unknown error", body.Type)
	}
}
