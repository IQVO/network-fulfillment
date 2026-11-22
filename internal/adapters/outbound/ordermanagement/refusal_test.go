package ordermanagement_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func refusalBody(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"type":"https://errors.order-management.warehouse-systems.dev/`+slug+`","title":"t","status":`+strconv.Itoa(status)+`,"detail":"detail of `+slug+`"}`)
}

func raiseReq() contract.HeldOrderRequest {
	return contract.HeldOrderRequest{NetworkRef: "po-1", SiteId: "site-1", RequiredShipBy: now().Add(time.Hour), Lines: map[shared.SKU]int{"SKU-1": 1}}
}

type call func(p *ordermanagement.Planner) error

var calls = map[string]call{
	"raise": func(p *ordermanagement.Planner) error {
		_, err := p.RaiseHeldOrder(context.Background(), raiseReq())
		return err
	},
	"release": func(p *ordermanagement.Planner) error { return p.ReleaseHeldOrder(context.Background(), "ord-1") },
}

func TestKeyedCalls_ReadTheRefusalByTheCalleesClass(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		slug     string
		released bool
	}{
		{"unknown-process-path: the catalogue is fed by events", http.StatusBadRequest, "unknown-process-path", true},
		{"line-ineligible-for-resolved-path", http.StatusUnprocessableEntity, "line-ineligible-for-resolved-path", true},
		{"concurrent-modification", http.StatusConflict, "concurrent-modification", true},
		{"order-not-held", http.StatusConflict, "order-not-held", true},
		{"a request validation is final", http.StatusBadRequest, "invalid-fulfillment-site-id", false},
		{"order-not-found is final", http.StatusNotFound, "order-not-found", false},
		{"idempotency-key-reused is final there", http.StatusConflict, "idempotency-key-reused", false},
		{"an unknown slug is terminal", http.StatusUnprocessableEntity, "a-slug-nobody-declared", false},
	}
	for op, do := range calls {
		for _, tc := range cases {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { refusalBody(w, tc.status, tc.slug) }))
				t.Cleanup(srv.Close)
				err := do(ordermanagement.NewPlanner(srv.URL, srv.Client()))
				var refused *ordermanagement.RefusalError
				if !errors.As(err, &refused) {
					t.Fatalf("err = %v, want a *RefusalError", err)
				}
				if refused.Released != tc.released || refused.Problem != tc.slug || refused.Status != tc.status || !strings.Contains(refused.Detail, "detail of") {
					t.Errorf("refusal = %+v, want released=%v", refused, tc.released)
				}
				// The text the runbook and the poller's log lines match on keeps its prefix.
				if !strings.Contains(err.Error(), "status "+strconv.Itoa(tc.status)) || !strings.Contains(err.Error(), tc.slug) {
					t.Errorf("error text = %q", err)
				}
			})
		}
	}
}

func TestKeyedCalls_ARefusalWithoutAProblemDocumentIsTerminal(t *testing.T) {
	for op, do := range calls {
		for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity} {
			t.Run(op+"/"+strconv.Itoa(status), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(status)
					_, _ = io.WriteString(w, "<html>")
				}))
				t.Cleanup(srv.Close)
				err := do(ordermanagement.NewPlanner(srv.URL, srv.Client()))
				var refused *ordermanagement.RefusalError
				if !errors.As(err, &refused) || refused.Released || refused.Problem != "" || refused.Status != status {
					t.Fatalf("err = %v (%+v), want a terminal refusal without a slug", err, refused)
				}
			})
		}
	}
}

// 5xx, 429 and the rest were never verdicts: plain errors, not refusals.
func TestKeyedCalls_OtherAnswersAreNotRefusals(t *testing.T) {
	for op, do := range calls {
		for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
			t.Run(op+"/"+strconv.Itoa(status), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { refusalBody(w, status, "concurrent-modification") }))
				t.Cleanup(srv.Close)
				err := do(ordermanagement.NewPlanner(srv.URL, srv.Client()))
				var refused *ordermanagement.RefusalError
				if err == nil || errors.As(err, &refused) {
					t.Fatalf("status %d: err = %v, want a plain error", status, err)
				}
			})
		}
	}
}

// The point of the change at the wire: a released refusal is retried under the SAME key
// and the same body, and the later answer completes it.
func TestKeyedCalls_AReleasedRefusalIsRetriedUnderTheSameKeyAndPasses(t *testing.T) {
	for op, do := range calls {
		t.Run(op, func(t *testing.T) {
			var keys, bodies []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				keys, bodies = append(keys, r.Header.Get("Idempotency-Key")), append(bodies, string(b))
				if len(keys) == 1 {
					refusalBody(w, http.StatusConflict, "concurrent-modification")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"ord-1","promiseDate":"2026-09-24T08:00:00Z","lines":[]}`)
			}))
			t.Cleanup(srv.Close)
			p := ordermanagement.NewPlanner(srv.URL, srv.Client())
			var refused *ordermanagement.RefusalError
			if err := do(p); !errors.As(err, &refused) || !refused.Released {
				t.Fatalf("first attempt: %v, want a released refusal", err)
			}
			if err := do(p); err != nil {
				t.Fatalf("second attempt: %v", err)
			}
			if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] || bodies[0] != bodies[1] {
				t.Errorf("keys = %q, bodies equal = %v: a retry must carry the same key and body", keys, bodies[0] == bodies[1])
			}
		})
	}
}

// The cancel is DELETE, not keyed: it keeps the answers of ADR 0024 section 4 and its
// plain statusError, and is never read through the table.
func TestCancelHeldOrder_IsNotReadThroughTheTable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		refusalBody(w, http.StatusConflict, "concurrent-modification")
	}))
	t.Cleanup(srv.Close)
	err := ordermanagement.NewPlanner(srv.URL, srv.Client()).CancelHeldOrder(context.Background(), "ord-1")
	var refused *ordermanagement.RefusalError
	if errors.As(err, &refused) || !errors.Is(err, contract.ErrHeldOrderAlreadyReleased) {
		t.Fatalf("err = %v, want the already-released sentinel and no RefusalError", err)
	}
}
