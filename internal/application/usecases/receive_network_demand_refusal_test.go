package usecases_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// omRefusing is order-management answering POST /orders: the first `refusals` requests
// with a problem document (status, slug), the rest with a feasible held order. It records
// the Idempotency-Key of every request.
type omRefusing struct {
	mu       sync.Mutex
	keys     []string
	refusals int // how many times to refuse; a negative number refuses forever
	status   int
	slug     string
}

func (o *omRefusing) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.keys = append(o.keys, r.Header.Get("Idempotency-Key"))
		if o.refusals < 0 || len(o.keys) <= o.refusals {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(o.status)
			_, _ = io.WriteString(w, `{"type":"https://errors.order-management.warehouse-systems.dev/`+o.slug+`","title":"t","status":`+strconv.Itoa(o.status)+`,"detail":"d"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"ord-1","promiseDate":"2026-09-24T08:00:00Z","lines":[]}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *fixture) receiveVia(planner *ordermanagement.Planner, at fixedClock) *usecases.ReceiveNetworkDemand {
	return &usecases.ReceiveNetworkDemand{
		Orders: f.orders, Gateway: f.gateway, Planner: planner, Translation: f.translation,
		Events: nopPublisher{}, Clock: at,
	}
}

// order-management now RELEASES the key on a RETRYABLE refusal: the raise that the poll
// loop already retries every pass passes once the catalogue knows the path, under the
// SAME key, and the order is answered inside its window.
func TestReceiveRefusals_AReleasedRefusalKeepsTheOrderNewAndALaterPollAnswersItUnderTheSameKey(t *testing.T) {
	om := &omRefusing{refusals: 2, status: http.StatusBadRequest, slug: "unknown-process-path"}
	srv := om.serve(t)
	f := newFixture(true)
	uc := f.receiveVia(ordermanagement.NewPlanner(srv.URL, srv.Client()), fixedClock{t: now()})

	for poll := 1; poll <= 2; poll++ {
		_, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1"))
		var refused *ordermanagement.RefusalError
		if !errors.As(err, &refused) || !refused.Released {
			t.Fatalf("poll %d: err = %v, want a released refusal", poll, err)
		}
		if o, _ := f.orders.FindByRef(context.Background(), "po-1"); o == nil || o.State() != networkorder.StateNew {
			t.Fatalf("poll %d: the order must stay NEW (pending), got %v", poll, o)
		}
		if _, submitted := f.gateway.Acknowledgement("po-1"); submitted {
			t.Fatalf("poll %d: nothing may be told to the network while the raise is refused", poll)
		}
	}
	o, err := uc.Execute(context.Background(), demand("po-1", "ASIN-1"))
	if err != nil || o.State() != networkorder.StateSubmitted {
		t.Fatalf("third poll = %v, %v: the later success must answer the order", o, err)
	}
	if len(om.keys) != 3 || om.keys[0] != "netful-raise-po-1" || om.keys[1] != om.keys[0] || om.keys[2] != om.keys[0] {
		t.Errorf("keys = %v, want the same key on every attempt", om.keys)
	}
}

// Whatever the class, a raise that never passes ends where it always ended: the order
// stays NEW inside its window (the poll retries), the window closes, the re-delivery
// leaves it to the sweep and RejectOverdueOrders rejects it ACKNOWLEDGEMENT_DEADLINE_MISSED.
func TestReceiveRefusals_ARaiseThatNeverPassesEndsRejectedAtTheDeadline(t *testing.T) {
	cases := map[string]string{
		"released (RETRYABLE) forever": "unknown-process-path",
		"final (FINAL)":                "invalid-fulfillment-site-id",
	}
	for name, slug := range cases {
		t.Run(name, func(t *testing.T) {
			om := &omRefusing{refusals: -1, status: http.StatusBadRequest, slug: slug}
			srv := om.serve(t)
			f := newFixture(true)
			planner := ordermanagement.NewPlanner(srv.URL, srv.Client())
			f.pollRefused(t, planner, 3)

			late := fixedClock{t: now().Add(networkorder.AcknowledgementWindow + time.Minute)}
			if o, err := f.receiveVia(planner, late).Execute(context.Background(), demand("po-1", "ASIN-1")); err != nil || o.State() != networkorder.StateNew {
				t.Fatalf("late poll = %v, %v: a closed window is left to the sweep", o, err)
			}
			sweep := &usecases.RejectOverdueOrders{Orders: f.orders, Planner: f.planner, Events: nopPublisher{}, Clock: late}
			if res, err := sweep.Execute(context.Background()); err != nil || res.Rejected != 1 {
				t.Fatalf("sweep = %+v, %v", res, err)
			}
			if o, _ := f.orders.FindByRef(context.Background(), "po-1"); o == nil || o.State() != networkorder.StateRejected {
				t.Fatalf("order = %v, want REJECTED at the deadline", o)
			}
			if len(om.keys) != 3 {
				t.Errorf("order-management saw %d raises, want 3 (one per poll inside the window, none after)", len(om.keys))
			}
		})
	}
}

// pollRefused runs n polls of the same demand inside the window; each must fail.
func (f *fixture) pollRefused(t *testing.T, planner *ordermanagement.Planner, n int) {
	t.Helper()
	for poll := range n {
		if _, err := f.receiveVia(planner, fixedClock{t: now()}).Execute(context.Background(), demand("po-1", "ASIN-1")); err == nil {
			t.Fatalf("poll %d: a refused raise must not answer the order", poll)
		}
	}
}
