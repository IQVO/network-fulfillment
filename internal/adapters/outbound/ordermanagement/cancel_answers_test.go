package ordermanagement_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/ordermanagement"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// ADR 0024 §4: the Planner turns order-management's answer to the cancel
// (DELETE /orders/{id}) into the classes the orphaned-hold sweep acts on.
// These tests go over real HTTP, so a status the adapter fails to classify
// shows up here and not in production.

func answering(t *testing.T, status int) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestCancelHeldOrder_ClassifiesOrderManagementsAnswer(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		wantNil      bool
		wantNotFound bool
		wantReleased bool
	}{
		{"204 cancelled", http.StatusNoContent, true, false, false},
		{"200 ok", http.StatusOK, true, false, false},
		{"404 the hold does not exist", http.StatusNotFound, false, true, false},
		{"409 already released (BR6)", http.StatusConflict, false, false, true},
		{"422 already released", http.StatusUnprocessableEntity, false, false, true},
		{"400 bad request is not a classified answer", http.StatusBadRequest, false, false, false},
		{"500", http.StatusInternalServerError, false, false, false},
		{"503 downstream unavailable", http.StatusServiceUnavailable, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := answering(t, tc.status)
			err := ordermanagement.NewPlanner(srv.URL, srv.Client()).CancelHeldOrder(context.Background(), "ord-1")

			if tc.wantNil != (err == nil) {
				t.Fatalf("err = %v, want nil=%v", err, tc.wantNil)
			}
			if got := errors.Is(err, contract.ErrHeldOrderNotFound); got != tc.wantNotFound {
				t.Fatalf("errors.Is(ErrHeldOrderNotFound) = %v, want %v (err=%v)", got, tc.wantNotFound, err)
			}
			if got := errors.Is(err, contract.ErrHeldOrderAlreadyReleased); got != tc.wantReleased {
				t.Fatalf("errors.Is(ErrHeldOrderAlreadyReleased) = %v, want %v (err=%v)", got, tc.wantReleased, err)
			}
		})
	}
}

// Release keeps treating 409 as a plain error: only the cancel's answers are
// classified (TestDo_NonSuccessStatusIsAnError pins the same for release).
func TestReleaseHeldOrder_409IsNotClassifiedAsAlreadyReleased(t *testing.T) {
	srv, _ := answering(t, http.StatusConflict)
	err := ordermanagement.NewPlanner(srv.URL, srv.Client()).ReleaseHeldOrder(context.Background(), "ord-1")
	if err == nil || errors.Is(err, contract.ErrHeldOrderAlreadyReleased) || errors.Is(err, contract.ErrHeldOrderNotFound) {
		t.Fatalf("release 409 err = %v, want a plain error", err)
	}
}

// TestBreaker_ClassifiedAnswersAreNotDependencyFailures: a 404 or a 409 is
// order-management answering, not failing. A backlog of orders already
// released must not trip the breaker that guards every other call to OM.
func TestBreaker_ClassifiedAnswersAreNotDependencyFailures(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity} {
		srv, hits := answering(t, status)
		rec := &recordingRecorder{}
		client := ordermanagement.NewBreakerClient(ordermanagement.NewPlanner(srv.URL, srv.Client()), rec)

		const calls = 12 // more than the consecutive-failure threshold
		for i := 0; i < calls; i++ {
			err := client.CancelHeldOrder(context.Background(), "ord-1")
			if errors.Is(err, ordermanagement.ErrCircuitOpen) {
				t.Fatalf("status %d: call %d was refused by an open breaker", status, i)
			}
		}
		if got := atomic.LoadInt32(hits); got != calls {
			t.Fatalf("status %d: server saw %d calls, want %d (none short-circuited)", status, got, calls)
		}
		if len(rec.states) != 0 {
			t.Fatalf("status %d: breaker moved through states %v, want it left closed", status, rec.states)
		}
	}
}

// TestBreaker_ServerErrorsOpenItAndTheOpenBreakerIsARetryNotAnAnswer: 5xx are
// real failures; once open the cancel is not even attempted and the error is
// neither classified answer, so the sweep retries it on the next pass.
func TestBreaker_ServerErrorsOpenItAndTheOpenBreakerIsARetryNotAnAnswer(t *testing.T) {
	srv, hits := answering(t, http.StatusServiceUnavailable)
	rec := &recordingRecorder{}
	client := ordermanagement.NewBreakerClient(ordermanagement.NewPlanner(srv.URL, srv.Client()), rec)

	for i := 0; i < 5; i++ {
		if err := client.CancelHeldOrder(context.Background(), "ord-1"); err == nil {
			t.Fatalf("call %d: a 503 must be an error", i)
		}
	}
	if rec.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 consecutive 503 = %d, want open (%d)", rec.last(), gobreakerOpen)
	}
	before := atomic.LoadInt32(hits)

	err := client.CancelHeldOrder(context.Background(), "ord-1")
	if !errors.Is(err, ordermanagement.ErrCircuitOpen) {
		t.Fatalf("while open: err = %v, want ErrCircuitOpen", err)
	}
	if errors.Is(err, contract.ErrHeldOrderNotFound) || errors.Is(err, contract.ErrHeldOrderAlreadyReleased) {
		t.Fatalf("an open breaker must never read as a classified answer: %v", err)
	}
	if atomic.LoadInt32(hits) != before {
		t.Fatal("order-management was called while the breaker was open")
	}
}
