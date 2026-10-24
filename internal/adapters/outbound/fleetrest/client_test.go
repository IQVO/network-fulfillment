package fleetrest_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
)

func noSleep(context.Context, time.Duration) error { return nil }

func newClient(t *testing.T, h http.HandlerFunc, mutate func(*fleetrest.Config)) *fleetrest.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg := fleetrest.Config{Dependency: "dep", BaseURL: srv.URL, Sleep: noSleep}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := fleetrest.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNew_RefusesARelativeBaseURL(t *testing.T) {
	if _, err := fleetrest.New(fleetrest.Config{Dependency: "dep", BaseURL: "inbound-receiving"}); err == nil {
		t.Fatal("a base URL without a scheme and host must be refused")
	}
}

func TestDo_Retries5xxThenSucceedsAndSendsHeadersAndBody(t *testing.T) {
	var calls atomic.Int32
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if r.Header.Get("Idempotency-Key") != "k1" || r.Header.Get("Content-Type") != "application/json" || r.URL.Path != "/a%20b/c" && r.URL.EscapedPath() != "/a%20b/c" {
			t.Errorf("request = %s %s headers=%v", r.Method, r.URL.EscapedPath(), r.Header)
		}
		w.WriteHeader(http.StatusCreated)
	}, nil)
	resp, err := c.Do(context.Background(), fleetrest.Request{Method: http.MethodPost, Path: []string{"a b", "c"}, Header: map[string]string{"Idempotency-Key": "k1"}, Body: []byte("{}")})
	if err != nil || resp.Status != http.StatusCreated || calls.Load() != 3 {
		t.Fatalf("resp=%+v err=%v calls=%d", resp, err, calls.Load())
	}
}

func TestDo_DoesNotRetryA4xx(t *testing.T) {
	var calls atomic.Int32
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnprocessableEntity)
	}, nil)
	resp, err := c.Do(context.Background(), fleetrest.Request{Method: http.MethodGet, Path: []string{"x"}})
	if err != nil || resp.Status != http.StatusUnprocessableEntity || calls.Load() != 1 {
		t.Fatalf("resp=%+v err=%v calls=%d", resp, err, calls.Load())
	}
}

func TestDo_GivesUpAfterTheAttemptsAndOpensTheBreaker(t *testing.T) {
	var calls atomic.Int32
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	},
		func(cfg *fleetrest.Config) { cfg.MaxAttempts = 3 })
	if _, err := c.Do(context.Background(), fleetrest.Request{Method: http.MethodGet, Path: []string{"x"}}); err == nil || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d, want 3 attempts and an error", err, calls.Load())
	}
	var opened bool
	for range 20 {
		_, err := c.Do(context.Background(), fleetrest.Request{Method: http.MethodGet, Path: []string{"x"}})
		if errors.Is(err, fleetrest.ErrCircuitOpen) {
			opened = true
			break
		}
	}
	if !opened {
		t.Error("persistent 5xx never opened the circuit breaker")
	}
}

func TestDo_ACancelledContextStopsTheCall(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Do(ctx, fleetrest.Request{Method: http.MethodGet, Path: []string{"x"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
