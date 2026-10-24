package productmaster_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/productmaster"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func clientFor(t *testing.T, h http.HandlerFunc) *productmaster.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	rest, err := fleetrest.New(fleetrest.Config{Dependency: "product-master", BaseURL: srv.URL, Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return productmaster.New(rest)
}

func TestResolveGtin_Answers(t *testing.T) {
	c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/gtins/00614141000036" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"gtin":"00614141000036","sku":"SKU-A","level":"EACH","unitsPerPack":1,"active":true,"effectiveFrom":"2026-10-06T20:00:00Z"}`))
	})
	got, err := c.ResolveGtin(context.Background(), "00614141000036")
	if err != nil || got != (contract.GtinResolution{GTIN: "00614141000036", SKU: "SKU-A", Level: "EACH", UnitsPerPack: 1, Active: true}) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestResolveGtin_Failures(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		is     error
	}{
		{"not found is a business fact", http.StatusNotFound, `{}`, contract.ErrGtinNotFound},
		{"an answer without a sku is not an answer", http.StatusOK, `{"gtin":"1","level":"EACH"}`, nil},
		{"an answer without a level is not an answer", http.StatusOK, `{"gtin":"1","sku":"S"}`, nil},
		{"garbage", http.StatusOK, `nope`, nil},
		{"a 400 is an error", http.StatusBadRequest, `{}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			_, err := c.ResolveGtin(context.Background(), "00614141000036")
			if err == nil || (tc.is != nil && !errors.Is(err, tc.is)) || (tc.is == nil && errors.Is(err, contract.ErrGtinNotFound)) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

type memCache struct {
	m      map[string]contract.GtinResolution
	reads  int
	writes int
	err    error
}

func (c *memCache) CachedGtin(_ context.Context, g string) (contract.GtinResolution, bool, error) {
	c.reads++
	r, ok := c.m[g]
	return r, ok, c.err
}

func (c *memCache) CacheGtin(_ context.Context, r contract.GtinResolution) error {
	c.writes++
	c.m[r.GTIN] = r
	return c.err
}

type countingResolver struct {
	calls int
	err   error
}

func (r *countingResolver) ResolveGtin(_ context.Context, g string) (contract.GtinResolution, error) {
	r.calls++
	if r.err != nil {
		return contract.GtinResolution{}, r.err
	}
	return contract.GtinResolution{GTIN: g, SKU: "SKU-A", Level: "EACH", UnitsPerPack: 1, Active: true}, nil
}

func TestCachingResolver_AsksProductMasterOncePerGtinAndNeverCachesAMiss(t *testing.T) {
	cache, inner := &memCache{m: map[string]contract.GtinResolution{}}, &countingResolver{}
	r := &productmaster.CachingResolver{Inner: inner, Cache: cache}
	for range 3 {
		if got, err := r.ResolveGtin(context.Background(), "G1"); err != nil || got.SKU != "SKU-A" {
			t.Fatalf("got %+v, %v", got, err)
		}
	}
	if inner.calls != 1 || cache.writes != 1 {
		t.Errorf("product-master asked %d times, cache written %d times, want 1 and 1", inner.calls, cache.writes)
	}
	inner.err = contract.ErrGtinNotFound
	if _, err := r.ResolveGtin(context.Background(), "G2"); !errors.Is(err, contract.ErrGtinNotFound) || cache.writes != 1 {
		t.Errorf("a miss was cached or swallowed: %v, writes=%d", err, cache.writes)
	}
}

func TestCachingResolver_CacheFailuresSurface(t *testing.T) {
	boom := errors.New("db down")
	r := &productmaster.CachingResolver{Inner: &countingResolver{}, Cache: &memCache{m: map[string]contract.GtinResolution{}, err: boom}}
	if _, err := r.ResolveGtin(context.Background(), "G1"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the cache failure", err)
	}
}
