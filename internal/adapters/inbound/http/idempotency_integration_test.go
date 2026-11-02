//go:build integration

package http_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
)

// TestHTTP_ShipmentConfirmationIdempotencyKey drives the Idempotency-Key
// middleware (fleet REST guideline section 2) over the real router and the
// real Postgres store: missing key 400, a 4xx cached and replayed, fresh key
// 204, byte-identical replay with Idempotency-Replayed, same key on another
// request 409, and N concurrent same-key requests producing one effect.
func TestHTTP_ShipmentConfirmationIdempotencyKey(t *testing.T) {
	api := newRESTAPI(t, true)
	ctx := context.Background()
	const path = "/network-orders/po-http-idem/shipment-confirmation"
	if _, err := api.receive.Execute(ctx, seededDemand("po-http-idem", "NPROD-A")); err != nil {
		t.Fatalf("receive: %v", err)
	}

	if status, _, problem := api.doKeyed(t, http.MethodPost, path, ""); status != http.StatusBadRequest || problemSlug(t, problem) != "idempotency-key-required" {
		t.Fatalf("missing key: %d %v", status, problem)
	}

	// A 4xx is deterministic: stored, and replayed even after the state moved.
	if status, _, problem := api.doKeyed(t, http.MethodPost, path, "early"); status != http.StatusConflict || problemSlug(t, problem) != "confirm-before-acknowledge" {
		t.Fatalf("early confirm: %d %v", status, problem)
	}
	if _, err := api.reconcile.Execute(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	status, hdr, problem := api.doKeyed(t, http.MethodPost, path, "early")
	if status != http.StatusConflict || hdr.Get("Idempotency-Replayed") != "true" || problemSlug(t, problem) != "confirm-before-acknowledge" {
		t.Fatalf("4xx replay: %d replayed=%q %v", status, hdr.Get("Idempotency-Replayed"), problem)
	}

	// N concurrent requests with one fresh key: one execution, the rest replay.
	const n = 8
	codes := make([]int, n)
	replayed := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, h, _ := api.doKeyed(t, http.MethodPost, path, "confirm-1")
			codes[i], replayed[i] = c, h.Get("Idempotency-Replayed")
		}(i)
	}
	wg.Wait()
	fresh := 0
	for i := range codes {
		if codes[i] != http.StatusNoContent {
			t.Fatalf("concurrent request %d: %d", i, codes[i])
		}
		if replayed[i] == "" {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d first executions among %d same-key requests, want exactly 1", fresh, n)
	}
	if got := countRows(api.outboxEventTypes(t), "NetworkOrderShipmentConfirmed"); got != 2 {
		t.Fatalf("NetworkOrderShipmentConfirmed outbox rows = %d, want 2 (one event: integration + analytics)", got)
	}

	// Byte-identical replay afterwards, and the same key on another path is 409.
	if status, hdr, _ := api.doKeyed(t, http.MethodPost, path, "confirm-1"); status != http.StatusNoContent || hdr.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay: %d replayed=%q", status, hdr.Get("Idempotency-Replayed"))
	}
	status, _, problem = api.doKeyed(t, http.MethodPost, "/network-orders/po-other/shipment-confirmation", "confirm-1")
	if status != http.StatusConflict || problemSlug(t, problem) != "idempotency-key-reused" {
		t.Fatalf("reused key: %d %v", status, problem)
	}
}
