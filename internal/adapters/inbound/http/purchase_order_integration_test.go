//go:build integration

package http_test

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// fileOrder does what the live retail-network adapter does when it first sees a
// purchase order: it records the poNumber -> networkRef map in Postgres and
// returns the minted ref, which the demand then carries into the use case.
func fileOrder(t *testing.T, a *restAPI, poNumber string) string {
	t.Helper()
	ref, err := retailnetwork.NewPGStore(a.pool).SavePurchaseOrder(context.Background(), retailnetwork.PurchaseOrder{
		PoNumber: poNumber, VendorID: "V-1", NodeID: "N-1",
		RequiredShipBy: restNow.Add(72 * time.Hour), AckBy: restNow.Add(24 * time.Hour),
		Lines: []retailnetwork.PurchaseOrderLine{{LineNo: 1, NetworkLineRef: "line-1", ListingID: "NPROD-A", NetworkProductID: "NPROD-A", Quantity: 2}},
	})
	if err != nil {
		t.Fatalf("file purchase order %s: %v", poNumber, err)
	}
	return string(ref)
}

// TestHTTP_PurchaseOrderLookupReadsTheOrderOverPostgres drives GET
// /purchase-orders/{poNumber} (ADR 0036) over the real stack: the map in
// Postgres, the order the real use case saved, and the sibling route's answer to
// compare with. Every state the route is asked about: an acknowledged order with
// its localOrderId, a NEW one and a REJECTED one without it, and the misses.
func TestHTTP_PurchaseOrderLookupReadsTheOrderOverPostgres(t *testing.T) {
	api := newRESTAPI(t, true)
	ctx := context.Background()

	// An acknowledged order: polled, held in order-management (om-1), settled.
	ackedRef := fileOrder(t, api, "PO-http-acked")
	api.gateway.Seed(seededDemand(ackedRef, "NPROD-A"))
	// A refused one: a product the ACL has no mapping for.
	rejectedRef := fileOrder(t, api, "PO-http-rejected")
	api.gateway.Seed(seededDemand(rejectedRef, "NPROD-MISSING"))
	api.poller.PollOnce(ctx)
	if _, err := api.reconcile.Execute(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// An order still NEW: saved by the repository, never answered.
	newRef := fileOrder(t, api, "PO-http-new")
	o, err := networkorder.Receive(shared.NetworkRef(newRef), "site-http-itest", restNow.Add(72*time.Hour),
		[]networkorder.Line{mustLine(t, "line-1", "NPROD-A", "SKU-A", 2)}, restNow)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := postgres.NewNetworkOrderRepo(api.pool).Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	for _, tc := range []struct {
		po, ref, state string
		local          bool
	}{
		{"PO-http-acked", ackedRef, "ACKNOWLEDGED", true},
		{"PO-http-rejected", rejectedRef, "REJECTED", false},
		{"PO-http-new", newRef, "NEW", false},
	} {
		status, header, order := api.do(t, http.MethodGet, "/purchase-orders/"+tc.po)
		if status != http.StatusOK {
			t.Fatalf("GET /purchase-orders/%s: status %d body %v", tc.po, status, order)
		}
		if ct := header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s Content-Type = %q, want application/json", tc.po, ct)
		}
		if order["state"] != tc.state || order["networkRef"] != tc.ref {
			t.Fatalf("%s = %v, want %s under %s", tc.po, order, tc.state, tc.ref)
		}
		if tc.local && order["localOrderId"] != "om-1" {
			t.Fatalf("%s = %v, want the order-management order om-1", tc.po, order)
		}
		if _, linked := order["localOrderId"]; linked != tc.local {
			t.Fatalf("%s: localOrderId present = %v, want %v (%v)", tc.po, linked, tc.local, order)
		}
		// The same order, field for field, as the sibling route reads it.
		_, _, byRef := api.do(t, http.MethodGet, "/network-orders/"+tc.ref)
		if !reflect.DeepEqual(order, byRef) {
			t.Fatalf("%s by number %v differs from by ref %v", tc.po, order, byRef)
		}
	}

	// The number is not the ref, in either direction.
	if status, _, problem := api.do(t, http.MethodGet, "/network-orders/PO-http-acked"); status != http.StatusNotFound || problemSlug(t, problem) != "network-order-not-found" {
		t.Fatalf("GET /network-orders/<poNumber>: status %d problem %v, want the sibling's own 404", status, problem)
	}
	if status, _, problem := api.do(t, http.MethodGet, "/purchase-orders/"+ackedRef); status != http.StatusNotFound || problemSlug(t, problem) != "purchase-order-not-found" {
		t.Fatalf("GET /purchase-orders/<networkRef>: status %d problem %v, want 404 purchase-order-not-found", status, problem)
	}
}

func TestHTTP_PurchaseOrderLookupMissesAndMalformedRequests(t *testing.T) {
	api := newRESTAPI(t, true)

	// Recorded in the map, no order stored yet (the poll saved the map row, the
	// use case has not run): the same 404 as an unknown number.
	fileOrder(t, api, "PO-http-early")

	for _, tc := range []struct {
		name, target, slug string
		status             int
	}{
		{"an unknown number", "/purchase-orders/PO-http-nope", "purchase-order-not-found", http.StatusNotFound},
		{"a number whose order is not stored yet", "/purchase-orders/PO-http-early", "purchase-order-not-found", http.StatusNotFound},
		{"a malformed number", "/purchase-orders/PO%20http", "invalid-purchase-order-number", http.StatusBadRequest},
		{"an escaped slash", "/purchase-orders/PO%2Fhttp", "invalid-purchase-order-number", http.StatusBadRequest},
		{"a query parameter", "/purchase-orders/PO-http-early?limit=10", "unknown-query-parameter", http.StatusBadRequest},
		{"a NUL byte in the path", "/purchase-orders/PO-4%0004", "resource-not-found", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, header, problem := api.do(t, http.MethodGet, tc.target)
			if status != tc.status || problemSlug(t, problem) != tc.slug {
				t.Fatalf("GET %s: status %d problem %v, want %d %s", tc.target, status, problem, tc.status, tc.slug)
			}
			if ct := header.Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("Content-Type = %q, want application/problem+json", ct)
			}
		})
	}
}
