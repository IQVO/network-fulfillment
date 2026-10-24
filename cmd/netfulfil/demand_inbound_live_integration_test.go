//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The inbound intake loop (ADR 0028) through the REAL entry point: run(ctx) with
// NETWORK_MODE=live against a real Postgres and Kafka (testcontainers), the
// Vendor API's inbound half, inbound-receiving and product-master as httptest
// fakes. Named to sort before e2e_integration_test.go (its last subtest stops
// the broker the tests share).

const (
	inSeller = "S-100"
	inPallet = "006141410000000036"
	inCartA  = "006141410000000012"
	inCartB  = "006141410000000029"
	inGtinA  = "00614141000036"
	inGtinB  = "00614141000043"
)

var inShipped = time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC)

type inboundRig struct {
	*liveRig
	inbound *inboundNetwork
	receive *fakeInboundReceiving
	master  *fakeProductMaster
}

func startInboundRig(t *testing.T, ctx context.Context) *inboundRig {
	t.Helper()
	live := startLiveRig(t, ctx)
	rig := &inboundRig{liveRig: live, inbound: live.inbound, receive: live.receive, master: live.master}
	rig.network.addNode(liveSite) // the site is already a node: adopted by its vendorNodeCode
	return rig
}

func (r *inboundRig) start(t *testing.T, ctx context.Context, overrides map[string]string) func() (int, error) {
	t.Helper()
	return r.liveRig.start(t, ctx, overrides)
}

func shipmentTree(id string) fakeShipment {
	return fakeShipment{
		ID: id, Seller: inSeller, NodeCode: liveSite, ShippedAt: inShipped,
		Pallets: []map[string]any{{"sscc": inPallet, "cartons": []map[string]any{{
			"sscc": inCartA, "parentSscc": inPallet, "kind": "HOMOGENEOUS_CASE", "grossWeightG": 4200,
			"dimsMm":   map[string]any{"l": 400, "w": 300, "h": 200},
			"contents": []map[string]any{{"gtin": inGtinA, "qtyEaches": 12, "lot": "L-77", "expiry": "2027-03-01"}},
		}}}},
		Cartons: []map[string]any{{
			"sscc": inCartB, "kind": "MIXED", "contents": []map[string]any{{"gtin": inGtinA, "qtyEaches": 6}, {"gtin": inGtinB, "qtyEaches": 4}},
		}},
	}
}

func (r *inboundRig) shipmentStatus(t *testing.T, ctx context.Context, ref string) string {
	t.Helper()
	return r.text(t, ctx, `SELECT status FROM inbound_shipments WHERE shipment_ref = $1`, ref)
}

// TestInboundIntake_PollToAsnRegistration is the whole intake: a shipment tree is
// polled, its GTINs resolved once, the ASN registered once with the hierarchical
// cartons and the per-SKU roll-up, and a restart or a replay changes nothing.
func TestInboundIntake_PollToAsnRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startInboundRig(t, ctx)
	rig.master.declare(inGtinA, "SKU-A", "EACH", 1)
	rig.master.declare(inGtinB, "SKU-B", "EACH", 1)
	rig.inbound.ship(shipmentTree("SHP-1"))
	stop := rig.start(t, ctx, nil)

	e2eWaitFor(t, ctx, 90*time.Second, "SHP-1 is REGISTERED", func() bool { return rig.shipmentStatus(t, ctx, "SHP-1") == "REGISTERED" })

	t.Run("one ASN, deterministic number and key, hierarchical cartons, rolled-up lines", func(t *testing.T) {
		expectEqual(t, "ASNs registered", rig.receive.asnCount(), 1)
		expectEqual(t, "asn number stored", rig.text(t, ctx, `SELECT asn_number FROM inbound_shipments WHERE shipment_ref = 'SHP-1'`), "NF-SHP-1")
		expectEqual(t, "Idempotency-Key", rig.receive.key("NF-SHP-1"), "nf-asn-NF-SHP-1")
		var asn struct {
			SupplierRef string `json:"supplierRef"`
			OwnerRef    string `json:"ownerRef"`
			Lines       []struct {
				LineNo      int    `json:"lineNo"`
				SKU         string `json:"sku"`
				ExpectedQty int64  `json:"expectedQty"`
			} `json:"lines"`
			Cartons []struct {
				SSCC       string `json:"sscc"`
				ParentSSCC string `json:"parentSscc"`
				Kind       string `json:"kind"`
				Contents   []struct {
					LineNo int    `json:"lineNo"`
					SKU    string `json:"sku"`
					GTIN   string `json:"gtin"`
					Qty    int64  `json:"qty"`
					Lot    string `json:"lot"`
					Expiry string `json:"expiry"`
				} `json:"contents"`
			} `json:"cartons"`
		}
		if err := json.Unmarshal([]byte(rig.receive.body("NF-SHP-1")), &asn); err != nil {
			t.Fatal(err)
		}
		expectEqual(t, "ownerRef", asn.OwnerRef, inSeller)
		expectEqual(t, "lines", len(asn.Lines), 2)
		expectEqual(t, "line 1 SKU-A in eaches (12 + 6)", fmt.Sprintf("%s:%d", asn.Lines[0].SKU, asn.Lines[0].ExpectedQty), "SKU-A:18")
		expectEqual(t, "line 2 SKU-B", asn.Lines[1].SKU, "SKU-B")
		expectEqual(t, "line 2 qty", asn.Lines[1].ExpectedQty, int64(4))
		expectEqual(t, "cartons", len(asn.Cartons), 2)
		expectEqual(t, "pallet carton parent", asn.Cartons[0].ParentSSCC, inPallet)
		expectEqual(t, "pallet carton lot", asn.Cartons[0].Contents[0].Lot, "L-77")
		expectEqual(t, "mixed carton kind", asn.Cartons[1].Kind, "MIXED")
		expectEqual(t, "mixed carton second content line", asn.Cartons[1].Contents[1].LineNo, 2)
	})

	t.Run("GTINs are resolved once and cached in Postgres", func(t *testing.T) {
		expectEqual(t, "product-master asked for the each GTIN A", rig.master.timesAsked(inGtinA), 1)
		expectEqual(t, "product-master asked for the each GTIN B", rig.master.timesAsked(inGtinB), 1)
		expectEqual(t, "cached GTINs", rig.count(t, ctx, `SELECT count(*) FROM gtin_sku_cache`), 2)
	})

	t.Run("the cursor is the shipment's shippedAt and later polls ask from it less the overlap", func(t *testing.T) {
		expectEqual(t, "cursor", rig.text(t, ctx, `SELECT to_char(shipped_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') FROM inbound_poll_cursors WHERE site_id = $1`, liveSite), "2026-10-24T13:00:00")
		e2eWaitFor(t, ctx, 30*time.Second, "a poll that carries the cursor minus the 2 minute overlap", func() bool {
			for _, q := range rig.inbound.pollQueries() {
				if strings.Contains(q, "createdAfter=2026-10-24T12%3A58%3A00Z") {
					return true
				}
			}
			return false
		})
	})

	t.Run("a restart registers nothing twice", func(t *testing.T) {
		mustStop(t, stop)
		before := rig.receive.requestCount()
		stop = rig.start(t, ctx, nil)
		e2eWaitFor(t, ctx, 60*time.Second, "two more inbound polls after the restart", func() bool {
			return len(rig.inbound.pollQueries()) >= 6
		})
		expectEqual(t, "POST /asns after the restart", rig.receive.requestCount(), before)
		expectEqual(t, "ASNs", rig.receive.asnCount(), 1)
		expectEqual(t, "recorded shipments", rig.count(t, ctx, `SELECT count(*) FROM inbound_shipments`), 1)
	})

	mustStop(t, stop)
}

// TestInboundIntake_UnknownGtinWaitsThenRegisters: a shipment with a GTIN
// product-master does not know is a recorded failure with a backoff, never a
// guess; once the product is declared the next due attempt registers it.
func TestInboundIntake_UnknownGtinWaitsThenRegisters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startInboundRig(t, ctx)
	rig.master.declare(inGtinA, "SKU-A", "EACH", 1) // GTIN B is not declared yet
	rig.inbound.ship(shipmentTree("SHP-2"))
	stop := rig.start(t, ctx, nil)

	e2eWaitFor(t, ctx, 90*time.Second, "SHP-2 recorded as a failure with its reason", func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM inbound_shipments WHERE shipment_ref = 'SHP-2' AND status = 'PENDING' AND attempts >= 1 AND last_error LIKE '%GTIN not found%'`) == 1
	})
	expectEqual(t, "ASNs registered while the product is unknown", rig.receive.asnCount(), 0)
	if !strings.Contains(rig.text(t, ctx, `SELECT last_error FROM inbound_shipments WHERE shipment_ref = 'SHP-2'`), inGtinB) {
		t.Fatal("the failure does not name the unknown GTIN")
	}
	if rig.count(t, ctx, `SELECT count(*) FROM inbound_shipments WHERE shipment_ref = 'SHP-2' AND next_attempt_at > now() + interval '30 seconds'`) != 1 {
		t.Fatal("the retry was not scheduled with a backoff")
	}

	rig.master.declare(inGtinB, "SKU-B", "EACH", 1)
	if _, err := rig.pool.Exec(ctx, `UPDATE inbound_shipments SET next_attempt_at = now() WHERE shipment_ref = 'SHP-2'`); err != nil {
		t.Fatal(err)
	}
	e2eWaitFor(t, ctx, 60*time.Second, "SHP-2 registers once the product is declared", func() bool { return rig.shipmentStatus(t, ctx, "SHP-2") == "REGISTERED" })
	expectEqual(t, "ASNs", rig.receive.asnCount(), 1)
	mustStop(t, stop)
}

// TestInboundIntake_AVerdictFromInboundReceivingParksTheShipment: a refusal about
// the data is recorded as REJECTED and not retried.
func TestInboundIntake_AVerdictFromInboundReceivingParksTheShipment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startInboundRig(t, ctx)
	rig.master.declare(inGtinA, "SKU-A", "EACH", 1)
	rig.master.declare(inGtinB, "SKU-B", "EACH", 1)
	rig.receive.refuse["NF-SHP-3"] = "duplicate-sscc"
	rig.inbound.ship(shipmentTree("SHP-3"))
	stop := rig.start(t, ctx, nil)

	e2eWaitFor(t, ctx, 90*time.Second, "SHP-3 is REJECTED", func() bool { return rig.shipmentStatus(t, ctx, "SHP-3") == "REJECTED" })
	asked, polls := rig.receive.requestCount(), len(rig.inbound.pollQueries())
	e2eWaitFor(t, ctx, 60*time.Second, "three more intake passes", func() bool { return len(rig.inbound.pollQueries()) >= polls+3 })
	expectEqual(t, "attempts after the verdict", rig.receive.requestCount(), asked)
	mustStop(t, stop)
}
