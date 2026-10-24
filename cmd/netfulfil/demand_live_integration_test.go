//go:build integration

package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
)

// The live demand loop (ADR 0026) through the REAL entry point: run(ctx) with
// NETWORK_MODE=live against a real Postgres and a real Kafka (testcontainers),
// a stateful fake of the retail network's Vendor API (vendor_api_fake_*) and a
// fake order-management. Only the two external services are fakes; the file
// is named to sort before e2e_integration_test.go, whose last subtest stops the
// broker it shares with the tests that run after it.

const (
	liveVendorID = "V-LIVE"
	liveSite     = e2eSite
	liveListing  = "LST-LIVE-1"
	liveSKU      = "SKU-LIVE"
	livePO       = "PO-LIVE-1"
)

// liveRig is one test's world: the containers, the two fakes, a pool for
// assertions, and the configuration files the service reads.
type liveRig struct {
	env       e2eEnv
	network   *vendorFake
	planner   *planFake
	pool      *pgxpool.Pool
	nodesFile string
}

func startLiveRig(t *testing.T, ctx context.Context) *liveRig {
	t.Helper()
	env := startE2EContainers(t, ctx)
	env.dir = t.TempDir()
	rig := &liveRig{env: env, network: newVendorFake(t, liveVendorID), planner: newPlanFake(t)}
	rig.env.omURL = rig.planner.srv.URL

	products := `{"products":[{"networkProductId":"` + liveListing + `","sku":"` + liveSKU + `"}]}`
	if err := os.WriteFile(filepath.Join(env.dir, "products.json"), []byte(products), 0o600); err != nil {
		t.Fatal(err)
	}
	rig.nodesFile = filepath.Join(env.dir, "nodes.json")
	nodes := `{"nodes":[{"siteCode":"` + liveSite + `","nodeType":"FULFILLMENT_CENTER","subtype":"SORTABLE","regionId":"SP",` +
		`"timezone":"America/Sao_Paulo","postalCode":"01000-000","countryCode":"BR"}]}`
	if err := os.WriteFile(rig.nodesFile, []byte(nodes), 0o600); err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(ctx, env.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rig.pool = pool
	return rig
}

// start boots the real service in live mode on a fresh listen address and
// returns its stop func (cancel, then run()'s exit code).
func (r *liveRig) start(t *testing.T, ctx context.Context, overrides map[string]string) func() (int, error) {
	t.Helper()
	r.env.addr = e2eFreeAddr(t)
	vars := map[string]string{
		"NETWORK_MODE":             "live",
		"NETWORK_BASE_URL":         r.network.srv.URL,
		"NETWORK_VENDOR_ID":        liveVendorID,
		"NETWORK_NODES_FILE":       "",
		"NETWORK_SEED_FILE":        "", // refused in live mode
		"PRODUCT_TRANSLATION_FILE": filepath.Join(r.env.dir, "products.json"),
	}
	for k, v := range overrides {
		vars[k] = v
	}
	return startE2EServiceWith(t, ctx, r.env, vars)
}

func (r *liveRig) text(t *testing.T, ctx context.Context, query string, args ...any) string {
	t.Helper()
	var s string
	err := r.pool.QueryRow(ctx, query, args...).Scan(&s)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("%s: %v", query, err)
	}
	return s
}

func (r *liveRig) count(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	return e2eCount(t, ctx, r.pool, query, args...)
}

func mustStop(t *testing.T, stop func() (int, error)) {
	t.Helper()
	code, err := stop()
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("run() exit code = %d, want 0", code)
	}
}

// expectEqual fails when got != want.
func expectEqual[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// TestLiveDemand_PollToShipmentConfirmation is scenario 1: a purchase order is
// polled, held in order-management, acknowledged (202), its transaction
// succeeds, the hold is released, and an explicit shipment confirmation
// (ADR 0014) requests the label and confirms. The network sees exactly one
// acknowledgement and one confirmation.
//
// The site is registered as a node first, from NETWORK_NODES_FILE.
func TestLiveDemand_PollToShipmentConfirmation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startLiveRig(t, ctx)
	rig.network.issue(livePO, liveSite, liveListing, 2, time.Now().UTC())
	stop := rig.start(t, ctx, map[string]string{"NETWORK_NODES_FILE": rig.nodesFile})

	e2eWaitFor(t, ctx, 90*time.Second, "the purchase order is an ACKNOWLEDGED network order", func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM network_orders WHERE state = 'ACKNOWLEDGED'`) == 1
	})
	ref := rig.text(t, ctx, `SELECT network_ref FROM network_orders`)

	t.Run("the demand loop ran against the network", func(t *testing.T) { expectDemandLoopRan(t, ctx, rig, ref) })
	t.Run("one held order, one acknowledgement, then the release", func(t *testing.T) { expectOneAnswerThenRelease(t, ctx, rig, ref) })
	t.Run("an explicit shipment confirmation requests the label and confirms once", func(t *testing.T) { expectShipmentConfirmedOnce(t, ctx, rig, ref) })
	t.Run("the events leave through the outbox", func(t *testing.T) { expectOutboxEvents(t, ctx, rig, ref) })

	mustStop(t, stop)
}

// expectDemandLoopRan checks that the node was registered and mapped, the order was minted, and the network's own deadline kept.
func expectDemandLoopRan(t *testing.T, ctx context.Context, rig *liveRig, ref string) {
	t.Helper()
	if !strings.HasPrefix(ref, "NR-") || strings.Contains(ref, livePO) {
		t.Fatalf("network ref %q must be minted, never derived from the purchase order number", ref)
	}
	expectEqual(t, "node registrations", rig.network.writeCount("/vendor/nodes"), 1)
	if codes := rig.network.nodeCodes(); !slices.Equal(codes, []string{liveSite}) {
		t.Fatalf("nodes on the network = %v, want [%s]", codes, liveSite)
	}
	expectEqual(t, "mapped node", rig.text(t, ctx, `SELECT node_id FROM network_nodes WHERE site_code = $1`, liveSite), "N-1")
	expectEqual(t, "order site", rig.text(t, ctx, `SELECT site_id FROM network_orders WHERE network_ref = $1`, ref), liveSite)

	// The network's own deadline, not the local 24 hours.
	var ackBy time.Time
	if err := rig.pool.QueryRow(ctx, `SELECT acknowledge_by FROM network_orders WHERE network_ref = $1`, ref).Scan(&ackBy); err != nil {
		t.Fatal(err)
	}
	if d := ackBy.Sub(rig.network.ackByOf(livePO)).Abs(); d > time.Second {
		t.Fatalf("acknowledge_by %s is %s away from the network's ackBy %s", ackBy, d, rig.network.ackByOf(livePO))
	}
}

// expectOneAnswerThenRelease checks that one held order in order-management, one acknowledgement on the network, then the release.
func expectOneAnswerThenRelease(t *testing.T, ctx context.Context, rig *liveRig, ref string) {
	t.Helper()
	raises, orders, releases, cancels := rig.planner.snapshot()
	expectEqual(t, "order-management raise requests", raises, 1)
	expectEqual(t, "order-management orders", orders, 1)
	expectEqual(t, "order-management cancels", cancels, 0)
	if !slices.Equal(releases, []string{"om-live-1"}) {
		t.Fatalf("order-management releases = %v, want [om-live-1]", releases)
	}
	if lines := rig.planner.heldLines(); !slices.Equal(lines, []string{liveSKU + " x2"}) {
		t.Fatalf("order-management lines = %v, want [%s x2] (the listing translated to the SKU)", lines, liveSKU)
	}
	expectEqual(t, "acknowledgement requests", rig.network.requestCount(http.MethodPost, "/vendor/acknowledgements"), 1)
	expectEqual(t, "acknowledgement writes", rig.network.writeCount("/vendor/acknowledgements"), 1)
	expectEqual(t, "network order state", rig.network.orderState(livePO), "ACKNOWLEDGED")
	expectEqual(t, "acknowledgement ledger", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'ACKNOWLEDGEMENT' AND subject = $1`, ref), "SUCCESS")
	if body := rig.network.bodyOf("/vendor/acknowledgements"); !strings.Contains(body, `"decision":"ACCEPT"`) || !strings.Contains(body, `"quantity":2`) {
		t.Fatalf("acknowledgement body = %s, want an ACCEPT echoing the ordered quantity", body)
	}
}

// expectShipmentConfirmedOnce checks that an explicit shipment confirmation requests the label and confirms once, and a replay sends nothing.
func expectShipmentConfirmedOnce(t *testing.T, ctx context.Context, rig *liveRig, ref string) {
	t.Helper()
	confirm := rig.env.url("/network-orders/" + ref + "/shipment-confirmation")
	if code, body := e2eDo(t, ctx, http.MethodPost, confirm, ""); code != http.StatusNoContent {
		t.Fatalf("confirm shipment = %d %s, want 204", code, body)
	}
	expectEqual(t, "CONFIRMED network orders", rig.count(t, ctx, `SELECT count(*) FROM network_orders WHERE network_ref = $1 AND state = 'CONFIRMED'`, ref), 1)
	expectEqual(t, "label requests", rig.network.requestCount(http.MethodPost, "/vendor/shipping-labels"), 1)
	expectEqual(t, "shipment confirmation requests", rig.network.requestCount(http.MethodPost, "/vendor/shipment-confirmations"), 1)
	expectEqual(t, "shipment confirmation writes", rig.network.writeCount("/vendor/shipment-confirmations"), 1)
	if body := rig.network.bodyOf("/vendor/shipping-labels"); !strings.Contains(body, `"packages":[{"packageNo":1}]`) {
		t.Fatalf("label body = %s, want exactly one package", body)
	}
	if body := rig.network.bodyOf("/vendor/shipment-confirmations"); !strings.Contains(body, `"trackingNumbers":["TRK-`+livePO+`-1"]`) {
		t.Fatalf("shipment confirmation body = %s, want the labelled tracking number", body)
	}
	e2eWaitFor(t, ctx, 15*time.Second, "the network shipped the order", func() bool { return rig.network.orderState(livePO) == "SHIPPED" })
	expectEqual(t, "labels persisted", rig.count(t, ctx, `SELECT count(*) FROM network_labels WHERE network_ref = $1`, ref), 1)
	expectEqual(t, "confirmation ledger", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'SHIPMENT_CONFIRMATION' AND subject = $1`, ref), "SUCCESS")

	// A replay is a no-op for the network.
	if code, body := e2eDo(t, ctx, http.MethodPost, confirm, ""); code != http.StatusNoContent {
		t.Fatalf("replayed confirm = %d %s, want 204", code, body)
	}
	expectEqual(t, "label requests after a replay", rig.network.requestCount(http.MethodPost, "/vendor/shipping-labels"), 1)
	expectEqual(t, "confirmation requests after a replay", rig.network.requestCount(http.MethodPost, "/vendor/shipment-confirmations"), 1)
}

// expectOutboxEvents checks that the events leave through the outbox, keyed by the network ref.
func expectOutboxEvents(t *testing.T, ctx context.Context, rig *liveRig, ref string) {
	t.Helper()
	want := []string{"NetworkOrderReceived|" + ref, "NetworkOrderSubmitted|" + ref, "NetworkOrderAcknowledged|" + ref, "NetworkOrderShipmentConfirmed|" + ref}
	events := e2eEventsOn(t, ctx, rig.env.brokers, outboundkafka.Topic, want...)
	for _, w := range want {
		expectEqual(t, w+" key", events[w].Key, ref)
	}
}

// TestLiveDemand_RestartAfterAcknowledgementSubmitted is scenario 2, the
// crash: the service is stopped after the acknowledgement was submitted and
// before any reconciliation, and started again on the SAME Postgres. The
// restart must not raise a second order in order-management and must not send
// a second acknowledgement: the persisted transaction is what it follows.
//
// The site is already a node on the network (adoption, no node file). The
// network keeps the acknowledgement's transaction PROCESSING until released,
// so the order is still SUBMITTED when the service stops and when it comes back.
func TestLiveDemand_RestartAfterAcknowledgementSubmitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	rig := startLiveRig(t, ctx)
	rig.network.addNode(liveSite)
	rig.network.holdAckTransactions()
	rig.network.issue(livePO, liveSite, liveListing, 3, time.Now().UTC())

	// First run: the poller's immediate pass answers the order; the reconcile
	// ticker (POLL_INTERVAL) never fires before the stop.
	stop := rig.start(t, ctx, map[string]string{"POLL_INTERVAL": "1h"})
	e2eWaitFor(t, ctx, 90*time.Second, "the order is SUBMITTED with its acknowledgement transaction stored", func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM network_orders WHERE state = 'SUBMITTED'`) == 1 &&
			rig.count(t, ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'ACKNOWLEDGEMENT' AND transaction_id IS NOT NULL`) == 1
	})
	ref := rig.text(t, ctx, `SELECT network_ref FROM network_orders`)
	txID := rig.text(t, ctx, `SELECT transaction_id FROM network_submissions WHERE kind = 'ACKNOWLEDGEMENT' AND subject = $1`, ref)
	txPath := "/vendor/transactions/" + txID
	mustStop(t, stop)

	t.Run("at the stop the answer was sent and never reconciled", func(t *testing.T) {
		expectEqual(t, "order state", rig.text(t, ctx, `SELECT state FROM network_orders WHERE network_ref = $1`, ref), "SUBMITTED")
		expectEqual(t, "acknowledgement requests", rig.network.requestCount(http.MethodPost, "/vendor/acknowledgements"), 1)
		expectEqual(t, "reads of the transaction", rig.network.requestCount(http.MethodGet, txPath), 0)
		raises, orders, releases, _ := rig.planner.snapshot()
		expectEqual(t, "order-management raise requests", raises, 1)
		expectEqual(t, "order-management orders", orders, 1)
		expectEqual(t, "order-management releases", len(releases), 0)
	})
	receivedBefore := rig.count(t, ctx, `SELECT count(*) FROM outbox_events WHERE event_type LIKE '%NetworkOrderReceived'`)
	submittedBefore := rig.count(t, ctx, `SELECT count(*) FROM outbox_events WHERE event_type LIKE '%NetworkOrderSubmitted'`)

	// Second run on the same database. The network still lists the purchase
	// order as ISSUED, so the first poll delivers it again.
	stop = rig.start(t, ctx, map[string]string{"POLL_INTERVAL": "500ms"})
	e2eWaitFor(t, ctx, 60*time.Second, "the restarted service reads the stored transaction (still PROCESSING)", func() bool {
		return rig.network.requestCount(http.MethodGet, txPath) >= 2
	})
	expectEqual(t, "order state while the transaction is PROCESSING", rig.text(t, ctx, `SELECT state FROM network_orders WHERE network_ref = $1`, ref), "SUBMITTED")
	expectEqual(t, "acknowledgement requests after the restart", rig.network.requestCount(http.MethodPost, "/vendor/acknowledgements"), 1)

	rig.network.releaseAcks()
	e2eWaitFor(t, ctx, 60*time.Second, "the order is ACKNOWLEDGED after the transaction succeeded", func() bool {
		return rig.count(t, ctx, `SELECT count(*) FROM network_orders WHERE state = 'ACKNOWLEDGED'`) == 1
	})
	e2eWaitFor(t, ctx, 30*time.Second, "the held order is released", func() bool {
		_, _, releases, _ := rig.planner.snapshot()
		return len(releases) == 1
	})

	t.Run("one order-management order and one acknowledgement in total", func(t *testing.T) {
		raises, orders, releases, cancels := rig.planner.snapshot()
		expectEqual(t, "order-management raise requests", raises, 1)
		expectEqual(t, "order-management orders", orders, 1)
		expectEqual(t, "order-management cancels", cancels, 0)
		if !slices.Equal(releases, []string{"om-live-1"}) {
			t.Fatalf("order-management releases = %v, want [om-live-1]", releases)
		}
		expectEqual(t, "acknowledgement requests", rig.network.requestCount(http.MethodPost, "/vendor/acknowledgements"), 1)
		expectEqual(t, "acknowledgement writes", rig.network.writeCount("/vendor/acknowledgements"), 1)
		expectEqual(t, "network order state", rig.network.orderState(livePO), "ACKNOWLEDGED")
		expectEqual(t, "network orders", rig.count(t, ctx, `SELECT count(*) FROM network_orders`), 1)
		expectEqual(t, "purchase order maps", rig.count(t, ctx, `SELECT count(*) FROM network_purchase_orders`), 1)
		expectEqual(t, "acknowledgement ledger rows", rig.count(t, ctx, `SELECT count(*) FROM network_submissions WHERE kind = 'ACKNOWLEDGEMENT'`), 1)
		expectEqual(t, "acknowledgement ledger", rig.text(t, ctx, `SELECT status FROM network_submissions WHERE kind = 'ACKNOWLEDGEMENT'`), "SUCCESS")
		expectEqual(t, "node registrations", rig.network.writeCount("/vendor/nodes"), 0)
	})

	t.Run("the restart published no second Received or Submitted event", func(t *testing.T) {
		expectEqual(t, "NetworkOrderReceived rows", rig.count(t, ctx, `SELECT count(*) FROM outbox_events WHERE event_type LIKE '%NetworkOrderReceived'`), receivedBefore)
		expectEqual(t, "NetworkOrderSubmitted rows", rig.count(t, ctx, `SELECT count(*) FROM outbox_events WHERE event_type LIKE '%NetworkOrderSubmitted'`), submittedBefore)
		events := e2eEventsOn(t, ctx, rig.env.brokers, outboundkafka.Topic, "NetworkOrderAcknowledged|"+ref)
		expectEqual(t, "NetworkOrderAcknowledged key", events["NetworkOrderAcknowledged|"+ref].Key, ref)
	})

	mustStop(t, stop)
}
