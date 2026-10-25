package retailnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

const (
	labelPath   = "/vendor/shipping-labels"
	confirmPath = "/vendor/shipment-confirmations"
)

var _ ports.NetworkGateway = (*LiveGateway)(nil)

// answerLabelsWith replaces the network's answer to a label request.
func answerLabelsWith(resp labelResponseDTO) func(http.ResponseWriter, *http.Request) bool {
	return func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPost || r.URL.Path != labelPath {
			return false
		}
		ok(w, resp)
		return true
	}
}

func (f *liveFixture) labelled(t *testing.T, ref shared.NetworkRef, packages int) []contract.LabelResult {
	t.Helper()
	got, err := f.g.RequestLabel(context.Background(), ref, packages)
	if err != nil {
		t.Fatalf("RequestLabel: %v", err)
	}
	return got
}

func (f *liveFixture) confirmRow(ref shared.NetworkRef) Submission {
	row, _ := f.store.row(KindShipmentConfirmation, string(ref))
	return row
}

// ---- RequestLabel ----

func TestLive_RequestLabelAsksForEveryPackageAndPersistsTheLabels(t *testing.T) {
	f, ref := orderFixture(t, nil)

	got := f.labelled(t, ref, 2)

	posts := f.requestsTo(http.MethodPost, labelPath)
	if len(posts) != 1 || posts[0].IdemKey != "nf-label-"+string(ref) {
		t.Fatalf("posts = %+v, want one with the deterministic key nf-label-<ref>", posts)
	}
	var body labelRequestDTO
	_ = json.Unmarshal([]byte(posts[0].Body), &body)
	if body.VendorID != "V-WS" || body.PoNumber != "PO-1" || len(body.Packages) != 2 || body.Packages[0].PackageNo != 1 || body.Packages[1].PackageNo != 2 {
		t.Errorf("body = %+v, want packages 1 and 2 for PO-1 and nothing else", body)
	}
	want := []contract.LabelResult{
		{LabelRef: "LBL-PO-1-1", TrackingNumber: "TRK-PO-1-1", Carrier: "CARRIER-X"},
		{LabelRef: "LBL-PO-1-2", TrackingNumber: "TRK-PO-1-2", Carrier: "CARRIER-X"},
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("labels = %+v, want %+v", got, want)
	}
	stored, _ := f.store.Labels(context.Background(), ref)
	if len(stored) != 2 || stored[1].PackageNo != 2 || stored[1].TrackingNumber != "TRK-PO-1-2" {
		t.Errorf("persisted labels = %+v, want both with their package numbers", stored)
	}
}

func TestLive_RequestLabelIsIdempotentPerRef(t *testing.T) {
	f, ref := orderFixture(t, nil)
	first := f.labelled(t, ref, 1)
	second := f.labelled(t, ref, 1)
	third := f.labelled(t, ref, 3) // the first count wins; the network would call a new body on the same key a reuse

	if n := len(f.requestsTo(http.MethodPost, labelPath)); n != 1 || f.sim.writes(labelPath) != 1 {
		t.Fatalf("POST x%d, network issued %d; want one request for three calls", n, f.sim.writes(labelPath))
	}
	if len(second) != 1 || second[0] != first[0] || len(third) != 1 {
		t.Errorf("second = %+v third = %+v, want the persisted label again", second, third)
	}
}

func TestLive_RequestLabelRefusesBadInputBeforeAnyIO(t *testing.T) {
	f, ref := orderFixture(t, nil)
	before := len(f.fake.requests())

	if _, err := f.g.RequestLabel(context.Background(), ref, 0); !errors.Is(err, ErrBadPackageCount) {
		t.Errorf("0 packages: err = %v, want ErrBadPackageCount", err)
	}
	if _, err := f.g.RequestLabel(context.Background(), "NR-nobody", 1); !errors.Is(err, ErrUnknownRef) {
		t.Errorf("unknown ref: err = %v, want ErrUnknownRef", err)
	}
	if len(f.fake.requests()) != before {
		t.Error("a bad request reached the network")
	}
}

func TestLive_RequestLabelRejectsAnAnswerThatDoesNotMatchTheRequest(t *testing.T) {
	cases := map[string]labelResponseDTO{
		"fewer labels than packages": {PoNumber: "PO-1", Labels: []labelDTO{{PackageNo: 1, LabelRef: "L", TrackingNumber: "T", Carrier: "C"}}},
		"a label with no tracking":   {PoNumber: "PO-1", Labels: []labelDTO{{PackageNo: 1, LabelRef: "L", Carrier: "C"}, {PackageNo: 2, LabelRef: "L2", TrackingNumber: "T2", Carrier: "C"}}},
	}
	for name, resp := range cases {
		t.Run(name, func(t *testing.T) {
			f, ref := orderFixture(t, nil)
			f.sim.inject = answerLabelsWith(resp)
			if _, err := f.g.RequestLabel(context.Background(), ref, 2); err == nil {
				t.Fatal("want an error: a shipment cannot be confirmed with labels that are not the ones asked for")
			}
			if stored, _ := f.store.Labels(context.Background(), ref); len(stored) != 0 {
				t.Errorf("persisted %+v from a bad answer", stored)
			}
		})
	}
}

func TestLive_RequestLabelNumbersPackagesByPositionWhenTheNetworkOmitsThem(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = answerLabelsWith(labelResponseDTO{PoNumber: "PO-1", Labels: []labelDTO{
		{LabelRef: "L-A", TrackingNumber: "T-A", Carrier: "C"}, {LabelRef: "L-B", TrackingNumber: "T-B", Carrier: "C"},
	}})
	f.labelled(t, ref, 2)

	stored, _ := f.store.Labels(context.Background(), ref)
	if len(stored) != 2 || stored[0].PackageNo != 1 || stored[1].PackageNo != 2 || stored[1].TrackingNumber != "T-B" {
		t.Fatalf("stored = %+v, want packages 1 and 2 in the order the network gave them", stored)
	}
}

func TestLive_RequestLabelSurvivesA5xxAndATimeoutWithOneIssue(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.callTimeout = 50 * time.Millisecond } })
	fail := failFirst(http.MethodPost, labelPath, 1, http.StatusServiceUnavailable, nil)
	slow := slowFirst(http.MethodPost, labelPath, 1, 300*time.Millisecond) // outlives the call timeout; the network still processes it
	f.sim.inject = func(w http.ResponseWriter, r *http.Request) bool { return fail(w, r) || slow(w, r) }

	got := f.labelled(t, ref, 1)

	if len(got) != 1 || f.sim.writes(labelPath) != 1 {
		t.Fatalf("labels %v, network issued %d; want one label set however many attempts it took", got, f.sim.writes(labelPath))
	}
	posts := f.requestsTo(http.MethodPost, labelPath)
	for i, p := range posts {
		if p.Body != posts[0].Body || p.IdemKey != posts[0].IdemKey {
			t.Errorf("attempt %d changed the body or key", i+1)
		}
	}
}

func TestLive_RequestLabelHonoursRetryAfterAndKeepsAnUnacceptedOrderRetryable(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = failFirst(http.MethodPost, labelPath, 1, http.StatusTooManyRequests, map[string]string{"Retry-After": "5"})
	f.labelled(t, ref, 1)
	if w := f.sl.recorded(); len(w) != 1 || w[0] != 5*time.Second {
		t.Fatalf("waits = %v, want the 5s Retry-After", w)
	}

	g, other := orderFixture(t, nil)
	g.sim.inject = refuseFirst(http.MethodPost, labelPath, 1, http.StatusConflict, "label-before-acceptance")
	if _, err := g.g.RequestLabel(context.Background(), other, 1); Problem(err) != "label-before-acceptance" {
		t.Fatalf("err = %v, want label-before-acceptance", err)
	}
	if stored, _ := g.store.Labels(context.Background(), other); len(stored) != 0 {
		t.Fatal("a refused label request persisted labels")
	}
	g.labelled(t, other, 1) // once the order is accepted the same request goes through
}

// ---- SubmitShipmentConfirmation ----

func TestLive_ShipmentConfirmationCarriesTheLabelledTrackingNumbersAndUTCShippedAt(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.labelled(t, ref, 2)
	brt := time.FixedZone("BRT", -3*3600)
	shippedAt := time.Date(2026, 10, 25, 15, 30, 0, 0, brt)

	if err := f.g.SubmitShipmentConfirmation(context.Background(), ref, shippedAt); err != nil {
		t.Fatalf("SubmitShipmentConfirmation: %v", err)
	}

	posts := f.requestsTo(http.MethodPost, confirmPath)
	if len(posts) != 1 || posts[0].IdemKey != "nf-ship-"+string(ref) {
		t.Fatalf("posts = %+v, want one with the deterministic key nf-ship-<ref>", posts)
	}
	var body shipmentConfirmationRequestDTO
	_ = json.Unmarshal([]byte(posts[0].Body), &body)
	if body.PoNumber != "PO-1" || body.VendorID != "V-WS" || !body.ShippedAt.Equal(shippedAt) ||
		len(body.TrackingNumbers) != 2 || body.TrackingNumbers[0] != "TRK-PO-1-1" || body.TrackingNumbers[1] != "TRK-PO-1-2" {
		t.Errorf("body = %+v, want exactly the persisted labels' tracking numbers in package order", body)
	}
	if !strings.Contains(posts[0].Body, `"shippedAt":"2026-10-25T18:30:00Z"`) {
		t.Errorf("body %s: shippedAt must be sent in UTC", posts[0].Body)
	}
	if row := f.confirmRow(ref); row.TransactionID == "" || row.Status != StatusSuccess {
		t.Errorf("ledger row = %+v, want the transaction id and the settled status recorded for operators", row)
	}
}

func TestLive_TheFirstShippedAtWinsAndASecondCallSendsNothing(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.labelled(t, ref, 1)
	first := time.Date(2026, 10, 25, 15, 0, 0, 0, time.UTC)

	for _, at := range []time.Time{first, first.Add(2 * time.Hour), first.Add(5 * time.Hour)} {
		if err := f.g.SubmitShipmentConfirmation(context.Background(), ref, at); err != nil {
			t.Fatalf("SubmitShipmentConfirmation(%s): %v", at, err)
		}
	}

	if n := len(f.requestsTo(http.MethodPost, confirmPath)); n != 1 || f.sim.writes(confirmPath) != 1 {
		t.Fatalf("POST x%d, network created %d; want one confirmation for three calls", n, f.sim.writes(confirmPath))
	}
	if !strings.Contains(string(f.confirmRow(ref).Body), `"shippedAt":"2026-10-25T15:00:00Z"`) {
		t.Errorf("stored body = %s, want the first shippedAt", f.confirmRow(ref).Body)
	}
}

func TestLive_ShipmentConfirmationWithoutALabelIsAnErrorBeforeAnyIO(t *testing.T) {
	f, ref := orderFixture(t, nil)
	before := len(f.fake.requests())

	err := f.g.SubmitShipmentConfirmation(context.Background(), ref, t0)

	if !errors.Is(err, ErrNoLabel) {
		t.Fatalf("err = %v, want ErrNoLabel", err)
	}
	if len(f.fake.requests()) != before || f.confirmRow(ref).Kind != "" {
		t.Error("a confirmation without a label reached the network or left a ledger row")
	}
}

func TestLive_AShipmentConfirmationThatNeverGotThroughIsResentWithTheFirstShippedAt(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 2 } })
	f.labelled(t, ref, 1)
	f.sim.inject = failFirst(http.MethodPost, confirmPath, 99, http.StatusBadGateway, nil)
	first := time.Date(2026, 10, 25, 15, 0, 0, 0, time.UTC)
	if err := f.g.SubmitShipmentConfirmation(context.Background(), ref, first); err == nil {
		t.Fatal("an outage must be an error")
	}
	if row := f.confirmRow(ref); row.Status != StatusSending || row.TransactionID != "" {
		t.Fatalf("row = %+v, want SENDING after the outage", row)
	}

	f.sim.inject = nil
	if err := f.g.SubmitShipmentConfirmation(context.Background(), ref, first.Add(3*time.Hour)); err != nil {
		t.Fatalf("the retry: %v", err)
	}

	posts := f.requestsTo(http.MethodPost, confirmPath)
	if last := posts[len(posts)-1]; last.Body != posts[0].Body || !strings.Contains(last.Body, "2026-10-25T15:00:00Z") {
		t.Errorf("the retry sent %s, want the stored body with the first shippedAt", last.Body)
	}
	if f.sim.writes(confirmPath) != 1 {
		t.Errorf("network created %d confirmations, want 1", f.sim.writes(confirmPath))
	}
}

func TestLive_ShipmentConfirmationRetriesA5xxAndWaitsOutA429(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.labelled(t, ref, 1)
	f.sim.inject = failFirst(http.MethodPost, confirmPath, 1, http.StatusTooManyRequests, map[string]string{"Retry-After": "2"})
	if err := f.g.SubmitShipmentConfirmation(context.Background(), ref, t0); err != nil {
		t.Fatalf("SubmitShipmentConfirmation: %v", err)
	}
	if w := f.sl.recorded(); len(w) != 1 || w[0] != 2*time.Second {
		t.Fatalf("waits = %v, want the 2s Retry-After", w)
	}

	g, other := orderFixture(t, nil)
	g.labelled(t, other, 1)
	g.sim.inject = failFirst(http.MethodPost, confirmPath, 2, http.StatusServiceUnavailable, nil)
	if err := g.g.SubmitShipmentConfirmation(context.Background(), other, t0); err != nil {
		t.Fatalf("SubmitShipmentConfirmation after 5xx: %v", err)
	}
	if posts := g.requestsTo(http.MethodPost, confirmPath); len(posts) != 3 || g.sim.writes(confirmPath) != 1 {
		t.Fatalf("POST x%d, network created %d; want 3 attempts and one confirmation", len(posts), g.sim.writes(confirmPath))
	}
}

func TestLive_ATransactionTheNetworkFailsForAConfirmationIsRecordedAndLoggedNotThrown(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.labelled(t, ref, 1)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: "ErrSimulatedFailure", Message: "simulated"})

	if err := f.g.SubmitShipmentConfirmation(context.Background(), ref, t0); err != nil {
		t.Fatalf("SubmitShipmentConfirmation: %v; the write was accepted, no use case reconciles it", err)
	}

	if row := f.confirmRow(ref); row.Status != StatusFailure || !strings.Contains(row.Errors, "ErrSimulatedFailure") {
		t.Errorf("row = %+v, want FAILURE with the network's errors for the operator", row)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "ErrSimulatedFailure") {
		t.Errorf("a failed confirmation must be logged at error level, got:\n%s", logs)
	}
}
