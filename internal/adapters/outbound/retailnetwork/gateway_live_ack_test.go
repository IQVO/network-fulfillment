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
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

const ackPath = "/vendor/acknowledgements"

var accept = contract.Acknowledgement{Accepted: true}

// orderFixture is a fixture that has already polled one ISSUED order (PO-1, two
// lines of 2 and 1) and knows its NetworkRef.
func orderFixture(t *testing.T, mutate func(*fixtureCfg)) (*liveFixture, shared.NetworkRef) {
	t.Helper()
	f := readyFixture(t, mutate)
	f.sim.setPage("", purchaseOrderPageDTO{PurchaseOrders: []purchaseOrderDTO{issuedPO("PO-1", "N-1", 2, 1)}})
	refs := f.poll(t)
	if len(refs) != 1 {
		t.Fatalf("setup: polled %v, want one order", refs)
	}
	return f, shared.NetworkRef(refs[0])
}

func (f *liveFixture) requestsWithPrefix(method, prefix string) int {
	n := 0
	for _, r := range f.fake.requests() {
		if r.Method == method && strings.HasPrefix(r.Path, prefix) {
			n++
		}
	}
	return n
}

func (f *liveFixture) ackRow(ref shared.NetworkRef) Submission {
	row, _ := f.store.row(KindAcknowledgement, string(ref))
	return row
}

// ---- SubmitAcknowledgement ----

func TestLive_AcceptWritesTheWholeOrderWithItsKeyAndPersistsTheTransaction(t *testing.T) {
	f, ref := orderFixture(t, nil)

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}

	posts := f.requestsTo(http.MethodPost, ackPath)
	if len(posts) != 1 || posts[0].IdemKey != "nf-ack-"+string(ref) {
		t.Fatalf("posts = %+v, want one with the deterministic key nf-ack-<ref>", posts)
	}
	var body acknowledgementRequestDTO
	if err := json.Unmarshal([]byte(posts[0].Body), &body); err != nil {
		t.Fatalf("body: %v", err)
	}
	want := acknowledgementRequestDTO{VendorID: "V-WS", PoNumber: "PO-1", Decision: "ACCEPT", Lines: []ackLineDTO{{LineNo: 1, Quantity: 2}, {LineNo: 2, Quantity: 1}}}
	if body.VendorID != want.VendorID || body.PoNumber != want.PoNumber || body.Decision != want.Decision || body.ReasonCode != "" ||
		len(body.Lines) != 2 || body.Lines[0] != want.Lines[0] || body.Lines[1] != want.Lines[1] {
		t.Errorf("body = %+v, want %+v: an acceptance echoes every line with its ordered quantity", body, want)
	}
	row := f.ackRow(ref)
	if row.TransactionID != "tx-1" || row.Status != StatusProcessing || row.IdempotencyKey != "nf-ack-"+string(ref) {
		t.Errorf("ledger row = %+v, want the transaction id stored as soon as the 202 was read", row)
	}
}

func TestLive_ASecondAcknowledgementOfTheSameRefIsANoOp(t *testing.T) {
	f, ref := orderFixture(t, nil)
	for range 3 {
		if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
			t.Fatalf("SubmitAcknowledgement: %v", err)
		}
	}
	if n := len(f.requestsTo(http.MethodPost, ackPath)); n != 1 || f.sim.writes(ackPath) != 1 {
		t.Fatalf("POST x%d, network created %d; want one write for three calls", n, f.sim.writes(ackPath))
	}
}

func TestLive_RejectMapsOurReasonToTheNetworksAndStillEchoesEveryLine(t *testing.T) {
	cases := map[shared.RejectionReason]string{
		shared.RejectionReasonInfeasibleDeadline: "CANNOT_MEET_SHIP_BY",
		shared.RejectionReasonUntranslatableSKU:  "UNKNOWN_LISTING",
	}
	for reason, wire := range cases {
		t.Run(string(reason), func(t *testing.T) {
			f, ref := orderFixture(t, nil)
			if err := f.g.SubmitAcknowledgement(context.Background(), ref, contract.Acknowledgement{Reason: reason}); err != nil {
				t.Fatalf("SubmitAcknowledgement: %v", err)
			}
			var body acknowledgementRequestDTO
			_ = json.Unmarshal([]byte(f.requestsTo(http.MethodPost, ackPath)[0].Body), &body)
			if body.Decision != "REJECT" || body.ReasonCode != wire || len(body.Lines) != 2 {
				t.Errorf("body = %+v, want REJECT + %s with both lines", body, wire)
			}
			if strings.Contains(f.requestsTo(http.MethodPost, ackPath)[0].Body, string(reason)) {
				t.Errorf("our reason %s leaked onto the wire", reason)
			}
		})
	}
}

func TestLive_ADeadlineMissedIsNeverWrittenAndLeavesNoLedgerRow(t *testing.T) {
	f, ref := orderFixture(t, nil)
	before := len(f.fake.requests())

	err := f.g.SubmitAcknowledgement(context.Background(), ref, contract.Acknowledgement{Reason: shared.RejectionReasonAcknowledgementDeadlineMissed})

	if err != nil || len(f.fake.requests()) != before {
		t.Fatalf("err = %v, new requests = %d; the network expires the order itself and would refuse a late write", err, len(f.fake.requests())-before)
	}
	if _, ok := f.store.row(KindAcknowledgement, string(ref)); ok {
		t.Error("a ledger row was written for an answer that is never sent")
	}
}

func TestLive_AnAnswerThatCannotBeSentFailsBeforeAnyIO(t *testing.T) {
	bad := map[string]contract.Acknowledgement{
		"accept with a reason":      {Accepted: true, Reason: shared.RejectionReasonInfeasibleDeadline},
		"reject without a reason":   {},
		"the fleet's own verdict":   {Reason: shared.RejectionReasonSubmissionFailed},
		"a reason nobody defined":   {Reason: "SOMETHING_ELSE"},
		"accept with a stray field": {Accepted: true, Reason: shared.RejectionReasonUntranslatableSKU},
	}
	for name, answer := range bad {
		t.Run(name, func(t *testing.T) {
			f, ref := orderFixture(t, nil)
			before := len(f.fake.requests())
			if err := f.g.SubmitAcknowledgement(context.Background(), ref, answer); !errors.Is(err, ErrBadAcknowledgement) {
				t.Fatalf("err = %v, want ErrBadAcknowledgement", err)
			}
			if len(f.fake.requests()) != before {
				t.Error("an unsendable answer reached the network")
			}
			if _, ok := f.store.row(KindAcknowledgement, string(ref)); ok {
				t.Error("an unsendable answer left a ledger row")
			}
		})
	}
}

func TestLive_AnUnknownRefIsAnErrorBeforeAnyIO(t *testing.T) {
	f := readyFixture(t, nil)
	if err := f.g.SubmitAcknowledgement(context.Background(), "NR-nobody", accept); !errors.Is(err, ErrUnknownRef) {
		t.Fatalf("err = %v, want ErrUnknownRef", err)
	}
	if len(f.fake.requests()) != 0 {
		t.Error("an unknown ref reached the network")
	}
}

func TestLive_TheLedgerRowWithTheExactBodyExistsBeforeTheNetworkIsCalled(t *testing.T) {
	f, ref := orderFixture(t, nil)
	var rowAtCall Submission
	f.sim.inject = func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Path == ackPath {
			rowAtCall = f.ackRow(ref)
		}
		return false
	}

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}

	sent := f.requestsTo(http.MethodPost, ackPath)[0]
	if rowAtCall.Status != StatusSending || rowAtCall.TransactionID != "" || string(rowAtCall.Body) != sent.Body {
		t.Fatalf("row at the time of the call = %+v, want SENDING with the very body that was sent", rowAtCall)
	}
}

// ---- failure injection on the write ----

func TestLive_ACrashedWriteIsResentFromTheStoredBodyAndKeyAndCreatesOneAnswer(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 2 } })
	f.sim.inject = failFirst(http.MethodPost, ackPath, 99, http.StatusInternalServerError, nil)

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err == nil {
		t.Fatal("a write that never got through must be an error so the caller retries")
	}
	if row := f.ackRow(ref); row.Status != StatusSending || row.TransactionID != "" {
		t.Fatalf("ledger row = %+v, want SENDING with no transaction after the outage", row)
	}

	f.sim.inject = nil
	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("the retry: %v", err)
	}

	posts := f.requestsTo(http.MethodPost, ackPath)
	last := posts[len(posts)-1]
	if last.Body != posts[0].Body || last.IdemKey != posts[0].IdemKey {
		t.Errorf("the retry changed the body or key: %+v vs %+v", last, posts[0])
	}
	if f.sim.writes(ackPath) != 1 || f.ackRow(ref).TransactionID == "" {
		t.Errorf("network created %d answers, row %+v; want one answer and the transaction stored", f.sim.writes(ackPath), f.ackRow(ref))
	}
}

func TestLive_A5xxIsRetriedWithTheSameKeyAndBodyAndNeverDoubles(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = failFirst(http.MethodPost, ackPath, 2, http.StatusServiceUnavailable, nil)

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}
	posts := f.requestsTo(http.MethodPost, ackPath)
	if len(posts) != 3 || f.sim.writes(ackPath) != 1 {
		t.Fatalf("POST x%d, network created %d; want 3 attempts and one answer", len(posts), f.sim.writes(ackPath))
	}
	for i, p := range posts {
		if p.Body != posts[0].Body || p.IdemKey != posts[0].IdemKey {
			t.Errorf("attempt %d changed the body or key", i+1)
		}
	}
}

func TestLive_A429OnTheWriteWaitsForRetryAfter(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = failFirst(http.MethodPost, ackPath, 1, http.StatusTooManyRequests, map[string]string{"Retry-After": "2"})

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}
	if w := f.sl.recorded(); len(w) != 1 || w[0] != 2*time.Second {
		t.Fatalf("waits = %v, want the 2s the network asked for", w)
	}
	if f.sim.writes(ackPath) != 1 {
		t.Fatalf("network created %d answers, want 1", f.sim.writes(ackPath))
	}
}

func TestLive_ATimedOutWriteThatTheNetworkDidProcessIsReplayedNotDoubled(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.callTimeout = 50 * time.Millisecond } })
	f.sim.inject = slowFirst(http.MethodPost, ackPath, 1, 300*time.Millisecond)

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}
	time.Sleep(350 * time.Millisecond) // let the abandoned first attempt reach the network too
	if f.sim.writes(ackPath) != 1 {
		t.Fatalf("network created %d answers; the same key and body must replay, not double", f.sim.writes(ackPath))
	}
}

func TestLive_AnOpenBreakerFailsTheWriteWithoutCallingAndKeepsTheRow(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 1 } })
	f.sim.inject = failFirst(http.MethodPost, ackPath, 99, http.StatusInternalServerError, nil)

	for range 5 {
		_ = f.g.SubmitAcknowledgement(context.Background(), ref, accept)
	}
	err := f.g.SubmitAcknowledgement(context.Background(), ref, accept)

	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	if n := len(f.requestsTo(http.MethodPost, ackPath)); n != 5 {
		t.Errorf("POST x%d, want 5: the open breaker must not reach the network", n)
	}
	if row := f.ackRow(ref); row.Status != StatusSending {
		t.Errorf("row = %+v, want it kept SENDING for the next attempt", row)
	}
}

func TestLive_ADefinitiveRefusalIsRecordedAsFailureAndNotRetried(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = refuseFirst(http.MethodPost, ackPath, 99, http.StatusConflict, "ack-window-closed")

	err := f.g.SubmitAcknowledgement(context.Background(), ref, accept)

	if !errors.Is(err, ErrWriteRefused) || Problem(err) != "ack-window-closed" {
		t.Fatalf("err = %v, want ErrWriteRefused carrying ack-window-closed", err)
	}
	if row := f.ackRow(ref); row.Status != StatusFailure || !strings.Contains(row.Errors, "ack-window-closed") {
		t.Fatalf("row = %+v, want FAILURE with the problem", row)
	}
	status, serr := f.g.SubmissionStatus(context.Background(), ref)
	if serr != nil || status != contract.SubmissionFailure {
		t.Fatalf("SubmissionStatus = %v, %v; want FAILURE so the order is rejected as submission-failed", status, serr)
	}
	if n := len(f.requestsTo(http.MethodPost, ackPath)); n != 1 {
		t.Errorf("POST x%d, want 1: the network answered, and caches that answer per key", n)
	}
}

func TestLive_ATransientConflictIsRetriedNotRecordedAsFailure(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = refuseFirst(http.MethodPost, ackPath, 1, http.StatusConflict, "concurrent-update")

	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); Problem(err) != "concurrent-update" {
		t.Fatalf("err = %v, want the concurrent-update problem", err)
	}
	if row := f.ackRow(ref); row.Status != StatusSending {
		t.Fatalf("row = %+v, want SENDING: a retryable conflict is not a verdict", row)
	}
	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if f.sim.writes(ackPath) != 1 {
		t.Fatalf("network created %d answers, want 1", f.sim.writes(ackPath))
	}
}

// ---- SubmissionStatus ----

func TestLive_SubmissionStatusIsNotSentWhenTheLedgerHasNoRow(t *testing.T) {
	f, ref := orderFixture(t, nil)
	before := len(f.fake.requests())

	for _, r := range []shared.NetworkRef{ref, "NR-never-seen"} {
		got, err := f.g.SubmissionStatus(context.Background(), r)
		if err != nil || got != contract.SubmissionNotSent {
			t.Fatalf("SubmissionStatus(%s) = %v, %v; want NOT_SENT", r, got, err)
		}
	}
	if len(f.fake.requests()) != before {
		t.Error("NOT_SENT needs no network call")
	}
}

func TestLive_SubmissionStatusFollowsThePersistedTransactionFromPendingToSuccess(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.setOutcome(txProcessing)
	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}

	got, err := f.g.SubmissionStatus(context.Background(), ref)
	if err != nil || got != contract.SubmissionPending {
		t.Fatalf("while PROCESSING = %v, %v; want PENDING", got, err)
	}
	f.sim.finish("tx-1", txSuccess)
	got, err = f.g.SubmissionStatus(context.Background(), ref)
	if err != nil || got != contract.SubmissionSuccess {
		t.Fatalf("after the network settled = %v, %v; want SUCCESS", got, err)
	}
	reads := f.requestsWithPrefix(http.MethodGet, "/vendor/transactions/")
	if again, _ := f.g.SubmissionStatus(context.Background(), ref); again != contract.SubmissionSuccess {
		t.Fatalf("a settled answer changed to %v", again)
	}
	if f.requestsWithPrefix(http.MethodGet, "/vendor/transactions/") != reads {
		t.Error("a terminal ledger row was asked about again")
	}
	if row := f.ackRow(ref); row.Status != StatusSuccess {
		t.Errorf("row = %+v, want the terminal status stored", row)
	}
}

func TestLive_ATransactionTheNetworkFailedIsReportedAsFailureWithItsErrors(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: "ErrSimulatedFailure", Message: "simulated"})
	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement: %v", err)
	}

	got, err := f.g.SubmissionStatus(context.Background(), ref)

	if err != nil || got != contract.SubmissionFailure {
		t.Fatalf("SubmissionStatus = %v, %v; want FAILURE", got, err)
	}
	if row := f.ackRow(ref); row.Status != StatusFailure || !strings.Contains(row.Errors, "ErrSimulatedFailure") {
		t.Errorf("row = %+v, want FAILURE carrying the network's errors", row)
	}
}

func TestLive_SubmissionStatusResendsAnAnswerWhoseCallNeverGotThrough(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 1 } })
	f.sim.inject = failFirst(http.MethodPost, ackPath, 1, http.StatusInternalServerError, nil)
	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err == nil {
		t.Fatal("setup: the first write should have failed")
	}

	got, err := f.g.SubmissionStatus(context.Background(), ref)

	if err != nil || got != contract.SubmissionSuccess {
		t.Fatalf("SubmissionStatus = %v, %v; want the stored answer re-sent and reconciled to SUCCESS", got, err)
	}
	posts := f.requestsTo(http.MethodPost, ackPath)
	if len(posts) != 2 || posts[1].Body != posts[0].Body || f.sim.writes(ackPath) != 1 {
		t.Errorf("posts = %d, network created %d; want one identical resend and one answer", len(posts), f.sim.writes(ackPath))
	}
}

func TestLive_AnExpiredKeyThatAnswersAlreadyAcknowledgedAdoptsTheOrdersTransaction(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 1 } })
	f.sim.inject = failFirst(http.MethodPost, ackPath, 1, http.StatusInternalServerError, nil)
	_ = f.g.SubmitAcknowledgement(context.Background(), ref, accept)

	f.sim.inject = refuseFirst(http.MethodPost, ackPath, 99, http.StatusConflict, "already-acknowledged")
	acked := issuedPO("PO-1", "N-1", 2, 1)
	acked.State = "ACCEPTED"
	acked.Acknowledgement = &acknowledgementDTO{Decision: "ACCEPT", TransactionID: "tx-77"}
	f.sim.addOrder(acked)
	f.sim.addTx(transactionDTO{TransactionID: "tx-77", Kind: "ACKNOWLEDGEMENT", Status: txSuccess})

	got, err := f.g.SubmissionStatus(context.Background(), ref)

	if err != nil || got != contract.SubmissionSuccess {
		t.Fatalf("SubmissionStatus = %v, %v; want SUCCESS through the adopted transaction", got, err)
	}
	if row := f.ackRow(ref); row.TransactionID != "tx-77" {
		t.Errorf("row = %+v, want tx-77 adopted", row)
	}
}

func TestLive_AlreadyAcknowledgedWithoutAnAdoptableTransactionStaysPending(t *testing.T) {
	f, ref := orderFixture(t, nil)
	f.sim.inject = refuseFirst(http.MethodPost, ackPath, 99, http.StatusConflict, "already-acknowledged")
	f.sim.addOrder(issuedPO("PO-1", "N-1", 2, 1)) // the order shows no acknowledgement

	got, err := f.g.SubmissionStatus(context.Background(), ref)
	if err != nil || got != contract.SubmissionNotSent {
		t.Fatalf("with no ledger row = %v, %v; want NOT_SENT", got, err)
	}
	if err := f.g.SubmitAcknowledgement(context.Background(), ref, accept); err != nil {
		t.Fatalf("SubmitAcknowledgement on 409 already-acknowledged: %v; the order is acknowledged, which is the end state wanted", err)
	}
	if got, err := f.g.SubmissionStatus(context.Background(), ref); err != nil || got != contract.SubmissionPending {
		t.Fatalf("SubmissionStatus = %v, %v; want PENDING until a transaction can be adopted", got, err)
	}
}

func TestLive_SubmissionStatusReportsAnOutageAsAnErrorAndKeepsTheRow(t *testing.T) {
	f, ref := orderFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 2 } })
	f.sim.setOutcome(txProcessing)
	_ = f.g.SubmitAcknowledgement(context.Background(), ref, accept)
	f.sim.inject = failFirst(http.MethodGet, "/vendor/transactions/tx-1", 99, http.StatusServiceUnavailable, nil)

	got, err := f.g.SubmissionStatus(context.Background(), ref)

	var se *serverError
	if !errors.As(err, &se) || got != "" {
		t.Fatalf("SubmissionStatus = %v, %v; want an error, never a guessed verdict", got, err)
	}
	if row := f.ackRow(ref); row.Status != StatusProcessing {
		t.Errorf("row = %+v, want it untouched", row)
	}
}
