package retailnetwork

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// The declaration of the node profile through the live gateway (ADR 0027), against
// the stateful fake Vendor API of gateway_live_support_test.go: it judges a
// profile revision the way node-topology does (not above the one it holds is a
// stale-revision failure of the transaction, never an HTTP refusal).

func profileFixture(t *testing.T) *liveFixture {
	t.Helper()
	f := readyFixture(t, nil)
	if err := f.store.SaveNode(context.Background(), "FC-1", "N-1"); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	return f
}

// profileWithFingerprint is the sample profile under another identity of content.
func profileWithFingerprint(fp string) contract.NodeProfile {
	p := sampleProfile()
	p.Fingerprint = fp
	return p
}

func (f *liveFixture) profilePuts() []seen {
	var out []seen
	for _, r := range f.fake.requests() {
		if r.Method == http.MethodPut {
			out = append(out, r)
		}
	}
	return out
}

func (f *liveFixture) declare(t *testing.T, p contract.NodeProfile) contract.ProfileResult {
	t.Helper()
	res, err := f.g.DeclareNodeProfile(context.Background(), p)
	if err != nil {
		t.Fatalf("DeclareNodeProfile: %v", err)
	}
	return res
}

func (f *liveFixture) profileRow(t *testing.T, revision, attempt int) Submission {
	t.Helper()
	row, ok := f.store.row(KindNodeProfile, profileSubject("FC-1", revision, attempt))
	if !ok {
		t.Fatalf("no NODE_PROFILE ledger row for revision %d attempt %d", revision, attempt)
	}
	return row
}

func TestDeclareNodeProfile_ASiteWithNoNodeIsNotRegisteredAndNothingIsSent(t *testing.T) {
	f := readyFixture(t, nil) // the node is on the network but not mapped here yet
	_, err := f.g.DeclareNodeProfile(context.Background(), sampleProfile())
	if !errors.Is(err, contract.ErrSiteNotRegistered) {
		t.Fatalf("err = %v, want ErrSiteNotRegistered", err)
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Fatalf("%d requests; a site with no node declares nothing", n)
	}
	if _, found, _ := f.store.NodeProfileState(context.Background(), "FC-1"); found {
		t.Fatal("a revision was taken for a site that has no node")
	}
}

func TestDeclareNodeProfile_TheFirstDeclarationIsRevisionOneWithItsDeterministicKey(t *testing.T) {
	f := profileFixture(t)

	res := f.declare(t, profileWithFingerprint("fp-1"))

	if res.Outcome != contract.ProfileConfirmed || res.Revision != 1 {
		t.Fatalf("result = %+v, want confirmed at revision 1", res)
	}
	puts := f.profilePuts()
	if len(puts) != 1 {
		t.Fatalf("PUTs = %d, want 1", len(puts))
	}
	if puts[0].Path != "/vendor/nodes/N-1/profile" || puts[0].IdemKey != "nf-profile-FC-1-1" {
		t.Errorf("PUT = %s key %q, want /vendor/nodes/N-1/profile with key nf-profile-FC-1-1", puts[0].Path, puts[0].IdemKey)
	}
	want, _ := buildNodeProfileBody("V-WS", 1, profileWithFingerprint("fp-1"))
	if puts[0].Body != string(want) {
		t.Errorf("body = %s, want %s", puts[0].Body, want)
	}
	if row := f.profileRow(t, 1, 1); row.Status != StatusSuccess || row.TransactionID == "" {
		t.Errorf("ledger row = %+v, want SUCCESS with the transaction id", row)
	}
	if st, _, _ := f.store.NodeProfileState(context.Background(), "FC-1"); st.Revision != 1 || st.Attempt != 1 || st.Fingerprint != "fp-1" {
		t.Errorf("state = %+v, want revision 1 attempt 1 fp-1", st)
	}
	if got := f.sim.heldRevision("N-1"); got != 1 {
		t.Errorf("the network holds revision %d, want 1", got)
	}
}

func TestDeclareNodeProfile_TheSameContentAgainSendsNothingAndAChangedOneIsTheNextRevision(t *testing.T) {
	f := profileFixture(t)
	f.declare(t, profileWithFingerprint("fp-1"))
	before := len(f.fake.requests())

	res := f.declare(t, profileWithFingerprint("fp-1"))
	if res.Outcome != contract.ProfileUnchanged || res.Revision != 1 {
		t.Fatalf("same content: %+v, want unchanged at revision 1", res)
	}
	if after := len(f.fake.requests()); after != before {
		t.Fatalf("same content made %d more requests, want none", after-before)
	}

	res = f.declare(t, profileWithFingerprint("fp-2"))
	if res.Outcome != contract.ProfileConfirmed || res.Revision != 2 {
		t.Fatalf("changed content: %+v, want confirmed at revision 2", res)
	}
	puts := f.profilePuts()
	if len(puts) != 2 || puts[1].IdemKey != "nf-profile-FC-1-2" || !strings.Contains(puts[1].Body, `"profileRevision":2`) {
		t.Fatalf("PUTs = %+v, want a second one under key nf-profile-FC-1-2 carrying revision 2", puts)
	}
}

func TestDeclareNodeProfile_AProcessingTransactionIsReconciledByItsPersistedIdWithoutASecondWrite(t *testing.T) {
	f := profileFixture(t)
	f.sim.setOutcome(txProcessing)

	if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfileDeclared || res.Revision != 1 {
		t.Fatalf("first call: %+v, want declared at revision 1", res)
	}
	if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfilePending {
		t.Fatalf("second call: %+v, want pending", res)
	}
	if n := len(f.profilePuts()); n != 1 {
		t.Fatalf("PUTs = %d while the transaction is processing, want 1", n)
	}

	f.sim.finish(f.profileRow(t, 1, 1).TransactionID, txSuccess)
	if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfileConfirmed {
		t.Fatalf("third call: %+v, want confirmed", res)
	}
	if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfileUnchanged {
		t.Fatalf("fourth call: %+v, want unchanged", res)
	}
	if n := len(f.profilePuts()); n != 1 {
		t.Fatalf("PUTs = %d, want 1 over the whole life of the revision", n)
	}
}

func TestDeclareNodeProfile_ARestartContinuesAboveWhatWasPersistedAndResendsNothingUnchanged(t *testing.T) {
	f := profileFixture(t)
	f.declare(t, profileWithFingerprint("fp-1"))
	f.declare(t, profileWithFingerprint("fp-2"))

	// A restart: a new gateway over the same database and the same network.
	restarted := newLiveGateway(f.g.client, f.store, LiveConfig{Sites: []string{"FC-1"}, Logger: f.g.logger})

	puts := len(f.profilePuts())
	res, err := restarted.DeclareNodeProfile(context.Background(), profileWithFingerprint("fp-2"))
	if err != nil || res.Outcome != contract.ProfileUnchanged || res.Revision != 2 {
		t.Fatalf("unchanged after a restart: %+v err=%v, want unchanged at revision 2", res, err)
	}
	if len(f.profilePuts()) != puts {
		t.Fatal("a restart re-sent a profile that was already declared")
	}

	res, err = restarted.DeclareNodeProfile(context.Background(), profileWithFingerprint("fp-3"))
	if err != nil || res.Revision != 3 || res.Outcome != contract.ProfileConfirmed {
		t.Fatalf("changed after a restart: %+v err=%v, want revision 3 (never 1, never 2 again)", res, err)
	}
}

func TestDeclareNodeProfile_ACallThatNeverGotAnAnswerIsResentWithTheSameKeyAndBody(t *testing.T) {
	// Two attempts per call keep the breaker (it opens after five failures in a
	// row) out of this test, which is about the ledger and not about the breaker.
	f := readyFixture(t, func(c *fixtureCfg) { c.client = func(cc *clientConfig) { cc.maxAttempts = 2 } })
	if err := f.store.SaveNode(context.Background(), "FC-1", "N-1"); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	var down atomic.Bool
	down.Store(true)
	f.sim.inject = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	}

	if _, err := f.g.DeclareNodeProfile(context.Background(), profileWithFingerprint("fp-1")); err == nil {
		t.Fatal("a network that does not answer must be an error")
	}
	if row := f.profileRow(t, 1, 1); row.Status != StatusSending || row.TransactionID != "" {
		t.Fatalf("row = %+v, want SENDING with no transaction: the body is kept for the retry", row)
	}
	stored := f.profileRow(t, 1, 1).Body

	down.Store(false)
	res := f.declare(t, profileWithFingerprint("fp-1"))
	if res.Outcome != contract.ProfileConfirmed || res.Revision != 1 {
		t.Fatalf("retry: %+v, want confirmed at revision 1 (the revision is not burnt by a failed send)", res)
	}
	puts := f.profilePuts()
	last := puts[len(puts)-1]
	if last.IdemKey != "nf-profile-FC-1-1" || last.Body != string(stored) {
		t.Errorf("retry = key %q body %s, want key nf-profile-FC-1-1 and the stored body", last.IdemKey, last.Body)
	}
	if w := f.sim.writes("/vendor/nodes/N-1/profile"); w != 1 {
		t.Errorf("the network created %d profile writes, want 1", w)
	}
}

func TestDeclareNodeProfile_AStaleRevisionReadsTheNetworksAndContinuesAboveIt(t *testing.T) {
	f := profileFixture(t)
	f.sim.holdRevision("N-1", 5) // for example our table was wiped

	res := f.declare(t, profileWithFingerprint("fp-1"))

	if res.Outcome != contract.ProfileConfirmed || res.Revision != 6 {
		t.Fatalf("result = %+v, want confirmed at revision 6, above the network's 5", res)
	}
	var seq []string
	for _, r := range f.fake.requests() {
		switch {
		case r.Method == http.MethodPut:
			seq = append(seq, "PUT "+r.IdemKey)
		case strings.HasPrefix(r.Path, "/vendor/nodes/"):
			seq = append(seq, "GET node")
		case strings.HasPrefix(r.Path, "/vendor/transactions/"):
			seq = append(seq, "GET tx")
		}
	}
	want := "PUT nf-profile-FC-1-1,GET tx,GET node,PUT nf-profile-FC-1-6,GET tx"
	if got := strings.Join(seq, ","); got != want {
		t.Fatalf("requests = %s\nwant       %s", got, want)
	}
	if row := f.profileRow(t, 1, 1); row.Status != StatusFailure || !strings.Contains(row.Errors, errStaleProfileRevision) {
		t.Errorf("revision 1 row = %+v, want FAILURE naming ErrStaleProfileRevision", row)
	}
	if row := f.profileRow(t, 6, 1); row.Status != StatusSuccess {
		t.Errorf("revision 6 row = %+v, want SUCCESS", row)
	}
	if st, _, _ := f.store.NodeProfileState(context.Background(), "FC-1"); st.Revision != 6 {
		t.Errorf("persisted revision = %d, want 6", st.Revision)
	}
	if !strings.Contains(f.logs.String(), "level=WARN") || !strings.Contains(f.logs.String(), "stale") {
		t.Errorf("a stale revision must be logged at WARN, got:\n%s", f.logs.String())
	}
}

func TestDeclareNodeProfile_AStaleFailureSeenOnALaterTickIsRecoveredThen(t *testing.T) {
	f := profileFixture(t)
	f.sim.setOutcome(txProcessing)
	f.declare(t, profileWithFingerprint("fp-1")) // declared, still processing
	f.sim.holdRevision("N-1", 9)
	f.sim.finish(f.profileRow(t, 1, 1).TransactionID, txSuccess) // the network settles it: 1 <= 9, stale
	f.sim.setOutcome(txSuccess)

	res := f.declare(t, profileWithFingerprint("fp-1"))

	if res.Outcome != contract.ProfileConfirmed || res.Revision != 10 {
		t.Fatalf("result = %+v, want confirmed at revision 10", res)
	}
}

func TestDeclareNodeProfile_WithNoRevisionInTheNodeViewTheClimbIsOnePerTickUntilItClears(t *testing.T) {
	f := profileFixture(t)
	f.sim.holdRevision("N-1", 3)
	f.sim.hideRevisions(true) // the view shows no profileRevision

	var revisions []int
	var outcomes []contract.ProfileOutcome
	for range 4 {
		res := f.declare(t, profileWithFingerprint("fp-1"))
		revisions, outcomes = append(revisions, res.Revision), append(outcomes, res.Outcome)
		if res.Outcome == contract.ProfileConfirmed {
			break
		}
	}

	if last := len(outcomes) - 1; outcomes[last] != contract.ProfileConfirmed || revisions[last] != 4 {
		t.Fatalf("outcomes %v revisions %v, want it to end confirmed at revision 4", outcomes, revisions)
	}
	for i := 1; i < len(revisions); i++ {
		if revisions[i] < revisions[i-1] {
			t.Fatalf("revisions %v went down", revisions)
		}
	}
	if outcomes[0] != contract.ProfileFailed {
		t.Errorf("first outcome = %v, want failed: one recovery per call, the next tick goes on", outcomes[0])
	}
}

func TestDeclareNodeProfile_ARetryableFailureIsLoggedAtErrorAndRetriedWithTheSameRevisionAndANewKey(t *testing.T) {
	f := profileFixture(t)
	f.sim.setOutcome(txFailure, submissionErrorDTO{Code: "ErrSimulatedFailure", Message: "simulated"})

	res := f.declare(t, profileWithFingerprint("fp-1"))
	if res.Outcome != contract.ProfileFailed || res.Revision != 1 || !strings.Contains(res.Errors, "ErrSimulatedFailure") {
		t.Fatalf("first call: %+v, want failed at revision 1 carrying the network's error", res)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "ErrSimulatedFailure") {
		t.Errorf("the network's error list must be logged at ERROR, got:\n%s", logs)
	}

	f.sim.setOutcome(txSuccess)
	res = f.declare(t, profileWithFingerprint("fp-1"))
	if res.Outcome != contract.ProfileConfirmed || res.Revision != 1 {
		t.Fatalf("retry: %+v, want confirmed at the SAME revision 1", res)
	}
	puts := f.profilePuts()
	if len(puts) != 2 || puts[1].IdemKey != "nf-profile-FC-1-1-2" || !strings.Contains(puts[1].Body, `"profileRevision":1`) {
		t.Fatalf("PUTs = %+v, want a second one for revision 1 under the attempt key nf-profile-FC-1-1-2", puts)
	}
	if st, _, _ := f.store.NodeProfileState(context.Background(), "FC-1"); st.Revision != 1 || st.Attempt != 2 {
		t.Errorf("state = %+v, want revision 1 attempt 2", st)
	}
}

func TestDeclareNodeProfile_ANonRetryableFailureIsNotResentUntilTheContentChanges(t *testing.T) {
	for _, code := range []string{errIncompleteOperationalProfile, errCutoffOutsideOperatingHours} {
		t.Run(code, func(t *testing.T) {
			f := profileFixture(t)
			f.sim.setOutcome(txFailure, submissionErrorDTO{Code: code, Message: "no"})

			if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfileFailed {
				t.Fatalf("first call: %+v, want failed", res)
			}
			f.sim.setOutcome(txSuccess)
			for range 3 {
				if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfileFailed || res.Revision != 1 {
					t.Fatalf("later call: %+v, want it to stay failed at revision 1", res)
				}
			}
			if n := len(f.profilePuts()); n != 1 {
				t.Fatalf("PUTs = %d, want 1: the same content that can never succeed is not sent again", n)
			}

			res := f.declare(t, profileWithFingerprint("fp-2"))
			if res.Outcome != contract.ProfileConfirmed || res.Revision != 2 {
				t.Fatalf("changed content: %+v, want confirmed at revision 2", res)
			}
		})
	}
}

func TestDeclareNodeProfile_AValidationRefusalOfThePutItselfIsFinalToo(t *testing.T) {
	f := profileFixture(t)
	var refuse atomic.Bool
	refuse.Store(true)
	f.sim.inject = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && refuse.Load() {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return true
		}
		return false
	}

	res := f.declare(t, profileWithFingerprint("fp-1"))
	if res.Outcome != contract.ProfileFailed || !strings.Contains(res.Errors, "validation-failed") {
		t.Fatalf("result = %+v, want failed naming validation-failed", res)
	}
	refuse.Store(false)
	if res := f.declare(t, profileWithFingerprint("fp-1")); res.Outcome != contract.ProfileFailed {
		t.Fatalf("again: %+v, want it still failed: our body was refused, resending it cannot help", res)
	}
	if n := len(f.profilePuts()); n != 1 {
		t.Fatalf("PUTs = %d, want 1", n)
	}
}

func TestDeclareNodeProfile_AProfileThatCannotBeSentIsRefusedBeforeAnyRevisionIsTaken(t *testing.T) {
	f := profileFixture(t)
	for name, mutate := range map[string]func(*contract.NodeProfile){
		"no cutoff":        func(p *contract.NodeProfile) { p.Cutoffs = nil },
		"no handling":      func(p *contract.NodeProfile) { p.HandlingTime = 0 },
		"no fingerprint":   func(p *contract.NodeProfile) { p.Fingerprint = "" },
		"no timezone":      func(p *contract.NodeProfile) { p.Timezone = "" },
		"no throughput":    func(p *contract.NodeProfile) { p.ThroughputPerDay = 0 },
		"a malformed time": func(p *contract.NodeProfile) { p.Cutoffs[0].LocalTime = "late" },
	} {
		p := profileWithFingerprint("fp-1")
		mutate(&p)
		if _, err := f.g.DeclareNodeProfile(context.Background(), p); !errors.Is(err, ErrBadProfile) {
			t.Errorf("%s: err = %v, want ErrBadProfile", name, err)
		}
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Fatalf("%d requests for profiles that cannot be sent", n)
	}
	if _, found, _ := f.store.NodeProfileState(context.Background(), "FC-1"); found {
		t.Fatal("a revision was taken for a profile that cannot be sent")
	}
}

// racingStore lets a competitor (another replica) win the guarded write just
// before ours, with other content at the very revision we were about to take.
type racingStore struct {
	*memStore
	once       bool
	competitor NodeProfileState
}

func (s *racingStore) AdvanceNodeProfile(ctx context.Context, st NodeProfileState) (NodeProfileState, error) {
	if !s.once {
		s.once = true
		competitor := s.competitor
		competitor.SiteCode, competitor.Revision = st.SiteCode, st.Revision
		if _, err := s.memStore.AdvanceNodeProfile(ctx, competitor); err != nil {
			return NodeProfileState{}, err
		}
	}
	return s.memStore.AdvanceNodeProfile(ctx, st)
}

func TestDeclareNodeProfile_WhenAnotherDeclarerWinsTheRevisionWeSendNothingAndWaitForTheNextTick(t *testing.T) {
	f := profileFixture(t)
	racing := &racingStore{memStore: f.store, competitor: NodeProfileState{Attempt: 1, Fingerprint: "fp-other", Content: []byte("{}")}}
	g := newLiveGateway(f.g.client, racing, LiveConfig{Sites: []string{"FC-1"}, Logger: f.g.logger})

	res, err := g.DeclareNodeProfile(context.Background(), profileWithFingerprint("fp-1"))
	if err != nil || res.Outcome != contract.ProfilePending || res.Revision != 1 {
		t.Fatalf("result = %+v err=%v, want pending at revision 1", res, err)
	}
	if n := len(f.profilePuts()); n != 0 {
		t.Fatalf("PUTs = %d; the revision belongs to the other declarer's content", n)
	}
}

func TestLastDeclared_ReturnsWhatWasDeclaredInOurTermsAndNothingBefore(t *testing.T) {
	f := profileFixture(t)
	ctx := context.Background()
	if _, found, err := f.g.LastDeclared(ctx, "FC-1"); found || err != nil {
		t.Fatalf("before any declaration: found=%v err=%v, want none", found, err)
	}

	p := profileWithFingerprint("fp-1")
	f.declare(t, p)

	got, found, err := f.g.LastDeclared(ctx, "FC-1")
	if err != nil || !found {
		t.Fatalf("LastDeclared: found=%v err=%v", found, err)
	}
	if got.SiteId != "FC-1" || got.Timezone != p.Timezone || got.HandlingTime != p.HandlingTime ||
		got.ThroughputPerDay != p.ThroughputPerDay || got.Fingerprint != "fp-1" || len(got.Cutoffs) != 2 {
		t.Fatalf("LastDeclared = %+v, want the profile that was declared", got)
	}
	if c := got.Cutoffs[0]; c.LocalTime != "18:00" || c.MaxOrders != 120 || len(c.Days) != 5 || c.Days[0] != 1 {
		t.Errorf("first cutoff = %+v, want 18:00, ceiling 120 and Monday..Friday", c)
	}
	if c := got.Cutoffs[1]; c.MaxOrders != 0 {
		t.Errorf("second cutoff = %+v, want an unknown ceiling to stay unknown (0)", c)
	}
}
