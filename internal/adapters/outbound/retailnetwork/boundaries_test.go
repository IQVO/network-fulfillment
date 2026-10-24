package retailnetwork

// Tests that pin boundaries and defaults the behavioural tests walk past. Each
// one exists because a mutant of the line it covers survived the mutation gate
// (gremlins unleash ./internal/adapters/outbound/retailnetwork): a comparison
// moved by one, a default that nothing observed, a message nobody read.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
	"github.com/claudioed/network-fulfillment/internal/resilience"
)

func TestNewAPIClient_DefaultsALoggerAndAClockAndKeepsTheOnesGiven(t *testing.T) {
	bare, err := newAPIClient(clientConfig{baseURL: "http://network.invalid"})
	if err != nil {
		t.Fatalf("newAPIClient: %v", err)
	}
	if bare.logger != slog.Default() {
		t.Fatal("without a logger the client must log through slog.Default(), not through nil")
	}
	if bare.now == nil || bare.sleep == nil {
		t.Fatal("without a clock and a sleeper the client must default both, not leave nil")
	}
	if d := time.Since(bare.now()); d < 0 || d > time.Minute {
		t.Fatalf("the default clock is %v off the wall clock", d)
	}

	mine := slog.New(slog.NewTextHandler(io.Discard, nil))
	fixed := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	given, err := newAPIClient(clientConfig{baseURL: "http://network.invalid", logger: mine, now: func() time.Time { return fixed }})
	if err != nil {
		t.Fatalf("newAPIClient: %v", err)
	}
	if given.logger != mine {
		t.Fatal("a logger that was given must be kept")
	}
	if !given.now().Equal(fixed) {
		t.Fatalf("a clock that was given must be kept: now = %v", given.now())
	}
}

func TestBreakerCooldown_IsTheConfiguredOneOrTheFleetDefault(t *testing.T) {
	for _, tc := range []struct{ in, want time.Duration }{
		{0, resilience.DefaultTimeout},
		{-time.Second, resilience.DefaultTimeout},
		{time.Nanosecond, time.Nanosecond},
		{40 * time.Millisecond, 40 * time.Millisecond},
	} {
		if got := breakerCooldown(tc.in); got != tc.want {
			t.Errorf("breakerCooldown(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSleepContext(t *testing.T) {
	if err := sleepContext(context.Background(), 0); err != nil {
		t.Fatalf("sleeping for nothing = %v, want nil", err)
	}
	if err := sleepContext(context.Background(), -time.Second); err != nil {
		t.Fatalf("a negative sleep = %v, want nil", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	// A zero sleep for a caller who gave up is answered from the context, every
	// time: were it left to the timer, its channel and ctx.Done() would both be
	// ready and select would pick one at random.
	for range 64 {
		if err := sleepContext(cancelled, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("a zero sleep for a caller who gave up = %v, want context.Canceled", err)
		}
	}
	if err := sleepContext(cancelled, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("an hour's sleep for a caller who gave up = %v, want context.Canceled", err)
	}
	if err := sleepContext(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("a short sleep = %v, want nil", err)
	}
}

func TestClient_429ExactlyAtTheCapIsWaitedForNotRefused(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.Header().Set("Retry-After", "30")
			problem(w, http.StatusTooManyRequests, "rate-limited")
			return
		}
		ok(w, map[string]any{"nodes": []any{}})
	})
	c, sl, _ := testClient(t, f.srv.URL, nil)

	if _, err := c.listNodes(context.Background()); err != nil {
		t.Fatalf("listNodes: %v (a wait of exactly maxRetryAfter is taken, only a longer one is refused)", err)
	}
	if got := sl.recorded(); len(got) != 1 || got[0] != maxRetryAfter {
		t.Fatalf("waits = %v, want exactly %v", got, maxRetryAfter)
	}
}

func TestClient_AnyStatusUpTo299IsASuccessfulAnswer(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(299)
		_, _ = w.Write([]byte(`{"nodes":[]}`))
	})
	c, _, _ := testClient(t, f.srv.URL, nil)

	if _, err := c.listNodes(context.Background()); err != nil {
		t.Fatalf("a 299 answer is still 2xx: %v", err)
	}
}

func TestAckDecision_TheTwoRefusalsSayWhichOneItIs(t *testing.T) {
	_, _, err := ackDecision(contract.Acknowledgement{Accepted: false})
	if !errors.Is(err, ErrBadAcknowledgement) || !strings.Contains(err.Error(), "needs a reason") {
		t.Fatalf("a rejection without a reason = %v, want ErrBadAcknowledgement saying it needs a reason", err)
	}
	_, _, err = ackDecision(contract.Acknowledgement{Accepted: false, Reason: shared.RejectionReason("SOMETHING_ELSE")})
	if !errors.Is(err, ErrBadAcknowledgement) || !strings.Contains(err.Error(), "is not one the network is told") || !strings.Contains(err.Error(), "SOMETHING_ELSE") {
		t.Fatalf("a reason the network is not told = %v, want ErrBadAcknowledgement naming it", err)
	}
}

func TestIdempotencyKey_ExactlyAtTheLimitIsKeptAndOneOverIsHashed(t *testing.T) {
	prefix := "nf-ack-"
	exact := strings.Repeat("s", maxIdempotencyKeyLength-len(prefix))
	if got := idempotencyKey("ack", exact); got != prefix+exact || len(got) != maxIdempotencyKeyLength {
		t.Fatalf("a key of exactly %d characters = %q (len %d), want it unchanged", maxIdempotencyKeyLength, got, len(got))
	}
	over := exact + "s"
	got := idempotencyKey("ack", over)
	if got == prefix+over || !strings.HasPrefix(got, prefix) || len(got) > maxIdempotencyKeyLength {
		t.Fatalf("a key of %d characters = %q, want it hashed within the limit", len(prefix+over), got)
	}
}

func TestStubGateway_TheFirstLabelKeepsThePlainRefAndTheNextOnesAreNumbered(t *testing.T) {
	got, err := newTestStubGateway().RequestLabel(context.Background(), "NR-7", 2)
	if err != nil || len(got) != 2 {
		t.Fatalf("RequestLabel = %v, %v; want two labels", got, err)
	}
	if got[0].LabelRef != "stub-label-NR-7" || got[0].TrackingNumber != "stub-tracking-NR-7" {
		t.Fatalf("package 1 = %+v, want the plain ref with no suffix", got[0])
	}
	if got[1].LabelRef != "stub-label-NR-7-2" || got[1].TrackingNumber != "stub-tracking-NR-7-2" {
		t.Fatalf("package 2 = %+v, want the ref suffixed -2", got[1])
	}
}

func TestLoadNodesFile_CoordinatesAreInclusiveAtTheirLimits(t *testing.T) {
	node := func(lat, lon string) string {
		return `{"nodes":[` + strings.Replace(strings.Replace(fullNode, `"lat":-23.55`, `"lat":`+lat, 1), `"lon":-46.63`, `"lon":`+lon, 1) + `]}`
	}
	for _, tc := range []struct {
		lat, lon string
		want     string // "" = accepted
	}{
		{"-90", "-180", ""},
		{"90", "180", ""},
		{"-90.000001", "0", "outside -90..90"},
		{"90.000001", "0", "outside -90..90"},
		{"0", "-180.000001", "outside -180..180"},
		{"0", "180.000001", "outside -180..180"},
	} {
		_, err := LoadNodesFile(writeNodes(t, node(tc.lat, tc.lon)))
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("lat %s lon %s: %v, want accepted (the limits are inclusive)", tc.lat, tc.lon, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("lat %s lon %s: %v, want %q", tc.lat, tc.lon, err, tc.want)
		}
	}
}

func TestNewLiveGateway_RefusesWhatItCannotRunWithout(t *testing.T) {
	for name, cfg := range map[string]LiveConfig{
		"no store":     {BaseURL: "http://network.invalid", VendorID: "V-1"},
		"no vendor id": {BaseURL: "http://network.invalid", Store: newMemStore()},
		"bad base URL": {BaseURL: "not a url", VendorID: "V-1", Store: newMemStore()},
	} {
		if g, err := NewLiveGateway(cfg); err == nil || g != nil {
			t.Errorf("%s: NewLiveGateway = %v, %v; want an error and no gateway", name, g, err)
		}
	}
	g, err := NewLiveGateway(LiveConfig{BaseURL: "http://network.invalid", VendorID: "V-1", Store: newMemStore(), Sites: []string{"b", "a"}, Nodes: []NodeDefinition{fcNode("c")}})
	if err != nil || g == nil {
		t.Fatalf("NewLiveGateway = %v, %v; want a gateway", g, err)
	}
	if got := strings.Join(g.sites, ","); got != "a,b,c" {
		t.Fatalf("sites = %s, want every configured site and every site of the node file, sorted: a,b,c", got)
	}
}

func TestNewLiveGateway_LogsThroughTheGivenLoggerElseTheClients(t *testing.T) {
	c, _, _ := testClient(t, "http://network.invalid", nil)
	if g := newLiveGateway(c, newMemStore(), LiveConfig{}); g.logger != c.logger {
		t.Fatal("without a logger of its own the gateway must use the client's")
	}
	mine := slog.New(slog.NewTextHandler(io.Discard, nil))
	if g := newLiveGateway(c, newMemStore(), LiveConfig{Logger: mine}); g.logger != mine {
		t.Fatal("a logger that was given must be the gateway's")
	}
}

func TestSend_ASettledFailureIsNotAnErrorAndAWriteRefusedBeforeIsNeverSentAgain(t *testing.T) {
	f := readyFixture(t, nil)
	ctx := context.Background()
	base := Submission{Kind: KindAcknowledgement, Subject: "NR-x", IdempotencyKey: "nf-ack-NR-x", Body: []byte(`{}`)}

	settled := base
	settled.Status, settled.TransactionID = StatusFailure, "tx-9"
	got, err := f.g.send(ctx, settled, "submitAcknowledgement", pathAcknowledgements)
	if err != nil || got.TransactionID != "tx-9" {
		t.Fatalf("send of a transaction the network already failed = %+v, %v; want it returned as it is", got, err)
	}

	refused := base
	refused.Status, refused.Errors = StatusFailure, "ack-window-closed"
	if _, err := f.g.send(ctx, refused, "submitAcknowledgement", pathAcknowledgements); !errors.Is(err, ErrWriteRefused) || !strings.Contains(err.Error(), "ack-window-closed") {
		t.Fatalf("send of a write the network refused = %v, want ErrWriteRefused carrying why", err)
	}
	if n := len(f.fake.requests()); n != 0 {
		t.Fatalf("%d requests were made; neither a settled failure nor an earlier refusal may be sent", n)
	}
}

func TestToStoredOrder_TheOrdersVendorWinsOverTheConfiguredOne(t *testing.T) {
	po := issuedPO("PO-1", "N-1", 2)
	po.VendorID = "V-FROM-THE-ORDER"
	if got := toStoredOrder(po, "V-CONFIGURED").VendorID; got != "V-FROM-THE-ORDER" {
		t.Fatalf("vendor = %q, want the one the purchase order names", got)
	}
	po.VendorID = ""
	if got := toStoredOrder(po, "V-CONFIGURED").VendorID; got != "V-CONFIGURED" {
		t.Fatalf("vendor = %q, want the configured one when the purchase order names none", got)
	}
}

func TestLabelsFromResponse_NamesThePackageThatHasNoTrackingNumber(t *testing.T) {
	resp := labelResponseDTO{Labels: []labelDTO{{LabelRef: "L1", TrackingNumber: "T1"}, {LabelRef: "L2"}}}
	_, err := labelsFromResponse(resp, 2)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("package %d has no tracking number", 2)) {
		t.Fatalf("err = %v, want it to name package 2", err)
	}
}
