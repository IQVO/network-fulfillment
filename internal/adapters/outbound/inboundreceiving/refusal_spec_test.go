package inboundreceiving

// asnRefusals is held against the byte-pinned copy of inbound-receiving's OpenAPI
// (testdata/inbound-receiving.openapi.yaml, a pin of contracts/pinned.json,
// refreshed by copying, never edited). What the pin can prove:
//   - the Idempotency-Key text lists the problem types the callee releases
//     (the sentence "These are ..." after "is NOT stored") and names the ones it replays; the
//     400/409/422 responses of POST /asns name the rest. Every slug named is
//     classified in the table, every released one is mapped retryable (but for
//     the documented exceptions below), and nothing the callee stores is mapped
//     retryable.
//   - it does NOT carry the callee's class table (that is its ADR 0027): a slug
//     no text names is held by the table's own entries.

import (
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/replayclass"
)

// releasedButTerminalHere are the slugs the callee releases that this caller reads
// as terminal, each with its reason. One that the pin no longer releases, or that
// the table maps retryable, fails the audit: the exception is stale.
var releasedButTerminalHere = map[string]string{
	"malformed-request": "mixed slug at the callee (invalid JSON, or an unparsable If-Match; its ADR 0027 judgement 1); this client sends the bytes of its own DTO and no If-Match, the same on every attempt, so a retry cannot pass",
}

// retryableByTheAdrOnly are slugs the callee's class table (ADR 0027) releases and
// its OpenAPI text does not list. One that the pin starts to list fails the audit:
// the exception is stale, the list is then the proof.
var retryableByTheAdrOnly = map[string]string{
	"idempotency-key-reused": "ADR 0027: RETRYABLE (answered by the middleware before the store, never stored); the OpenAPI names it as a 409 but does not list it among the released ones",
}

// answeredBeforeTheTable are named slugs RegisterAsn never classifies.
var answeredBeforeTheTable = map[string]string{
	"asn-already-exists": "RegisterAsn answers success for it before it reads the table (the ASN is ours, its number is derived from the shipment)",
}

// specReading is what the pin says: released are the problem types the
// Idempotency-Key text lists as not stored; named is every problem type that text
// or a 400/409/422 response of POST /asns names.
type specReading struct{ released, named []string }

const (
	releaseAnchor = "These are"
	releaseEnd    = "Rows are kept"
)

// notSlugs are kebab-case words of the spec prose that are not problem types.
var notSlugs = map[string]bool{}

func slugsIn(text string) []string {
	seen := map[string]bool{}
	parts := strings.Split(text, "`")
	for i := 1; i < len(parts); i += 2 {
		if isSlug(parts[i]) && !notSlugs[parts[i]] {
			seen[parts[i]] = true
		}
	}
	return sortedKeys(seen)
}

// isSlug accepts a lower-case kebab word of at least two parts: a wildcard family
// such as `invalid-*` is not a slug and a header name has capitals.
func isSlug(s string) bool {
	if !strings.Contains(s, "-") || strings.HasPrefix(s, "-") || strings.HasSuffix(s, "-") {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && r != '-' {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func words(s string) string { return strings.Join(strings.Fields(s), " ") }

// declaredStatuses are the refusal statuses POST /asns declares (it declares no 404;
// RegisterAsn reads a 404 through the table all the same).
var declaredStatuses = []string{"400", "409", "422"}

func readRegisterOperation(t *testing.T, doc *openapi3.T) specReading {
	t.Helper()
	path := doc.Paths.Find("/asns")
	if path == nil || path.Post == nil {
		t.Fatal("the pinned spec has no POST /asns")
	}
	var keyText string
	for _, p := range path.Post.Parameters {
		if p.Value != nil && p.Value.Name == "Idempotency-Key" {
			keyText = words(p.Value.Description)
		}
	}
	i, end := strings.Index(keyText, releaseAnchor), strings.Index(keyText, releaseEnd)
	if i < 0 || end < i {
		t.Fatalf("the Idempotency-Key text no longer has %q ... %q: the callee reworded how it lists the released refusals; read the new text and update the anchors and the table together", releaseAnchor, releaseEnd)
	}
	named := map[string]bool{}
	for _, s := range slugsIn(keyText) {
		named[s] = true
	}
	for _, status := range declaredStatuses {
		resp := path.Post.Responses.Value(status)
		if resp == nil || resp.Value == nil || resp.Value.Description == nil {
			t.Fatalf("the pinned POST /asns no longer declares a %s response", status)
		}
		for _, s := range slugsIn(words(*resp.Value.Description)) {
			named[s] = true
		}
	}
	return specReading{released: slugsIn(keyText[i:end]), named: sortedKeys(named)}
}

// auditTable returns what is wrong between a table and the spec's reading.
func auditTable(table replayclass.Table, r specReading) []string {
	released := map[string]bool{}
	for _, s := range r.released {
		released[s] = true
	}
	problems := auditReleased(table, released)
	problems = append(problems, auditAdrOnly(table, released)...)
	problems = append(problems, auditNamedAndRetried(table, r, released)...)
	sort.Strings(problems)
	return problems
}

// auditReleased: every released slug is retryable here but the documented exceptions.
func auditReleased(table replayclass.Table, released map[string]bool) []string {
	var problems []string
	for s := range released {
		_, excepted := releasedButTerminalHere[s]
		switch {
		case excepted && table.Retryable(s):
			problems = append(problems, "the exception for "+s+" is stale: the table maps it retryable")
		case !excepted && !table.Retryable(s):
			problems = append(problems, "spec releases "+s+" under the key but the table does not map it retryable")
		}
	}
	for s := range releasedButTerminalHere {
		if !released[s] {
			problems = append(problems, "the exception for "+s+" is stale: the pin no longer releases it")
		}
	}
	return problems
}

// auditAdrOnly: the slugs only the callee's ADR releases are still unlisted by the pin and retryable here.
func auditAdrOnly(table replayclass.Table, released map[string]bool) []string {
	var problems []string
	for s := range retryableByTheAdrOnly {
		switch {
		case released[s]:
			problems = append(problems, "the exception for "+s+" is stale: the pin now releases it")
		case !table.Retryable(s):
			problems = append(problems, "the exception for "+s+" is stale: the table does not map it retryable")
		}
	}
	return problems
}

// auditNamedAndRetried: every named slug is classified, and nothing retried is a stored refusal.
func auditNamedAndRetried(table replayclass.Table, r specReading, released map[string]bool) []string {
	var problems []string
	for _, s := range r.named {
		if _, skip := answeredBeforeTheTable[s]; !skip && table.Class(s) == replayclass.Unset {
			problems = append(problems, "spec names "+s+" and the table classifies it neither retryable nor terminal")
		}
	}
	for _, s := range table.Slugs(replayclass.Retryable) {
		if _, adrOnly := retryableByTheAdrOnly[s]; !released[s] && !adrOnly {
			problems = append(problems, "the table retries "+s+" but the spec does not release it: a stored refusal would only be replayed")
		}
	}
	return problems
}

func TestEveryProblemTypeThePinnedSpecNamesIsClassifiedAndEveryReleasedOneRetries(t *testing.T) {
	r := readRegisterOperation(t, loadPinnedSpec(t))
	if len(r.released) < 20 || len(r.named) <= len(r.released) {
		t.Fatalf("read %d released and %d named problem types from the pin; the extraction is broken: %+v", len(r.released), len(r.named), r)
	}
	if problems := auditTable(asnRefusals, r); len(problems) > 0 {
		t.Fatalf("asnRefusals drifted from %s:\n  %s", specCopy, strings.Join(problems, "\n  "))
	}
}

// The slugs the issue names are read from the pin as released and the final ones as
// not released: the test above would pass vacuously if the extraction lost them.
func TestThePinReleasesTheRefusalsThisCallerRetries(t *testing.T) {
	released := map[string]bool{}
	for _, s := range readRegisterOperation(t, loadPinnedSpec(t)).released {
		released[s] = true
	}
	for _, want := range []string{"unknown-sku", "concurrent-modification", "duplicate-sscc"} {
		if !released[want] {
			t.Errorf("the pin does not release %s", want)
		}
	}
	for _, final := range []string{"asn-already-exists", "asn-terminal", "receipt-closed", "appointment-cancelled", "invalid-sscc"} {
		if released[final] {
			t.Errorf("the pin releases %s: it is stored and replayed, a retry cannot pass", final)
		}
	}
	if released["concurrent-update"] {
		t.Error("the pin releases concurrent-update: inbound-receiving answers concurrent-modification")
	}
}

// The audit itself must fail when the table drifts: drop an entry, flip a released
// slug to terminal, map a stored one retryable, or keep a stale exception.
func TestTheAuditFailsWhenTheTableDrifts(t *testing.T) {
	r := readRegisterOperation(t, loadPinnedSpec(t))
	var retry, terminal []string
	for _, s := range asnRefusals.Slugs(replayclass.Retryable) {
		if s != "unknown-sku" && s != "malformed-request" {
			retry = append(retry, s)
		}
	}
	for _, s := range asnRefusals.Slugs(replayclass.Terminal) {
		if s != "asn-terminal" && s != "malformed-request" {
			terminal = append(terminal, s)
		}
	}
	// unknown-sku: released, flipped to terminal. asn-terminal: stored, dropped.
	// receipt-closed: stored, mapped retryable. malformed-request: a stale exception.
	retry = append(retry, "receipt-closed", "malformed-request")
	terminal = withoutSlug(terminal, "receipt-closed")
	terminal = append(terminal, "unknown-sku")
	got := strings.Join(auditTable(replayclass.NewTable(retry, terminal), r), "\n")
	for _, want := range []string{
		"spec releases unknown-sku under the key but the table does not map it retryable",
		"spec names asn-terminal and the table classifies it neither retryable nor terminal",
		"the table retries receipt-closed",
		"the exception for malformed-request is stale",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the audit missed %q:\n%s", want, got)
		}
	}
	if clean := auditTable(asnRefusals, r); len(clean) != 0 {
		t.Errorf("the shipped table is not clean: %v", clean)
	}
}

func withoutSlug(in []string, drop string) []string {
	var out []string
	for _, s := range in {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}
