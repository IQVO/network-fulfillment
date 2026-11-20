package ordermanagement

// heldOrderRefusals is held against order-management's own class table.
//
//   - testdata/order-management-adr-0054-classes.tsv is a COPY of the table of its ADR 0054
//     (the pin of its OpenAPI does not carry the classes: its text names the slugs of
//     the routes this client calls, but lists the released ones only once PR #305 is
//     merged and the pin is refreshed). The table here must equal the copy: every slug
//     the callee classes RETRYABLE is retryable here, every FINAL or OUTCOME one is
//     terminal, and no slug is invented.
//   - the pinned OpenAPI names the problem types of POST /orders and
//     POST /orders/{id}/releases: every one of them must be classified.

import (
	"bufio"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/replayclass"
)

const classCopy = "testdata/order-management-adr-0054-classes.tsv"

// calleeClasses reads the copy of the callee's class table: slug -> RETRYABLE | FINAL | OUTCOME.
func calleeClasses(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(classCopy)
	if err != nil {
		t.Fatalf("the copy of the callee's class table: %v", err)
	}
	defer func() { _ = f.Close() }()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 3 {
			t.Fatalf("%s: want slug, status, class, got %q", classCopy, line)
		}
		if _, dup := out[cols[0]]; dup {
			t.Fatalf("%s: %s twice", classCopy, cols[0])
		}
		out[cols[0]] = cols[2]
	}
	return out
}

// releasedButTerminalHere are the slugs the callee classes RETRYABLE that this caller
// reads as terminal, each with its reason (none today). One that the callee classes
// otherwise, or that the table maps retryable, fails the audit.
var releasedButTerminalHere = map[string]string{}

func auditAgainstTheCalleesClasses(table replayclass.Table, callee map[string]string) []string {
	var problems []string
	for slug, class := range callee {
		_, diverges := releasedButTerminalHere[slug]
		switch {
		case class == "RETRYABLE" && !diverges && !table.Retryable(slug):
			problems = append(problems, slug+" is RETRYABLE at the callee and not retryable here")
		case class == "RETRYABLE" && diverges && table.Retryable(slug):
			problems = append(problems, "the exception for "+slug+" is stale: the table maps it retryable")
		case class != "RETRYABLE" && table.Class(slug) != replayclass.Terminal:
			problems = append(problems, slug+" is "+class+" at the callee and not terminal here")
		}
	}
	for slug := range releasedButTerminalHere {
		if callee[slug] != "RETRYABLE" {
			problems = append(problems, "the exception for "+slug+" is stale: the callee does not class it RETRYABLE")
		}
	}
	for _, c := range []replayclass.Class{replayclass.Retryable, replayclass.Terminal} {
		for _, slug := range table.Slugs(c) {
			if _, known := callee[slug]; !known {
				problems = append(problems, slug+" is in the table but not in the callee's class table")
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func TestTheTableEqualsTheCalleesClassTable(t *testing.T) {
	callee := calleeClasses(t)
	retry, final := 0, 0
	for _, class := range callee {
		if class == "RETRYABLE" {
			retry++
		} else {
			final++
		}
	}
	if len(callee) != 32 || retry != 18 || final != 14 {
		t.Fatalf("the copy has %d problem types (%d RETRYABLE, %d FINAL or OUTCOME), ADR 0054 section 4 says 32 (18, 14)", len(callee), retry, final)
	}
	if problems := auditAgainstTheCalleesClasses(heldOrderRefusals, callee); len(problems) > 0 {
		t.Fatalf("heldOrderRefusals drifted from %s:\n  %s", classCopy, strings.Join(problems, "\n  "))
	}
}

func TestTheClassAuditFailsWhenTheTableDrifts(t *testing.T) {
	callee := calleeClasses(t)
	var retry, terminal []string
	for _, s := range heldOrderRefusals.Slugs(replayclass.Retryable) {
		if s != "concurrent-modification" {
			retry = append(retry, s)
		}
	}
	for _, s := range heldOrderRefusals.Slugs(replayclass.Terminal) {
		if s != "order-not-found" {
			terminal = append(terminal, s)
		}
	}
	retry = append(retry, "order-not-found", "invented-slug") // FINAL here retried; a slug nobody declared
	terminal = append(terminal, "concurrent-modification")    // RETRYABLE at the callee, terminal here
	got := strings.Join(auditAgainstTheCalleesClasses(replayclass.NewTable(retry, terminal), callee), "\n")
	for _, want := range []string{
		"concurrent-modification is RETRYABLE at the callee and not retryable here",
		"order-not-found is FINAL at the callee and not terminal here",
		"invented-slug is in the table but not in the callee's class table",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the audit missed %q:\n%s", want, got)
		}
	}
}

// The problem types the pinned OpenAPI names on the two keyed routes this client calls
// are all classified (an unclassified one would silently read as terminal).
func TestEveryProblemTypeThePinnedSpecNamesOnTheKeyedRoutesIsClassified(t *testing.T) {
	doc := loadPinnedSpec(t)
	named := map[string]bool{}
	for _, route := range []string{"/orders", "/orders/{id}/releases"} {
		path := doc.Paths.Find(route)
		if path == nil || path.Post == nil {
			t.Fatalf("the pinned spec has no POST %s", route)
		}
		for _, status := range []string{"400", "404", "409", "422"} {
			resp := path.Post.Responses.Value(status)
			if resp == nil || resp.Value == nil || resp.Value.Description == nil {
				continue
			}
			parts := strings.Split(*resp.Value.Description, "`")
			for i := 1; i < len(parts); i += 2 {
				if isKebabSlug(parts[i]) {
					named[parts[i]] = true
				}
			}
		}
	}
	for _, want := range []string{"unknown-process-path", "concurrent-modification", "line-ineligible-for-resolved-path", "order-not-held", "idempotency-key-reused"} {
		if !named[want] {
			t.Errorf("the extraction lost %s: it reads %d slugs", want, len(named))
		}
	}
	var missing []string
	for slug := range named {
		if heldOrderRefusals.Class(slug) == replayclass.Unset {
			missing = append(missing, slug)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("the pinned spec names %v and the table classifies them neither retryable nor terminal", missing)
	}
}

// isKebabSlug accepts a lower-case kebab word of at least two parts: a header name has
// capitals, a wildcard family a star.
func isKebabSlug(s string) bool {
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
