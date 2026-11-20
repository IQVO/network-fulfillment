package replayclass_test

import (
	"testing"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/replayclass"
)

func TestTable_ClassifiesBySlugAndUnknownIsTerminal(t *testing.T) {
	table := replayclass.NewTable([]string{"released-one", "released-two"}, []string{"stored-one"})
	for slug, want := range map[string]replayclass.Class{
		"released-one": replayclass.Retryable,
		"released-two": replayclass.Retryable,
		"stored-one":   replayclass.Terminal,
		"never-heard":  replayclass.Unset,
		"":             replayclass.Unset,
	} {
		if got := table.Class(slug); got != want {
			t.Errorf("Class(%q) = %v, want %v", slug, got, want)
		}
	}
	if !table.Retryable("released-one") {
		t.Error("a released slug must be retryable")
	}
	for _, slug := range []string{"stored-one", "never-heard", ""} {
		if table.Retryable(slug) {
			t.Errorf("%q must read as terminal: only a slug the table lists as released is retried", slug)
		}
	}
}

func TestNewTable_RefusesASlugClassifiedTwice(t *testing.T) {
	for name, build := range map[string]func(){
		"in both lists":         func() { replayclass.NewTable([]string{"a"}, []string{"a"}) },
		"twice as retryable":    func() { replayclass.NewTable([]string{"a", "a"}, nil) },
		"twice as terminal":     func() { replayclass.NewTable(nil, []string{"a", "a"}) },
		"an empty slug (retry)": func() { replayclass.NewTable([]string{""}, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("a table that classifies a slug twice, or the empty slug, must not build")
				}
			}()
			build()
		})
	}
}

func TestTable_SlugsAreSortedAndPerClass(t *testing.T) {
	table := replayclass.NewTable([]string{"b", "a"}, []string{"d", "c"})
	if got := table.Slugs(replayclass.Retryable); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("retryable slugs = %v", got)
	}
	if got := table.Slugs(replayclass.Terminal); len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Errorf("terminal slugs = %v", got)
	}
}

func TestProblemOf_ReadsTheSlugAndTheDetail(t *testing.T) {
	cases := []struct {
		name, body, slug, detail string
	}{
		{"a problem document", `{"type":"https://errors.x.dev/unknown-sku","detail":"SKU-9"}`, "unknown-sku", "SKU-9"},
		{"a bare type", `{"type":"unknown-sku","detail":"d"}`, "unknown-sku", "d"},
		{"no type", `{"detail":"d"}`, "", "d"},
		{"not JSON", `<html>bad gateway</html>`, "", ""},
		{"empty", ``, "", ""},
		{"a trailing slash is no slug", `{"type":"https://errors.x.dev/"}`, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slug, detail := replayclass.ProblemOf([]byte(tc.body))
			if slug != tc.slug || detail != tc.detail {
				t.Errorf("ProblemOf = %q, %q; want %q, %q", slug, detail, tc.slug, tc.detail)
			}
		})
	}
}
