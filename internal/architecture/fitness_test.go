// Package architecture holds fitness tests (the Go analogue of ArchUnit,
// via github.com/arch-go/arch-go) that enforce fleet-wide invariants learned
// the expensive way -- each rule below corresponds to a real, already-lived
// incident somewhere in the warehouse-systems fleet. See HARNESS.md's
// "fitness functions" section for the full incident list.
//
// TEMPLATE NOTE (warehouse-harness-template v1): TestNoAuthMiddlewareReintroduced,
// TestKafkaConsumerGroupNeverHardcodedInline and
// TestKafkaIntegrationTestsUseTestcontainers are universal -- keep them
// verbatim (they don't reference modulePath or any repo-specific literal).
// TestMCPAdapterDependencyRule is CONDITIONAL: only port it if this repo
// has an internal/adapters/inbound/mcp package. If this repo is one of the
// two with a stricter no-sibling-context-calls rule
// (process-path-management, labor-performance as of this writing), also
// port that rule from its own architecture_test.go -- it's repo-specific
// and not included here.
package architecture

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestMCPAdapterDependencyRule encodes ADR-0008: the MCP inbound adapter is
// additive, never load-bearing for the OLTP composition root. It may depend
// only on the application and domain layers (never on outbound adapters,
// never on cmd), and — the direction that actually matters for keeping it
// additive — nothing else in this codebase may depend on it. If some other
// package started importing internal/adapters/inbound/mcp, the MCP surface
// would have silently become a dependency other code relies on rather than
// a pure inbound entrypoint cmd/mcp wires up and nothing else touches.
func TestMCPAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("mcp adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.adapters.inbound.mcp.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.internal.adapters.inbound.mcp.**",
							"**.internal.application.**",
							"**.internal.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the mcp adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.internal.adapters.inbound.mcp.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// assertArchGoPasses is a shared helper for the two-return-shape arch-go
// result (DependenciesRuleResult here; the file's other tests use their own
// assertPass with the same semantics — kept separate to avoid touching
// existing tests' helper functions).
func assertArchGoPasses(t *testing.T, result *archgo.Result) {
	t.Helper()

	if result.Pass {
		return
	}

	if result.DependenciesRuleResult != nil {
		for _, r := range result.DependenciesRuleResult.Results {
			if r.Passes {
				continue
			}
			for _, v := range r.Verifications {
				if v.Passes {
					continue
				}
				for _, d := range v.Details {
					t.Errorf("%s: %s", v.Package, d)
				}
			}
		}
	}

	t.FailNow()
}

// ---------------------------------------------------------------------
// Source-scanning fitness tests below. arch-go's DSL only expresses import
// graph shape; these three invariants are about literal content (a string,
// a missing import) so they're enforced the same way
// internal/architecture/zerowrite does it in warehouse-ops-agent: walk the
// real .go files under internal/, skip generated/_test.go where noted, and
// fail with the exact file:line that violates the rule.
// ---------------------------------------------------------------------

// goFilesUnder returns every non-test .go file under root (relative to the
// module root), or every .go file including tests when includeTests is true.
func goFilesUnder(t *testing.T, root string, includeTests bool) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !includeTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// authPatternRE matches the literal shapes a reintroduced bearer/JWT/API-key
// auth middleware would contain in source. Deliberately narrow (case
// sensitive on "Bearer " and "Authorization" header checks, exact import
// paths for known JWT libraries) so it doesn't false-positive on the
// existing CORS AllowedHeaders: []string{"Authorization"} allowlist entry
// or on the historical/comment-only mentions of the fleet's auth revert —
// this test scans non-comment, non-test Go source only, and every current
// hit in this codebase is exactly those two known-safe shapes, verified
// clean before this test was added.
var authPatternRE = regexp.MustCompile(`"Bearer |golang-jwt/jwt|dgrijalva/jwt-go|lestrrat-go/jwx`)

// TestNoAuthMiddlewareReintroduced encodes the fleet-wide 2026-09-11 REST +
// MCP static-bearer-auth revert: every endpoint is deliberately
// unauthenticated pending a fresh auth-model decision. An agent
// "helpfully" re-adding a bearer/JWT middleware to internal/adapters/inbound
// should fail CI, not ship silently. This does not flag the CORS
// AllowedHeaders allowlist (which legitimately lists "Authorization" as a
// header name a browser may send, not a check this service performs) or
// doc comments that reference the revert in prose.
func TestNoAuthMiddlewareReintroduced(t *testing.T) {
	for _, path := range goFilesUnder(t, "../adapters/inbound", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if authPatternRE.MatchString(line) {
				t.Errorf("%s:%d: matches a reintroduced-auth pattern: %q — the fleet-wide static-bearer auth layer was deliberately reverted 2026-09-11 (unauthenticated pending a fresh auth-model decision); if this is intentional, update this test alongside the ADR documenting the new decision", path, lineNo, trimmed)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// groupIDLiteralRE matches a kafka-go ReaderConfig's GroupID field being
// assigned a bare double-quoted string literal, e.g. `GroupID: "my-group"`.
// A symbol reference (`GroupID: AnalyticsConsumerGroup`, `GroupID: groupID,
// `GroupID: uniqueConsumerGroup()`) never matches this.
var groupIDLiteralRE = regexp.MustCompile(`GroupID:\s*"[^"]+"`)

// TestKafkaConsumerGroupNeverHardcodedInline encodes a real, already-lived
// incident: wes-work-planning's OLTP consumer group was a hardcoded literal
// (`"wes-work-planning"`), which made a locally-run e2e-tests process join
// the SAME consumer group as the live in-cluster Deployment on the shared
// fleet Kafka broker — Kafka's rebalance protocol then handed the single
// partition to only one of the two group members, silently starving
// whichever process lost the race (fixed in wes-work-planning#67 by making
// the group id env-configurable). This test doesn't ban long-lived named
// consumer groups outright (the analytics projector's
// AnalyticsConsumerGroup constant is a deliberate, correct exception — see
// its own doc comment: only one instance of that consumer ever runs, so
// there's nothing to collide with) — it bans the shape that caused the
// incident: a GroupID assigned directly from an inline string literal
// rather than through a named symbol (const, var, or function call) that
// a reviewer can trace back to its definition and reasoning.
func TestKafkaConsumerGroupNeverHardcodedInline(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			if groupIDLiteralRE.MatchString(line) {
				t.Errorf("%s:%d: GroupID assigned an inline string literal: %q — use a named const/var or a function call (see internal/adapters/outbound/facilitycache's uniqueConsumerGroup() for the per-process-unique pattern, or the AnalyticsConsumerGroup const for the single-instance pattern) so the group id's lifetime/uniqueness reasoning is traceable and a locally-run process can never silently join a live cluster's group", path, lineNo, strings.TrimSpace(line))
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// TestKafkaIntegrationTestsUseTestcontainers encodes the fleet-wide rule
// (corrected 2026-09-06): a Kafka-touching `-tags=integration` test MUST
// start its own broker via testcontainers-go/modules/kafka, never gate on
// os.Getenv("KAFKA_BROKERS") + t.Skip, and never hardcode localhost:9092.
// This fleet's CI `integration` job provisions Postgres only — a
// skip-gated Kafka test silently skips in CI and proves nothing there,
// while testcontainers is the only variant that actually exercises the
// Kafka assertions on a runner. Scans every _integration_test.go file that
// imports segmentio/kafka-go (this fleet's Kafka client) or references
// GroupID/kafka.Reader/kafka.Writer, and requires it to also import
// testcontainers-go/modules/kafka.
func TestKafkaIntegrationTestsUseTestcontainers(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", true) {
		if !strings.HasSuffix(path, "_integration_test.go") {
			continue
		}

		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		content := string(src)

		if !integrationTestTouchesKafka(content) {
			continue
		}

		hasSkipGate, hasHardcodedBroker := scanKafkaIntegrationTestRules(t, path)

		assertKafkaIntegrationTestFollowsFleetRules(t, path, content, hasSkipGate, hasHardcodedBroker)
	}
}

// integrationTestTouchesKafka reports whether an integration test's source
// references this fleet's Kafka client or its broker config at all.
func integrationTestTouchesKafka(content string) bool {
	return strings.Contains(content, "segmentio/kafka-go") ||
		strings.Contains(content, "kafka.Reader") ||
		strings.Contains(content, "kafka.Writer") ||
		strings.Contains(content, "KAFKA_BROKERS")
}

// scanKafkaIntegrationTestRules inspects the file line-by-line so a comment
// that merely MENTIONS the banned shapes (e.g. explaining that a real broker
// via testcontainers is used specifically so the test needs no
// KAFKA_BROKERS/localhost:9092) doesn't false-positive the same way a
// commented-out `t.Skip` line shouldn't either. It reports whether the file
// gates on os.Getenv("KAFKA_BROKERS") and/or hardcodes localhost:9092.
func scanKafkaIntegrationTestRules(t *testing.T, path string) (hasSkipGate, hasHardcodedBroker bool) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		if strings.Contains(line, `os.Getenv("KAFKA_BROKERS")`) {
			hasSkipGate = true
		}
		if strings.Contains(line, "localhost:9092") {
			hasHardcodedBroker = true
		}
	}
	f.Close()
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return hasSkipGate, hasHardcodedBroker
}

// assertKafkaIntegrationTestFollowsFleetRules fails the test when a
// Kafka-touching integration test violates the testcontainers rule: gating
// on an env-provided broker, hardcoding localhost:9092, or not importing
// testcontainers-go/modules/kafka.
func assertKafkaIntegrationTestFollowsFleetRules(t *testing.T, path, content string, hasSkipGate, hasHardcodedBroker bool) {
	t.Helper()

	if hasSkipGate {
		t.Errorf("%s: gates on os.Getenv(\"KAFKA_BROKERS\") — this fleet's CI integration job provisions Postgres only, so a skip-gated Kafka test silently skips in CI and proves nothing there; start a real broker via testcontainers-go/modules/kafka instead", path)
	}
	if hasHardcodedBroker {
		t.Errorf("%s: hardcodes localhost:9092 — a fresh CI runner has no broker at that address; start one via testcontainers-go/modules/kafka instead", path)
	}
	if !strings.Contains(content, "testcontainers-go/modules/kafka") {
		t.Errorf("%s: touches Kafka but does not import github.com/testcontainers/testcontainers-go/modules/kafka — Kafka-touching integration tests in this fleet must start their own broker via testcontainers, never assume/skip on an external one", path)
	}
}

const tcPostgresImport = "testcontainers-go/modules/postgres"

// dbEnvGateRE matches the env lookups that gate a Postgres integration test
// on an externally provisioned database.
var dbEnvGateRE = regexp.MustCompile(`os\.(Getenv|LookupEnv)\("(ANALYTICS_)?DATABASE_URL"\)`)

// touchesPostgres reports whether the test source talks to Postgres through
// pgx/pgxpool or database/sql (import-path check; these are real drivers,
// unlike the kafka-go type-only import the Kafka sensor has to special-case).
func touchesPostgres(content string) bool {
	return strings.Contains(content, `"github.com/jackc/pgx`) ||
		strings.Contains(content, `"database/sql"`)
}

// importsTCPostgres reports whether the file (or, via a shared helper, a
// sibling .go file in the same package directory) imports the
// testcontainers postgres module. A package's tests commonly share one
// helper (e.g. outboxDB in the postgres package), so the helper file
// satisfies the obligation for the whole directory.
func importsTCPostgres(t *testing.T, path string) bool {
	t.Helper()
	siblings, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.go"))
	if err != nil {
		t.Fatalf("glob siblings of %s: %v", path, err)
	}
	for _, s := range siblings {
		src, err := os.ReadFile(s)
		if err != nil {
			t.Fatalf("read %s: %v", s, err)
		}
		if strings.Contains(string(src), `"github.com/testcontainers/`+tcPostgresImport+`"`) {
			return true
		}
	}
	return false
}

// postgresIntegrationViolations returns every way one _integration_test.go
// file breaks the testcontainers-Postgres rule: gating on DATABASE_URL /
// ANALYTICS_DATABASE_URL (env lookup or a DB-env-tied t.Skip) in non-comment
// source, or using a Postgres driver with no testcontainers postgres import
// reachable in its package. Pure over (path) so the violation test can feed
// it a bad fixture.
func postgresIntegrationViolations(t *testing.T, path string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out []string
	lineNo := 0
	scanner := bufio.NewScanner(strings.NewReader(string(src)))
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		switch {
		case dbEnvGateRE.MatchString(line):
			out = append(out, fmt.Sprintf("%s:%d: reads a DATABASE_URL env var (%q) — an env-gated Postgres test silently skips wherever the var is unset; start a real database via testcontainers-go/modules/postgres instead", path, lineNo, strings.TrimSpace(line)))
		case strings.Contains(line, "t.Skip") && strings.Contains(strings.ToUpper(line), "DATABASE_URL"):
			out = append(out, fmt.Sprintf("%s:%d: t.Skip tied to a missing DATABASE_URL (%q) — Postgres integration tests must boot their own database via testcontainers, never skip", path, lineNo, strings.TrimSpace(line)))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	if touchesPostgres(string(src)) && !importsTCPostgres(t, path) {
		out = append(out, fmt.Sprintf("%s: uses pgx/database/sql but neither this file nor a sibling .go file in its package imports github.com/testcontainers/testcontainers-go/modules/postgres — Postgres integration tests must start their own database via testcontainers (reuse the package's shared helper)", path))
	}
	return out
}

// TestPostgresIntegrationTestsUseTestcontainers mirrors the Kafka rule for
// Postgres: a `-tags=integration` test MUST boot its own database via
// testcontainers-go/modules/postgres, never gate on DATABASE_URL /
// ANALYTICS_DATABASE_URL + t.Skip (a skip-gated test silently proves nothing
// wherever the variable is unset), and a file that touches pgx/database/sql
// must have the testcontainers postgres import reachable in its package.
func TestPostgresIntegrationTestsUseTestcontainers(t *testing.T) {
	// internal/ and cmd/ — the only trees that hold Go sources; walking the
	// repo root would crawl docs/ and web/ node_modules for nothing.
	var paths []string
	for _, root := range []string{"..", "../../cmd"} {
		paths = append(paths, goFilesUnder(t, root, true)...)
	}
	for _, path := range paths {
		if !strings.HasSuffix(path, "_integration_test.go") {
			continue
		}
		for _, v := range postgresIntegrationViolations(t, path) {
			t.Error(v)
		}
	}
}

// TestPostgresIntegrationSensorFailsOnBadFixtures proves the sensor above
// can actually fail: each fixture breaks the rule one way and must be
// reported, while a compliant fixture and a comment-only mention must not.
func TestPostgresIntegrationSensorFailsOnBadFixtures(t *testing.T) {
	cases := []struct {
		name    string
		files   map[string]string
		wantErr string // substring of a violation; "" means no violation expected
	}{
		{
			name:    "env gate",
			files:   map[string]string{"bad_integration_test.go": "package x\n\nfunc f() {\n	dsn := os.Getenv(\"DATABASE_URL\")\n	_ = dsn\n}\n"},
			wantErr: "reads a DATABASE_URL env var",
		},
		{
			name:    "analytics env gate",
			files:   map[string]string{"bad_integration_test.go": "package x\n\nfunc f() {\n	_ = os.Getenv(\"ANALYTICS_DATABASE_URL\")\n}\n"},
			wantErr: "reads a DATABASE_URL env var",
		},
		{
			name:    "skip tied to missing db env",
			files:   map[string]string{"bad_integration_test.go": "package x\n\nfunc f(t *testing.T) {\n	t.Skip(\"DATABASE_URL not set\")\n}\n"},
			wantErr: "t.Skip tied to a missing DATABASE_URL",
		},
		{
			name:    "pgx without testcontainers",
			files:   map[string]string{"bad_integration_test.go": "package x\n\nimport \"github.com/jackc/pgx/v5/pgxpool\"\n\nvar _ *pgxpool.Pool\n"},
			wantErr: "uses pgx/database/sql but neither this file nor a sibling",
		},
		{
			name: "pgx with helper in sibling file",
			files: map[string]string{
				"ok_integration_test.go":     "package x\n\nimport \"github.com/jackc/pgx/v5/pgxpool\"\n\nvar _ *pgxpool.Pool\n",
				"helper_integration_test.go": "package x\n\nimport tcpostgres \"github.com/testcontainers/testcontainers-go/modules/postgres\"\n\nvar _ = tcpostgres.Run\n",
			},
		},
		{
			name:  "comment mention is not a violation",
			files: map[string]string{"ok_integration_test.go": "package x\n\n// never reads os.Getenv(\"DATABASE_URL\") and never t.Skip(\"DATABASE_URL\")\nfunc f() {}\n"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fixtureViolations(t, tc.files)
			if tc.wantErr == "" {
				if len(got) != 0 {
					t.Fatalf("compliant fixture reported violations: %v", got)
				}
				return
			}
			if !anyContains(got, tc.wantErr) {
				t.Fatalf("sensor did not flag the bad fixture (want substring %q), got %v", tc.wantErr, got)
			}
		})
	}
}

// fixtureViolations writes the fixture files into a temp dir and runs the
// sensor on the one that is not a "helper_" file (helpers exist only to
// satisfy the sibling-import check).
func fixtureViolations(t *testing.T, files map[string]string) []string {
	t.Helper()
	dir := t.TempDir()
	var target string
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		if !strings.HasPrefix(name, "helper_") {
			target = p
		}
	}
	return postgresIntegrationViolations(t, target)
}

func anyContains(items []string, sub string) bool {
	for _, s := range items {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// domainTagRE matches a Go struct tag that carries serialisation metadata
// (`json:"..."` or `db:"..."`) inside a backtick-delimited tag literal.
var domainTagRE = regexp.MustCompile("`[^`]*\\b(json|db):\"[^`]*`")

// domainTagWhitelist lists non-test files under internal/domain/** that may
// keep serialisation tags, each with the reason. Empty: wire shape belongs
// to adapters (see internal/adapters/outbound/eventwire).
var domainTagWhitelist = map[string]string{}

// domainTagViolations returns file:line violations for one file: any
// non-comment line declaring a `json:"` / `db:"` struct tag. Pure over
// (path) so the fixture test can feed it bad input.
func domainTagViolations(t *testing.T, path string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out []string
	lineNo := 0
	scanner := bufio.NewScanner(strings.NewReader(string(src)))
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		if domainTagRE.MatchString(line) {
			out = append(out, fmt.Sprintf("%s:%d: domain struct carries a serialisation tag (%q) — wire/storage shape is an adapter concern; map through an adapter-owned DTO (see internal/adapters/outbound/eventwire) and keep internal/domain/** tag-free", path, lineNo, strings.TrimSpace(line)))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}

// TestDomainHasNoSerialisationTags: no `json:"` / `db:"` struct tags in
// non-test files under internal/domain/** (ADR: serialisation lives in
// adapters, so a wire-format change never touches the domain).
func TestDomainHasNoSerialisationTags(t *testing.T) {
	for _, path := range goFilesUnder(t, "../domain", false) {
		if reason, ok := domainTagWhitelist[filepath.ToSlash(path)]; ok && reason != "" {
			continue
		}
		for _, v := range domainTagViolations(t, path) {
			t.Error(v)
		}
	}
}

// TestDomainTagSensorFailsOnBadFixtures proves the sensor can fail: tagged
// structs are reported, an untagged struct and comment mentions are not.
func TestDomainTagSensorFailsOnBadFixtures(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"json tag", "package x\n\ntype E struct {\n	A string `json:\"a\"`\n}\n", true},
		{"db tag", "package x\n\ntype E struct {\n	A string `db:\"a\"`\n}\n", true},
		{"multi tag", "package x\n\ntype E struct {\n	A string `yaml:\"a\" json:\"a,omitempty\"`\n}\n", true},
		{"untagged struct", "package x\n\ntype E struct {\n	A string\n}\n", false},
		{"comment mention", "package x\n\n// A used to be `json:\"a\"`\ntype E struct {\n	A string\n}\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "fixture.go")
			if err := os.WriteFile(p, []byte(tc.body), 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			got := domainTagViolations(t, p)
			if tc.wantErr && len(got) == 0 {
				t.Fatal("sensor did not flag the tagged fixture")
			}
			if !tc.wantErr && len(got) != 0 {
				t.Fatalf("compliant fixture reported violations: %v", got)
			}
		})
	}
}
