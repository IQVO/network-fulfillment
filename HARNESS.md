# HARNESS.md — warehouse-harness-template v1

This file is the canonical description of every sensor and guide in this
template: what it does, what it costs, and when in the change lifecycle it
runs. Every instantiated repo should keep a copy of this file (or a link
to this one) so an agent working in that repo can answer "what harness do
I have, and why" without archaeology.

Reference: Birgitta Böckeler, "Harness engineering for coding agent users"
(martinfowler.com, 02 Apr 2026). Model: **guides** (feedforward) + **sensors**
(feedback), each either **computational** or **inferential**, spread across
three regulation categories: **maintainability**, **architecture fitness**,
**behaviour**.

---

## Guides (feedforward)

| File | Category | Purpose |
|---|---|---|
| `AGENTS.md` / `CLAUDE.md` | computational | Entry point every agent session reads first: repo shape, commands, conventions. |
| `.claude/rules/domain-model.md` | computational | Ubiquitous language, aggregates, domain events — keeps an agent's vocabulary aligned with the real domain. |
| `.claude/rules/rest-api.md` | computational | Route → use case mapping, error/auth conventions. |
| `.claude/rules/integration-events.md` | computational | Kafka publish/consume contract, consumer-group rules. |
| `.claude/skills/how-to-*.md` | inferential | Reusable how-tos for recurring task shapes (add an endpoint, add an integration event, add a frontend remote, write an ADR, test). |
| ADRs (`adr/` or `docs/adr/`) | computational | Point-in-time architecture decisions with rationale — the fleet's real source of truth for "why," not just "what." |

## Sensors (feedback) — lifecycle position

| Sensor | Category | Type | Lifecycle | Blocking? | Cost |
|---|---|---|---|---|---|
| `make fmt-check` / `lefthook pre-commit` | maintainability | computational | pre-commit | yes (local) | seconds |
| `lint` (golangci-lint) | maintainability | computational | pre-commit, PR | yes | seconds |
| `test` (`go test -race`) | maintainability | computational | pre-commit (fast subset), PR | yes | seconds–minutes |
| `coverage` (90% gate) | maintainability | computational | pre-push, PR | yes | minutes |
| `bdd` (godog/Gherkin) | behaviour | computational | pre-push, PR | yes | seconds |
| `arch-test` (arch-go fitness) | architecture fitness | computational | pre-push, PR | yes | seconds |
| `integration` (testcontainers) | behaviour | computational | PR | yes | minutes |
| `mutation-fast` (gremlins, one aggregate) | maintainability | computational | PR | yes | ~1 min |
| `mutation` (gremlins, full domain) | maintainability | computational | weekly schedule | no (advisory report) | tens of minutes |
| `vuln` (govulncheck) | maintainability | computational | PR | yes | seconds |
| `api-lint` (Spectral) | maintainability | computational | PR | yes | seconds |
| `docs-api-drift` | maintainability | computational | PR | yes | seconds |
| `web` (vitest/tsc/eslint) | maintainability | computational | PR | yes | minutes |
| `helm-lint` | maintainability | computational | PR (targeting main) | yes | seconds |
| `trivy-scan` | maintainability | computational | PR (targeting main) | yes (CRITICAL/HIGH w/ fix) | minutes |
| `CodeQL` | maintainability | computational | PR, schedule | yes (conversation-resolution) | minutes |
| `Scorecard` | maintainability | computational | schedule | no | minutes |
| `drift` (deadcode, `go mod tidy -diff`, knip, coverage-quality) | maintainability | computational | weekly schedule | **no — advisory** | minutes |
| `/code-review`, `/architecture-review`, `/domain-review` | architecture fitness / behaviour | **inferential** | invoked by hand (local-only, per user decision 2026-09-13) | no | LLM cost per invocation |

**New sensors default to advisory for ~2 weeks, then auto-promote to
blocking** (user decision, Phase 5 of the harness-coverage-expansion plan)
unless the sensor is inherently non-deterministic (inferential/LLM-based),
in which case it stays advisory until its precision is proven — see the
plan's Phase 4 risk notes.

## Fitness functions and the incidents that motivated them

Every rule in `internal/architecture/fitness_test.go` corresponds to a real,
already-lived incident, not a hypothetical:

- **TestHexagonalArchitecture** (6 rules) — the base ports-and-adapters
  dependency direction. Uncontroversial, but never assume it holds without
  running it: `order-management` shipped this same file for a period
  without ever wiring it into CI, and no one noticed.
- **TestMCPAdapterDependencyRule** (conditional, only if `cmd/mcp` exists)
  — ADR-0008: MCP servers are additive inbound adapters. Nothing may
  depend on the MCP package; it may depend only on application+domain.
- **TestNoAuthMiddlewareReintroduced** — encodes the fleet-wide
  2026-09-11 static-bearer-auth revert. An agent "helpfully" re-adding
  auth middleware fails CI instead of shipping unreviewed.
- **TestKafkaConsumerGroupNeverHardcodedInline** — wes-work-planning's
  hardcoded consumer group id let a locally-run e2e-tests process silently
  join the SAME consumer group as the live in-cluster Deployment on the
  shared platform Kafka broker, starving one of the two processes via
  Kafka's rebalance protocol. Fixed in wes-work-planning#67; this test
  keeps it fixed.
- **TestKafkaIntegrationTestsUseTestcontainers** — this fleet's CI
  `integration` job provisions Postgres only. A Kafka-touching
  `-tags=integration` test that gates on `os.Getenv("KAFKA_BROKERS")` +
  `t.Skip`, or hardcodes `localhost:9092`, silently skips or fails on
  every CI runner and proves nothing. testcontainers is the only variant
  that actually exercises the assertions.

## Mutation testing: what it catches that coverage doesn't

`go test -cover` measures whether a line EXECUTED during a test run.
gremlins measures whether a test would actually FAIL if that line's logic
were subtly wrong (each mutant is one such subtle change: `>=` to `>`, a
negated boolean, a swapped operand). 100% line coverage with a surviving
mutant means a test exercises the code path but asserts nothing about its
actual behaviour on that path — the "green tests that assert nothing"
failure mode the `drift` job's coverage-quality check is built to catch.

**gremlins fails when the measured value is `<=` the configured threshold**
(not `<`) — see `.gremlins.yaml`'s own comment. Always measure first
(`make mutation-full`), then set the threshold strictly below what you
measured; never copy a sibling repo's numbers.

**Pitfall: a tagless `switch { case ...: ... }` can under-report mutator
coverage versus the equivalent `if / if / return` chain**, even with
identical test coverage. Confirmed concretely on warehouse-ops-agent PR #58:
a pure threshold-classifier's mutator coverage measured 89.91% (below a
96% gate) as a switch, then 97.17–97.25% after rewriting as an if-chain,
with zero test changes. If a pure classification function's mutator
coverage comes in surprisingly low despite full branch coverage in
`go test -cover`, try the if-chain rewrite before assuming tests are
missing a case.

## Credential handling in this harness

Never embed a real password in a `scheme://user:PASSWORD@host` URL
anywhere in committed files, including local-dev-only test fixtures.
Some agent tooling treats that literal shape as a credential leak and
will silently strip/redact it on write AND read-back — a write that
"succeeds" but reads back unchanged is that guard firing, not a bug to
retry around. Use a password-less URL plus a separate `PGPASSWORD` env
var (shell-launched processes), or your driver's own password-injection
API (`pgx.ParseConfig` + setting `.Password` explicitly, for a single Go
process that needs several different DB passwords at once — see
e2e-tests' `dbOpen` helper for the concrete pattern).

## Instantiating this template

Run `scripts/new-service.sh <service-name> <richest-aggregate-pkg>` from a
fresh repo checkout. It substitutes every `{{SERVICE}}` / `{{SERVICE_REPO}}`
/ `{{RICHEST_AGGREGATE}}` placeholder and tells you which files still need
manual content (the `.claude/rules/*.md` skeletons, `.gremlins.yaml`'s
measured thresholds). See `scripts/new-service.sh`'s own header comment for
the full instantiation checklist.

## Agent runtime harness (v3): one logic, three runtimes

v3 makes the guides LOAD and puts sensors INSIDE the agent loop, for Claude Code, OpenCode and Codex.

| Piece | File(s) | Cost | Lifecycle position |
|---|---|---|---|
| Skills that actually load | `.claude/skills/<name>/SKILL.md` (frontmatter `name`+`description`); `.agents/skills` symlink for Codex; OpenCode scans `.claude/skills` natively | ~50 tokens each until used | on task match |
| Review skills (explicit, v4: isolated subagents) | `.claude/agents/{code-review,architecture-review,domain-review}.md`, invoked via `/code-review`, `/architecture-review`, `/domain-review` | LLM | on demand, pre-merge |
| Path-scoped rules | `.claude/rules/*.md` with `paths:`; CLAUDE.md carries a generated pointer table for runtimes without scoping | tokens only when matching files are touched | on edit |
| Guard + feedback hooks | `scripts/harness/hook.py` (`pre` / `post` / `stop`) + adapters `.claude/settings.json`, `.codex/hooks.json`, `.opencode/plugins/harness.ts` | ms to seconds | every tool call; end of turn |
| Fast gate | `make check-fast` (fmt-check, vet, arch-test, tests of changed packages) | seconds | Stop hook, before "done" |
| Guide freshness | `scripts/harness/guide_lint.py` / `make guide-lint` (CI job `guide-lint`) | CPU, ms | PR |
| Red-sensor loop | `scripts/harness/red_issue.py` steps in scheduled jobs -> `harness:red` issue | none | weekly |
| Advisory AI review | `.github/workflows/ai-review.yml` (needs `ANTHROPIC_API_KEY`; no-ops without it) | API spend | PR into develop |
| Health report | `scripts/harness-health.py` (does it WORK: skills load, hooks wired, scheduled runs green) | gh calls | weekly cron |

Rules live ONLY in `hook.py`; the three adapters translate events and cannot drift. The hooks block
pushes to develop/main, `--no-verify`, force-push, `rm -rf` on non-scratch paths, destructive git, and
edits to generated files; they feed gofmt/vet findings back after each edit. Every message states
WHAT / WHY / FIX. `HARNESS_PROTECT_THRESHOLDS=1` (fix-agent mode) additionally forbids editing gates
(`.gremlins.yaml`, `.golangci.yml`, `internal/architecture/**`, CI). `HARNESS_OFF=1` disables the lot.

Runtime notes (verified against real runs, 2026-10-03):
- Claude Code: hooks run in any trusted workspace; project `permissions.allow` is ignored until the user
  accepts the workspace trust dialog once.
- Codex: project hooks run only after the user trusts them once (`/hooks`), or per run with
  `--dangerously-bypass-hook-trust` (+ `--enable hooks`, project trusted). Codex may write files through
  shell heredocs rather than `apply_patch`, so the PostToolUse gofmt feedback can be skipped; the Stop
  gate (`make check-fast`) and lefthook are the backstop.
- OpenCode 2.x: the plugin uses `ctx.permission.hook("evaluate")` (deny) and `ctx.tool.hook`; the 1.x
  `server()` hooks are exported from the same file. The idle -> re-prompt stop gate is best-effort.

Template tooling that must NOT be copied into an instantiated service: `tools/` (`migrate_v3.py`, `migrate_v4.py`,
`harness_eval.py`/`harness_eval2.py`/`harness_probe.py`, brings/measures an existing repo against the template),
`benchmarks/wms-bench/` (evaluates repos FROM the template, not a per-repo artifact), `scripts/harness-audit.py`,
`scripts/harness-health.py`, `scripts/new-service.sh`.
Run `python3 tools/migrate_v3.py --repo <path>` then `python3 tools/migrate_v4.py --repo <path>` to (re)apply the
managed files to an existing service; both are idempotent and together are the engine of the weekly template sync.

## Config sensors (`repo_lint`) and the incident protocol

`scripts/harness/repo_lint.py` (stdlib only, runs in the `guide-lint` CI job) checks the configuration mistakes that
reached CI in practice: **R1** workflow `working-directory`/`cache-dependency-path` that do not exist (checkout-path
aware), **R2** `needs:` naming jobs that do not exist, **R3** Dependabot entries that cannot work (missing directory or
manifest; an npm tree with a `file:` dependency outside the repo), **R4** a stale GitHub owner in registry/docs/chart
URLs, **R5** unsubstituted `{{PLACEHOLDER}}`s, **R6** missing Dependabot coverage, **R7** repo scripts a workflow runs
from the wrong directory, **R8** a docs workflow that does not build on `pull_request` (or whose Pages deploy job is not
gated off PRs), **R9** a chart test (`charts/<c>/tests/*.sh`, `test_*.py`) that no workflow runs (directly, through a
`make` target CI runs, or from another chart test), **R10** a chart ConfigMap that no Deployment/workload in the chart
references (`envFrom.configMapRef`, `configMapKeyRef`, `configMap` volume), **R11** a coverage package set defined
outside the Makefile (a workflow `run:` line passing `-coverpkg` itself) or a Makefile `coverage` gate that no workflow
runs (directly or through a target that has `coverage` as a prerequisite; in this repository `ci.yml.template` counts
as a workflow). `# repo-lint: ignore` suppresses one line
(say why; for R9 put it in the first lines of the test, for R10 on the ConfigMap's `kind:` line). Tests:
`scripts/harness/test_repo_lint.py`; the cases named `incident` are the real incidents and must keep failing the lint.
`ci.yml.template` ships the matching `chart-tests` job (every chart test, PyYAML installed hash-pinned from
`.github/requirements/chart-tests.txt`, a test printing SKIP fails), and its `test` job runs `make coverage`, so the
coverage scope and threshold live only in the Makefile (`COVERAGE_PKGS`, `COVERAGE_THRESHOLD`).

**Incident protocol.** Every failure that was not caught before CI ends with: the fix, a sensor or guide that would have
caught it, that sensor rolled out to the whole fleet, and a row in [INCIDENTS.md](INCIDENTS.md). A row with no guard is
an open gap and says so. A check whose message needed a human to explain it is not done: sensor output is a prompt, so
it must state WHAT, WHY and FIX.

## Versioning

This is `harness-template: v1`. Record that string in each instantiated
repo's `AGENTS.md` so a fleet-wide audit (see the `harness-audit` tool in
the companion `warehouse-systems-fleet-ops` skill) can tell which repos are
behind the template and by how much.

## Harness² comparison (v4)

A published research paper's Harness² (arXiv:2610.14224, "Recursive Agent Harnessing for an Open World")
edits an agent's own harness DURING a task (Propose-Probe-Compose: propose an edit, probe it
against the same task with no ground truth needed, compose the winner) and keeps a persistent
"improvement harness" of adopted edits across tasks. This template's v1-v3 harness only ever
changed on a human's cadence: an incident happens, a human writes the INCIDENTS.md row and the
fitness test, `migrate_v3.py` rolls it out. v4 closes three concrete gaps that comparison
surfaced, each scoped to something this fleet had ALREADY identified as a problem (not a
speculative import of the paper's generality):

- **No task-level recursion.** EVALS.md rounds 2-3 found "a docs convention that is only a
  sentence in a skill is followed about half the time by everyone" and could not tell whether
  rewording the guide would change that, or whether to stop spending on prose and build a sensor
  instead (its own stated conclusion). `tools/harness_probe.py` answers exactly that question with
  real `claude -p` runs scored by `harness_eval2.py`'s existing gates: baseline wording vs a
  candidate, same task, interleaved — Compose prints adopt/keep/inconclusive, never a guess.
- **Harnessing-level recursion was 100% manual.** `red_issue.py --draft` (new) drafts the FIRST
  PASS of an INCIDENTS.md row and a `t.Skip`-marked fitness-test stub from a failing scheduled
  job, and opens it as a draft PR — mirroring Harness²'s "experience reflector" for the drafting
  step only. It never fills in root cause or a real assertion, and never merges; a human (or the
  existing fix agent) still does the adoption Harness² automates, because hard architecture calls
  in this fleet stay human decisions.
- **Two of the paper's 8 harness components were missing.** `.claude/agents/*.md` promotes
  `code-review`/`architecture-review`/`domain-review` from prompted skills to isolated-context
  subagents (the "subagents" component); `.claude/memory/MEMORY.md` gives each repo a durable,
  repo-specific heuristics file (the "memory" component) distinct from INCIDENTS.md (incidents)
  and EVALS.md (measured harness claims).

What v4 deliberately does NOT import: Harness²'s executor posture. Their README runs with the
Codex sandbox disabled and network access unrestricted, because the paper needs unconstrained
agent behaviour to measure. This fleet's `hook.py` defaults closed (blocks destructive git,
`--no-verify`, edits to generated files/gates) and stays that way — loosening it toward Harness²'s
benchmark posture would trade a measured research convenience for a production-safety regression
this fleet does not need.

`benchmarks/wms-bench/` is the evaluation-rigor half of the comparison: Harness² tracks scores
per harness config across JobBench/LAB/WorkBuddy-Bench; this fleet's EVALS.md never had a fixed,
repeatable suite to do the equivalent across harness-template versions. `benchmarks/wms-bench/`
is seeded with 3 tasks (reusing `harness_eval.py`/`harness_eval2.py`'s prompts and gates
unchanged) and a `run.py` that names results by the target repo's version marker, so v3-vs-v4
(and v4-vs-v5) becomes a diff of two JSONL files instead of a fresh ad hoc round each time.
