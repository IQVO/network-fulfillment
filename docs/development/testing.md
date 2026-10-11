---
id: testing
title: Testing and CI
sidebar_label: Testing
---

# Testing and CI

The test pyramid as it is in this repo on `develop`, the `make` targets that
run each layer, and what every CI job in `.github/workflows/ci.yml` does.

## Layers at a glance

| Layer | Where | How many | Run with | Needs |
| --- | --- | --- | --- | --- |
| Unit (domain, use cases, adapters with fakes, `httptest`) | `internal/**/*_test.go`, `cmd/netfulfil/*_test.go` | 49 test files without a build tag (not counting the BDD and architecture files below) | `make test` | Go only |
| BDD / acceptance (godog) | `features/*.feature`, step code in `features_*_test.go` at the repo root | 9 feature files, 76 `Scenario`/`Scenario Outline` blocks; 2 tagged `@known-bug` are excluded; the rest expand to 126 executed scenarios, 853 steps | `make bdd` | Go only |
| Integration (testcontainers) | files with `//go:build integration` | 10 files | `make integration` | Docker |
| Architecture fitness | `internal/architecture/` | 5 files, 13 `Test*` functions | `make arch-test` | Go only |
| Mutation | `gremlins` on `./internal/domain/networkorder` | thresholds in `.gremlins.yaml` | `make mutation` | `gremlins` v0.6.0 |
| API contract lint | `apis/openapi.yaml` | Spectral, `.spectral.yaml` | CI `api-lint` only | Node 20 |
| Chart tests | `charts/network-fulfillment/tests/*.py` | 2 scripts | CI `helm-lint` only | Helm, Python, PyYAML |
| Frontend | `web/src/**/*.test.ts(x)` | 2 test files (vitest) | `npm test` in `web/` | Node; **no CI job** |

There is no contract test against a running server (no schemathesis), no
consumer-driven contract test, and no eval suite in this repo.

## Unit tests

`make test` runs `go test ./... -race`. No database, broker or network is
needed: the default build has the in-memory repositories, the stub gateway and
fake ports. Notable suites:

- `internal/domain/networkorder` and `internal/domain/capabilityoffer`: every
  lifecycle transition and invariant, plus `ParseState`/`ParseBasis`.
- `internal/application/usecases`: each use case, including
  `failure_paths_test.go` (planner, gateway and repository failures),
  `cycle_time_eligibility_test.go` (ADR 0017) and `events_test.go`.
- `internal/adapters/outbound/kafka/wire_golden_test.go`: the CloudEvents wire
  shape of every event against `testdata/wire/{events,analytics}/*.json`.
- `internal/adapters/outbound/events/testdata/log_publisher.golden`: the log
  publisher's output.
- `internal/adapters/inbound/http/otel_metrics_test.go`: the otelhttp
  instruments the REST handler emits.
- `cmd/netfulfil/retry_test.go`, `wiring_test.go`: the boot retry, the
  in-memory fallback when `DATABASE_URL` is unset, and the
  `MIGRATIONS_DATABASE_URL` split.

**Coverage gate**: `make coverage` (and the CI `test` job) measure coverage
of `./internal/domain/...` and `./internal/application/...` only, and fail
below **90 %**.

## BDD acceptance suite (ADR 0018)

`features_test.go` (`TestFeatures`) runs every `features/*.feature` with
godog in strict mode against the **real** routers: the OLTP
`Server.Routes()` and the reports chi router, wired over the in-memory
adapters. Orders are created the way production creates them: by seeding the
real `StubGateway` and running the real poller.

| Feature file | Scenarios (blocks) |
| --- | --- |
| `acknowledgement_lifecycle.feature` | 9 |
| `acknowledgement_report.feature` | 15 |
| `capability_offers.feature` | 12 |
| `inbound_status.feature` | 6 |
| `integration_events.feature` | 5 |
| `network_order_intake.feature` | 10 (1 `@known-bug`) |
| `network_order_queries.feature` | 7 |
| `operations.feature` | 6 |
| `shipment_confirmation.feature` | 6 (1 `@known-bug`) |

`@known-bug` scenarios state the correct behaviour for a defect the suite
found and are excluded until it is fixed. The test-only variable `BDD_TAGS`
overrides the default filter `~@known-bug`:

```bash
BDD_TAGS=@known-bug go test . -run TestFeatures -v   # watch the two known bugs fail
```

The two excluded scenarios are described in
[troubleshooting](../operations/troubleshooting.md): an order that failed
during an order-management outage is never answered later, and an empty
`networkRef` on the shipment confirmation is a 307 redirect, not a 422.

Output of `go test . -run TestFeatures -count=1 -v` on this branch:

```text
126 scenarios (126 passed)
853 steps (853 passed)
--- PASS: TestFeatures (0.25s)
```

## Integration tests

`make integration` runs `go test -tags=integration ./... -race -count=1`.
Every test starts its own Postgres or Kafka with testcontainers
(`testcontainers-go` v0.44.0, modules `postgres` and `kafka`); there is no
`DATABASE_URL` service container and no skip gate. Two fitness tests enforce
that (`TestPostgresIntegrationTestsUseTestcontainers`,
`TestKafkaIntegrationTestsUseTestcontainers`).

| File | Covers |
| --- | --- |
| `internal/adapters/outbound/postgres/integration_test.go` | shared fixtures, migrations against a real Postgres |
| `.../postgres/network_order_repo_integration_test.go` | the order repository |
| `.../postgres/capability_offer_repo_integration_test.go` | the offer repository |
| `.../postgres/outbox_integration_test.go` | outbox write and relay |
| `.../postgres/pool_limits_integration_test.go` | `MaxConns` and `statement_timeout` really applied |
| `.../postgres/rehydrate_validation_integration_test.go` | corrupt stored state is refused on load |
| `internal/adapters/outbound/analyticsstore/postgres_integration_test.go` | projection and report store |
| `internal/adapters/outbound/kafka/publisher_integration_test.go` | both publishers against a real broker |
| `internal/adapters/inbound/kafka/analytics_projector_integration_test.go` | consumer to projection, end to end |
| `internal/adapters/inbound/kafka/analytics_dlq_integration_test.go` | retry and dead-letter path |

The `make help` text still says integration "needs a running Postgres"; the
Makefile comment and the tests themselves only need Docker.

## Architecture fitness tests

`make arch-test` runs `go test ./internal/architecture/... -v`:

| Test | Guards |
| --- | --- |
| `TestHexagonalArchitecture` | `arch-go` rules: domain depends on nothing, application on domain, adapters on both |
| `TestMCPAdapterDependencyRule` | the MCP adapter depends only on application and domain, and nothing but `cmd/mcp` imports it |
| `TestNoAuthMiddlewareReintroduced` | no auth middleware on REST or MCP (fleet rule) |
| `TestKafkaConsumerGroupNeverHardcodedInline` | no reader's `GroupID` is an inline string literal; it must come from a named constant, variable or function |
| `TestKafkaIntegrationTestsUseTestcontainers`, `TestPostgresIntegrationTestsUseTestcontainers`, `TestPostgresIntegrationSensorFailsOnBadFixtures` | integration tests start real containers |
| `TestDomainHasNoSerialisationTags`, `TestDomainTagSensorFailsOnBadFixtures` | no JSON tags in `internal/domain` (wire shape lives in `eventwire`) |
| `TestCloudEventsOnly` | every producer and consumer goes through `internal/adapters/kafka/cloudevents` |
| `TestReplayConsumersSetCommitInterval` | FirstOffset replay readers set a `CommitInterval` |
| `TestEventCatalogueMatchesContract`, `TestEventCatalogueDetector` | every CloudEvents `type` this service declares for itself in `apis/asyncapi.yaml` also appears in an ADR under `docs/adr/` (the type catalogue); the detector test checks the checker |
| `violation_test.go` | not a test: the shared `archViolation` helper that turns a failed rule into a what/why/fix message (managed by the harness template) |

## Mutation testing

`make mutation` runs `gremlins unleash ./internal/domain/networkorder` with
`.gremlins.yaml`: one worker, `timeout-coefficient` 30, thresholds
`efficacy: 99` and `mutant-coverage: 92`. gremlins fails when the measured
value is **at or below** a threshold. The file records the measurement the
thresholds came from: 14 killed, 0 lived, 1 not covered (the
`AcknowledgementWindow` constant), so 100 % efficacy and 93.33 % mutant
coverage. That measurement predates `internal/domain/capabilityoffer`;
re-measure before widening the fast subset. `make mutation-full` runs the
whole `./internal/domain` tree; no CI job runs it.

## Make targets

| Target | Runs |
| --- | --- |
| `make check-fast` | `fmt-check`, `vet`, `arch-test`, then `go test` on the packages changed vs `HEAD` |
| `make check` | `fmt-check`, `vet`, `build`, `lint` (golangci-lint v2.14.0), `test` |
| `make check-all` | `check` plus `coverage` (90 % gate), `arch-test`, `bdd` |
| `make test` / `bdd` / `integration` / `arch-test` | the layers above |
| `make coverage` | race tests with `-coverpkg` on domain and application, then the gate |
| `make mutation` / `mutation-full` | gremlins, fast subset / whole domain |
| `make vuln` | `govulncheck ./...` |
| `make lint` | `golangci-lint run ./...` |
| `make guide-lint` / `harness-test` | `scripts/harness/guide_lint.py` / `test_hook.py` |

Git hooks (`lefthook.yml`, after `lefthook install`): pre-commit runs
`fmt-check`, `vet`, `lint`; pre-push runs `make check`.

## CI

`.github/workflows/ci.yml` runs on push to `main` and `develop`, on pull
requests into them, weekly (`cron: 0 6 * * 1`) and on manual dispatch.

| Job | Runs | When |
| --- | --- | --- |
| `lint` | golangci-lint v2.14.0 | every run |
| `guide-lint` | `guide_lint.py`, `repo_lint.py`, `test_hook.py`, `test_repo_lint.py` | every run |
| `complexity` | golangci-lint with only `gocyclo`, `gocognit`, `cyclop`, `funlen`, `nestif`, plus an informational gocyclo >10 report | every run |
| `test` | `go build`, `go vet`, race unit tests with coverage, the 90 % gate, uploads `coverage.out` | every run |
| `bdd` | `go test ./... -run TestFeatures -v` | every run |
| `integration` | build and vet with `-tags=integration`, then the integration suite | every run |
| `api-lint` | Spectral on `apis/openapi.yaml` (`--fail-severity=warn`) | every run |
| `mutation-fast` | `gremlins unleash ./internal/domain/networkorder` | every run |
| `vuln` | `govulncheck ./...` | every run |
| `helm-lint` | `helm lint charts/network-fulfillment` and the two chart wiring scripts | every run |
| `arch-test` | `go test ./internal/architecture/... -v` | every run |
| `trivy-scan` | builds the image, fails on fixable CRITICAL/HIGH | PRs into `main` only |
| `docker-publish` | push, cosign sign, SBOM attest (needs `lint`, `complexity`, `test`, `bdd`) | push to `main` |
| `release` | semver tag, chart push, GitHub Release (needs `docker-publish`) | push to `main` |

A separate `.github/workflows/ai-review.yml` exists alongside `ci.yml`.

Not present, compared with the harness template: a scheduled exhaustive
`mutation` job (the weekly cron re-runs the jobs above instead; `.gremlins.yaml`
still mentions it), `drift`, `docs-api-drift` (there is no docs site) and a
`web` job.

`develop` requires `lint`, `test`, `mutation-fast`, `vuln`, `arch-test`,
`helm-lint`, `integration` and `api-lint` (strict). See the
[runbook](../operations/runbook.md#branch-protection).
