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
| Unit (domain, use cases, adapters with fakes, `httptest`) | `internal/**/*_test.go`, `cmd/netfulfil/*_test.go` | 68 test files without a build tag (not counting the BDD and architecture files below) | `make test` | Go only |
| BDD / acceptance (godog) | `features/*.feature`, step code in `features_*_test.go` at the repo root | 10 feature files, 83 `Scenario`/`Scenario Outline` blocks that expand to 135 executed scenarios, 951 steps; no scenario is tagged `@known-bug` any more | `make bdd` | Go only |
| Integration (testcontainers) | files with `//go:build integration` | 20 files | `make integration` | Docker |
| Architecture fitness | `internal/architecture/` | 11 files, 25 `Test*` functions | `make arch-test` | Go only |
| Mutation | `gremlins` on `./internal/domain/networkorder` | thresholds in `.gremlins.yaml` | `make mutation` | `gremlins` v0.6.0 |
| API contract lint | `apis/openapi.yaml` | Spectral, `.spectral.yaml` | CI `api-lint` only | Node 20 |
| Chart tests | `charts/network-fulfillment/tests/*.py` | 2 scripts | CI `helm-lint` only | Helm, Python, PyYAML |
| Frontend | `web/src/**/*.test.ts(x)` | 2 test files (vitest) | `npm test` in `web/` | Node; **no CI job** |

There is no contract test against a running server (no schemathesis), no
consumer-driven contract test, and no eval suite in this repo.

## Unit tests

`make test` runs `go test ./... -race`. No database, broker or network is
needed: unit and BDD tests use the in-memory repositories (test doubles, not a
runtime mode), the stub gateway and fake ports. Notable suites:

- `internal/domain/networkorder` and `internal/domain/capabilityoffer`: every
  lifecycle transition and invariant, plus `ParseState`/`ParseBasis`. For the
  hold (ADR 0024): `network_order_hold_test.go` (`RecordHold`, `SettleHold`,
  `HoldOrphaned` at every state and at the grace boundary) and
  `network_order_committed_test.go` (`CommittedLocalOrderId`).
- `internal/application/usecases`: each use case, including
  `failure_paths_test.go` (planner, gateway and repository failures),
  `cycle_time_eligibility_test.go` (ADR 0017) and `events_test.go`. For ADR
  0024: `sweep_orphaned_holds_test.go` (every order-management answer, the
  grace, the outcome never marked done on a failure, one event per hold) and
  `hold_recording_test.go` (the hold is recorded at raise; `freeHold` records
  `CANCELLED` with the rejection).
- `internal/adapters/outbound/ordermanagement/cancel_answers_test.go`: the
  cancel's 404 and 409/422 become the two contract sentinels, and the breaker
  counts them as answers, not failures.
- `internal/adapters/inbound/{http,mcp}/hold_exposure_test.go`: `localOrderId`
  stays absent for a `NEW` or `REJECTED` order even though the hold is recorded.
- `cmd/netfulfil/orphan_hold_config_test.go`: `ORPHAN_HOLD_SWEEP_ENABLED` (on by
  default, a typo keeps it on) and `ORPHAN_HOLD_GRACE` (`0` valid, negative or
  malformed uses `1h`).
- `internal/adapters/outbound/telemetry/orphan_hold_metrics_test.go`: the
  `network_fulfillment.orphaned_holds.swept` counter and its `outcome` attribute.
- The node profile (ADR 0027 part 1):
  `internal/application/usecases/declare_node_profiles_test.go` (what the caches
  support, the skips and their logged reasons, ceilings never lowered, a fingerprint
  that follows content and not order, one site's failure never stops the others);
  `internal/adapters/outbound/retailnetwork/profile_request_test.go` and
  `contract_test.go` (the body against the vendored OpenAPI, which is byte-identical
  to vendor-connectivity's `apis/openapi.yaml`);
  `gateway_live_profile_test.go` (the revision, restart, retry, stale-revision
  recovery and race paths against a stateful fake Vendor API that judges a revision
  as node-topology does); `pgstore_profile_integration_test.go` (`integration` tag:
  the guarded monotonic write, 16 racing writers, migration 0010 up/down/up with
  data, and a fresh database migrated through every migration, down one step and up
  again);
  `processpathcache/schedule_test.go`; `telemetry/node_profile_metrics_test.go`;
  `cmd/netfulfil/profile_test.go` (`NETWORK_PROFILE_INTERVAL`, the wiring and the
  worker's start and stop); and the chart's `render_test.sh` and `test_env_parity.py`.
- `internal/adapters/outbound/kafka/wire_golden_test.go`: the CloudEvents wire
  shape of every event against `testdata/wire/{events,analytics}/*.json`.
- `internal/adapters/outbound/events/testdata/log_publisher.golden`: the log
  publisher's output.
- `internal/adapters/inbound/http/otel_metrics_test.go`: the otelhttp
  instruments the REST handler emits.
- `cmd/netfulfil/retry_test.go`, `wiring_test.go`: the boot retry, the
  mandatory `DATABASE_URL`/`KAFKA_BROKERS` boot errors, and the
  `MIGRATIONS_DATABASE_URL` split.
- `cmd/netfulfil/outbox_relay_config_test.go`: `OUTBOX_MAX_ATTEMPTS` parsing
  (`TestOutboxMaxAttempts`, `TestDefaultOutboxMaxAttemptsIsTwenty`) and that
  the relay the composition root builds carries it
  (`TestWireEventPublisher_OutboxRelayIsCapped`).
- `internal/adapters/outbound/postgres/outbox_trace_headers_test.go`: the
  trace-header injection (`TestTraceHeaders_NoSpanAddsNothing`,
  `TestTraceHeaders_SpanInjectsTraceparent`) and the merge in which an
  encoder-set header wins (`TestWithTraceHeaders_EncoderKeyWinsAndInputUntouched`).
- `cmd/*/log_level_test.go` (all four binaries): `LOG_LEVEL=warning` means
  warn (`TestNewLoggerAcceptsWarning`, `TestNewLoggerLevels`).

**Coverage gate**: `make coverage` (and the CI `test` job) measure coverage
of `./internal/domain/...` and `./internal/application/...` only, and fail
below **90 %**.

The Makefile is the single source of the coverage scope and threshold
(gap 16d): the CI `test` job runs `make coverage` instead of its own
`go test -coverpkg=...` command, so the local and the PR gate cannot drift;
harness `repo_lint` rule R11 (coverage-scope) forbids a workflow passing
`-coverpkg` itself. Change the scope or the threshold in the Makefile only.

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
| `network_order_intake.feature` | 10 |
| `network_order_queries.feature` | 7 |
| `operations.feature` | 6 |
| `orphaned_holds.feature` | 7 (ADR 0024: cancel after the window and grace, a 404 is done, a 409 is not an orphan and not retried, a failing cancel is retried and never marked done, `SUBMITTED` is never swept, `RejectOverdueOrders` frees a recorded hold first) |
| `shipment_confirmation.feature` | 6 |

A scenario tagged `@known-bug` states the correct behaviour for a defect the
suite found and is excluded until it is fixed. None is tagged today: the order
that failed during an order-management outage is now answered on a later pass,
and an empty `networkRef` on the shipment confirmation is a 422. The test-only
variable `BDD_TAGS` overrides the default filter `~@known-bug`
(`BDD_TAGS=@known-bug go test . -run TestFeatures -v` would run only tagged
scenarios, and there are none).

Output of `go test . -run TestFeatures -count=1 -v` on this branch:

```text
135 scenarios (135 passed)
951 steps (951 passed)
--- PASS: TestFeatures (0.31s)
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
| `.../postgres/hold_integration_test.go` | the hold (ADR 0024): `TestNetworkOrderRepo_SavesEveryStateTheAggregateCanReach` (a `REJECTED` order that keeps its id is saved, which the dropped CHECK used to refuse), `TestNetworkOrderRepo_HoldOutcomeRoundTrips`, `TestNetworkOrderRepo_SettlingAnOpenHoldIsPersisted`, `TestNetworkOrderRepo_ListOpenHoldsReturnsOnlyOpenHoldsOfDecidedOrOverdueCandidates` |
| `.../postgres/migration_0005_integration_test.go` | `TestMigration0005_UpDownUp`: migration `0005` up, down (clears the ids the restored CHECK would refuse, `SUBMITTED` data survives) and up again, on a bare Postgres |
| `.../postgres/capability_offer_repo_integration_test.go` | the offer repository |
| `.../postgres/outbox_integration_test.go` | outbox write and relay |
| `.../postgres/outbox_trace_integration_test.go` | trace context across the outbox (ADR 0022), real Postgres and Kafka: `TestOutboxTrace_TraceparentCrossesTheOutboxToKafka` (rows written in a span store its `traceparent`; a relay pass on a background context sends it to both topics; it extracts to the producing span) and `TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged` |
| `.../postgres/outbox_relay_cap_integration_test.go` | relay attempt cap (ADR 0021): `TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes` (a poison row, then a good row with the same key, which publishes once the poison row hits the cap and is logged at ERROR), `TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain` (the runbook's re-arm SQL), `TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows` (`0` = unlimited) |
| `.../postgres/pool_limits_integration_test.go` | `MaxConns` and `statement_timeout` really applied |
| `.../postgres/rehydrate_validation_integration_test.go` | corrupt stored state is refused on load |
| `internal/adapters/outbound/analyticsstore/postgres_integration_test.go` | projection and report store |
| `.../analyticsstore/unit_of_work_integration_test.go` | analytics UnitOfWork (ADR 0023): `TestUnitOfWork_ClaimAndProjectionRollBackTogether` (the consumer claim, the projection claim and the rollup row roll back together), `TestUnitOfWork_CommitsClaimAndProjectionAndJoinsNestedScope` |
| `internal/adapters/inbound/kafka/analytics_atomic_claim_integration_test.go` | claim and apply in one transaction (ADR 0023), real Kafka and Postgres: `TestAnalyticsConsumer_CrashBetweenClaimAndApply_RedeliveryAppliesExactlyOnce` (the projector stops after the claim and before the apply commits: no claim is left, and the redelivery in the same group applies the event exactly once) |
| `internal/adapters/outbound/kafka/publisher_integration_test.go` | both publishers against a real broker |
| `internal/adapters/inbound/kafka/analytics_projector_integration_test.go` | consumer to projection, end to end |
| `internal/adapters/inbound/kafka/analytics_dlq_integration_test.go` | retry and dead-letter path |
| `internal/adapters/inbound/http/rest_api_integration_test.go` | the real REST handler chain over Postgres, the outbox and a real poller: poll, persist, read back, and the shipment-confirmation 204 / 409 / 404 paths |
| `internal/adapters/inbound/mcp/streamable_http_integration_test.go` | the MCP wire contract over Streamable HTTP with the SDK client and Postgres: `initialize`, `tools/list` (every tool `readOnlyHint: true`), `tools/call`, `resources/read` |
| `internal/application/usecases/usecases_wiring_integration_test.go` | the write use cases over Postgres, the unit of work and the outbox fan-out, with the real order-management HTTP planner against a stub |

The `make help` text still says integration "needs a running Postgres"; the
Makefile comment and the tests themselves only need Docker.

## Architecture fitness tests

`make arch-test` runs `go test ./internal/architecture/... -v`:

| Test | Guards |
| --- | --- |
| `TestHexagonalArchitecture` | `arch-go` rules: domain depends on nothing, application on domain, adapters on both |
| `TestMCPAdapterDependencyRule` | the MCP adapter depends only on application and domain, and nothing but `cmd/mcp` imports it |
| `TestMCPOptionalToolInputsCarryOmitempty`, `TestMCPOptionalToolInputsSensorFailsOnBadFixtures` | an MCP tool input field whose `jsonschema` description says "optional" carries `omitempty`, or the SDK marks it required (gap fix D16) |
| `TestOutboxRelaysPassAttemptCap`, `TestOutboxRelayCapSensorFailsOnBadFixtures` | every non-test `NewOutboxRelay` call under `cmd/` passes `WithMaxAttempts` (gap fix D12, ADR 0021) |
| `TestKafkaWritersSetRequiredAcks`, `TestKafkaWriterAcksSensorBites` (`kafka_writer_acks_fitness_test.go`) | every kafka-go `Writer` literal in a non-test file under `internal/` or `cmd/` sets `RequiredAcks` (the DLQ writer and both event publishers use `kafkago.RequireAll`, synchronous): kafka-go defaults to `RequireNone` (acks=0), which silently drops a write on a broker blip while `WriteMessages` returns nil (fleet rollout 2026-10-13) |
| `TestDeadLetterCallsGuardCancellation` (+ `TestDeadLetterShutdownSensorBites`, `kafka_dlq_shutdown_fitness_test.go`) | Every function in a non-test file under `internal/adapters/inbound/kafka` that sends to the dead-letter topic must reference `ctx.Err()` (any context-named value) or `context.Canceled` in its own body, or carry a `//dlq:deterministic <reason>` comment on the call line or the line above it. A handler that fails because the process is stopping must not dead-letter a good message and commit past it (network-fulfillment #89); a guard in the caller does not count. The sensor test proves the detector flags the pre-fix shape. |
| `TestKafkaWriterTopicNotSetTwice` (+ `TestKafkaWriterTopicSensorBites`, `kafka_writer_topic_fitness_test.go`) | kafka-go rejects every write whose `Message.Topic` is set when the `Writer` also has a `Topic`. Per Go package under `internal/` and `cmd/`: once any kafka-go `Message` literal sets `Topic`, every `Writer` literal in that package must leave `Topic` empty, unless it carries `//kafka:fixed-topic-writer <reason>` and no Topic-setting Message is provably written through it. Writers configured after construction and `WriterConfig` are not covered. (Escape: warehouse-planning's domain DLQ writer, where no dead letter ever landed.) |
| `TestMCPAdapterStaysReadOnly`, `TestMCPReadOnlySensorFailsOnBadFixtures` | the MCP adapter calls no state-changing method (`Save`, `Publish`, `Execute`, `Exec`, `Insert`, `Update`, `Upsert`, `Delete`) and sets no `ReadOnlyHint: false`, so the "Read-only access" server instructions stay true (gap fix D17) |
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
| `test` | `go build`, `go vet`, then `make coverage` (race unit tests with the Makefile's coverage scope and 90 % gate), uploads `coverage.out` (generated by `make coverage`, gitignored, never tracked) | every run |
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
