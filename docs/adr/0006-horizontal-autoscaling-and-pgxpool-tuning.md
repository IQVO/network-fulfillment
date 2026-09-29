---
id: 0006-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0006-horizontal-autoscaling-and-pgxpool-tuning
title: "6. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning"
sidebar_label: "6. HPA + pgxpool tuning"
sidebar_position: 6
description: "ADR 0006 — Phase 3 (scalability) for network-fulfillment, ported from order-management's reference PR (ADR 0026): an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-reports max 3, frontend max 3; analytics-projector and mcp both explicitly excluded, for two different real reasons), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against this fleet's shared Postgres instance's real max_connections=100 ceiling and PgBouncer (warehouse-infra PR #43) sitting in front of the OLTP path in transaction-pooling mode."
---

# 6. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record.
This is Phase 3 (scalability) of the fleet production-readiness plan,
following Phase 2 (resilience, ADR 0004) and the Kafka partition-affinity
fix (ADR 0005). It ports `order-management`'s reference PR #110 (ADR
0026) to this repo's own chart shape and binary set, the same role that
PR played for the other fleet repos in this wave, and it also folds in
`warehouse-infra` PR #43 (PgBouncer in front of the shared Postgres
instance, transaction-pooling mode), which had not yet landed when ADR
0026 was written.

## Context

Before this change, this chart's one `HorizontalPodAutoscaler` template
unconditionally targeted the `api` Deployment only, wired to a flat
`autoscaling.enabled/minReplicas/maxReplicas/targetCPUUtilizationPercentage`
block — untested against the other three Deployments this chart renders
(`mcp`, `frontend`, `analytics-projector`, `analytics-reports` — five
Deployments total counting `api`), and never assessed for whether
scaling `analytics-projector` past 1 replica was even safe given its
Kafka consumer-group membership. `replicaCount` was hardcoded to 1
fleet-wide with no HPA anywhere else.

Separately, no pool in this codebase set an explicit
`pgxpool.Config.MaxConns`: both the OLTP pool
(`internal/adapters/outbound/postgres/pool.go`, used by `cmd/netfulfil`
and `cmd/mcp`) and the two analytics pools
(`internal/adapters/outbound/analyticsstore/pool.go`'s `NewPool`, used
by `cmd/netfulfil-projector`, and `NewReadOnlyPool`, used by
`cmd/netfulfil-reports`) ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query — a bad index, a
lock wait, a wide date-range read the analytics report might grow to
need — could hold a pooled connection indefinitely with nothing to
cancel it.

### Repo-specific shape check: which Deployments actually exist here

Verified from the chart and `cmd/` before designing anything, rather
than assumed from `order-management`'s shape: this repo has the same
five-binary set order-management's reference PR covers —
`cmd/netfulfil` (`api`), `cmd/mcp` (`mcp`), `cmd/netfulfil-projector`
(`analytics-projector`), `cmd/netfulfil-reports` (`analytics-reports`),
plus `frontend` (a Module Federation remote served by its own
nginx-unprivileged pod, no Go binary of its own). `charts/
network-fulfillment/templates/{deployment,mcp-deployment,
projector-deployment,reports-deployment,frontend-deployment}.yaml`
confirm exactly these five.

Two repo-specific facts changed the per-workload HPA table from a
mechanical copy of order-management's:

1. **`api`'s own Kafka relationship is the OPPOSITE of
   order-management's `api`.** `cmd/netfulfil` has NO inbound Kafka
   consumer at all — its poller
   (`internal/adapters/inbound/poller`) drives the network gateway on a
   plain `time.Ticker`, and its only Kafka involvement is as an
   OUTBOUND publisher/outbox relay (`wireEventPublisher`,
   `internal/adapters/outbound/kafka`), which every replica may do
   independently with no shared-group concept at all. This makes `api`
   *easier* to reason about than order-management's `api` (which does
   have an inbound `RepromiseConsumer` under a stable shared group to
   verify), not harder: there is no Kafka consumer-group question for
   this Deployment whatsoever.
2. **`analytics-projector`'s consumer-group convention is the OPPOSITE
   of order-management's `analytics-projector`.** order-management's
   analytics consumer uses a STABLE, shared group name
   (`AnalyticsConsumerGroup`), the fleet's normal N-replicas-share-
   partitions pattern, which is why that repo's projector gets an HPA
   capped at 2. This repo's analytics consumer
   (`internal/adapters/inbound/kafka/analytics_consumer.go`) instead
   mints a FRESH, per-process-unique group id on every start
   (`NewUniqueConsumerGroup(AnalyticsConsumerGroupPrefix)`, appending
   hostname+PID+timestamp) and every reader starts from
   `kafkago.FirstOffset` — the fleet's *other* documented convention
   (AGENTS.md hard rule 7), used here specifically so a fresh projector
   instance never inherits an earlier instance's already-advanced
   offset and reports healthy having replayed nothing. That is correct
   for exactly ONE running instance; it becomes actively wrong at N>1,
   because each of the N would independently replay the WHOLE topic
   from the earliest offset and double/triple-project every historical
   event, rather than sharing partitions the way a stable shared group
   does. **`analytics-projector` therefore gets NO HPA in this chart at
   all** — the opposite of order-management's `analytics-projector`,
   for a real, verified, code-level reason, not an oversight.

### Finding: shared Postgres, PgBouncer now in front, and the honest ceiling

Verified fleet-wide facts (not re-derived here, cited from the
verified state at the time this PR was written): all ~10 backend
services in this fleet share ONE Postgres server instance, each with
its own logical database/role, `max_connections=100` — the unmodified
Bitnami chart default, never overridden by `warehouse-infra`'s
Terraform. `warehouse-infra` PR #43 (merged) additionally put PgBouncer
in front of that shared instance in transaction-pooling mode, and
**every service's OLTP `DATABASE_URL` Secret is already re-pointed at
PgBouncer** — no code or chart change in this repo was needed for that
part, because this chart already sources `DATABASE_URL` from a Secret
(`database.existingSecret`/`database.url`) whose value is an
infrastructure concern, not a chart-rendered literal. Analytics DSNs
(`ANALYTICS_DATABASE_URL`) stay DIRECT against Postgres, mirroring PR
#43's own reasoning: the analytics pools are few, long-lived,
low-connection-count processes (one projector, up to 3 reports
replicas) for which PgBouncer's transaction-pooling multiplexing buys
little, while the OLTP path is the one whose real server-side
connection count PgBouncer actually bounds regardless of how many
`api`/`mcp` replicas are pooling on the app side.

This changes what "sized against `max_connections=100`" means for the
OLTP pool specifically, compared to order-management's ADR 0026 (written
before PR #43 existed): `pgxpool.Config.MaxConns` still bounds each
`api`/`mcp` process's own **client-side** pool size — the number of
concurrent queries one process can have in flight — but with PgBouncer
absorbing real server-side multiplexing in front of it, the OLTP
`MaxConns` value can stay as generous as order-management's own
identically-shaped pool (10) without that number needing to be treated
as a literal 1:1 slice of the 100 real server connections the way ADR
0026's worst-case accounting did. The analytics pools, being direct
against Postgres with no PgBouncer in front, keep the same
literal-server-connection accounting ADR 0026 used.

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/netfulfil`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP plus an inbound poller and acknowledgement sweep, both driven by this same process's own ticker. No inbound Kafka consumer at all (see the repo-specific shape check above) — its only Kafka role is as an outbound publisher/outbox relay, safe at any N. Its in-memory dev mode forgets state on restart, but a real (Postgres-backed) deployment has every replica sharing the SAME `NetworkOrderRepo`, so N replicas read/write one consistent store. Matches order-management's identically-shaped `api` max/target exactly. |
| `analytics-projector` (`cmd/netfulfil-projector`) | **No — deliberately excluded, not just disabled** | — | — | — | Its Kafka consumer group id is minted FRESH per process instance (`NewUniqueConsumerGroup`) and starts from `kafkago.FirstOffset` — the opposite convention from order-management's stable shared `AnalyticsConsumerGroup`. Running N>1 replicas would each independently replay the ENTIRE analytics topic from the earliest offset, double/triple-projecting every historical event rather than sharing partitions. Fixing this to be safely scalable needs a real code change (a stable shared group name, changing the replay-on-restart semantics), out of scope here. `replicaCount` stays a plain, manually-set value; no `autoscaling.projector` block exists in `values.yaml` at all. |
| `analytics-reports` (`cmd/netfulfil-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`, matching order-management's `analytics-reports`. |
| `frontend` (nginx-unprivileged serving the built `netfulfil_mfe` bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | Same reason as order-management's `mcp`: the `github.com/modelcontextprotocol/go-sdk` `StreamableHTTPHandler` this adapter wraps (`internal/adapters/inbound/mcp/server.go`) keeps per-process, in-memory session state keyed by the MCP protocol's own `Mcp-Session-Id` header. `charts/network-fulfillment/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` could land on a different pod than the one that created the session. Fixing it needs either `sessionAffinity: ClientIP` (partial) or an external/shared session store — a real code change, out of scope here. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists. |

Every enabled block defaults `enabled: false` in `values.yaml`
(`autoscaling.<api|reports|frontend>.{enabled,minReplicas,maxReplicas,
targetCPUUtilizationPercentage}`) — this PR makes per-workload HPA
possible and verified-correct; it does not turn any of it on.

**No replicas-vs-HPA fight.** Each of the three HPA-eligible Deployment
templates guards its `spec.replicas` field with `{{- if not
.Values.autoscaling.<x>.enabled }}` — when a workload's HPA is enabled,
its Deployment renders with NO `replicas` field at all. Verified
directly with `helm template`:

- Default values → 0 `HorizontalPodAutoscaler` resources render, every
  Deployment keeps its static `replicas:` field.
- `autoscaling.api.enabled=true` + `autoscaling.reports.enabled=true` +
  `autoscaling.frontend.enabled=true` + `analytics.enabled=true` +
  `frontend.enabled=true` → exactly 3 `HorizontalPodAutoscaler`
  resources render (`api`, `analytics-reports`, `frontend` — none for
  `analytics-projector` or `mcp`, by design).
- Only `autoscaling.api.enabled=true` (with `analytics.enabled=true`
  and `frontend.enabled=true` so their Deployments render at all) →
  exactly 1 HPA renders, only the `api` Deployment loses its
  `replicas:` field; `analytics-projector`/`analytics-reports`/
  `frontend` all keep theirs untouched — mixed enablement is
  independent per workload.

`helm lint` passes; the existing chart wiring tests
(`charts/network-fulfillment/tests/test_credential_wiring.py`,
`test_data_files.py`) pass unchanged; `go build`/`go vet` are
unaffected (no Go code path is touched by the chart changes alone).

### 2. pgxpool MaxConns

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.NewPool`) | `cmd/netfulfil` (`api`), `cmd/mcp` (`mcp`) | **10** | Matches order-management's identically-shaped OLTP pool. `api`'s HPA ceiling of 4 replicas × 10 = 40 client-side connections; with PgBouncer (transaction-pooling mode, `warehouse-infra` PR #43) sitting in front of every service's OLTP `DATABASE_URL`, this is a bound on this process's own concurrency, not a literal slice reserved against the 100 real server connections the way a direct-to-Postgres pool would be — PgBouncer is what actually multiplexes real server-side connections across however many app-side pools (this service's own `api`/`mcp` plus every sibling service's own OLTP pools) draw from it. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/netfulfil-projector` | **5** | The projector has no HPA (fixed at 1 replica) and does single-row `ON CONFLICT` upserts one event at a time; a small, flat pool is enough. Matches order-management's analytics writer pool. This pool is DIRECT against Postgres (analytics DSNs are not behind PgBouncer, per PR #43's own split), so its 5 is a literal slice of the shared instance's 100. |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/netfulfil-reports` | **5** (`ReportsMaxConns`) | `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections against the analytical database, direct against Postgres like the writer above. Matches order-management's analytics reader pool. |

Direct-against-Postgres worst case for this service's analytics path
alone: projector fixed at 1 × 5 = 5, reports at its HPA ceiling of 3 ×
5 = 15, total 20 of the shared instance's 100 real connections — before
counting any other of the ~10 fleet services' own analytics pools
against that same ceiling. The OLTP path's 40-connections-at-HPA-
ceiling number is a client-side pool-size bound behind PgBouncer, not
directly comparable to that 100-connection ceiling the way ADR 0026's
pre-PgBouncer accounting treated it — PgBouncer's own configured pool
size (a `warehouse-infra`/PR #43 concern, not this chart's) is what
actually determines how many real server connections the OLTP path
consumes regardless of how many `api`/`mcp` client-side pools exist
fleet-wide. That PgBouncer-side pool sizing is explicitly NOT
re-examined by this PR — it is `warehouse-infra` PR #43's own decision,
out of scope here.

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.
AfterConnect`, running `SET statement_timeout = '<value>'` on every new
physical connection as it's established:

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (`NetworkOrderRepo`'s `Save`/`FindByRef`/`ListUnanswered`/`ListAll`, the outbox writer and relay) is a single-aggregate or small bounded-scan operation keyed by `network_ref` or the partial `idx_network_orders_unanswered` index, normally low-single-digit milliseconds. Matches order-management's OLTP value exactly — same shape of query, same reasoning. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying the analytics topic from the earliest offset (every restart, per this repo's own per-process-unique-group convention — see the repo-specific shape check above) issues upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request, but the projector has no replica to fail over to (fixed at 1), so it still cannot be unbounded. Matches order-management's analytics writer value. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The acknowledgement report aggregates counts across the analytical database's projection tables — wider-shaped than the OLTP side's always-single-aggregate-by-ref reads — so it gets more headroom, but still a hard ceiling. Matches order-management's analytics reader value. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`, ported directly from order-management's own test of
the identical shape):

- `TestNewPool_AppliesStatementTimeoutToNewConnections` — opens a pool
  against a real `postgres:16-alpine` container with a short test-only
  timeout (200ms), confirms `SHOW statement_timeout` reads back the
  configured value, then runs `SELECT pg_sleep(2)` and asserts Postgres
  itself cancels it (SQLSTATE 57014) rather than letting it run the
  full 2s, and confirms the pool is still usable afterward.
- `TestNewPool_AppliesMaxConns` — acquires exactly `maxConns`
  connections from a pool configured with `MaxConns=2`, then asserts a
  further `Acquire` blocks until `context.DeadlineExceeded`, proving
  `MaxConns` is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container.

## Consequences

- HPA is now possible, correct, and independently verified per
  workload for three of this chart's five Deployments — but **off by
  default everywhere**. Merging this PR changes no production replica
  counts; `MaxConns`/`statement_timeout` are the only behavior change
  that takes effect on deploy, and both are conservative relative to
  today's unbounded defaults.
- `analytics-projector` and `mcp` both remain explicitly un-autoscaled,
  for two DIFFERENT real reasons (a per-process-unique Kafka consumer
  group replaying from the earliest offset, and in-memory MCP session
  state with no sticky routing) — a future contributor must not
  mechanically copy `api`'s or order-management's HPA shape onto either
  without re-solving its specific blocker first.
- The OLTP connection-budget accounting in this ADR deliberately
  differs from order-management's ADR 0026: this repo's `api`/`mcp`
  pools sit behind PgBouncer (`warehouse-infra` PR #43), so their
  `MaxConns` is a client-side concurrency bound, not a literal reserved
  slice of `max_connections=100`. This is a real, documented deviation
  from the reference PR's math, not an oversight — PgBouncer's own
  pool-size configuration (out of scope here) is what now determines
  the OLTP path's actual server-side connection footprint.
- The analytics pools (direct against Postgres, no PgBouncer) keep the
  same literal-connection accounting as order-management: 20 of the
  shared instance's 100 connections at this service's own analytics
  HPA ceiling, before any other fleet service's analytics pools are
  counted — the same honest, not-yet-fully-closed residual risk ADR
  0026 flagged, still open fleet-wide.
- `max_connections=100` itself remains an unexamined Bitnami chart
  default. This ADR, like ADR 0026, treats it as a hard external
  constraint to work within, not something in scope to change.

## References

- `order-management` PR #110 / ADR 0026 — the reference this PR ports,
  including the original per-workload HPA table shape and pgxpool
  tuning pattern.
- `warehouse-infra` PR #43 — PgBouncer in front of the shared Postgres
  instance in transaction-pooling mode; every service's OLTP
  `DATABASE_URL` Secret already points at it, no chart change needed
  here for that part. Its own pool-size configuration is out of scope
  for this PR.
- ADR 0004 (`0004-resilience-circuit-breakers-retry-dlq-shutdown.md`) —
  Phase 2, the resilience wave this Phase 3 change builds on.
- ADR 0005 (`0005-kafka-hash-balancer-for-partition-affinity.md`) — the
  most recent prior change to this repo's Kafka adapters, unaffected by
  this PR.
- AGENTS.md hard rule 7 — the per-process-unique consumer group
  convention `analytics-projector` follows, and the reason it is
  excluded from HPA here.
