---
id: 0025-postgres-and-kafka-are-the-only-runtime
slug: /adr/0025-postgres-and-kafka-are-the-only-runtime
title: "25. Postgres and Kafka are the only runtime"
sidebar_label: "25. Postgres and Kafka only"
description: "ADR 0025 — network-fulfillment no longer has an in-memory mode, a log-only publisher or opt-in Kafka/capability pipelines: DATABASE_URL and KAFKA_BROKERS are mandatory, the outbox relay and the capability-offer pipeline are always on, and /readyz checks Postgres and Kafka."
---

# 25. Postgres and Kafka are the only runtime

## Status

**Accepted** (2026-10-19). Supersedes the optional-runtime parts of
[ADR 0003](./0003-transactional-outbox.md) (the direct, no-database publishing
mode and the independent `EVENT_PUBLISHER` / `DATABASE_URL` toggles) and of
[ADR 0022](./0022-trace-context-crosses-the-outbox.md) (the "direct mode"
without a database). No schema or event contract changes.

## Context

Until now the binaries could boot in several shapes: no `DATABASE_URL`
meant in-memory repositories, no `EVENT_PUBLISHER` meant a log-only
publisher, `EVENT_PUBLISHER=kafka` without a database meant direct
publishing with no outbox, and `CAPABILITY_OFFER_ENABLED` unset meant no
capability-offer pipeline. Each shape was a way to run green without the
infrastructure the service exists to use. Two things went wrong with that:

- Cheap-to-misconfigure deployments. The kind overlay once ran the API on the
  log publisher, so no event reached either topic and the projector consumed
  an empty topic while every probe was green.
- Tests that could not see real-database failures. The in-memory adapters
  accepted text that Postgres rejects (a NUL byte aborts the surrounding
  transaction, SQLSTATE 22021 then 25P02), so a hostile request was a 200 in
  tests and a 500 in production.

On 2026-10-19 the fleet decided that a service whose design needs Postgres
and Kafka runs on Postgres and Kafka, and says so at boot.

## Decision

Postgres and Kafka are the only runtime for `network-fulfillment`.

- `DATABASE_URL` and `KAFKA_BROKERS` are mandatory. A missing one is a boot
  error that names the variable (`DATABASE_URL is required`,
  `KAFKA_BROKERS is required`).
- There is no in-memory mode, no log-only publisher and no direct
  (outbox-less) publishing. The `memory` adapters stay in the tree as test
  doubles for unit and BDD tests; no `cmd/` binary imports them (sensor:
  `TestRuntimeBinaryHasNoInMemoryAdapters`).
- `EVENT_PUBLISHER` and `CAPABILITY_OFFER_ENABLED` are kept only as guards:
  unset (or `kafka` / `true` respectively) is the single runtime, any other
  value is a boot error naming the variable. They switch nothing off.
- The outbox relay and the capability-offer pipeline (two Kafka caches, the
  inventory client, the recompute ticker, `GET /capability-offers`) are always
  on.
- `/readyz` is the drain gate, a Postgres ping and a Kafka broker-metadata
  check. While a dependency is down it answers 503 `application/problem+json`
  of type `dependency-unavailable`, naming the dependency. Topics are never
  checked. `/healthz` stays liveness only and does not consult either.
- A NUL byte is rejected before it reaches Postgres: 404 in the request
  path, 400 in the query string or the body (`rejectNUL`, the first
  middleware).

## Consequences

- Local runs need both services: `docker-compose.yml` starts them
  (`KAFKA_HOST_PORT` moves the broker off `localhost:9092`, which the kind
  cluster owns). The quickstart and the configuration reference describe only
  this runtime.
- A deployment that forgets Postgres or Kafka fails loudly: the process exits
  at boot, and the Helm chart refuses to render without a database Secret and
  `kafka.brokers`.
- Tests that need real behaviour run on testcontainers
  (`TestRunOnRealPostgresAndKafka`); fast unit and BDD tests keep the doubles.
- A dependency outage now takes the pod out of the Service (503 on
  `/readyz`) instead of being invisible. The pod is not restarted:
  `/healthz` is independent.
- `netfulfil` and the projector exit non-zero when the relay or a consumer
  dies, so the orchestrator restarts them instead of leaving a live process
  that does nothing.

### What changed per binary

| Binary | Before | Now |
| --- | --- | --- |
| `cmd/netfulfil` | memory without `DATABASE_URL`; log publisher without `EVENT_PUBLISHER`; relay and capability pipeline opt-in | `DATABASE_URL` and `KAFKA_BROKERS` mandatory; outbox, relay and capability pipeline always on; `/readyz` checks Postgres and Kafka |
| `cmd/mcp` | memory without `DATABASE_URL` | `DATABASE_URL` mandatory (reads the OLTP database `netfulfil` writes) |
| `cmd/netfulfil-projector` | `KAFKA_BROKERS` defaulted to `localhost:9092` | `KAFKA_BROKERS` and `ANALYTICS_DATABASE_URL` mandatory; exits non-zero when its consumer dies |
| `cmd/netfulfil-reports` | already required `ANALYTICS_DATABASE_URL` | unchanged |

The product-translation adapter moved to
`internal/adapters/outbound/producttranslation` and the clock to
`internal/adapters/outbound/system`, so `memory` holds only doubles.

## Deliberately out of scope

Recorded here so they are not mistaken for done:

- **REST sync-edge `*_MODE` variables** (the `permissive` / `strict` modes of
  the clients to sibling contexts, ADR 0001 and ADR 0004) are unchanged. They
  are about how this service treats a failing sibling over REST, not about
  Postgres or Kafka. Follow-up: decide the same question for them per sibling
  context.
- **No stable consumer-group id.** The capability caches derive their own
  group names (`network-fulfillment-...-cache-<hostname>-<pid>-<unix-nanos>`),
  so there is no environment variable for a stable group id and every start
  replays from the earliest offset and leaves an abandoned group behind.
  Follow-up.
- **Telemetry shutdown delay.** With no reachable OTLP collector, shutdown
  waits about 5 s for the exporter to give up. It never blocks boot.
- **Readiness probe override in `warehouse-infra`.** The chart default points
  the `readinessProbe` at `/readyz`. `helm-values/network-fulfillment.yaml` on
  `warehouse-infra` `develop` was checked on 2026-10-19 and sets no
  `readinessProbe` (and no `/healthz` override), so the chart default applies.
  If an overlay ever points it at `/healthz`, dependency outages will not
  remove the pod from the Service.
