---
id: runbook
title: Runbook
sidebar_label: Runbook
---

# Runbook

How network-fulfillment is deployed, started, migrated, scaled and operated.
Everything here is read from the code, the Helm chart
(`charts/network-fulfillment`) and, where named, `warehouse-infra`'s
`terraform/network-fulfillment.tf` on its `develop` branch. This page replaces
the older `docs/operations-notes.md`, which is now a pointer here.

Related pages: [configuration](configuration.md) (every env var),
[observability](observability.md), [troubleshooting](troubleshooting.md),
[testing and CI](../development/testing.md).

## Deployables

One image, four Go binaries, one Helm chart named `network-fulfillment`
(`charts/network-fulfillment/Chart.yaml`, chart version `0.1.0`). Names below
assume the release name `network-fulfillment`, which is what
`warehouse-infra`'s ArgoCD Application uses, so `fullname` is
`network-fulfillment`.

| Binary | Deployment | Service (port -> target) | Probes | Enabled by | Replicas |
| --- | --- | --- | --- | --- | --- |
| `cmd/netfulfil` | `network-fulfillment` (component `api`) | `network-fulfillment` 80 -> 8080 | startup `GET /healthz` (2 s x 30), liveness `GET /healthz` (every 10 s after 5 s), readiness `GET /readyz` (every 5 s after 3 s) | always | `replicaCount` 1, or HPA `autoscaling.api` (1-4, 70 % CPU) when enabled |
| `cmd/mcp` | `network-fulfillment-mcp` | `network-fulfillment-mcp` 8090 -> 8090 | liveness and readiness `GET /healthz` | `mcp.enabled` | `mcp.replicaCount` 1, no HPA by design |
| `cmd/netfulfil-projector` | `network-fulfillment-projector` | none (admin port 8091 only) | liveness and readiness `GET /healthz` on `admin` | `analytics.enabled` | `analytics.projector.replicaCount` 1, no HPA by design |
| `cmd/netfulfil-reports` | `network-fulfillment-reports` | `network-fulfillment-reports` 80 -> 8092 | liveness and readiness `GET /healthz` | `analytics.enabled` | 1, or HPA `autoscaling.reports` (1-3) |
| `web/` (nginx) | `network-fulfillment-frontend` | `network-fulfillment-frontend` 80 -> 8080 | `GET /healthz` | `frontend.enabled` | 1, or HPA `autoscaling.frontend` (1-3) |

All HPA blocks default to `enabled: false`. When one is enabled, the matching
Deployment omits `replicas:` so the HPA owns it.

### In the local kind cluster

`warehouse-infra` deploys this chart from `terraform/network-fulfillment.tf`
(not from `local.services`) through an ArgoCD Application. As read from that
file on `warehouse-infra` `develop`, the overlay sets:

- `config.networkMode = "stub"` on purpose: the cluster can never talk to a
  real network, and no credentials Secret exists.
- `config.orderManagementUrl = http://order-management.<apps namespace>.svc.cluster.local:80`,
  `sweepInterval = "5m"`, `pollInterval = "1m"`.
- `productTranslation.mappings`: `ASIN-LOCAL-1 -> sku-1`, `ASIN-LOCAL-2 -> sku-2`.
- `stubDemand.demands`: one order `po-local-1` for `site-1`, one line of
  `ASIN-LOCAL-1`, `requiredShipBy = "+36h"`.
- `database.existingSecret = network-fulfillment-db` (keys `DATABASE_URL`
  through PgBouncer, and `MIGRATIONS_DATABASE_URL` direct to Postgres).
- `analytics.enabled = true` with `analytics.database.existingSecret = network-fulfillment-analytics-db`.
- `mcp.enabled = var.deploy_mcp_servers`, and the frontend remote.
- A Kong Ingress (or an HTTPRoute when Gateway API is on) at
  `/api/network-fulfillment` with strip-path, plus a second route
  `/api/network-fulfillment/reports` rewritten to `/reports` on the reports
  Service.

That overlay does **not** set `kafka.enabled` or `config.eventPublisher`, so
the API pod runs with the log publisher: no event reaches either topic and
the projector consumes an empty analytics topic. Turn on `kafka.enabled=true`
and `config.eventPublisher=kafka` in the overlay to get events and the
analytics report in the cluster.

### Releases

On push to `main`, CI's `docker-publish` job pushes
`ghcr.io/iqvo/network-fulfillment`, signs it keylessly with cosign and attests
an SPDX SBOM. `release` then bumps semver from the latest `vX.Y.Z` tag,
re-tags the image, pushes the chart to `oci://ghcr.io/iqvo`, and cuts the git
tag and a GitHub Release with the chart `.tgz`. The kind cluster does not use
those images: `warehouse-infra` builds `warehouse/network-fulfillment:local-<hash>`
from the sibling checkout and loads it into kind.

## Startup sequence (`cmd/netfulfil`)

`run()` in `cmd/netfulfil/main.go` does, in order:

1. `telemetry.Setup` (OTLP exporters, never blocks on the Collector).
2. Wire the network gateway from `NETWORK_MODE`; fail on `live`.
3. Load `NETWORK_SEED_FILE` into the stub gateway, if set.
4. Load `PRODUCT_TRANSLATION_FILE`, or log the "every network order will be
   rejected" warning.
5. With `DATABASE_URL`: run OLTP migrations against `MIGRATIONS_DATABASE_URL`,
   open the pool, ping it. Both steps are retried.
6. With `CAPABILITY_OFFER_ENABLED=true`: dial both Kafka caches (retried),
   start them, and wait until both have replayed their topic.
7. Wire the circuit-broken order-management client, the event publisher (log,
   direct Kafka, or outbox + relay) and the use cases.
8. Start the tickers, the poller (which polls once immediately), the outbox
   relay, then the HTTP listener.

The HTTP server only starts listening after steps 1-7. Until then
`/healthz` does not answer, which is why the chart has a `startupProbe`
(30 x 2 s = 60 s).

**Boot retry (ADR 0011).** Steps 5 and 6 go through `retry` in
`cmd/netfulfil/retry.go`: 5 attempts, with waits of 1, 2, 4 and 8 s between
them (15 s of waiting, plus the time each attempt takes). It exists because
every Istio-injected pod's first outbound dial is reset about 10 s after
start. After the last attempt the process exits 1 and logs
`cannot wire order repository` (or `cannot wire capability offer pipeline`).
The code comments and the README call this a "~31 s budget" (they add a
fifth 16 s wait the loop never takes); the loop as written waits 15 s.

`cmd/mcp` runs the same OLTP migrations at its own startup, **without** the
retry, then opens its pool. `cmd/netfulfil-projector` runs the analytical
migrations without retry. Both exit on failure and rely on the kubelet
restarting them.

## Probes and readiness

| Endpoint | Process | Behaviour |
| --- | --- | --- |
| `GET /healthz` | all four | `200 {"status":"ok"}`. Liveness only: it never touches Postgres, Kafka or the poller. |
| `GET /readyz` | `netfulfil` | `200 {"status":"ready"}` from the moment the listener is up, `503 {"status":"not_ready"}` once shutdown has started. It does not re-check dependencies; everything it depends on was checked before the listener started. |

`GET /inbound-status` is the operational health check that matters: see
[observability](observability.md#inbound-status).

## Graceful shutdown

On `SIGTERM`/`SIGINT`, `shutdownGracefully` (`cmd/netfulfil/shutdown.go`):

1. flips `/readyz` to 503;
2. stops the HTTP server and drains in-flight requests;
3. cancels the outbox relay and waits for its current pass;
4. waits for the poller's in-flight pass (an interrupted pass does not advance
   the watermark, so that demand is re-fetched on the next boot);
5. waits for the two cache consumers, if enabled;
6. closes the publishers and the pool.

All waits share one 10 s budget; a step that overruns logs a warning such as
`outbox relay did not stop before the shutdown deadline`. The other three
binaries stop their HTTP server with a 10 s budget; the projector cancels its
consumer first.

## Database and migrations

| Database | Owner | Tables (migration) | Applied by |
| --- | --- | --- | --- |
| OLTP (`DATABASE_URL`) | `netfulfil` | `network_orders`, `network_order_lines` (0001), `outbox_events` (0002), state `SUBMITTED` added (0003), `capability_offers` (0004) | `netfulfil` at startup, and `cmd/mcp` at startup |
| Analytical (`ANALYTICS_DATABASE_URL`) | `netfulfil-projector` | `analytics_processed_events`, `analytics_consumed_events`, `acknowledgement_rollup` (0001), column `orders_rejected_submission_failed` (0002) | `netfulfil-projector` at startup |

- Migrations ship in the image (`/app/migrations`, `/app/migrations/analytics`)
  and run with `golang-migrate` (`internal/adapters/outbound/postgres/migrate.go`).
  There is no separate migration Job.
- Use `MIGRATIONS_DATABASE_URL` whenever `DATABASE_URL` goes through
  PgBouncer. `golang-migrate` takes a session-level `pg_advisory_lock`, which
  does not work in PgBouncer's transaction-pooling mode, and concurrent
  replicas then crash-loop (ADR 0007). In the kind cluster the Secret carries
  both keys.
- Every migration has a `.down.sql`, but nothing in the repo runs them. A
  rollback is a manual `migrate ... down` against the direct DSN.
- The reports binary only reads; its pool is pinned read-only.

## Kafka

One broker for the whole platform (in-cluster Bitnami; `localhost:9092` from
the host through the kind cluster's external access). Every message is a
CloudEvents 1.0 structured-mode event with header
`content-type: application/cloudevents+json; charset=UTF-8` (ADR 0008).

### Produced (only when `EVENT_PUBLISHER=kafka`)

| Topic | Producer | Key | Content |
| --- | --- | --- | --- |
| `warehouse.network-fulfillment.events` | `netfulfil` (direct or via the outbox relay) | `networkRef`, `kafka.Hash` balancer (ADR 0005) | every domain event, `dataschema urn:warehouse:network-fulfillment:events:<Event>:v<N>` |
| `warehouse.network-fulfillment.analytics` | same | same | the same events, `dataschema urn:warehouse:network-fulfillment:analytics:<Event>:v<N>` |
| `warehouse.network-fulfillment.analytics.dlq` | `netfulfil-projector` | the source message key | poison analytics messages (see below) |

Both writers set `AllowAutoTopicCreation: true`. Event types are listed in
[ecosystem/integration.md](../ecosystem/integration.md#events-published).

### Consumed

| Topic | Consumer | Group id | Start | Purpose |
| --- | --- | --- | --- | --- |
| `warehouse.network-fulfillment.analytics` | `netfulfil-projector` | `network-fulfillment-analytics-<hostname>-<pid>-<unix-nanos>`, new on every start | earliest | analytics read model |
| `warehouse.process-path-management.events` | `netfulfil` (only with `CAPABILITY_OFFER_ENABLED=true`) | `network-fulfillment-process-path-capability-cache-<hostname>-<pid>-<unix-nanos>` | earliest | path cycle times and the site CPT schedule |
| `warehouse.work-planning.events` | `netfulfil` (same flag) | `network-fulfillment-path-capacity-cache-<hostname>-<pid>-<unix-nanos>` | earliest | remaining path capacity per `(pathId, cutoffAt)` |

Every group id is unique per process on purpose: each process rebuilds its
state from the whole topic, and a shared group would resume from an old
offset and look ready with an empty cache. The side effect is that old groups
pile up on the broker; they hold no partitions and can be deleted.

### Dead letters

- **Analytics consumer** (`internal/adapters/inbound/kafka/analytics_consumer.go`):
  a message that is not a valid CloudEvent goes to
  `warehouse.network-fulfillment.analytics.dlq` immediately. A recognised event
  whose dedupe step or projection fails 3 times (backoff from 100 ms, max 2 s)
  also goes there. The DLQ message keeps the original key, value and headers
  and adds `x-dlq-source-topic`, `x-dlq-error` and `x-dlq-failed-at`. The
  offset is committed either way, so one poison message never blocks the
  partition. Unknown event types are skipped and committed.
- **Capability caches**: no DLQ. A non-CloudEvent or undecodable message is
  logged (`skipping non-CloudEvents message` or `... message handling failed`)
  and skipped.

## Transactional outbox (ADR 0003)

Active when `DATABASE_URL` is set **and** `EVENT_PUBLISHER=kafka`.

- Each use case saves the aggregate and inserts its event rows in one
  transaction: one `outbox_events` row per (event x topic), so two rows per
  domain event. The CloudEvent bytes, including its `id`, are stored, so a
  redelivery carries the same id.
- The relay (`internal/adapters/outbound/postgres/outbox_relay.go`) runs inside
  every `netfulfil` replica. Each pass claims up to 100 unpublished rows in id
  order with `FOR UPDATE SKIP LOCKED`, sends them one by one and sets
  `published_at`. On the first send failure it records `attempts` and
  `last_error` on that row, commits, and stops the pass so later events do not
  overtake it. An empty pass sleeps `OUTBOX_RELAY_INTERVAL`.
- Delivery is at least once: a crash between send and `UPDATE` republishes
  the row. Consumers dedupe on the CloudEvents `id`.
- Published rows are never deleted. There is no housekeeping job for
  `outbox_events`.

## Background work in `netfulfil`

| Job | Cadence | What it does |
| --- | --- | --- |
| Poller | immediately at start, then every `POLL_INTERVAL` | `PollDemand(since)` on the gateway, then `ReceiveNetworkDemand` per unit. The watermark advances only after a pass with no failure. |
| `ReconcileSubmittedOrders` | every `POLL_INTERVAL` (first run after one interval) | settles every `SUBMITTED` order from the gateway's submission status. |
| `SweepAcknowledgementDeadlines` | every `SWEEP_INTERVAL` | publishes `AcknowledgementDeadlineAtRisk` for every overdue `NEW` order, on every pass while it stays overdue. |
| `RejectOverdueOrders` | every `SWEEP_INTERVAL`, own ticker | cancels the hold (if linked) and rejects each overdue `NEW` order with `ACKNOWLEDGEMENT_DEADLINE_MISSED`. |
| `RecomputeCapabilityOffers` | every `RECOMPUTE_INTERVAL`, only when enabled | upserts one `capability_offers` row per known SKU. |
| Outbox relay | continuous | see above. |

Use-case details: [ddd/use-cases.md](../ddd/use-cases.md).

## Scaling

- **Connection budget** (ADR 0006): OLTP pool `MaxConns` 10 per process
  (`netfulfil` and `mcp` each), projector 5, reports 5. At the HPA ceilings
  (api 4, reports 3) with one mcp and one projector, that is 50 OLTP plus 20
  analytical connections against the fleet's single Postgres
  (`max_connections` 100 per the comment in `postgres/pool.go`, shared with
  every other context).
- **`netfulfil` replicas.** Every replica runs its own poller, all four
  tickers and its own outbox relay. The relay is safe across replicas
  (`SKIP LOCKED`). The poller and the use cases are not coordinated:
  `ReceiveNetworkDemand`'s idempotency is a `FindByRef` followed by a save,
  with no lock, so two replicas that fetch the same demand at the same moment
  can both raise a held order in order-management. In stub mode every replica
  also has its own seeded stub gateway. Keep `replicaCount` at 1 (the default,
  HPA disabled) unless that race is acceptable.
- **`mcp`**: fixed at one replica. The MCP SDK keeps sessions in process
  memory keyed by `Mcp-Session-Id`, and the Service has no session affinity.
- **projector**: fixed at one replica. Its consumer group is unique per
  process, so a second replica would replay the whole topic independently.
  The projection is idempotent per event id, so the counters would not double,
  but the work would.
- **reports** and **frontend** are stateless and can use their HPAs.

## Routine procedures

### Check that demand is flowing

```bash
curl -s http://localhost:8000/api/network-fulfillment/inbound-status
curl -s http://localhost:8000/api/network-fulfillment/network-orders
```

`polls` should grow by one per `POLL_INTERVAL`; `since` should be set after
the first clean pass; `overdue` should be 0. See
[troubleshooting](troubleshooting.md) when it is not.

### Confirm a shipment

```bash
curl -i -X POST http://localhost:8000/api/network-fulfillment/network-orders/po-local-1/shipment-confirmation
```

`204` on success or when the order is already `CONFIRMED`; `409
confirm-before-acknowledge` unless the order is `ACKNOWLEDGED`; `404` for an
unknown ref (ADR 0014).

### Change the product dictionary or the stub demand

Edit `productTranslation.mappings` or `stubDemand.demands` in the
environment's values. Both files are read once, at boot. The Deployment's
`checksum/config` annotation covers `configmap.yaml` only, not the
`-data` ConfigMap that holds `products.json` and `demand.json`, so a
mapping change alone does not roll the pod. Restart it:

```bash
kubectl -n <apps namespace> rollout restart deployment/network-fulfillment
```

### Rotate database credentials

The chart reads `DATABASE_URL` / `MIGRATIONS_DATABASE_URL` from
`database.existingSecret` (`network-fulfillment-db` in kind) and
`ANALYTICS_DATABASE_URL` from `analytics.database.existingSecret`. Update the
Secret, then `kubectl rollout restart` the api and mcp Deployments (OLTP) or
the projector and reports Deployments (analytics). The `checksum/secret`
annotation only tracks the chart's own `secret.yaml`, not an existing Secret.

### Re-publish an event

Unpublished rows (`published_at IS NULL`) are retried by the relay on every
pass; nothing to do. To send an already-published row again, clear its mark:

```sql
UPDATE outbox_events SET published_at = NULL WHERE id = <id>;
```

The relay sends the stored bytes, with the same CloudEvents `id`, so
consumers that dedupe on `id` ignore it if they already applied it.

### Replay the analytics read model

The projector replays the analytics topic from the earliest offset on every
start, but each event id is claimed once in `analytics_processed_events`, so
a restart alone changes nothing. To rebuild the counters (for example after
migration 0002 added `orders_rejected_submission_failed`, which is not
back-filled), stop the projector, truncate `acknowledgement_rollup`,
`analytics_processed_events` and `analytics_consumed_events` in the analytical
database, and start it again. It rebuilds from whatever the topic still
retains.

### Replay a dead letter

Read the DLQ topic, fix the cause named in `x-dlq-error`, and produce the
message value back to `warehouse.network-fulfillment.analytics` with the same
key. The projector dedupes on the CloudEvents `id`.

## Reports API (`netfulfil-reports`)

Declared in `apis/openapi.yaml` and served by
`internal/adapters/inbound/http/reports_handler.go`. Through Kong:
`/api/network-fulfillment/reports/...`.

| Route | Query | Response |
| --- | --- | --- |
| `GET /reports/acknowledgement` | `from`, `to` (RFC 3339, required; `from` inclusive, `to` exclusive, compared with the UTC day bucket), `granularity` (optional, only `day`) | `{"rows":[{"dayBucket","ordersReceived","ordersAcknowledged","ordersRejectedUntranslatableSku","ordersRejectedDomain","acknowledgementDeadlinesMissed","ordersRejectedSubmissionFailed","avgAcknowledgementLatencySeconds"}]}`. A missing or malformed parameter is `400 invalid-report-query`; a store error is `500 report-store-error`. |
| `GET /reports/acknowledgement/freshness` | none | `{"lagSeconds": <now - newest projected event time>}`; `0` when nothing has been projected. |
| `GET /healthz` | none | `200 {"status":"ok"}` |

`ordersRejectedDomain` counts `INFEASIBLE_DEADLINE` rejections.
`ordersAcknowledged` counts settled acknowledgements (`NetworkOrderAcknowledged`
v2) plus historic v1 events still on the topic.

## Branch protection

As read from the GitHub API on 2026-10-09: `develop` requires `lint`, `test`,
`mutation-fast`, `vuln`, `arch-test`, `helm-lint`, `integration` and
`api-lint`, with `strict: true`. `bdd`, `complexity` and `guide-lint` run on
every PR and are blocking jobs, but are not in that required list. CI job
details: [development/testing.md](../development/testing.md#ci).
