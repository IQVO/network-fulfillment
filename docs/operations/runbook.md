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

Postgres and Kafka are mandatory (ADR 0025): the chart refuses to render
without a Postgres DSN and Kafka brokers, so the overlay must set
`kafka.enabled=true` (or `kafka.brokers`) and the database Secret.

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

0. Validate the mandatory configuration (`loadBootConfig`): `DATABASE_URL`,
   `KAFKA_BROKERS`, and with `NETWORK_MODE=live` also `NETWORK_BASE_URL` and
   `NETWORK_VENDOR_ID` (one error names every missing one). Nothing has been
   dialled yet.
1. `telemetry.Setup` (OTLP exporters, never blocks on the Collector).
2. Live only: load `NETWORK_NODES_FILE` if set (strict; a bad file stops the
   boot with `cannot load network node definitions`).
3. Load `PRODUCT_TRANSLATION_FILE`, or log the "every network order will be
   rejected" warning.
4. Run OLTP migrations against `MIGRATIONS_DATABASE_URL`,
   open the pool, ping it. Both steps are retried.
5. Wire the network gateway from `NETWORK_MODE`, **after** the pool: the live
   gateway keeps its identifier maps and submission ledger in this Postgres.
   The stub gateway then loads `NETWORK_SEED_FILE`, if set; a seed file against
   the live gateway stops the boot.
6. Dial both Kafka caches (retried),
   start them, and wait until both have replayed their topic.
7. Wire the circuit-broken order-management client, the outbox event publisher
   and the relay and the use cases. One `circuit_breaker_state` registry serves
   order-management and, in live mode, `retail-network`.
8. Start the tickers, the poller (which polls once immediately), live only the
   node profile worker (one pass immediately, ADR 0027), the outbox relay, then
   the HTTP listener.

The HTTP server only starts listening after steps 1-7. Until then
`/healthz` does not answer, which is why the chart has a `startupProbe`
(30 x 2 s = 60 s).

**Boot retry (ADR 0011).** Steps 5 and 6 go through `retry` in
`cmd/netfulfil/retry.go`: 5 attempts, with waits of 1, 2, 4 and 8 s between
them (15 s of waiting, plus the time each attempt takes). It exists because
every Istio-injected pod's first outbound dial is reset about 10 s after
start. After the last attempt the process exits 1 and logs
`cannot wire order repository` (or `cannot wire capability offer pipeline`).
The code comment in `retry.go` states the same 15 s (1+2+4+8; there is no
fifth wait after the last attempt).

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
5. waits for the node profile worker, live only (a declaration cut off mid-call is
   a stored ledger row, resumed under the same key by the next boot);
6. waits for the two cache consumers, if enabled;
7. closes the publishers and the pool.

All waits share one 10 s budget; a step that overruns logs a warning such as
`outbox relay did not stop before the shutdown deadline`. The other three
binaries stop their HTTP server with a 10 s budget; the projector cancels its
consumer first.

## Database and migrations

| Database | Owner | Tables (migration) | Applied by |
| --- | --- | --- | --- |
| OLTP (`DATABASE_URL`) | `netfulfil` | `network_orders`, `network_order_lines` (0001), `outbox_events` (0002), state `SUBMITTED` added (0003), `capability_offers` (0004), columns `hold_outcome` and `hold_settled_at` on `network_orders` (0005, ADR 0024), `network_nodes`, `network_purchase_orders`, `network_purchase_order_lines`, `network_submissions`, `network_labels` (0006, ADR 0026; empty in stub mode), `inbound_shipments`, `inbound_poll_cursors`, `gtin_sku_cache` and the `NODE_PROFILE` and `INBOUND_RECEIPT` kinds of the ledger (0008, ADR 0028), `inbound_receipt_events`, `inbound_receipt_cartons`, `inbound_receipt_findings`, `inbound_receipts` (0009, ADR 0028), `network_node_profiles` (0010, ADR 0027; the three live-mode groups are empty in stub mode) | `netfulfil` at startup, and `cmd/mcp` at startup |
| Analytical (`ANALYTICS_DATABASE_URL`) | `netfulfil-projector` | `analytics_processed_events`, `analytics_consumed_events`, `acknowledgement_rollup` (0001), column `orders_rejected_submission_failed` (0002) | `netfulfil-projector` at startup |

- Migrations ship in the image (`/app/migrations`, `/app/migrations/analytics`)
  and run with `golang-migrate` (`internal/adapters/outbound/postgres/migrate.go`).
  There is no separate migration Job.
- Use `MIGRATIONS_DATABASE_URL` whenever `DATABASE_URL` goes through
  PgBouncer. `golang-migrate` takes a session-level `pg_advisory_lock`, which
  does not work in PgBouncer's transaction-pooling mode, and concurrent
  replicas then crash-loop (ADR 0007). In the kind cluster the Secret carries
  both keys.
- Migration `0005_hold_outcome` is additive for data (two nullable columns,
  new CHECKs, a partial index) and **drops** the `local_order_requires_answer`
  CHECK, so a `NEW` or `REJECTED` order now carries `local_order_id`: the hold
  is recorded the moment order-management creates it. An older binary keeps
  working against the new schema. Its `.down.sql` sets `local_order_id` to
  `NULL` on `NEW` and `REJECTED` rows before restoring the old CHECK, so a
  rollback forgets which holds those orders had.
- Migration `0006_retail_network_map` only adds tables (nothing is altered, and
  the tables have no foreign key to `network_orders`), so an older binary keeps
  working against it. Its `.down.sql` drops them: rolling back forgets every
  purchase order mapping and ledger row, which makes every order the live
  gateway still owes an answer on unanswerable. Never run it while live mode
  has run.
- Every migration has a `.down.sql`, but nothing in the repo runs them. A
  rollback is a manual `migrate ... down` against the direct DSN.
- The reports binary only reads; its pool is pinned read-only.

## Kafka

One broker for the whole platform (in-cluster Bitnami; `localhost:9092` from
the host through the kind cluster's external access). Every message is a
CloudEvents 1.0 structured-mode event with header
`content-type: application/cloudevents+json; charset=UTF-8` (ADR 0008).

### Produced

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
| `warehouse.process-path-management.events` | `netfulfil` | `network-fulfillment-process-path-capability-cache-<hostname>-<pid>-<unix-nanos>` | earliest | path cycle times and the site CPT schedule |
| `warehouse.work-planning.events` | `netfulfil` | `network-fulfillment-path-capacity-cache-<hostname>-<pid>-<unix-nanos>` | earliest | remaining path capacity per `(pathId, cutoffAt)` |

Every group id is unique per process on purpose: each process rebuilds its
state from the whole topic, and a shared group would resume from an old
offset and look ready with an empty cache. The side effect is that old groups
pile up on the broker; they hold no partitions and can be deleted.

### Dead letters

- **Analytics consumer** (`internal/adapters/inbound/kafka/analytics_consumer.go`):
  a message that is not a valid CloudEvent goes to
  `warehouse.network-fulfillment.analytics.dlq` immediately. A recognised event
  whose claim+apply step fails 3 times (backoff from 100 ms, max 2 s) also goes
  there; the log line carries `phase=claim_and_apply`. The dedupe claim
  (`analytics_consumed_events`) and the projection write commit in one
  analytics transaction ([ADR 0023](../adr/0023-analytics-claim-and-apply-in-one-transaction.md)),
  so a failed attempt or a projector stop leaves no claim behind and the
  redelivered message is applied exactly once. A shutdown is never
  dead-lettered. The DLQ message keeps the original key, value and headers
  and adds `x-dlq-source-topic`, `x-dlq-error` and `x-dlq-failed-at`. The
  offset is committed either way, so one poison message never blocks the
  partition. Unknown event types are skipped and committed.
- **Capability caches**: no DLQ. A non-CloudEvent or undecodable message is
  logged (`skipping non-CloudEvents message` or `... message handling failed`)
  and skipped.

## Transactional outbox (ADR 0003)

Always active (ADR 0025).

- Each use case saves the aggregate and inserts its event rows in one
  transaction: one `outbox_events` row per (event x topic), so two rows per
  domain event. The CloudEvent bytes, including its `id`, are stored, so a
  redelivery carries the same id.
- The relay (`internal/adapters/outbound/postgres/outbox_relay.go`) runs inside
  every `netfulfil` replica. Each pass claims up to 100 unpublished rows in id
  order with `FOR UPDATE SKIP LOCKED`, sends them one by one and sets
  `published_at`. On the first send failure it records `attempts` and
  `last_error` on that row, commits, logs a WARN and stops the pass so later
  events do not overtake it. An empty pass sleeps `OUTBOX_RELAY_INTERVAL`.
- **Attempt cap** ([ADR 0021](../adr/0021-outbox-relay-attempt-cap.md)): once
  a row has failed `OUTBOX_MAX_ATTEMPTS` sends (default 20, `0` = unlimited)
  the relay logs it **at ERROR** and excludes it from future claims, so one
  poison row cannot stop the relay forever. Later rows, **including rows for
  the same `networkRef`**, then overtake it. The row is not deleted or moved
  (there is no DLQ topic for the outbox): it stays in `outbox_events` with
  `published_at IS NULL`, `attempts` and `last_error`.
- Delivery is at least once: a crash between send and `UPDATE` republishes
  the row. Consumers dedupe on the CloudEvents `id`.
- Published rows are never deleted. There is no housekeeping job for
  `outbox_events`.

### An outbox row hit the attempt cap

Symptom: an ERROR log
`outbox relay: row reached the attempt cap and is excluded from future claims; ...`
carrying `outbox_id`, `topic`, `event_type`, `key`, `attempts`,
`max_attempts` and `error`. Consumers never received that event; later events
for the same `networkRef` may already have been delivered.

1. List the excluded rows (use the configured cap; 20 by default):

   ```sql
   SELECT id, topic, event_type, convert_from(key, 'UTF8') AS key, attempts, last_error, created_at
   FROM outbox_events
   WHERE published_at IS NULL AND attempts >= 20
   ORDER BY id;
   ```

2. Fix the cause named in `last_error` (topic missing, message too large,
   broker ACL, ...).
3. Re-arm the row so the next relay pass claims it again:

   ```sql
   UPDATE outbox_events SET attempts = 0 WHERE id = <outbox_id>;
   ```

   It is delivered late, after later rows with the same key; consumers dedupe
   on the CloudEvents `id`, so a re-armed row is never applied twice. If the
   event must not be delivered at all any more, leave it excluded and record
   why: deleting an unpublished outbox row is a manual data change and is not
   part of this procedure.

## Background work in `netfulfil`

| Job | Cadence | What it does |
| --- | --- | --- |
| Poller | immediately at start, then every `POLL_INTERVAL` | `PollDemand(since)` on the gateway, then `ReceiveNetworkDemand` per unit. The watermark advances only after a pass with no failure. The watermark is kept in the process and starts at zero, so every boot re-reads what the network still lists. In live mode each poll first converges the sites onto network nodes (see [the live adapter](#the-live-retail-network-adapter)). |
| `ReconcileSubmittedOrders` | every `POLL_INTERVAL` (first run after one interval) | settles every `SUBMITTED` order from the gateway's submission status. In live mode that is the network's transaction record, and an answer whose call never got through is sent again from the stored body. |
| `SweepAcknowledgementDeadlines` | every `SWEEP_INTERVAL` | publishes `AcknowledgementDeadlineAtRisk` for every overdue `NEW` order, on every pass while it stays overdue. |
| `RejectOverdueOrders` | every `SWEEP_INTERVAL`, own ticker | cancels the order-management hold (if one is recorded and still open) and rejects each overdue `NEW` order with `ACKNOWLEDGEMENT_DEADLINE_MISSED`. |
| `SweepOrphanedHolds` | every `SWEEP_INTERVAL`, own ticker, unless `ORPHAN_HOLD_SWEEP_ENABLED=false` | the backstop for `RejectOverdueOrders` (ADR 0024): cancels the order-management hold of a `NEW` or `REJECTED` order that nobody decided, once `now > acknowledgeBy + ORPHAN_HOLD_GRACE`, records the outcome and publishes `OrphanedHoldCancelled`. Never changes the order's state. See [A held order is stuck in order-management](#a-held-order-is-stuck-in-order-management). |
| `RecomputeCapabilityOffers` | every `RECOMPUTE_INTERVAL`, only when enabled | upserts one `capability_offers` row per known SKU. |
| `DeclareNodeProfiles` | live mode only: immediately at start, then every `NETWORK_PROFILE_INTERVAL` (default `5m`; `0` disables) | derives each site's operational profile from the schedule, cycle-time and capacity caches and declares it to the network when its content changed (ADR 0027). See [the node profile](#the-node-profile). |
| Outbox relay | continuous | see above. |

Use-case details: [ddd/use-cases.md](../ddd/use-cases.md).

## Scaling

- **Connection budget** (ADR 0006): OLTP pool `MaxConns` 10 per process
  (`netfulfil` and `mcp` each), projector 5, reports 5. At the HPA ceilings
  (api 4, reports 3) with one mcp and one projector, that is 50 OLTP plus 20
  analytical connections against the fleet's single Postgres
  (`max_connections` 100 per the comment in `postgres/pool.go`, shared with
  every other context).
- **`netfulfil` replicas.** Every replica runs its own poller, every ticker
  (including the orphaned-hold sweep) and its own outbox relay. The relay is
  safe across replicas (`SKIP LOCKED`). The sweeps take no row lock: two
  replicas can cancel the same orphaned hold (order-management's cancel is
  idempotent) and both publish `OrphanedHoldCancelled`, so a consumer must
  tolerate a rare duplicate (ADR 0024). The poller and the use cases are not coordinated:
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

### Change the product dictionary, the stub demand or the node definitions

Edit `productTranslation.mappings`, `stubDemand.demands` (stub mode) or
`networkNodes.nodes` (live mode) in the environment's values. The files are
read once, at boot. The Deployment's
`checksum/config` annotation covers `configmap.yaml` only, not the
`-data` ConfigMap that holds `products.json`, `demand.json` and `nodes.json`, so a
change alone does not roll the pod. Restart it:

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

### A held order is stuck in order-management

**Symptom.** A held (never released) order in `order-management` keeps stock
reserved for a network order this context never committed to. Typical signs:
usable stock for the SKU stays low with no shipment behind it; a `NEW` order
sits past `acknowledgeBy` (`/inbound-status` `overdue > 0`); a `REJECTED` order
whose rejection never freed its stock.

Why it happens: `ReceiveNetworkDemand` raises the order as a **held** order
(`releaseOnAllocation: false`), which reserves real inventory, and
order-management has no timeout of its own (its ADR 0020). Only this context
decides the hold, by release (`POST /orders/{id}/release`) or cancel
(`DELETE /orders/{id}`). If neither happens, for example because the cancel
failed while order-management was down, the reservation would last forever.
[ADR 0024](../adr/0024-orphaned-order-management-hold-sweep.md) records the
order-management order id on the `network_orders` row as soon as the hold
exists, and `SweepOrphanedHolds` cancels the ones nobody decided.

**What the sweep does, and when.**

1. `acknowledgeBy` is `receivedAt + 24 h`. Once `now` is past it,
   `RejectOverdueOrders` (every `SWEEP_INTERVAL`) cancels the hold and rejects
   the order. That is the normal path.
2. `SweepOrphanedHolds` is the backstop on the same `SWEEP_INTERVAL`. A hold is
   orphaned only when all of these hold: the row has `local_order_id`, no
   `hold_outcome`, `state` is `NEW` or `REJECTED`, and `now` is strictly after
   `acknowledgeBy + ORPHAN_HOLD_GRACE` (default `1h`). So an orphan is freed
   within `ORPHAN_HOLD_GRACE + SWEEP_INTERVAL` after the window closes
   (with the kind overlay's `sweepInterval = "5m"` and the default grace,
   about 1 h 5 min).
3. It calls `DELETE /orders/{id}` and records the answer in `hold_outcome`
   (with `hold_settled_at`), so the next pass finds nothing:
   - `2xx`, or `404`: outcome `CANCELLED`, event `OrphanedHoldCancelled`
     (`answer` `CANCELLED` or `NOT_FOUND`);
   - `409` or `422`: order-management says the order was already released.
     Not an orphan: outcome `ALREADY_RELEASED`, no event, never retried;
   - anything else (`5xx`, any other status, timeout, transport error, open
     circuit breaker): nothing is recorded, the hold stays open and the next
     pass tries again, with no retry limit.
4. It never changes the order's state and publishes no
   `NetworkOrderRejected`. A `NEW` order whose hold the sweep cancelled is
   rejected by `RejectOverdueOrders` on its next pass, which skips the cancel
   because the outcome is already recorded.

`SUBMITTED`, `ACKNOWLEDGED` and `CONFIRMED` orders are never swept: this
context committed to them or is waiting for the network's verdict.

**Confirm it.** `GET /network-orders/{networkRef}` and the MCP tools do **not**
show the hold id for a `NEW` or `REJECTED` order (`localOrderId` appears only
from `SUBMITTED` on) and do not show the outcome, so use the database. The
first query is the sweep's own candidate list (an index serves it); the second
shows what it already settled:

```sql
SELECT network_ref, state, acknowledge_by, local_order_id
FROM network_orders
WHERE local_order_id IS NOT NULL AND hold_outcome IS NULL AND state IN ('NEW', 'REJECTED')
ORDER BY acknowledge_by;

SELECT network_ref, state, local_order_id, hold_outcome, hold_settled_at
FROM network_orders
WHERE hold_outcome IS NOT NULL
ORDER BY hold_settled_at DESC LIMIT 20;
```

A row from the first query with `acknowledge_by + ORPHAN_HOLD_GRACE` still in
the future is not orphaned yet. A row that is older than `ORPHAN_HOLD_GRACE +
SWEEP_INTERVAL` means the sweep is off, failing or not running. Then check:

- **Log lines (`netfulfil`).** WARN `orphaned holds swept` with `examined`,
  `cancelled`, `alreadyReleased` and `failed`, logged only on a pass that acted
  on at least one hold. ERROR `orphaned-hold sweep failed` (field `err`) when
  the candidate list could not be read. WARN `orphaned-hold sweep disabled by
  ORPHAN_HOLD_SWEEP_ENABLED: a hold nobody decides stays reserved in
  order-management` at boot when it is off. WARN `invalid
  ORPHAN_HOLD_SWEEP_ENABLED, keeping the orphaned-hold sweep on` or `invalid
  ORPHAN_HOLD_GRACE, using the default` for a typo. The sweep logs counts, not
  the reason a single cancel failed.
- **Metric.** Counter `network_fulfillment.orphaned_holds.swept` (unit
  `{hold}`, OTLP only, not on `GET /metrics`), attribute `outcome` =
  `cancelled`, `already_released` or `failed`, one data point per hold per
  pass. `failed` growing means order-management is answering the cancel with
  something other than 2xx, 404, 409 or 422 (see [observability](observability.md#orphaned-hold-sweep-metric)).
- **Breaker.** `circuit_breaker_state{dependency="order-management"}` = `2`
  (open) makes every cancel fail without a call.
- **Event.** `OrphanedHoldCancelled` on both topics. `SELECT created_at, topic FROM outbox_events WHERE event_type LIKE '%OrphanedHoldCancelled';`

**What `OrphanedHoldCancelled` means.** A hold leaked, and the sweep freed it.
It is a reported fact, not a state change: the order keeps its state. `data`:
`networkRef`, `siteId`, `localOrderId` (the order-management order that was
held; for an order never submitted this event is the only place it is
published), `acknowledgeBy`, `answer` (`CANCELLED` = order-management cancelled
it now; `NOT_FOUND` = it no longer existed, so it was already gone), `at`.
One event per hold, except that two replicas can both cancel and both publish.
Its presence means the normal path failed first (order-management down, breaker
open, a crash): look for what stopped `RejectOverdueOrders`. An `alreadyReleased`
count is different: someone released that order in order-management, so its
stock is committed to the floor and the sweep leaves it alone; decide in
order-management whether it should ship.

**If order-management answers not-found or 5xx.**

- `404` is treated as done (`NOT_FOUND`). If you expected the hold to exist,
  check that `ORDER_MANAGEMENT_URL` points at the order-management that raised
  it and that `local_order_id` is the id you see there.
- `5xx`, timeout or open breaker: no action needed on this side. The row stays
  open and every `SWEEP_INTERVAL` the sweep tries again, so it settles once
  order-management is healthy (the breaker lets a probe through after 30 s).
  Fix order-management, then re-run the first query until it returns no row
  past the grace. If one hold keeps failing while others settle, cancel it by
  hand in order-management with `DELETE /orders/{localOrderId}`; the next pass
  then gets a 2xx or 404 and records the outcome.

**When to turn the sweep off.** Rarely, and only on purpose: set
`ORPHAN_HOLD_SWEEP_ENABLED=false` (chart value `config.orphanHoldSweepEnabled:
false`, then roll the pod) while order-management must not receive cancels
(a migration or a restore), while `ORDER_MANAGEMENT_URL` points at the wrong
service (every cancel would then be a `404`, which the sweep records as done
although the real hold still exists), or to stop a sweep that is cancelling
holds it should not while you investigate. The default is on because a leaked
hold is the worse failure; an unparseable value keeps it on. While it is off,
nothing frees a hold that `RejectOverdueOrders` missed. Turn it back on
afterwards: the first pass frees everything that accumulated. To change only
the delay, set `ORPHAN_HOLD_GRACE` (a Go duration, `0` = none).

**Holds the sweep cannot see (hand steps).**

1. `SUBMITTED` order that never settles (the network keeps answering
   `PENDING`): not swept, because cancelling could contradict a later
   `SUCCESS`. List them with `list_network_orders` `state=SUBMITTED`.
2. `ACKNOWLEDGED` order whose release call failed (the failure is not logged
   and nothing retries it): release it in order-management with
   `POST /orders/{localOrderId}/release`. Never cancel it: the network was told
   we will ship.
3. A hold raised before this context could record its id (a crash between
   order-management's commit and this context's write) and not re-delivered
   inside the window: the row has `local_order_id IS NULL`. Nothing can name
   the hold, because this context never reads order-management state. Find the
   held order in order-management and cancel it there with `DELETE /orders/{id}`.
4. Orders rejected before migration `0005` whose raise had succeeded have no
   recorded id either; same hand step.

## The live retail-network adapter

`NETWORK_MODE=live` ([ADR 0026](../adr/0026-live-retail-network-adapter-demand-loop.md))
makes the gateway call the retail network's Vendor API. The chart's default, the
kind cluster and every e2e run stay `stub`; the flip to `live` in an environment
is a values change (`config.networkMode`, `config.networkBaseUrl`,
`config.networkVendorId`, optionally `networkNodes.nodes`) and the render refuses
it without the first three. There is no credential and no timeout variable.

**Before the flip.** The database must be one that live mode can own. An order
that was answered by the stub has no row in `network_purchase_orders`, and the
live gateway cannot answer for it: reconciling a `SUBMITTED` one or confirming an
`ACKNOWLEDGED` one fails with `retail-network: no purchase order is recorded for
this ref` (the reconcile pass does not log that failure; the order just stays
`SUBMITTED`). Settle those first; this lists them:

```sql
SELECT o.network_ref, o.state, o.acknowledge_by
FROM network_orders o
LEFT JOIN network_purchase_orders p ON p.network_ref = o.network_ref
WHERE p.network_ref IS NULL AND o.state IN ('SUBMITTED', 'ACKNOWLEDGED')
ORDER BY o.acknowledge_by;
```

**The tables (migration 0006)** are written only by the adapter and are empty in
stub mode:

| Table | One row per | Holds |
| --- | --- | --- |
| `network_nodes` | site | `site_code` to the network's `node_id` |
| `network_purchase_orders` | purchase order | the minted `network_ref` (`NR-...`) to `po_number`, the vendor and node, `required_ship_by`, the network's `ack_by` |
| `network_purchase_order_lines` | purchase order line | line number, `network_line_ref` (`line-<n>`), listing id, `network_product_id`, ordered quantity |
| `network_submissions` | `(kind, subject)` | the ledger: `kind` is `ACKNOWLEDGEMENT`, `SHIPMENT_CONFIRMATION`, `NODE_REGISTRATION`, `NODE_PROFILE` or `INBOUND_RECEIPT` (migration 0008 admits the last two), `subject` the network ref (or site code, or `<site>#<revision>#<attempt>` for a profile), the idempotency key, the exact request body, the network's `transaction_id`, `status`, `errors` |
| `network_labels` | package | `label_ref`, `tracking_number`, `carrier` from the label request |

**Ledger statuses.** The row, with its key and exact body, is written before the
first HTTP call; the `transaction_id` is stored as soon as the `202` is read.

| `status` | `transaction_id` | Meaning |
| --- | --- | --- |
| `SENDING` | none | The call has not been confirmed: a crash mid-call, a `5xx`/timeout after the retries, an open breaker, or an answer that is not a final refusal (`403`, `404`, `409 concurrent-update`, `422 idempotency-key-reused`). The write may or may not have reached the network; the stored body and key are what any retry sends, so it cannot be created twice. |
| `PROCESSING` | set | The network accepted the write (`202`) and has not settled the transaction. |
| `SUCCESS` | set | The network settled it. Terminal. |
| `FAILURE` | set, or none | The transaction failed, or the network refused the write outright (`409 ack-window-closed`, `illegal-transition`, `confirm-without-label`, `label-before-acceptance`; `422 validation-failed`); `errors` says why. Terminal: a settled row is never updated again. |

Stuck rows, oldest first (an index serves this):

```sql
SELECT kind, subject, status, transaction_id, errors, created_at, updated_at
FROM network_submissions
WHERE status IN ('SENDING', 'PROCESSING') AND updated_at < now() - interval '10 minutes'
ORDER BY updated_at;
```

**What to do with each kind of row.**

- `ACKNOWLEDGEMENT`, `SENDING` or `PROCESSING`: nothing to re-arm. While the
  order is `SUBMITTED`, `ReconcileSubmittedOrders` sends the stored answer again
  or reads the transaction every `POLL_INTERVAL`. If it stays unsettled, find out
  why. A lookup that fails does not abort the pass and **is not logged by the use
  case**: the order just stays `SUBMITTED`. The signals are this query,
  `circuit_breaker_state{dependency="retail-network"}` = `2` (open: no call is
  attempted) and the client's `WARN` `retail-network call will be retried` (`op`,
  `attempt`, `err`). A transaction that stays `PROCESSING` on the network is the
  network's to settle. This service never sweeps a `SUBMITTED` order, so it
  stays `SUBMITTED` here (list them with `list_network_orders` `state=SUBMITTED`)
  even after the network has expired the purchase order at `ackBy`.
- `ACKNOWLEDGEMENT`, `FAILURE`: the order is rejected here with
  `SUBMISSION_FAILED` and its hold cancelled by the reconcile pass. There is no
  re-arm: a re-delivered purchase order finds the same `network_ref` and the
  same rejected order, and the network returns the purchase order to `ISSUED` and
  expires it at `ackBy`.
- `SHIPMENT_CONFIRMATION`, any unsettled or `FAILURE` row: the order is already
  `CONFIRMED` here, and **no use case retries or reconciles the confirmation**
  (ADR 0026, Consequences; ADR 0018 finding 3). A transaction that is
  already `FAILURE` when the confirmation is sent is logged at `ERROR` (`the
  network failed a shipment confirmation; the order stays accepted there until an
  operator acts`) with the `errors` text; one still `PROCESSING` at that moment
  is never read again, so its row stays `PROCESSING` and nothing is logged. Calling the confirm
  endpoint again answers `204` without sending anything, because the order is
  `CONFIRMED`. Check the order on the network and act there; this service has no
  re-send for it today.
- `NODE_REGISTRATION`, `SENDING` or `PROCESSING`: converges by itself on each
  poll (a `WARN` `node convergence incomplete; the next tick retries` carries the
  cause). `FAILURE`: the network refused to register the site and the site gets
  no demand. Fix the cause (usually `networkNodes.nodes`, whose values are the
  network's to judge), then delete the settled row so the next poll writes a new
  one, and roll the pod if the file changed:

  ```sql
  DELETE FROM network_submissions
  WHERE kind = 'NODE_REGISTRATION' AND subject = '<site code>' AND status = 'FAILURE';
  ```

  The idempotency key is `nf-node-<site code>` and the network keeps a key for 24
  hours: a corrected body sent under the old key inside that window is answered
  `idempotency-key-reused`, which is logged on every poll and succeeds once the
  first attempt is 24 hours old.

**Purchase orders that are skipped.** A purchase order whose node is not mapped to
one of our sites is skipped with `WARN` `retail-network: purchase order skipped,
its node is not mapped to any of our sites` (`nodeId`, `poNumber`); there is no
metric for it. The watermark moves on, so the order is read again only on the next
boot (the watermark starts at zero), and the network expires it at its `ackBy`
otherwise. To recover: make the node known (add a `networkNodes.nodes` entry for
its site, or register the node on the network under the site code as its
`vendorNodeCode`), then restart the pod before `ackBy`. A site with neither a node
nor a definition logs `WARN` `site has no node on the network and no node
definition to register one from` on every poll.

**Signals.** `retail-network:` log lines (`WARN` for skips, node convergence and
node listing failures, `ERROR` for a failed shipment confirmation, `Info` once per
process for the one remaining supply-loop no-op: `SubmitAvailability` does
nothing in live mode until ADR 0027 part 2; the node profile has its own signals
below); the poller's `poll demand failed`;
`circuit_breaker_state{dependency="retail-network"}` on `GET /metrics` next to
`dependency="order-management"`; `GET /inbound-status` for the mode and the
poller counters. `/readyz` does not depend on the network (ADR 0026 Decision 10):
a network outage leaves the pod ready, so alert on the gauge and the poll errors.

**What is and is not proven about a crash.** The ledger row is written before the
call, so a process death between the steps is designed to end with one order here
and one write on the network. The integration test stops the service gracefully
(a context cancel) between the acknowledgement being sent and reconciled; there is
no test that kills the process (ADR 0026, As built 8).

### The node profile

In live mode a worker declares each site's operational profile
([ADR 0027](../adr/0027-supply-loop-node-profile-and-inventory-feed.md), part 1):
once at boot and then every `NETWORK_PROFILE_INTERVAL` (default `5m`; `0`
disables it, with a `WARN`). The network judges our acknowledgement deadlines by
the declared handling time (`ackBy = min(issuedAt + ackWindow, requiredShipBy -
handlingTime)`), so a profile that is wrong is worse than none, and one the local
caches cannot support is **not declared**: the site is skipped and the reason is
logged.

**State (migration 0010).** `network_node_profiles` holds, per site, the revision
in force, the current attempt at it, the fingerprint of its content and that
content. The revision only goes up, is persisted before the call that carries it
and is bumped only when the content changes:

```sql
SELECT site_code, revision, attempt, fingerprint, updated_at FROM network_node_profiles;

SELECT subject, status, transaction_id, errors, updated_at
FROM network_submissions
WHERE kind = 'NODE_PROFILE'
ORDER BY updated_at DESC;
```

**Signals.** The counter `network_fulfillment.node_profile.declarations` by
`outcome` and the gauge `network_fulfillment.node_profile.revision` by `site`
(OTLP, like the other business metrics; they are not on `/metrics`). Logs:

| Outcome / log | Meaning | What to do |
| --- | --- | --- |
| `skipped_no_node`, `INFO` `node profile: the site has no node on the network yet` | the demand loop has not registered or adopted the site's node | nothing; it converges at a poll (see `NODE_REGISTRATION`) |
| `skipped_no_schedule`, `WARN` `no cutoff schedule in the cache` | process-path-management has published no usable CPT schedule for the site (`SITE_ID` and the node file's sites are looked up by id) | check the site id and that the schedule event exists; nothing is declared meanwhile |
| `skipped_incomplete`, `WARN` `no eligible path has a cycle time that fits the schedule` or `no capacity figure is known for any cutoff` | a cycle-time p95 or a remaining-capacity figure has not been observed yet | wait for the Kafka caches; nothing is guessed |
| `failed`, `ERROR` `the network failed a node profile declaration` (`errors`) | the transaction failed; the list is the network's own | see the row below |
| `WARN` `the network already holds a profile revision at or above ours (stale)` | a wiped table or another declarer; the gateway reads the node's `profileRevision` and declares the same content above it, once per tick | none unless it repeats every tick |
| `WARN` `the node profile declaration stays failed until the profile changes` | a final failure (below) is still standing | fix the cause; a changed profile is sent by itself |

**What to do with a `NODE_PROFILE` row.**

- `SENDING` or `PROCESSING`: converges by itself. The next pass resends the stored
  body under the same key, or reads the transaction by its persisted id; nothing
  is sent twice.
- `FAILURE` with `ErrIncompleteOperationalProfile`, `ErrCutoffOutsideOperatingHours`
  or a `422 validation-failed` on the `PUT`: the same content can never succeed, so
  it is **not** sent again until the content changes (the next change of schedule,
  cycle time or capacity). A real fault in how the body is built is a code bug:
  read `errors`, which is the network's own message.
- `FAILURE` with anything else: retried on a later pass at the same revision under
  a new key (`nf-profile-<site>-<revision>-<attempt>`, because the network replays
  a failed transaction for the same key).
- Nothing to re-arm by hand and no row to delete: unlike a node registration, the
  revision is advanced by the code, never by editing the table. Do not lower
  `revision` in `network_node_profiles`: the network refuses a revision that is not
  above the one it holds (`ErrStaleProfileRevision`), and the gateway would only
  climb back.

**Stub mode** never starts the worker and the tables stay empty. The stub gateway
records the last profile it was handed and sends nothing.

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

## The inbound loop (ADR 0028)

Live mode only. Seller shipments polled from the Vendor API become ASNs in
inbound-receiving. State is in Postgres (`inbound_shipments`, `inbound_poll_cursors`,
`gtin_sku_cache`, migration 0008); nothing is lost by a restart.

**Where a shipment stands.** `SELECT shipment_ref, asn_number, status, attempts,
last_error, next_attempt_at FROM inbound_shipments ORDER BY shipped_at;`

- `REGISTERED`: the ASN exists in inbound-receiving. Nothing to do.
- `PENDING` with `last_error`: not registered yet and retrying with a backoff of one
  minute doubling to one hour. `GTIN not found` means product-master does not know the
  GTIN: declare the product (the next due attempt registers the shipment). To retry
  at once, `UPDATE inbound_shipments SET next_attempt_at = now() WHERE shipment_ref = ...`.
  `unknown-sku` means inbound-receiving's product projection has not caught up.
- `REJECTED`: inbound-receiving refused the data (a duplicate SSCC, a carton set that
  does not add up). Logged at `ERROR` (`inbound shipment rejected, it will not be
  retried`). Fix the cause upstream, then `UPDATE inbound_shipments SET status = 'PENDING',
  next_attempt_at = now() WHERE shipment_ref = ...`.

**Signals.** `inbound shipments processed` (Info) and `inbound shipment not registered
yet, retrying` (Warn) log lines; `circuit_breaker_state{dependency="inbound-receiving"}` and
`{dependency="product-master"}` on `GET /metrics`.

**The receipt return.** `SELECT asn_number, status, attempts, last_error, next_attempt_at FROM
inbound_receipts;` shows each closed receipt: `SENT` (the network has it; the transaction id is
in `network_submissions` where `kind = 'INBOUND_RECEIPT'`), `PENDING` with `last_error` (retrying
with the same backoff), `REJECTED` (the network refused it for good; `inbound receipt refused, it
will not be retried` at `ERROR`, with the network's errors in `last_error`; fix the cause, then
`UPDATE inbound_receipts SET status = 'PENDING', next_attempt_at = now() WHERE asn_number = ...`;
the stored request is replayed byte for byte). The consumer's group is
`INBOUND_RECEIPT_CONSUMER_GROUP`: check its lag on `warehouse.inbound-receiving.events`; messages it
could not handle are on `warehouse.inbound-receiving.events.dlq`. A receipt that never closes is
not reported: the dock has to close it in inbound-receiving. If the consumer stops on its own the
process exits with code 1 (`the receipt consumer stopped on its own; shutting down`).
