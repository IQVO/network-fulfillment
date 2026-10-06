# Network Fulfillment

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> CloudEvents 1.0 event envelopes, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world
> company**. References to a major e-commerce retailer's Selling Partner API describe a public
> API this exercise integrates against as a learning target.

The anti-corruption layer between the `warehouse-systems` fleet and an
external retail fulfillment network — a **Supporting Subdomain** bounded
context that is **Conformist** to the network upstream and an
**Anti-Corruption Layer** for everything downstream of it in this fleet.
Since ADR 0009 the counterpart is the fleet's own `retail-network` service
playing the network's role, not a real retailer's API.

## Status

**Implemented, stub-only.** `develop` is promoted to `main`, and `main`
has tagged releases (`v0.1.0` on 2026-09-26 through `v0.1.4` on
2026-10-03; `develop` may be ahead of the latest tag). ADR 0001 is
`Accepted` (2026-09-23), together with its companion `order-management`
ADR 0020 — neither is meaningful without the other. ADR 0009 (Accepted)
amends it.

📄 [ADR 0001 — Network Fulfillment as a bounded context](docs/adr/0001-network-fulfillment-bounded-context.md)
· [ADR index](docs/adr/README.md)
· [DDD artifact pack (ddd-crew)](docs/ddd/README.md)

What exists today:

- **`NetworkOrder` aggregate** (`internal/domain/networkorder`) —
  `NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED`, and `REJECTED` from
  `NEW` or `SUBMITTED`; one answer per order, acknowledged in full or
  rejected in full, a 24h `acknowledgeBy` deadline fixed at receipt, and a
  persisted one-to-one `localOrderId` link allowed once the order is at
  least `SUBMITTED`.
- **`CapabilityOffer` aggregate** (`internal/domain/capabilityoffer`, ADR
  0001 §8) — the advertisable quantity per `(sku, siteId)`:
  `min(physicalAvailable, throughputFeasible)` with a `PHYSICAL` /
  `THROUGHPUT_CONSTRAINED` basis, never more than physical stock. Opt-in
  via `CAPABILITY_OFFER_ENABLED=true`.
- **Inbound leg** — a poller (`internal/adapters/inbound/poller`) calls the
  network gateway every `POLL_INTERVAL` (default `1m`) and feeds each unit of
  demand to `ReceiveNetworkDemand`. Its watermark advances only on a fully
  successful pass, and receipt is idempotent on `networkRef`.
- **`ReceiveNetworkDemand`** — translates network product ids to SKUs (one
  unknown product rejects the whole order), saves the order `NEW` and
  publishes `NetworkOrderReceived`, raises a **held** order in
  `order-management` (`POST /orders` with `releaseOnAllocation: false`,
  `allowPartialShipment: false`, `requiredShipBy`), treats a returned
  `promiseDate` as "feasible" and a null one as "not feasible", then either
  submits the acknowledgement (order goes `SUBMITTED`, linked to the local
  order, **not yet released**) or cancels the hold (`DELETE /orders/{id}`)
  and rejects. No promise math is done here.
- **`ReconcileSubmittedOrders`** — every `POLL_INTERVAL`, asks the gateway
  for each `SUBMITTED` order's transaction status (ADR 0001 §5): `SUCCESS`
  settles it `ACKNOWLEDGED` and releases the hold
  (`POST /orders/{id}/release`); `FAILURE` cancels the hold and rejects
  (`SUBMISSION_FAILED`); `PENDING` leaves it for the next pass.
- **`SweepAcknowledgementDeadlines`** — every `SWEEP_INTERVAL` (default
  `1m`), publishes `AcknowledgementDeadlineAtRisk` for every order still
  `NEW` past its deadline. It never mutates the aggregate (ADR 0001 §6).
- **`RejectOverdueOrders`** — on its own ticker (same `SWEEP_INTERVAL`),
  cancels the held local order (if linked) and rejects every overdue `NEW`
  order (`ACKNOWLEDGEMENT_DEADLINE_MISSED`). It never acknowledges late.
- **`ConfirmNetworkOrderShipment`** (ADR 0014) — the one write endpoint,
  `POST /network-orders/{networkRef}/shipment-confirmation`: `ACKNOWLEDGED
  -> CONFIRMED`, publishes `NetworkOrderShipmentConfirmed`, then tells the
  network. Idempotent on an already-`CONFIRMED` order.
- **`RecomputeCapabilityOffers`** — every `RECOMPUTE_INTERVAL` (default
  `1m`, only when `CAPABILITY_OFFER_ENABLED=true`), for every SKU in the
  translation dictionary at `SITE_ID`: physical stock from
  `inventory-storage` (`GET /inventory/{sku}/usable`, synchronous REST) and
  remaining path capacity before the site's next CPT from two Kafka-fed
  caches (`warehouse.process-path-management.events`,
  `warehouse.work-planning.events`). The offer is persisted for the read
  surface; it is **not** yet submitted to the network.
- **REST** (`internal/adapters/inbound/http`): `GET /healthz`,
  `GET /readyz`, `GET /metrics` (the `circuit_breaker_state` gauge),
  `GET /inbound-status` (network mode, poller counters, unanswered/overdue
  counts), `GET /network-orders` (unanswered orders),
  `GET /network-orders/{networkRef}`, `GET /capability-offers` (when
  enabled) and the single write route
  `POST /network-orders/{networkRef}/shipment-confirmation`. Demand itself
  still arrives only by polling.
- **MCP server** (`cmd/mcp`, Streamable HTTP on `MCP_ADDR`, default
  `:8090`, mounted at `/` and `/mcp`) — read-only tools
  `get_network_order`, `list_network_orders`, `list_capability_offers`,
  and `get_acknowledgement_report` (when `REPORTS_BASE_URL` is set); the
  `network-order://network-fulfillment/{networkRef}` resource; and the
  `answer_network_demand` prompt.
- **Analytics data product** (ADR 0002) — `cmd/netfulfil-projector`
  consumes this service's own analytics topic into a separate analytical
  Postgres (`migrations/analytics/`), and `cmd/netfulfil-reports` serves
  `GET /reports/acknowledgement` and `GET /reports/acknowledgement/freshness`
  over a read-only pool.
- **Persistence** — Postgres (`migrations/0001`–`0004`: `network_orders`,
  `network_order_lines`, `outbox_events`, `capability_offers`) when
  `DATABASE_URL` is set, and in-memory repositories when it is not. If
  `DATABASE_URL` is set but Postgres cannot be reached, the service refuses
  to boot instead of falling back. Migrations run at startup
  (`MIGRATIONS_DATABASE_URL` for a direct, non-PgBouncer connection, ADR
  0007).
- **Curated ACL files** — `PRODUCT_TRANSLATION_FILE` (network product id ->
  SKU; without it every order is rejected as untranslatable, and a warning is
  logged at startup; ADR 0013) and `NETWORK_SEED_FILE` (stub demand; refuses
  to boot against a non-stub gateway; ADR 0012). The Helm chart renders both
  from `productTranslation.mappings` and `stubDemand.demands`.
- **Resilience** (ADR 0004) — one circuit breaker guards every
  `order-management` call, the analytics consumer retries in-process and
  dead-letters to `<topic>.dlq`, and shutdown flips `/readyz` first, then
  drains HTTP, the outbox relay and the poller.
- **Boot resilience** (ADR 0011) — the migration run, the database ping and
  the capability-offer cache dials are retried with backoff (~31s budget)
  to survive this cluster's first-dial connection reset, and the chart has
  a `startupProbe`. After the budget the service still refuses to boot.
- **Packaging** — `Dockerfile`, `web/` (a Module Federation remote, ADR
  0010) and `charts/network-fulfillment/` (API, MCP, projector, reports and
  frontend Deployments, HPA per ADR 0006). The chart's Python tests live in
  `charts/network-fulfillment/tests/`. It is deployed to the kind cluster by
  `warehouse-infra`, and Kong serves it at `/api/network-fulfillment`.
- **Kafka + transactional outbox (ADR 0003)** — `EVENT_PUBLISHER=kafka` fans
  every domain event out to both the integration topic
  (`warehouse.network-fulfillment.events`) and the analytics topic
  (`warehouse.network-fulfillment.analytics`), keyed by `networkRef` with a
  hash balancer (ADR 0005). With `DATABASE_URL` unset both are published
  directly (no transaction to bind them to — the in-memory dev mode); with
  `DATABASE_URL` set, saving the aggregate and enqueuing its event(s) for
  both topics happen in one Postgres transaction (the `outbox_events`
  table), and a background relay drains it to the broker. The default
  (`EVENT_PUBLISHER` unset) publishes to a log-only publisher.
- **CloudEvents 1.0, mandatory (ADR 0008)** — every Kafka message on both
  topics is a CloudEvents 1.0 structured-mode event (header
  `content-type: application/cloudevents+json; charset=UTF-8`,
  `source=/warehouse/network-fulfillment`,
  `type=com.warehouse.wes.network-fulfillment.networkorder.<EventName>`,
  `subject=<networkRef>`, `dataschema=urn:warehouse:network-fulfillment:<events|analytics>:<EventName>:v1`).
  Every consumer here accepts only CloudEvents. Catalogue:
  `apis/asyncapi.yaml`.

Not built yet:

- **A live `retail-network` adapter.** `NETWORK_MODE=live` refuses to boot
  with "not implemented yet" (and requires `NETWORK_BASE_URL`). Only the
  stub gateway exists, so no call to a real counterpart is made.
- **Submitting `CapabilityOffer` outward.** `NetworkGateway.SubmitAvailability`
  exists on the port and in the stub, but no use case calls it yet.
- **A `PackageManifested`-driven shipment confirmation.** ADR 0014 chose
  the explicit endpoint until a persisted correlation exists.
- The `adapters/outbound/network/` -> `outbound/retailnetwork/` package
  rename (ADR 0009), and BDD `features/`.

## Why this context exists

The fleet models a building that knows what it can do: `process-path-management`
owns per-path `cycleTimeP95` and a site-scoped CPT (Critical Pull Time)
schedule, `wes-work-planning` publishes remaining capacity per `(path, CPT)`,
and `order-management` derives a real delivery promise from both.

Offering that capability to an external retail network turns out **not** to
be a matter of publishing it. No operation in the network's API accepts a
declaration of cutoffs or cycle times. Capability reaches a network only
through three signals, all of them consequences rather than declarations:

| Signal | Meaning |
| --- | --- |
| Inventory update | what we claim we can ship |
| Order acknowledgement | what we commit to — within 24h, whole order, fill-or-kill |
| Shipment confirmation | whether we actually did |

The network infers our capability from the gap between the second and the
third. So the interesting decision is **what to advertise and what to commit
to** — and this fleet already owns every fact needed to decide both well.

## The idea worth building

Most integrations of this shape advertise physical stock on hand. This fleet
can do better, because it knows whether the building can actually *move*
those units before the next departure:

```
advertisedQuantity = min(
    physicalAvailable,                       inventory-storage
    throughputFeasibleBefore(nextCutoff)     process-path-management
                                             CPTSchedule (eligible paths),
                                             × wes-work-planning remaining
                                               PathCapacity for that (path, CPT)
)
```

400 units in the building but a path that can only carry 120 more before the
18:00 cutoff means advertising 400 sells a promise the floor cannot keep.
Advertising 120 sells capability honestly. `CapabilityOffer.Compute()`
implements this; when no capacity figure is known it falls back to physical
stock rather than advertising zero.

## Boundary

- **Conformist upstream.** The network's vocabulary — purchase orders, line
  sequence numbers, product identifiers, acknowledgement codes, selling
  parties — stops at this context's adapter and is never adopted by the
  fleet.
- **No ship-to PII reaches this context** (ADR 0009 tightened ADR 0001's
  "PII stops here"): only a `poNumber`, lines, quantities and
  `requiredShipBy` come in, and a label request returns only
  `{labelRef, trackingNumber, carrier}`. The context still owns the fleet's
  only external SLA (the 24-hour acknowledgement clock), which is one reason
  it is a separate context rather than an adapter inside `order-management`.
- **Promise math is not duplicated here.** Deadline feasibility is asked of
  `order-management`'s existing `PromisePolicy`, per the companion ADR.

## Shape

```
NetworkOrder      networkRef, siteId, requiredShipBy, acknowledgeBy, lines,
                  localOrderId, receivedAt
                  NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED
                  NEW | SUBMITTED -> REJECTED
                  invariant: acknowledged in full or rejected in full

CapabilityOffer   (sku, siteId) -> advertisedQuantity + basis + computedAt
                  PHYSICAL | THROUGHPUT_CONSTRAINED
                  invariant: 0 <= advertisedQuantity <= physicalAvailable
```

```
cmd/netfulfil/                 OLTP composition root (REST, poller, tickers, outbox relay)
cmd/mcp/                       MCP server (Streamable HTTP, read-only)
cmd/netfulfil-projector/       analytics writer (Kafka -> analytical Postgres)
cmd/netfulfil-reports/         analytics reader (REST over a read-only pool)
internal/
  domain/networkorder/         NetworkOrder aggregate
  domain/capabilityoffer/      CapabilityOffer aggregate
  domain/shared/               NetworkRef, NetworkLineRef, NetworkProductId, SKU,
                               LocalOrderId, SiteId, domain events
  application/contract/        InboundDemand, HeldOrderRequest/Result, NextCutoff, ...
  application/ports/           NetworkOrderRepo, CapabilityOfferRepo, NetworkGateway,
                               FulfillmentPlanner, ProductTranslation,
                               ProcessPathCapability, PathCapacity,
                               InventoryAvailability, EventPublisher, UnitOfWork, Clock
  application/usecases/        ReceiveNetworkDemand, ReconcileSubmittedOrders,
                               SweepAcknowledgementDeadlines, RejectOverdueOrders,
                               ConfirmNetworkOrderShipment, RecomputeCapabilityOffers
  adapters/inbound/http/       REST (+ the reports router)
  adapters/inbound/poller/     the inbound leg
  adapters/inbound/mcp/        MCP tools, resource, prompt
  adapters/inbound/kafka/      analytics consumer (own topic)
  adapters/kafka/cloudevents/  the CloudEvents 1.0 envelope
  adapters/outbound/network/   NETWORK_MODE switch + stub gateway + seed file
  adapters/outbound/ordermanagement/  held-order planner (order-management REST) + breaker
  adapters/outbound/inventoryclient/  inventory-storage usable-quantity REST client
  adapters/outbound/processpathcache/ process-path-management Kafka cache
  adapters/outbound/pathcapacitycache/ wes-work-planning Kafka cache
  adapters/outbound/kafka/     integration + analytics publishers
  adapters/outbound/postgres/  repositories, outbox, unit of work, migrations runner
  adapters/outbound/memory/    in-memory repositories + product translation file
  adapters/outbound/analyticsstore/  analytical projection + report store
  architecture/                arch-go hexagonal + fleet fitness tests
migrations/                    OLTP schema (+ migrations/analytics/)
apis/openapi.yaml              REST contract (Spectral-linted in CI)
apis/asyncapi.yaml             event catalogue
web/                           Module Federation remote
charts/network-fulfillment/    Helm chart (+ Python wiring tests)
docs/adr/                      ADRs (index: docs/adr/README.md)
docs/ddd/                      ddd-crew DDD artifact pack
```

`NETWORK_MODE=live|stub` gates every outbound call to the network.
It defaults to `stub`, and any unrecognised value is also treated as `stub`.
The kind cluster, `e2e-tests` and CI run in `stub` and never need a
credential. (ADR 0009 §4 removed the earlier `sandbox` tier.)

## Configuration

`cmd/netfulfil`:

| Env var | Default | Meaning |
| --- | --- | --- |
| `NETWORK_MODE` | `stub` | `live` currently refuses to boot (not implemented) |
| `NETWORK_BASE_URL` | unset | live-mode target (ADR 0009 §4); required when `NETWORK_MODE=live` |
| `ORDER_MANAGEMENT_URL` | `http://localhost:8080` | base URL for the held-order calls |
| `DATABASE_URL` | unset (in-memory) | set = Postgres or refuse to boot |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | direct (non-pooled) connection for migrations only (ADR 0007) |
| `MIGRATIONS_PATH` | `/app/migrations` | override for a local run from the repo root |
| `PRODUCT_TRANSLATION_FILE` | unset | `{"products":[{"networkProductId","sku"}]}` |
| `NETWORK_SEED_FILE` | unset | stub demand, `{"demands":[...]}`; stub mode only |
| `POLL_INTERVAL` | `1m` | Go duration; poller and reconciliation cadence |
| `SWEEP_INTERVAL` | `1m` | Go duration; sweep and overdue-rejection cadence |
| `PORT` | `8080` | HTTP listen port |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5188` | comma-separated; GET/OPTIONS only |
| `EVENT_PUBLISHER` | `log` (default) | `kafka` publishes to both topics; with `DATABASE_URL` set this activates the transactional outbox (ADR 0003) instead of publishing directly |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list (publishers and capability-offer caches) |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Go duration; sleep between empty outbox relay passes (outbox mode only) |
| `CAPABILITY_OFFER_ENABLED` | `false` | `true` wires the two Kafka caches, the inventory client, the recompute ticker and `GET /capability-offers`; boot blocks until both caches replay |
| `SITE_ID` | `site-1` | the single site capability offers are computed for |
| `RECOMPUTE_INTERVAL` | `1m` | Go duration; capability-offer recompute cadence |
| `INVENTORY_STORAGE_URL` | `http://localhost:8080` | base URL for `GET /inventory/{sku}/usable` |

`cmd/mcp`: `MCP_ADDR` (`:8090`), `DATABASE_URL`, `MIGRATIONS_DATABASE_URL`,
`MIGRATIONS_PATH`, `REPORTS_BASE_URL` (unset = no report tool).
`cmd/netfulfil-projector`: `ANALYTICS_DATABASE_URL` (required),
`ADMIN_ADDR` (`:8091`), `KAFKA_BROKERS`, `ANALYTICS_MIGRATIONS_PATH`
(`migrations/analytics`), `LOG_LEVEL`. `cmd/netfulfil-reports`:
`ANALYTICS_DATABASE_URL` (required), `HTTP_ADDR` (`:8092`), `LOG_LEVEL`.

## Local quality gate

```bash
make check-fast   # fmt-check vet arch-test  (run before saying "done")
make check        # fmt-check vet build lint test  (what lefthook pre-push runs)
make check-all    # check + coverage (90% gate) + arch-test + bdd
make integration  # testcontainers Postgres and Kafka
make mutation     # gremlins on ./internal/domain/networkorder
make vuln         # govulncheck
make guide-lint   # agent guides: references resolve, context budget
```

CI (`.github/workflows/ci.yml`) runs `lint`, `guide-lint`, `complexity`,
`test`, `integration`, `api-lint`, `mutation-fast`, `vuln`, `helm-lint` and
`arch-test` on every PR into `develop`/`main`; `trivy-scan` only on PRs into
`main`; `docker-publish` and `release` on push to `main`. See
[docs/operations-notes.md](docs/operations-notes.md).

## Related decisions elsewhere in the fleet

| Repo | ADR | Why it matters here |
| --- | --- | --- |
| `order-management` | 0020 | The companion: `releaseOnAllocation` hold, `PromisePolicy.FeasibleBy`, `Network` promise basis |
| `order-management` | 0014 | The promise is a CPT window derived from fulfillment capability |
| `order-management` | 0004 | Release is the cancellation boundary — why network demand must be held, not optimistically released |
| `order-management` | 0017 | Per-shipment-group promising — the rule network demand must *not* use |
| `process-path-management` | 0010 | The fulfillment capability contract this context advertises against |
| `fulfillment-execution` | 0025 | The sweep pattern the acknowledgement-deadline sweep mirrors |
| `retail-network` | 0001 | The in-ecosystem counterpart (draft in `docs/planning/`) |

## Git workflow

GitFlow: `feature/*` branches off `develop`, PR into `develop`
(`gh pr create --base develop`); `develop` promotes to `main` for release,
and a push to `main` publishes the image, the Helm chart and a `vX.Y.Z`
GitHub Release.
