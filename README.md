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

## Status

**Implemented on `develop`, stub-only, not yet released.** ADR 0001 is
`Accepted` (2026-09-23), together with its companion `order-management`
ADR 0020 — neither is meaningful without the other. There is no `main`
branch yet.

📄 [ADR 0001 — Network Fulfillment as a bounded context](docs/adr/0001-network-fulfillment-bounded-context.md)

What exists today:

- **`NetworkOrder` aggregate** (`internal/domain/networkorder`) —
  `NEW -> ACKNOWLEDGED | REJECTED`, `ACKNOWLEDGED -> CONFIRMED`; one answer
  per order, acknowledged in full or rejected in full, a 24h
  `acknowledgeBy` deadline fixed at receipt, and a persisted one-to-one
  `localOrderId` link that is only allowed once acknowledged.
- **Inbound leg** — a poller (`internal/adapters/inbound/poller`) calls the
  network gateway every `POLL_INTERVAL` (default `1m`) and feeds each unit of
  demand to `ReceiveNetworkDemand`. Its watermark advances only on a fully
  successful pass, and receipt is idempotent on `networkRef`.
- **`ReceiveNetworkDemand`** — translates network product ids to SKUs (one
  unknown product rejects the whole order), raises a **held** order in
  `order-management` (`POST /orders` with `releaseOnAllocation: false`,
  `allowPartialShipment: false`, `requiredShipBy`), treats a returned
  `promiseDate` as "feasible" and a null one as "not feasible", then
  acknowledges and releases (`POST /orders/{id}/release`) or rejects and
  cancels (`DELETE /orders/{id}`). No promise math is done here.
- **`SweepAcknowledgementDeadlines`** — every `SWEEP_INTERVAL` (default `1m`
  in the binary, `5m` in the chart) rejects any order still `NEW` past its
  deadline and cancels its held local order.
- **Read-only REST** (`apis/openapi.yaml`): `GET /healthz`,
  `GET /inbound-status` (poller counters, unanswered/overdue counts),
  `GET /network-orders` (unanswered orders), `GET /network-orders/{networkRef}`.
  There is deliberately no write endpoint: demand arrives only by polling.
- **Persistence** — Postgres (`migrations/0001_init.*.sql`, one
  `network_orders` table plus its lines) when `DATABASE_URL` is set, and an
  in-memory repository when it is not. If `DATABASE_URL` is set but Postgres
  cannot be reached, the service refuses to boot instead of falling back.
  Migrations run at startup.
- **Curated ACL files** — `PRODUCT_TRANSLATION_FILE` (network product id ->
  SKU; without it every order is rejected as untranslatable, and a warning is
  logged at startup) and `NETWORK_SEED_FILE` (stub demand; refuses to boot
  against a non-stub gateway). The Helm chart renders both from
  `productTranslation.mappings` and `stubDemand.demands`.
- **Boot resilience** — the migration run and the database ping are retried
  with backoff (~31s budget) to survive this cluster's first-dial connection
  reset, and the chart has a `startupProbe`. After the budget the service
  still refuses to boot.
- **Packaging** — `Dockerfile` and `charts/network-fulfillment/`. The chart
  fails to render if `config.networkMode` is `sandbox`/`live` and no
  credentials are provided (checked by `charts/network-fulfillment/tests/`).
  It is deployed to the kind cluster by `warehouse-infra`, and Kong serves it
  at `/api/network-fulfillment`.
- **Kafka + transactional outbox (ADR 0003)** — `EVENT_PUBLISHER=kafka` fans
  every domain event out to both the integration topic
  (`warehouse.network-fulfillment.events`) and the analytics topic
  (`warehouse.network-fulfillment.analytics`). With `DATABASE_URL` unset both
  are published directly (no transaction to bind them to — the in-memory dev
  mode); with `DATABASE_URL` set, saving the aggregate and enqueuing its
  event(s) for both topics happen in one Postgres transaction (the
  `outbox_events` table), and a background relay drains it to the broker.
  The default (`EVENT_PUBLISHER` unset) still publishes to a log-only
  publisher.
- **CloudEvents 1.0, mandatory (ADR 0008)** — every Kafka message on both
  topics is a CloudEvents 1.0 structured-mode event (header
  `content-type: application/cloudevents+json; charset=UTF-8`,
  `source=/warehouse/network-fulfillment`,
  `type=com.warehouse.wes.network-fulfillment.networkorder.<EventName>`,
  `subject=<networkRef>`, `dataschema=urn:warehouse:network-fulfillment:<events|analytics>:<EventName>:v1`).
  The analytics projector accepts only CloudEvents and dead-letters anything
  else. Catalogue: `apis/asyncapi.yaml`.

Not built yet:

- **`NETWORK_MODE=live`**: refuses to boot with "not implemented
  yet". Only the stub gateway exists, so no call to a real network is made.
- **`CapabilityOffer`** / throughput-constrained advertised availability,
  and shipment confirmation. `NetworkOrder.ConfirmShipment` exists in the
  domain, but no use case calls it.
- **Customer PII**: no ship-to data is modelled or stored yet.
- No `web/` remote, no MCP server, no BDD `features/`.

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
                                             cycleTimeP95 + CPTSchedule,
                                             × wes-work-planning remaining
                                               PathCapacity for that (path, CPT)
)
```

400 units in the building but a path that can only carry 120 more before the
18:00 cutoff means advertising 400 sells a promise the floor cannot keep.
Advertising 120 sells capability honestly.

## Boundary

- **Conformist upstream.** The network's vocabulary — purchase orders, line
  sequence numbers, product identifiers, acknowledgement codes, selling
  parties — stops at this context's adapter and is never adopted by the
  fleet.
- **This context holds the fleet's only customer PII** (ship-to name,
  address, phone) and its only external SLA (the 24-hour acknowledgement
  clock). Both are reasons it is a separate context rather than an adapter
  inside `order-management`.
- **Promise math is not duplicated here.** Deadline feasibility is asked of
  `order-management`'s existing `PromisePolicy`, per the companion ADR.

## Shape

```
NetworkOrder      networkRef, siteId, requiredShipBy, acknowledgeBy, lines,
                  localOrderId                                  (implemented)
                  NEW -> ACKNOWLEDGED -> CONFIRMED
                      -> REJECTED
                  invariant: acknowledged in full or rejected in full

CapabilityOffer   (sku, siteId) -> advertisedQuantity + basis     (planned)
                  Physical | ThroughputConstrained
                  invariant: advertisedQuantity <= physicalAvailable
```

```
cmd/netfulfil/                 composition root (the only binary)
internal/
  domain/networkorder/         NetworkOrder aggregate
  domain/shared/               NetworkRef, NetworkLineRef, NetworkProductId, SKU, ...
  application/contract/        InboundDemand, HeldOrderRequest/Result
  application/ports/           NetworkOrderRepo, NetworkGateway, FulfillmentPlanner,
                               ProductTranslation, EventPublisher, Clock
  application/usecases/        ReceiveNetworkDemand, SweepAcknowledgementDeadlines
  adapters/inbound/http/       read-only REST
  adapters/inbound/poller/     the inbound leg
  adapters/outbound/network/   NETWORK_MODE switch + stub gateway + seed file
  adapters/outbound/ordermanagement/  held-order planner (order-management REST)
  adapters/outbound/postgres/  repository + migrations runner
  adapters/outbound/memory/    in-memory repository + product translation file
  architecture/                arch-go hexagonal + fleet fitness tests
migrations/                    Postgres schema
apis/openapi.yaml              REST contract (Spectral-linted in CI)
charts/network-fulfillment/    Helm chart (+ Python wiring tests)
```

`NETWORK_MODE=live|stub` gates every outbound call to the network.
It defaults to `stub`, and any unrecognised value is also treated as `stub`.
The kind cluster, `e2e-tests` and CI run in `stub` and never need a
credential. (ADR 0009 §4 removed the earlier `sandbox` tier.)

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `NETWORK_MODE` | `stub` | `live` currently refuses to boot (not implemented) |
| `NETWORK_BASE_URL` | unset | live-mode target (ADR 0009 §4); only read when `NETWORK_MODE=live` |
| `ORDER_MANAGEMENT_URL` | `http://localhost:8080` | base URL for the held-order calls |
| `DATABASE_URL` | unset (in-memory) | set = Postgres or refuse to boot |
| `MIGRATIONS_PATH` | `/app/migrations` | override for a local run from the repo root |
| `PRODUCT_TRANSLATION_FILE` | unset | `{"products":[{"networkProductId","sku"}]}` |
| `NETWORK_SEED_FILE` | unset | stub demand, `{"demands":[...]}`; stub mode only |
| `POLL_INTERVAL` | `1m` | Go duration |
| `SWEEP_INTERVAL` | `1m` | Go duration |
| `PORT` | `8080` | HTTP listen port |
| `EVENT_PUBLISHER` | `log` (default) | `kafka` publishes to both topics; with `DATABASE_URL` set this activates the transactional outbox (ADR 0003) instead of publishing directly |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list, only read when `EVENT_PUBLISHER=kafka` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Go duration; sleep between empty outbox relay passes (outbox mode only) |

## Local quality gate

```bash
make check        # fmt-check vet build lint test  (what lefthook pre-push runs)
make check-all    # check + coverage (90% gate) + arch-test + bdd
make integration  # testcontainers Postgres
make mutation     # gremlins on ./internal/domain/networkorder
make vuln         # govulncheck
```

CI (`.github/workflows/ci.yml`) runs `lint`, `test`, `integration`,
`api-lint`, `mutation-fast`, `vuln`, `helm-lint` and `arch-test` on every
PR into `develop`/`main`.

## Related decisions elsewhere in the fleet

| Repo | ADR | Why it matters here |
| --- | --- | --- |
| `order-management` | 0020 | The companion: `releaseOnAllocation` hold, `PromisePolicy.FeasibleBy`, `Network` promise basis |
| `order-management` | 0014 | The promise is a CPT window derived from fulfillment capability |
| `order-management` | 0004 | Release is the cancellation boundary — why network demand must be held, not optimistically released |
| `order-management` | 0017 | Per-shipment-group promising — the rule network demand must *not* use |
| `process-path-management` | 0010 | The fulfillment capability contract this context advertises against |
| `fulfillment-execution` | 0025 | The sweep pattern the acknowledgement-deadline sweep mirrors |

## Git workflow

GitFlow: `feature/*` branches off `develop`, PR into `develop`
(`gh pr create --base develop`); `develop` promotes to `main` for release.
This repo has not been released yet, so there is no `main` branch.
