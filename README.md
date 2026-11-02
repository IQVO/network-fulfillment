# Network Fulfillment

[![CI](https://github.com/IQVO/network-fulfillment/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/IQVO/network-fulfillment/actions/workflows/ci.yml?query=branch%3Adevelop) [![coverage gate](https://img.shields.io/badge/coverage%20gate-90%25-brightgreen)](https://github.com/IQVO/network-fulfillment/blob/develop/Makefile) [![Go](https://img.shields.io/badge/go-1.27.3-00ADD8?logo=go&logoColor=white)](https://github.com/IQVO/network-fulfillment/blob/develop/go.mod) [![release](https://img.shields.io/github/v/release/IQVO/network-fulfillment?sort=semver)](https://github.com/IQVO/network-fulfillment/releases) [![docs](https://img.shields.io/badge/docs-iqvo.github.io-blue)](https://iqvo.github.io/network-fulfillment/) [![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/IQVO/network-fulfillment/blob/develop/LICENSE)

The anti-corruption layer between the `warehouse-systems` fleet and an
external retail fulfillment network — a **Supporting Subdomain** bounded
context that is **Conformist** to the network upstream and an
**Anti-Corruption Layer** for everything downstream of it in this fleet.
Since ADR 0009 the counterpart is the fleet's own `retail-network` service
playing the network's role, not a real retailer's API.

## Disclaimer

This project is a domain-driven design study. It is not associated with, endorsed by or affiliated with any company, and it uses no company's code, data, logos or trademarks. All names, figures and examples are illustrative or synthetic.

---

## Documentation

The full documentation site is **<https://iqvo.github.io/network-fulfillment/>**. It is built from
`docs/` in this repository (Docusaurus; `cd docs && npm ci && npm run build`) and written from the
code, the two API contracts and the migrations. The aggregator's pack for this context, with the
fleet-wide network pages, is on
[warehouse-docs](https://iqvo.github.io/warehouse-docs/contexts/network-fulfillment); where the two
differ, this site and the code win.

| Area | Pages |
|---|---|
| Overview | [Introduction](https://iqvo.github.io/network-fulfillment/docs/overview/introduction), [Architecture](https://iqvo.github.io/network-fulfillment/docs/overview/architecture), [Quickstart](https://iqvo.github.io/network-fulfillment/docs/overview/quickstart) |
| Operations | [Configuration](https://iqvo.github.io/network-fulfillment/docs/operations/configuration), [Runbook](https://iqvo.github.io/network-fulfillment/docs/operations/runbook), [Observability](https://iqvo.github.io/network-fulfillment/docs/operations/observability), [Troubleshooting](https://iqvo.github.io/network-fulfillment/docs/operations/troubleshooting) |
| Development | [Testing and CI](https://iqvo.github.io/network-fulfillment/docs/development/testing) |
| Domain-driven design | [Use cases](https://iqvo.github.io/network-fulfillment/docs/ddd/use-cases), [Subdomain classification](https://iqvo.github.io/network-fulfillment/docs/ddd/subdomain-classification), [Ubiquitous language](https://iqvo.github.io/network-fulfillment/docs/ddd/ubiquitous-language), [DDD artifacts index](https://iqvo.github.io/network-fulfillment/docs/ddd/ddd-artifacts) |
| API reference | [Overview](https://iqvo.github.io/network-fulfillment/docs/api-reference/overview) (REST, events, MCP, A2A), [Event catalogue](https://iqvo.github.io/network-fulfillment/docs/api-reference/events) |
| Ecosystem | [Integration](https://iqvo.github.io/network-fulfillment/docs/ecosystem/integration), [MCP tools](https://iqvo.github.io/network-fulfillment/docs/mcp/tools) |
| Decisions | [ADR index](https://iqvo.github.io/network-fulfillment/docs/adr), in this repository [`docs/adr/`](docs/adr/index.md) |

Repository-local sources of truth: [`apis/openapi.yaml`](apis/openapi.yaml),
[`apis/asyncapi.yaml`](apis/asyncapi.yaml) and [`docs/adr/`](docs/adr/index.md).

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

## Status

**Implemented, stub-only.** `develop` is promoted to `main`, and `main`
has tagged releases (`v0.1.0` on 2026-09-26 through `v0.1.4` on
2026-10-03; `develop` may be ahead of the latest tag). ADR 0001 is
`Accepted` (2026-09-23), together with its companion `order-management`
ADR 0020 — neither is meaningful without the other. ADR 0009 (Accepted)
amends it.

📄 [ADR 0001 — Network Fulfillment as a bounded context](docs/adr/0001-network-fulfillment-bounded-context.md)
· [ADR index](docs/adr/index.md)
· [DDD artifact pack (ddd-crew)](docs/docs/ddd/ddd-artifacts.md)

What exists today:

- **`NetworkOrder` aggregate** (`internal/domain/networkorder`) —
  `NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED`, and `REJECTED` from
  `NEW` or `SUBMITTED`; one answer per order, acknowledged in full or
  rejected in full, a 24h `acknowledgeBy` deadline fixed at receipt, and a
  persisted one-to-one `localOrderId` link to the `order-management` hold.
  The hold is recorded on the order the moment it is raised, while the order
  is still `NEW` (`RecordHold`, ADR 0024), and carries an outcome
  (`CANCELLED` or `ALREADY_RELEASED`, unset while the hold is open). REST and
  MCP still show `localOrderId` only from `SUBMITTED` on.
- **`CapabilityOffer` aggregate** (`internal/domain/capabilityoffer`, ADR
  0001 §8) — the advertisable quantity per `(sku, siteId)`:
  `min(physicalAvailable, throughputFeasible)` with a `PHYSICAL` /
  `THROUGHPUT_CONSTRAINED` basis, never more than physical stock. Always
  computed: Postgres and Kafka are the only runtime (ADR 0025).
- **Inbound leg** — a poller (`internal/adapters/inbound/poller`) calls the
  network gateway every `POLL_INTERVAL` (default `1m`) and feeds each unit of
  demand to `ReceiveNetworkDemand`. Its watermark advances only on a fully
  successful pass, and receipt is idempotent on `networkRef`.
- **`ReceiveNetworkDemand`** — translates network product ids to SKUs (one
  unknown product rejects the whole order), saves the order `NEW` and
  publishes `NetworkOrderReceived`, raises a **held** order in
  `order-management` (`POST /orders` with `releaseOnAllocation: false`,
  `allowPartialShipment: false`, `requiredShipBy`) and records the returned
  order id on the order at once, treats a returned
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
  cancels the order-management hold (recorded since the raise, so this now
  fires for a `NEW` order) and rejects every overdue `NEW` order
  (`ACKNOWLEDGEMENT_DEADLINE_MISSED`). It never acknowledges late.
- **`SweepOrphanedHolds`** (ADR 0024, gap OM-0020) — on its own ticker (same
  `SWEEP_INTERVAL`, on by default, `ORPHAN_HOLD_SWEEP_ENABLED`), the backstop
  for a hold nobody decided: for a `NEW` or `REJECTED` order with a recorded,
  unsettled hold, once `now > acknowledgeBy + ORPHAN_HOLD_GRACE` (default
  `1h`) it calls `DELETE /orders/{id}`. A 2xx or 404 records the outcome and
  publishes `OrphanedHoldCancelled`; a 409 or 422 (already released) is
  recorded without an event; a 5xx, timeout or open breaker is retried on the
  next pass. It never changes the order's state. Counter
  `network_fulfillment.orphaned_holds.swept`. Runbook:
  [A held order is stuck in order-management](docs/docs/operations/runbook.md#a-held-order-is-stuck-in-order-management).
- **`ConfirmNetworkOrderShipment`** (ADR 0014, ADR 0026) — the one write endpoint,
  `POST /network-orders/{networkRef}/shipment-confirmation`: requests the label
  from the gateway first (one package), then `ACKNOWLEDGED -> CONFIRMED`,
  publishes `NetworkOrderShipmentConfirmed`, then tells the network. A label
  failure leaves the order `ACKNOWLEDGED` and retryable. Idempotent on an
  already-`CONFIRMED` order.
- **`RecomputeCapabilityOffers`** — every `RECOMPUTE_INTERVAL` (default
  `1m`), for every SKU in the
  translation dictionary at `SITE_ID`: physical stock from
  `inventory-storage` (`GET /inventory/{sku}/usable`, synchronous REST) and
  remaining path capacity before the site's next CPT from two Kafka-fed
  caches (`warehouse.process-path-management.events`,
  `warehouse.work-planning.events`). The offer is persisted for the read
  surface; it is **not** yet submitted to the network.
- **Persistence** — Postgres (`migrations/0001`–`0009`: `network_orders`,
  `network_order_lines`, `outbox_events`, `capability_offers`, the hold
  outcome columns of `network_orders`, the live adapter's five tables,
  below, and the inbound loop's tables, ADR 0028). `DATABASE_URL` is mandatory: there
  is no in-memory mode (ADR 0025), and a Postgres that cannot be reached
  fails the boot. Migrations run at startup
  (`MIGRATIONS_DATABASE_URL` for a direct, non-PgBouncer connection, ADR
  0007).
- **Live retail-network adapter: the demand loop** (ADR 0026,
  `internal/adapters/outbound/retailnetwork`). `NETWORK_MODE=live` calls the
  retail network's Vendor API: polls its purchase orders, answers each once
  (accept, or reject with a reason), reconciles the answer by a persisted
  transaction id, requests the label and confirms the shipment, every write with
  a deterministic idempotency key, bounded retries, `429` backoff and one circuit
  breaker (`circuit_breaker_state{dependency="retail-network"}` on `/metrics`).
  It keeps the network's identifiers out of the domain and persists the meeting
  point in migration 0006: `network_nodes` (site code to node id),
  `network_purchase_orders` and `network_purchase_order_lines` (minted
  `networkRef` to purchase order number, listing and ordered quantity),
  `network_submissions` (the ledger: idempotency key, exact request body,
  transaction id and status per acknowledgement, shipment confirmation and node
  registration) and `network_labels`. The tables stay empty in stub mode.
  Environment (the full table is in
  [configuration](docs/docs/operations/configuration.md)): `NETWORK_MODE` (default
  `stub`; anything but `live` is `stub`); `NETWORK_BASE_URL` and
  `NETWORK_VENDOR_ID`, required only in live mode (one boot error names every
  missing one); `NETWORK_NODES_FILE`, optional and live only (node definitions
  to register, strict JSON); `NETWORK_SEED_FILE`, stub only. No credential and
  no timeout variable exist. A purchase order whose node is not mapped is
  skipped with a warning (ADR 0026, As built 1). Operations:
  [live adapter runbook](docs/docs/operations/runbook.md#the-live-retail-network-adapter).
  Mode `stub` stays the default everywhere, including the cluster.
- **Live retail-network adapter: the node profile** (ADR 0027 part 1). In live
  mode a worker derives each site's operational profile (cutoffs, handling time,
  throughput) only from the local caches (the CPT schedule and cycle-time p95 of
  process-path-management, the remaining capacity of wes-work-planning) and
  declares it with `PUT /vendor/nodes/{nodeId}/profile` under a `profileRevision`
  this service owns: persisted before the call (migration 0010,
  `network_node_profiles`), bumped only when the content changes and never
  lowered. A profile the caches cannot support is skipped with a logged reason,
  never guessed. `NETWORK_PROFILE_INTERVAL` (default `5m`, `0` disables) sets the
  cadence; metrics `network_fulfillment.node_profile.declarations` and
  `.revision`. Operations:
  [node profile runbook](docs/docs/operations/runbook.md#the-node-profile).
- **Live retail-network adapter: the inventory feed** (ADR 0027 part 2). In live
  mode a second worker tells the network what each site can ship with
  `POST /vendor/inventory`: one feed per site per pass, built from the
  `CapabilityOffer`s the last recompute pass persisted (the advertised quantity, with
  `PHYSICAL` or `THROUGHPUT_CONSTRAINED` as its basis; an unknown figure is never
  sent), under the listing each SKU has at that site in the product dictionary. The
  feed is complete only when every listed SKU has a fresh offer (a complete feed
  zeroes what it omits), is split above 5000 lines, is sent when its content
  changed and, unchanged, repeated at most hourly. A listing the network refuses as
  not on the node is left out of the feeds for 24 h and logged at `ERROR`.
  Persisted in migration 0011 (`network_inventory_feeds`,
  `network_rejected_listings` and the ledger kind `INVENTORY`).
  `NETWORK_INVENTORY_INTERVAL` (default `5m`, `0` disables) sets the cadence.
  Operations: [inventory feed runbook](docs/docs/operations/runbook.md#the-inventory-feed).
- **Curated ACL files** — `PRODUCT_TRANSLATION_FILE` (network product id ->
  SKU, with an optional `site` per entry because a listing is one item at one
  node; without the file every order is rejected as untranslatable, and a warning
  is logged at startup; ADR 0013, ADR 0027 §10), `NETWORK_SEED_FILE` (stub demand; refuses
  to boot against the live gateway; ADR 0012) and `NETWORK_NODES_FILE` (live
  only: the nodes to register on the network, ADR 0026). The Helm chart renders
  them from `productTranslation.mappings`, `stubDemand.demands` and
  `networkNodes.nodes`.
- **Resilience** (ADR 0004) — one circuit breaker guards every
  `order-management` call, the analytics consumer retries in-process and
  dead-letters to `<topic>.dlq`, and shutdown flips `/readyz` first, then
  drains HTTP, the outbox relay and the poller.
- **Boot resilience** (ADR 0011) — the migration run, the database ping and
  the capability-offer cache dials are retried with backoff (5 attempts, 15 s
  of waits in total) to survive this cluster's first-dial connection reset, and the chart has
  a `startupProbe`. After the budget the service still refuses to boot.
- **Packaging** — `Dockerfile`, `web/` (a Module Federation remote, ADR
  0010) and `charts/network-fulfillment/` (API, MCP, A2A, projector, reports and
  frontend Deployments, HPA per ADR 0006). The chart's Python tests live in
  `charts/network-fulfillment/tests/`. It is deployed to the kind cluster by
  `warehouse-infra`, and Kong serves it at `/api/network-fulfillment`.
- **Inbound loop: seller shipments in, receipts out** (ADR 0028). In live mode a
  ticker (`INBOUND_INTAKE_INTERVAL`) polls the Vendor API's inbound shipments of
  every owned site from a persisted cursor and registers each one as an ASN in
  `inbound-receiving` (`POST /asns`): the SSCC/pallet/carton tree becomes
  `Asn.cartons`, the lines are the per-SKU roll-up in eaches, `ownerRef` is the
  seller, the ASN number is derived from the shipment reference (a replay is a
  no-op), and GTINs become SKUs through `product-master` `GET /gtins/{gtin}`
  (cached in Postgres; an unknown GTIN leaves the shipment `PENDING` with a
  recorded error and a backoff, never a guess). State: migration 0008,
  `inbound_shipments`, `inbound_poll_cursors`, `gtin_sku_cache`. Environment:
  `INBOUND_RECEIVING_URL` and `PRODUCT_MASTER_URL` (required in live mode),
  `INBOUND_INTAKE_INTERVAL` (default `30s`). Stub mode runs no inbound loop.
  **The receipt return** (migration 0009): a consumer group of its own
  (`INBOUND_RECEIPT_CONSUMER_GROUP`, required in live mode) reads inbound-receiving's
  `ReceiptCartonReceived`, `CartonDiscrepancyRaised` and `ReceiptClosed` into a local
  projection (only the ASNs this service registered); on close it reports the receipt through
  `POST /vendor/inbound-receipts` exactly once per shipment: per carton `RECEIVED`, `MISSING`,
  `UNEXPECTED` or `DAMAGED`, the counted eaches per GTIN and compliance codes from the carton
  discrepancies, retried with backoff and the transaction id kept in the ledger.

Not built yet:

- **A `PackageManifested`-driven shipment confirmation.** ADR 0014 chose
  the explicit endpoint until a persisted correlation exists.
- **A real package count.** The label is requested for exactly one package.
- **Reconciling a shipment confirmation.** The network's `FAILURE` of one is
  logged at `ERROR` and recorded in `network_submissions`; no use case acts on it.

## Architecture

```
NetworkOrder      networkRef, siteId, requiredShipBy, acknowledgeBy, lines,
                  localOrderId, holdOutcome, holdSettledAt, receivedAt
                  NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED
                  NEW | SUBMITTED -> REJECTED
                  invariant: acknowledged in full or rejected in full
                  localOrderId = the order-management hold, recorded at raise;
                  holdOutcome = CANCELLED | ALREADY_RELEASED, unset while open

CapabilityOffer   (sku, siteId) -> advertisedQuantity + basis + computedAt
                  PHYSICAL | THROUGHPUT_CONSTRAINED
                  invariant: 0 <= advertisedQuantity <= physicalAvailable
```

```
cmd/netfulfil/                 OLTP composition root (REST, poller, tickers, outbox relay)
cmd/mcp/                       MCP server (Streamable HTTP, read-only)
cmd/a2a/                       A2A agent (JSON-RPC over HTTP, Agent Card at /.well-known/agent-card.json, read-only)
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
                               InventoryAvailability, EventPublisher, UnitOfWork, Clock,
                               OrphanHoldMetrics
  application/usecases/        ReceiveNetworkDemand, ReconcileSubmittedOrders,
                               SweepAcknowledgementDeadlines, RejectOverdueOrders,
                               SweepOrphanedHolds,
                               ConfirmNetworkOrderShipment, RecomputeCapabilityOffers
  adapters/inbound/http/       REST (+ the reports router)
  adapters/inbound/poller/     the inbound leg
  adapters/inbound/mcp/        MCP tools, resource, prompt
  adapters/inbound/a2a/        A2A skills: network_demand_status, advertised_availability (both read)
  adapters/inbound/kafka/      analytics consumer (own topic)
  adapters/kafka/cloudevents/  the CloudEvents 1.0 envelope
  adapters/outbound/retailnetwork/  NETWORK_MODE switch, stub gateway + seed file, live Vendor API gateway (ADR 0026)
  adapters/outbound/ordermanagement/  held-order planner (order-management REST) + breaker
  adapters/outbound/inventoryclient/  inventory-storage usable-quantity REST client
  adapters/outbound/processpathcache/ process-path-management Kafka cache
  adapters/outbound/pathcapacitycache/ wes-work-planning Kafka cache
  adapters/outbound/kafka/     integration + analytics publishers
  adapters/outbound/postgres/  repositories, outbox, unit of work, migrations runner
  adapters/outbound/memory/    in-memory repositories (test doubles only; ADR 0025)
  adapters/outbound/producttranslation/  product translation file dictionary
  adapters/outbound/analyticsstore/  analytical projection + report store
  architecture/                arch-go hexagonal + fleet fitness tests
migrations/                    OLTP schema (+ migrations/analytics/)
apis/openapi.yaml              REST contract (Spectral-linted in CI)
apis/asyncapi.yaml             event catalogue
web/                           Module Federation remote
charts/network-fulfillment/    Helm chart (+ Python wiring tests)
docs/adr/                      ADRs (index: docs/adr/index.md)
docs/ddd/                      ddd-crew DDD artifact pack
```

`NETWORK_MODE=live|stub` gates every outbound call to the network.
It defaults to `stub`, and any unrecognised value is also treated as `stub`.
The kind cluster, `e2e-tests` and CI run in `stub` and never need a
credential. (ADR 0009 §4 removed the earlier `sandbox` tier.)

The layering is enforced as an executable test, not a convention:
`internal/architecture/architecture_test.go` (arch-go), with sibling fleet
fitness tests (`runtime_no_memory_fitness_test.go`,
`kafka_writer_topic_fitness_test.go`, `kafka_writer_acks_fitness_test.go`,
`kafka_dlq_shutdown_fitness_test.go`, `outbox_relay_cap_fitness_test.go`,
`events_fitness_test.go`, `catalogue_fitness_test.go`,
`mcp_optional_inputs_test.go`, `mcp_read_only_test.go`,
`a2a_fitness_test.go`, `violation_test.go`).

## Integration (Kafka + transactional outbox + CloudEvents)

- **Transactional outbox (ADR 0003)** — every domain event fans out to both the integration topic
  (`warehouse.network-fulfillment.events`) and the analytics topic
  (`warehouse.network-fulfillment.analytics`), keyed by `networkRef` with a
  hash balancer (ADR 0005). Saving the aggregate and enqueuing its event(s)
  for both topics happen in one Postgres transaction (the `outbox_events`
  table), and a background relay, always on, drains it to the broker.
  `KAFKA_BROKERS` is mandatory (ADR 0025); there is no log-only publisher.
- **CloudEvents 1.0, mandatory (ADR 0008)** — every Kafka message on both
  topics is a CloudEvents 1.0 structured-mode event (header
  `content-type: application/cloudevents+json; charset=UTF-8`,
  `source=/warehouse/network-fulfillment`,
  `type=com.warehouse.wes.network-fulfillment.networkorder.<EventName>`,
  `subject=<networkRef>`,
  `dataschema=urn:warehouse:network-fulfillment:<events|analytics>:<EventName>:v<N>`).
  Every consumer here accepts only CloudEvents. Catalogue:
  `apis/asyncapi.yaml`.

Published event types (entity `networkorder`): `NetworkOrderReceived`,
`NetworkOrderSubmitted`, `NetworkOrderAcknowledged` (**v2**: until ADR 0016
the unsuffixed type was published at `SUBMITTED`; because its meaning
changed it is now published as `NetworkOrderAcknowledged.v2` with a `:v2`
dataschema — the v1 type is no longer published), `NetworkOrderRejected`,
`NetworkOrderShipmentConfirmed`, `AcknowledgementDeadlineAtRisk`,
`OrphanedHoldCancelled`:

```
com.warehouse.wes.network-fulfillment.networkorder.<EventName>

com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived
com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged.v2
```

Consumed (Kafka-fed caches and loops; every consumer accepts only
CloudEvents and dead-letters poison to `<topic>.dlq`):

| Topic | Types consumed | Feeds |
| --- | --- | --- |
| `warehouse.process-path-management.events` | `CPTScheduleChanged`, `ProcessPathCreated`, `ProcessPathUpdated`, `ProcessPathDeactivated` | the path/CPT cache under `CapabilityOffer` |
| `warehouse.work-planning.events` | `PathCapacityChanged` | the remaining-capacity cache under `CapabilityOffer` |
| `warehouse.inbound-receiving.events` | `ReceiptCartonReceived`, `CartonDiscrepancyRaised`, `ReceiptClosed` | the inbound receipt return (ADR 0028, live mode; `INBOUND_RECEIPT_CONSUMER_GROUP`) |
| `warehouse.network-fulfillment.analytics` | this service's own analytics twins | `cmd/netfulfil-projector` under `ANALYTICS_*` |

## Running it

Postgres and Kafka are the only runtime (ADR 0025): `DATABASE_URL` and
`KAFKA_BROKERS` are mandatory — there is no in-memory mode — and a missing
variable is a boot error naming it.

```sh
docker compose up -d --wait   # postgres:16 (netfulfil/netfulfil/netfulfil) on :5432
                              # + a real single-node Kafka (KRaft) on :9092
                              # (POSTGRES_HOST_PORT / KAFKA_HOST_PORT pick free ports)
export DATABASE_URL="postgres://netfulfil:netfulfil@localhost:5432/netfulfil?sslmode=disable"
export KAFKA_BROKERS=localhost:9092
export MIGRATIONS_PATH=migrations     # the default /app/migrations assumes the container
go run ./cmd/netfulfil                # migrations run automatically at startup; :8080
```

Stub mode (the default) needs no credential and no sibling: seed demand from
`NETWORK_SEED_FILE` and a product dictionary from
`PRODUCT_TRANSLATION_FILE` (the Helm chart renders both).

### Configuration

`cmd/netfulfil`:

| Env var | Default | Meaning |
| --- | --- | --- |
| `NETWORK_MODE` | `stub` | `live` calls the retail network's Vendor API (ADR 0026); any other value is `stub` |
| `NETWORK_BASE_URL` | unset | Vendor API base URL; required when `NETWORK_MODE=live`, ignored otherwise |
| `NETWORK_VENDOR_ID` | unset | vendor account the service acts as; required when `NETWORK_MODE=live`, ignored otherwise |
| `NETWORK_NODES_FILE` | unset | optional, live only: node definitions to register on the network (strict JSON) |
| `NETWORK_PROFILE_INTERVAL` | `5m` | live only: how often the node profile is derived and, if its content changed, declared (ADR 0027); `0` disables it |
| `NETWORK_INVENTORY_INTERVAL` | `5m` | live only: how often the inventory feed is planned and, if its content changed, sent (ADR 0027 part 2); `0` disables it |
| `INBOUND_RECEIVING_URL` | unset | inbound-receiving base URL (`POST /asns`); required when `NETWORK_MODE=live` (ADR 0028), ignored otherwise |
| `PRODUCT_MASTER_URL` | unset | product-master base URL (`GET /gtins/{gtin}`); required when `NETWORK_MODE=live`, ignored otherwise |
| `INBOUND_INTAKE_INTERVAL` | `30s` | Go duration; cadence of the inbound intake and receipt-return loops (live only) |
| `INBOUND_RECEIPT_CONSUMER_GROUP` | unset | this service's durable consumer group on inbound-receiving's events topic; required when `NETWORK_MODE=live` (ADR 0028), ignored otherwise |
| `ORDER_MANAGEMENT_URL` | `http://localhost:8080` | base URL for the held-order calls |
| `DATABASE_URL` | none | **required**: Postgres DSN; a missing or unreachable database fails the boot (ADR 0025) |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | direct (non-pooled) connection for migrations only (ADR 0007) |
| `MIGRATIONS_PATH` | `/app/migrations` | override for a local run from the repo root |
| `PRODUCT_TRANSLATION_FILE` | unset | `{"products":[{"networkProductId","sku"}]}` |
| `NETWORK_SEED_FILE` | unset | stub demand, `{"demands":[...]}`; stub mode only |
| `POLL_INTERVAL` | `1m` | Go duration; poller and reconciliation cadence |
| `SWEEP_INTERVAL` | `1m` | Go duration; cadence of the deadline sweep, the overdue rejection and the orphaned-hold sweep |
| `ORPHAN_HOLD_SWEEP_ENABLED` | `true` | orphaned-hold sweep (ADR 0024); `false` turns it off (a hold nobody decides then stays reserved in `order-management`); an unparseable value keeps it on |
| `ORPHAN_HOLD_GRACE` | `1h` | Go duration added to `acknowledgeBy` before a hold counts as orphaned; `0` = no grace; malformed or negative uses the default |
| `PORT` | `8080` | HTTP listen port |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5188` | comma-separated; methods GET, POST, OPTIONS |
| `EVENT_PUBLISHER` | unset | only `kafka` (or unset, which means `kafka`) is accepted; anything else fails the boot. Events always go through the transactional outbox (ADR 0003) |
| `KAFKA_BROKERS` | none | **required**: comma-separated broker list (outbox relay and capability-offer caches) |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Go duration; sleep between empty outbox relay passes |
| `OUTBOX_MAX_ATTEMPTS` | `20` | outbox relay attempt cap (ADR 0021): a row that failed this many sends is logged at ERROR and excluded until re-armed (`UPDATE outbox_events SET attempts = 0 WHERE id = ...`); `0` = unlimited |
| `CAPABILITY_OFFER_ENABLED` | unset | only `true` (or unset, which means `true`) is accepted; anything else fails the boot. The two Kafka caches, the inventory client, the recompute ticker and `GET /capability-offers` are always wired; boot blocks until both caches replay |
| `SITE_ID` | `site-1` | the single site capability offers are computed for |
| `RECOMPUTE_INTERVAL` | `1m` | Go duration; capability-offer recompute cadence |
| `INVENTORY_STORAGE_URL` | `http://localhost:8080` | base URL for `GET /inventory/{sku}/usable` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTel Collector (OTLP/gRPC); see [Observability](#observability) |
| `OTEL_SERVICE_NAME` | `network-fulfillment` | `service.name` resource attribute |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`/`warning`, `error` (same in all four binaries) |

`cmd/mcp`: `MCP_ADDR` (`:8090`), `DATABASE_URL` (required), `MIGRATIONS_DATABASE_URL`,
`MIGRATIONS_PATH`, `REPORTS_BASE_URL` (unset = no report tool), plus the
OTel vars (`OTEL_SERVICE_NAME` defaults to `network-fulfillment-mcp`),
`LOG_LEVEL`.
`cmd/a2a`: `A2A_ADDR` (`:8093`), `A2A_AGENT_URL` (required, absolute http(s) URL), `DATABASE_URL`
(required), `MIGRATIONS_DATABASE_URL`, `MIGRATIONS_PATH`, the OTel vars
(`OTEL_SERVICE_NAME` defaults to `network-fulfillment-a2a`), `LOG_LEVEL`.
`cmd/netfulfil-projector`: `ANALYTICS_DATABASE_URL` (required),
`ADMIN_ADDR` (`:8091`), `KAFKA_BROKERS` (required), `ANALYTICS_MIGRATIONS_PATH`
(`migrations/analytics`), the OTel vars (`OTEL_SERVICE_NAME` defaults to
`network-fulfillment-projector`), `LOG_LEVEL`. `cmd/netfulfil-reports`:
`ANALYTICS_DATABASE_URL` (required), `HTTP_ADDR` (`:8092`), the OTel vars
(`OTEL_SERVICE_NAME` defaults to `network-fulfillment-reports`), `LOG_LEVEL`.

## Observability

All five binaries wire OpenTelemetry (ADR 0019) through
`internal/adapters/outbound/telemetry`: a TracerProvider and a MeterProvider
exporting over **OTLP/gRPC**, Go runtime metrics and the W3C trace-context
propagator (trace context crosses the outbox, ADR 0022). Export is
non-blocking: with no Collector listening, telemetry is dropped and the
service serves normally. Business counters follow the fleet standard-metrics
convention (ADR 0020): `network_fulfillment.orphaned_holds.swept`,
`network_fulfillment.node_profile.declarations` and
`network_fulfillment.node_profile.revision`.

The OLTP service additionally serves **Prometheus text exposition** at
`GET /metrics`, carrying the circuit-breaker gauge
`circuit_breaker_state{dependency="..."}` (one breaker guards every
order-management call, one the retail-network adapter; ADR 0004). `GET
/healthz` is liveness; `GET /readyz` is the drain gate plus dependency
checks — a failed dependency answers `503` problem+json naming it. Logs are
one JSON line per event (`slog`, stdout), level from `LOG_LEVEL`.

## API

Errors are **RFC 7807** (`application/problem+json`) throughout. Every route
below is declared at the same path in `apis/openapi.yaml`. Demand itself
still arrives only by polling — there is no create-order endpoint.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` / `/readyz` | liveness / readiness (drain gate + dependencies) |
| `GET` | `/metrics` | Prometheus exposition (the `circuit_breaker_state` gauge) |
| `GET` | `/inbound-status` | network mode, poller counters, unanswered/overdue counts |
| `GET` | `/network-orders` | unanswered orders |
| `GET` | `/network-orders/{networkRef}` | one order (with `localOrderId` from `SUBMITTED` on) |
| `POST` | `/network-orders/{networkRef}/shipment-confirmation` | the one write route: label request, `ACKNOWLEDGED -> CONFIRMED`, `NetworkOrderShipmentConfirmed`, notify the network. Idempotent on an already-`CONFIRMED` order; a label failure leaves the order `ACKNOWLEDGED` and retryable (ADR 0014, ADR 0026) |
| `GET` | `/capability-offers` | the persisted offers (when capability offers are enabled — they always are unless `CAPABILITY_OFFER_ENABLED=false`, which fails the boot) |
| `GET` | `/reports/acknowledgement` | the acknowledgement report (`cmd/netfulfil-reports`, `:8092`) |
| `GET` | `/reports/acknowledgement/freshness` | the report's freshness |

`apis/openapi.yaml` (OpenAPI 3.0.3, Spectral-linted in CI) documents every
route with full schemas and a shared `Problem` component;
`apis/asyncapi.yaml` (AsyncAPI 2.6) specifies both topics and all messages.

### Analytics data product (acknowledgement report)

ADR 0002: `cmd/netfulfil-projector` consumes this service's own analytics
topic into a separate analytical Postgres (`migrations/analytics/`, claimed
and applied in one transaction, ADR 0023), and `cmd/netfulfil-reports`
serves `GET /reports/acknowledgement` and
`GET /reports/acknowledgement/freshness` over a read-only pool:

```sh
export ANALYTICS_DATABASE_URL="postgres://netfulfil:***@localhost:5432/netfulfil_analytics?sslmode=disable"
KAFKA_BROKERS=localhost:9092 go run ./cmd/netfulfil-projector   # :8091, the only writer
ANALYTICS_DATABASE_URL="$ANALYTICS_DATABASE_URL" go run ./cmd/netfulfil-reports   # :8092
curl http://localhost:8092/reports/acknowledgement http://localhost:8092/reports/acknowledgement/freshness
```

## Helm chart

```sh
helm install network-fulfillment ./charts/network-fulfillment \
  --set database.url="postgres://netfulfil:***@postgres:5432/netfulfil?sslmode=disable" \
  --set kafka.brokers="kafka:9092"
```

One chart, six Deployments: the API, plus optional `mcp`, `a2a`, analytics
(`analytics.enabled`: projector + reports) and `frontend` components (all
off by default), with HPA per ADR 0006. Mode `stub` stays the default
everywhere, including the cluster. The curated ACL files are rendered from
`productTranslation.mappings`, `stubDemand.demands` and `networkNodes.nodes`.
An `otel` values block points the binaries at the in-cluster Collector.
Chart wiring is asserted by `charts/network-fulfillment/tests/`
(`render_test.sh` — also renders the three `warehouse-infra-*` values sets —
`test_credential_wiring.py`, `test_data_files.py`, `test_env_parity.py`,
`test_kong_config.py`, `test_service_selectors.py`); `helm-lint` runs
`ct lint` in CI and `chart-tests` runs the Python tests. Deployed to the
kind cluster by `warehouse-infra`; Kong serves it at
`/api/network-fulfillment`.

### Running the MCP server in Kubernetes

`cmd/mcp` (Streamable HTTP on `MCP_ADDR`, default `:8090`, mounted at `/`
and `/mcp`) is a separate, opt-in deployable (`mcp.enabled`): read-only
tools `get_network_order`, `list_network_orders`, `list_capability_offers`,
and `get_acknowledgement_report` (when `REPORTS_BASE_URL` is set); the
`network-order://network-fulfillment/{networkRef}` resource; and the
`answer_network_demand` prompt. Read-only is enforced by a fitness test
(`mcp_read_only_test.go`): no write tool — shipment confirmation stays REST
only. `GET /healthz` backs the probes; there is no authentication layer in
front of it.

### Running the A2A agent in Kubernetes

ADR 0030; `cmd/a2a` (JSON-RPC on `A2A_ADDR`, default `:8093`, mounted at
`/` and `/a2a`, Agent Card at `/.well-known/agent-card.json`,
`GET /healthz`) is a read-only, message-only Agent2Agent agent over the same
read ports as REST and MCP, with two curated skills called as a
`SendMessage` data part `{"skill": "<id>", "arguments": {...}}`:
`network_demand_status` (`{networkRef}` for one order, or `{state}` /
nothing for a list; the two are exclusive) and `advertised_availability`
(the stored capability offers, optionally `{sku, siteId}`). Read-only
because the MCP charter is: no write skill (shipment confirmation stays REST
only). Rejections come back as in-band RFC 7807 `problem`s with the REST
classification (404 / 400 / 500). `A2A_AGENT_URL` (an absolute http(s) URL
the card advertises) is mandatory, `DATABASE_URL` too; no Kafka. Helm:
`a2a.enabled` (default off), Kong at `/api/a2a/network-fulfillment`.
A2A carries delegation only; facts still leave as CloudEvents.

## Operator micro-frontend (`web/`)

`web/` is the `netfulfil_mfe` Module Federation remote (ADR 0010): the
operator screens over this service's REST API. Standalone development:

```sh
cd web && npm ci && npm run dev   # http://localhost:5188 (talks to :8080)
```

Deployed to the kind cluster it is built into a static bundle and served by
its own `nginx-unprivileged` pod at `/mfes/network-fulfillment/`
(`web/Dockerfile`; Vite's `base` is `/mfes/network-fulfillment/`, which is
how the Nginx web gateway routes it). The remote reads its API origin at
runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (the console shell's
`/config.json`); a production build with no runtime config fails loudly —
`web/src/config.ts`. Enable with `frontend.enabled` in the chart.

## Local development / quality gate

```bash
make check-fast   # fmt-check vet arch-test  (run before saying "done")
make check        # fmt-check vet build lint test  (what lefthook pre-push runs)
make check-all    # check + coverage (90% gate) + arch-test + bdd
make bdd          # godog/Gherkin acceptance suite (features/, ADR 0018)
make integration  # testcontainers Postgres and Kafka
make mutation     # gremlins on ./internal/domain/networkorder
make vuln         # govulncheck
make guide-lint   # agent guides: references resolve, context budget
```

Git hooks are managed by [lefthook](https://github.com/evilmartians/lefthook)
(`lefthook.yml`): pre-commit runs `fmt-check` + `vet` + `lint`; pre-push runs
`make check`. Activate once per clone: `brew install lefthook && lefthook
install`.

## Tests

```sh
go test ./... -race                                  # unit (incl. the godog suites: lifecycle, orders, holds, offers, ops, inbound, events, reports, wave 2)
go test -tags=integration ./... -race -count=1       # testcontainers Postgres and Kafka
go test ./features* -run TestFeatures -v             # the godog/Gherkin acceptance suites (ADR 0018)
go test ./internal/architecture/... -v               # arch-go + fleet fitness tests
make coverage                                        # coverage, CI gate >= 90%
```

CI (`.github/workflows/ci.yml`) runs `lint`, `guide-lint`, `complexity`,
`test`, `bdd`, `integration`, `api-lint`, `mutation-fast`, `vuln`,
`helm-lint`, `chart-tests`, `arch-test`, `trivy-scan` (PRs into `main`
only) and `web` on every PR into `develop`/`main`; `drift` and the
exhaustive `mutation` run on schedule/manual dispatch only;
`docker-publish` and `release` on push to `main`. See
[docs/docs/development/testing.md](docs/docs/development/testing.md#ci).

## Architecture Decision Records

The ADR index lives at [docs/adr/index.md](docs/adr/index.md) — 30
decisions, from the bounded context itself through the transactional
outbox, the CloudEvents mandate, the live retail-network adapter
(ADR 0026–0028) to the A2A inbound adapter.

## Related decisions elsewhere in the fleet

| Repo | ADR | Why it matters here |
| --- | --- | --- |
| `order-management` | 0020 | The companion: `releaseOnAllocation` hold, `PromisePolicy.FeasibleBy`, `Network` promise basis. Its accepted deferral ("sweep orphaned holds past the acknowledgement window") is closed here by ADR 0024 |
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

## License

MIT — see [LICENSE](LICENSE).
