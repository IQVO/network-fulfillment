---
id: introduction
title: Introduction
sidebar_label: Introduction
---

# network-fulfillment

network-fulfillment is the bounded context that lets the warehouse fleet take
orders from an external retail fulfillment network and answer them. It is the
Anti-Corruption Layer between that network and everything else in
`warehouse-systems`: the network's vocabulary (purchase orders, product ids,
acknowledgement codes) stops here, and the fleet only ever sees SKUs,
quantities, a site and a deadline.

- **Subdomain**: Supporting ([why](../ddd/subdomain-classification.md)).
- **Tier**: `wes`. Every event it publishes is typed
  `com.warehouse.wes.network-fulfillment.networkorder.<Event>`.
- **Counterpart**: the fleet's own `retail-network` service plays the network
  (ADR 0009). Only an in-process stub gateway exists on `develop`;
  `NETWORK_MODE=live` refuses to boot.
- **Status**: implemented, stub only. A study project, not a production
  system (see the repository README).

## What it owns

- **`NetworkOrder`**: one unit of network demand and the single answer given
  to it. `NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED`, or `REJECTED` from
  `NEW` or `SUBMITTED`. Acknowledged in full or rejected in full, within 24 h
  of receipt.
- **The 24 h acknowledgement clock**: the fleet's only external SLA. A sweep
  reports overdue orders and a separate job rejects them.
- **`CapabilityOffer`** (opt-in): the quantity per SKU and site this context
  would advertise, `min(physicalAvailable, throughputFeasible)`, so the
  network is never offered more than the building can move before the next
  cutoff. Computed and stored, not yet sent to the network.
- **The product dictionary**: network product id to SKU, loaded from
  `PRODUCT_TRANSLATION_FILE`. An order with one unknown product is rejected.

It does **not** own the promise math: whether a deadline is feasible is asked
of `order-management` through a held order (ADR 0001 §7).

## Main capabilities

| Capability | How | Page |
| --- | --- | --- |
| Receive demand | poll the network every `POLL_INTERVAL`, translate, raise a held order in order-management, answer | [use cases](../ddd/use-cases.md#receivenetworkdemand) |
| Settle answers | reconcile each `SUBMITTED` order with the network's transaction status, then release or cancel the hold | [use cases](../ddd/use-cases.md#reconcilesubmittedorders) |
| Guard the SLA | report and reject orders still unanswered after 24 h | [use cases](../ddd/use-cases.md#sweepacknowledgementdeadlines) |
| Confirm shipment | `POST /network-orders/{networkRef}/shipment-confirmation` | [use cases](../ddd/use-cases.md#confirmnetworkordershipment) |
| Advertise capability | recompute offers from inventory-storage stock and path capacity | [use cases](../ddd/use-cases.md#recomputecapabilityoffers) |
| Publish facts | six CloudEvents types on `warehouse.network-fulfillment.events` | [integration](../ecosystem/integration.md#events-published) |
| Report | acknowledgement and translation report from an analytics data product | [runbook](../operations/runbook.md#reports-api-netfulfil-reports) |
| Answer agents | read-only MCP tools | [MCP tools](../mcp/tools.md) |

## Binaries

`cmd/netfulfil` (REST API and every background job), `cmd/mcp` (MCP server),
`cmd/netfulfil-projector` (analytics writer) and `cmd/netfulfil-reports`
(analytics reader), plus the `web/` remote for `warehouse-console`. See
[architecture](architecture.md).

## Documentation map

**Overview**

- [Architecture](architecture.md): binaries, hexagonal layout, data stores, diagram
- [Quickstart](quickstart.md): build, test, run locally, first calls

**Operations**

- [Configuration](../operations/configuration.md): every env var per binary
- [Runbook](../operations/runbook.md): deployment, probes, migrations, Kafka, outbox, scaling, procedures, reports API
- [Observability](../operations/observability.md): metrics, traces, logs, alerts
- [Troubleshooting](../operations/troubleshooting.md): symptom, cause, check, fix

**Development**

- [Testing and CI](../development/testing.md)

**Integration and interfaces**

- [Integration](../ecosystem/integration.md): every upstream and downstream
- [MCP tools](../mcp/tools.md)
- REST contract: `apis/openapi.yaml`; event catalogue: `apis/asyncapi.yaml`

**Domain**

- [Use cases](../ddd/use-cases.md)
- [Subdomain classification](../ddd/subdomain-classification.md)
- [DDD artifact pack](../ddd/README.md): canvases, context map, EventStorming, diagrams, glossary, domain events

**Decisions**

- [ADR index](../adr/README.md)
