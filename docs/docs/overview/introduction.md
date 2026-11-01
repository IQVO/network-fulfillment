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

- **Subdomain**: Supporting ([why](/docs/ddd/subdomain-classification)).
- **Tier**: `wes`. Every event it publishes is typed
  `com.warehouse.wes.network-fulfillment.networkorder.<Event>`.
- **Counterpart**: the fleet's own `retail-network` service plays the network
  (ADR 0009). `NETWORK_MODE` picks the gateway: the in-process stub (the
  default) or, with `live`, the demand loop against the network's Vendor API
  (ADR 0026; the supply loop is ADR 0027, not built).
- **Status**: implemented; the stub is the default everywhere and `live` is
  opt-in. A study project, not a production system (see the repository README).

## What it owns

- **`NetworkOrder`**: one unit of network demand and the single answer given
  to it. `NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED`, or `REJECTED` from
  `NEW` or `SUBMITTED`. Acknowledged in full or rejected in full, within 24 h
  of receipt.
- **The 24 h acknowledgement clock**: the fleet's only external SLA. A sweep
  reports overdue orders and a separate job rejects them. A third pass is the
  backstop for the `order-management` hold each order raises: it cancels a hold
  nobody decided once the window plus a grace has passed (ADR 0024).
- **`CapabilityOffer`** (always on): the quantity per SKU and site this context
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
| Receive demand | poll the network every `POLL_INTERVAL`, translate, raise a held order in order-management, answer | [use cases](/docs/ddd/use-cases#receivenetworkdemand) |
| Settle answers | reconcile each `SUBMITTED` order with the network's transaction status, then release or cancel the hold | [use cases](/docs/ddd/use-cases#reconcilesubmittedorders) |
| Guard the SLA | report and reject orders still unanswered after 24 h | [use cases](/docs/ddd/use-cases#sweepacknowledgementdeadlines) |
| Free orphaned holds | cancel the order-management hold of an order nobody decided, after `acknowledgeBy + ORPHAN_HOLD_GRACE` | [use cases](/docs/ddd/use-cases#sweeporphanedholds), [runbook](/docs/operations/runbook#a-held-order-is-stuck-in-order-management) |
| Confirm shipment | `POST /network-orders/{networkRef}/shipment-confirmation` | [use cases](/docs/ddd/use-cases#confirmnetworkordershipment) |
| Advertise capability | recompute offers from inventory-storage stock and path capacity | [use cases](/docs/ddd/use-cases#recomputecapabilityoffers) |
| Publish facts | seven CloudEvents types on `warehouse.network-fulfillment.events` | [integration](/docs/ecosystem/integration#events-published) |
| Report | acknowledgement and translation report from an analytics data product | [runbook](/docs/operations/runbook#reports-api-netfulfil-reports) |
| Answer agents | read-only MCP tools | [MCP tools](/docs/mcp/tools) |

## Binaries

`cmd/netfulfil` (REST API and every background job), `cmd/mcp` (MCP server),
`cmd/netfulfil-projector` (analytics writer) and `cmd/netfulfil-reports`
(analytics reader), plus the `web/` remote for `warehouse-console`. See
[architecture](/docs/overview/architecture).

## Documentation map

**Overview**

- [Architecture](/docs/overview/architecture): binaries, hexagonal layout, data stores, diagram
- [Quickstart](/docs/overview/quickstart): build, test, run locally, first calls

**Operations**

- [Configuration](/docs/operations/configuration): every env var per binary
- [Runbook](/docs/operations/runbook): deployment, probes, migrations, Kafka, outbox, scaling, procedures, reports API
- [Observability](/docs/operations/observability): metrics, traces, logs, alerts
- [Troubleshooting](/docs/operations/troubleshooting): symptom, cause, check, fix

**Development**

- [Testing and CI](/docs/development/testing)

**Integration and interfaces**

- [Integration](/docs/ecosystem/integration): every upstream and downstream
- [MCP tools](/docs/mcp/tools)
- REST contract: `apis/openapi.yaml`; event catalogue: `apis/asyncapi.yaml`

**Domain**

- [Use cases](/docs/ddd/use-cases)
- [Subdomain classification](/docs/ddd/subdomain-classification)
- [DDD artifact pack](/docs/ddd/ddd-artifacts): canvases, context map, EventStorming, diagrams, glossary, domain events

**Decisions**

- [ADR index](https://github.com/IQVO/network-fulfillment/blob/develop/docs/adr/README.md)
