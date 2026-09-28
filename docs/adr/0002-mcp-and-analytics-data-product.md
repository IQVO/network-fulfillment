---
id: 0002-mcp-and-analytics-data-product
slug: /adr/0002-mcp-and-analytics-data-product
title: 2. An MCP server and an analytics data product, on the same terms as the other eight bounded contexts
sidebar_label: 2. MCP server and analytics data product
description: "ADR 0002 — network-fulfillment ships a read-only MCP server (three tools, one resource, one prompt) over its existing use cases, and a separate analytics data product (the 'Network Order Acknowledgement & Translation' report) built from its own domain events on a dedicated Kafka topic, projected into a separate database and served read-only. Both are additive: no change to the OLTP domain or application layers, no write surface, and a consumer-group id unique per process instance."
---

# 2. An MCP server and an analytics data product, on the same terms as the other eight bounded contexts

## Status

**Accepted (2026-09-26).** Companion to [ADR 0001](./0001-network-fulfillment-bounded-context.md);
neither introduces a new domain concept, and both build entirely on facts
that ADR 0001 already established — the four domain events this context
already raises, and the read use cases the OLTP HTTP surface already
serves.

## Context

### Parity with the rest of the fleet is the actual driver

Every other bounded context in `warehouse-systems` — `order-management`,
`inventory-storage`, `wes-work-planning`, `workforce-management`,
`facility-layout`, `process-path-management`, `fulfillment-execution`,
`labor-performance` — already ships two things this context did not yet
have: a read-only MCP server over its use cases, and a per-service
analytics data product built from its own domain events. The fleet's MCP
governance charter treats this as a federated standard, not an optional
extra: one set of global rules (architecture, tool curation, naming,
annotations), each context owning its own server. `fulfillment-execution`
is the charter's reference implementation; this record adopts the same
shape rather than inventing a new one, for the same reason every other
context did — a ninth bespoke pattern would cost more to review and
maintain than reusing the eight that already exist and are already
trusted.

The two decisions are recorded together because they share almost every
mechanical piece: the MCP server's one analytics-backed tool calls the
reports binary this ADR also introduces, and both additions build on the
same substrate — the domain events ADR 0001's companion left as an open
seam and this context's most recent change (the four `NetworkOrder`
events: `NetworkOrderReceived`, `NetworkOrderAcknowledged`,
`NetworkOrderRejected`, `NetworkOrderShipmentConfirmed`) has now filled
in.

### What was already true, and what was missing

This context already has exactly the substrate the rest of the fleet
built its MCP servers and analytics data products on:

- **Past-tense domain events**, already wired into `ReceiveNetworkDemand`
  and `SweepAcknowledgementDeadlines`, and already fanned out to a Kafka
  integration topic (`warehouse.network-fulfillment.events`) when
  `EVENT_PUBLISHER=kafka`.
- **Curated read use cases** — finding one order by network reference,
  listing orders, and (new here) generating a bucketed report — reached
  today only through the OLTP HTTP surface `internal/adapters/inbound/http`
  serves.
- **The dual inbound-adapter pattern** every sibling with an MCP server
  already uses: MCP as an *additional* driving adapter over the same
  application layer, never a second copy of the logic.

What was missing was everything downstream of the events: no analytics
topic, no analytical database, no projector, no read-only reports binary,
and no MCP server exposing any of it — or the two read use cases the OLTP
HTTP API already served — to an AI client.

### The report this context can honestly produce

The one report this context is positioned to answer better than any
other in the fleet is about the boundary it *is*: what the network sent,
and how honestly and quickly this context answered it. Four facts define
that boundary and are directly measurable from the events already raised:

```
ordersReceived                     NetworkOrderReceived
ordersAcknowledged                 NetworkOrderAcknowledged
ordersRejectedUntranslatableSku    NetworkOrderRejected, reason=UNTRANSLATABLE_SKU
ordersRejectedDomain               NetworkOrderRejected, reason=INFEASIBLE_DEADLINE
acknowledgementDeadlinesMissed     NetworkOrderRejected, reason=ACKNOWLEDGEMENT_DEADLINE_MISSED
avgAcknowledgementLatencySeconds   time between NetworkOrderReceived and its answer
```

Splitting rejections by reason is not decoration. `UNTRANSLATABLE_SKU` is
a **catalogue gap** — this platform's own product-translation dictionary
is incomplete, and the fix is an operator adding a mapping.
`INFEASIBLE_DEADLINE` is a **genuine domain refusal** — order-management's
promise policy said the deadline cannot be met, and the fix (if any) is
capacity, not data entry. Collapsing the two into one "rejected" count
would make an operator unable to tell "we are losing sales because our
dictionary is stale" from "we are losing sales because the floor is
saturated" — two problems with completely different owners and
completely different fixes. `acknowledgementDeadlinesMissed` is neither:
it is the sweep's own operational failure signal (ADR 0001 §6), and
keeping it as its own count is what lets an operator see the SLA-miss
rate independent of either rejection reason.

Bucketing by day, rather than serving a single running total, is what
makes the report answer a trend question ("is our average latency
creeping up") rather than only a snapshot one.

## Decision

We add an MCP server and an analytics data product to `network-fulfillment`,
on the same terms as every other bounded context in the fleet.

### 1. Three curated, read-only MCP tools

```
get_network_order            one order by the network's own reference
list_network_orders           every order, optionally filtered by state
get_acknowledgement_report    the day-bucketed report, for a time window
```

Every tool is annotated `ReadOnlyHint: true`, because every one of them
is. This is not a placeholder pending a future write tool: this context's
only inbound leg is a poll against the external network (ADR 0001 §5),
and there is no legitimate way for an MCP client to originate demand,
answer the network, or mutate a `NetworkOrder` — doing so from this
surface would let an AI client silently commit this platform to (or
refuse) a real retailer's purchase order, which is exactly the outcome
ADR 0001 drew this context's boundary to make deliberate rather than
accidental. A tool set this narrow also never carries customer PII:
`get_network_order` returns both product vocabularies and the
acknowledgement state, and stops there — ship-to name, address and phone
stay where ADR 0001 §3 put them, out of reach of this surface entirely.

`get_acknowledgement_report` calls the reports REST service rather than
opening the analytical database itself, so no process touches a datastore
it does not own — the same rule the reports binary itself follows for the
OLTP database.

One resource (`network-order://network-fulfillment/{networkRef}`) and one
prompt (`answer_network_demand`, the operating procedure for using the
three tools together) round out the server, following the governance
charter's naming and curation rules exactly.

### 2. A separate analytics topic, projector, and reports binary

- **`warehouse.network-fulfillment.analytics`** — a second Kafka topic,
  published by a second outbound publisher fanned out alongside the
  existing integration publisher (`fanOutPublisher` in
  `cmd/netfulfil/main.go`), so a single `EVENT_PUBLISHER=kafka` run feeds
  both the integration topic and this one without a second flag. The
  integration topic and its one known consumer are untouched.
- **`cmd/netfulfil-projector`** — the analytics **writer**. Consumes the
  analytics topic and is the only process that writes the analytical
  database. Runs the analytical migrations (`migrations/analytics`) on
  start.
- **`cmd/netfulfil-reports`** — the **read-only reader**. Opens the
  analytical database and serves `GET /reports/acknowledgement` (RFC 7807
  error bodies, matching the rest of this repo's HTTP surface). Never
  writes, never migrates.
- **A separate analytical database**, reached via `ANALYTICS_DATABASE_URL`
  — distinct from the OLTP `DATABASE_URL` the main binary and the MCP
  server share — so a runaway report query or a projection rebuild can
  never contend with `PollDemand` or the acknowledgement sweep.

`internal/analytics/report` depends on nothing internal except itself; the
consumer and store adapters depend on it. The OLTP domain and application
layers are unmodified by this record, and `arch-test`
(`TestHexagonalArchitecture/OLTP_domain_must_not_import_the_analytics_store`,
`.../OLTP_application_must_not_import_the_analytics_store`) enforces that
they stay that way.

### 3. A consumer group id unique per process instance

The projector mints its Kafka consumer group id at process start —
prefix, hostname, PID, and a nanosecond timestamp
(`NewUniqueConsumerGroup`, `internal/adapters/inbound/kafka`) — rather
than reading a fixed group id from configuration.

This fleet has been bitten by the alternative twice already, in two
different services: a shared, fixed consumer group id means a *fresh*
process instance resumes from whatever offset the *previous* instance
already committed, and is marked ready having replayed nothing of its
own. For an at-least-once integration consumer that is merely a missed
message; for an analytics projector whose entire read model is *built
from replaying its own topic*, it is worse — a redeployed projector with
a shared group id would come up healthy holding an empty or stale
analytical table, and nothing about its health check would say so. Every
projector instance in this fleet must therefore be able to replay from
the earliest offset on its own, and a unique-per-process group id is what
makes that the only thing that can happen.

### 4. Deployment is additive and opt-in

`mcp.enabled` and `analytics.enabled` both default to `false` in the Helm
chart, matching the fleet's convention that a new workload never appears
in an existing release without an explicit flag. `app.kubernetes.io/component`
distinguishes the four possible pods (`api`, `mcp`, `analytics-projector`,
`analytics-reports`) so their Services select disjoint pod sets — this
fleet's own recurring Service-selector-collision bug, closed here the same
way every sibling chart closes it.

## Alternatives considered

- **Fold the report into the existing OLTP HTTP surface, no separate
  binary or database.** Rejected for the same reason every sibling
  rejected it: an analytical query load sitting on the transactional
  database can contend with `PollDemand` and the acknowledgement sweep,
  and this context's sweep already has a 24-hour SLA riding on it staying
  fast.
- **One combined `ordersRejected` count instead of splitting by reason.**
  Rejected in §Context above: it would make a catalogue gap
  indistinguishable from a genuine capacity refusal, and the two have
  different owners and different fixes.
- **A fixed, well-known Kafka consumer group id for the projector.**
  Rejected: this is the exact failure mode named in §3, already lived
  twice in this fleet.
- **Expose a write tool for operator convenience (e.g. "re-submit an
  acknowledgement").** Rejected: this context's inbound leg is a poll and
  its outbound submissions are asynchronous and reconciled (ADR 0001 §5);
  a write tool would let an AI client originate a submission outside that
  reconciliation loop, which is precisely the kind of accidental
  commitment ADR 0001 drew this boundary to prevent.

## Consequences

**Easier**

- This context now answers "what happened to this order" and "how well
  are we translating and acknowledging demand" to any MCP client, on the
  same terms as every other bounded context — no bespoke pattern to
  learn or maintain.
- The catalogue-gap vs. domain-refusal split gives an operator a report
  that names which of two entirely different problems is actually
  happening, instead of one undifferentiated rejection count.
- Analytics cannot contend with OLTP or with the acknowledgement sweep:
  separate topic, separate database, separate binaries.

**Harder**

- One more topic, two more binaries, and a second database to operate.
  Mitigated by reusing this repo's own OLTP Postgres chart pattern and
  Kafka scaffolding rather than inventing new plumbing.
- The report is eventually consistent, lagging OLTP truth by however long
  the projector takes to catch up. This is the deliberate data-mesh
  tradeoff every sibling analytics data product already makes, but it
  must be communicated to report consumers rather than assumed away.
- A second producer path (the analytics publisher) for the same domain
  events must be kept in step with the report's actual inputs as new
  event types are added.
- First deploy has an empty report until events flow; a historical
  backfill requires replaying `warehouse.network-fulfillment.analytics`
  from the earliest offset into a fresh projector, so Kafka retention
  must cover the desired backfill window.

## Rollout

Each step is its own feature branch and PR into `develop`, CI green
before merge:

1. Domain events (`NetworkOrderReceived`, `NetworkOrderAcknowledged`,
   `NetworkOrderRejected`, `NetworkOrderShipmentConfirmed`) wired into
   `ReceiveNetworkDemand` and `SweepAcknowledgementDeadlines`, with full
   emission-order and payload test coverage. Landed ahead of this record.
2. Kafka integration + analytics publishers, the analytics data model,
   the analytics store adapters, and the analytics consumer with its
   unique-per-process consumer group. Landed ahead of this record.
3. `cmd/netfulfil-projector`, `cmd/netfulfil-reports`, the reports HTTP
   handler, and the OpenAPI extension. Landed ahead of this record.
4. `cmd/mcp` and its three read-only tools, one resource, one prompt.
   Landed ahead of this record.
5. **This ADR**, plus the Dockerfile and Helm chart changes needed to
   deploy all four binaries and gate the two new workloads behind
   `mcp.enabled` / `analytics.enabled` (this record).
6. `warehouse-infra` wiring `mcp.enabled` / `analytics.enabled` for the
   kind cluster, once this PR merges — recorded there when applied, per
   this fleet's convention.
