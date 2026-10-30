---
id: 0030-a2a-inbound-adapter
slug: /adr/0030-a2a-inbound-adapter
title: "30. A2A inbound adapter (a read-only third driving adapter)"
sidebar_label: "30. A2A inbound adapter"
sidebar_position: 30
description: "ADR 0030 (Accepted): network-fulfillment is an Agent2Agent (A2A) agent through a third driving adapter over the same read ports as REST and MCP. Two curated read skills, network_demand_status and advertised_availability, in a message-only agent served by cmd/a2a on port 8093. Read-only because the MCP charter is read-only; A2A carries delegation only, never data sync."
---

# 30. A2A inbound adapter (a read-only third driving adapter)

## Status

**Accepted** (2026-10-29). Mirrors the reference rollout in
[wes-work-planning ADR 0046](https://iqvo.github.io/wes-work-planning/docs/adr/0046-a2a-inbound-adapter)
and in workforce-management; follows [ADR 0002](./0002-mcp-and-analytics-data-product.md)
(the MCP adapter this one is modelled on and whose read-only charter it inherits),
[ADR 0008](./0008-cloudevents-mandatory-event-envelope.md) (CloudEvents on every Kafka
message) and [ADR 0025](./0025-postgres-and-kafka-are-the-only-runtime.md) (Postgres and
Kafka are the only runtime).

## Context

The fleet is adopting the Agent2Agent protocol so a bounded context can **delegate** a
goal to another context's agent. This context already exposes its reads over REST and
MCP, and the two questions a delegating agent would put to it are the two things its
MCP surface already answers: *where does the network's demand stand?* and *what
availability would you advertise to the network?*

Three transports now carry three different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts ([ADR 0008](./0008-cloudevents-mandatory-event-envelope.md)) |
| MCP | an agent's **tools** |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same** read ports
as the REST and MCP adapters (`ports.NetworkOrderRepo`, `ports.CapabilityOfferRepo`,
`ports.Clock`), built on the official `a2aproject/a2a-go/v2` SDK (pinned v2.7.0) and
served by a new `cmd/a2a` binary: JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card
at `/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`, `GET /healthz`.
`cmd/a2a` is a clone of `cmd/mcp`'s composition plus the mandatory `A2A_AGENT_URL`
(an absolute http(s) URL, the one the card advertises; the process refuses to boot
otherwise, before any dependency is touched).

### The agent is read-only: the write gate

A skill may change state only if the context's MCP surface already offers that write.
It does not: the MCP charter here is read-only (ADR 0002, enforced by
`TestMCPAdapterStaysReadOnly`), the one write this context has, shipment confirmation
(`POST /network-orders/{networkRef}/shipment-confirmation`, [ADR 0014](./0014-explicit-shipment-confirmation-endpoint.md)),
is REST only, and demand itself enters only by polling the network (ADR 0001 section 5).
Delegation must never widen what the context exposes, so the A2A agent has **no write
skill**, and `Deps` holds no use case, event publisher or unit of work. Consequently
`cmd/a2a` wires no Kafka, outbox or relay (like `cmd/mcp`) and the chart renders neither
`KAFKA_BROKERS` nor `EVENT_PUBLISHER` for it. `TestA2AAdapterStaysReadOnly` fails if the
adapter calls a state-changing method or `Deps` gains a write dependency.

### Two curated skills, not one per endpoint

| Skill | Kind | What it does |
|---|---|---|
| `network_demand_status` | read | with `networkRef`: one order, or a not-found problem; with `state` (`NEW`, `SUBMITTED`, `ACKNOWLEDGED`, `REJECTED`, `CONFIRMED`): the orders in that state; with neither: every order. `networkRef` and `state` are mutually exclusive. Each order carries both vocabularies per line, its state, the acknowledgement deadline, and whether that deadline passed unanswered. |
| `advertised_availability` | read | the stored `CapabilityOffer`s (advertised quantity per SKU and site, and whether it is `PHYSICAL` or `THROUGHPUT_CONSTRAINED`), optionally narrowed by `sku` and/or `siteId`. Reads the latest persisted snapshot; computes nothing. |

Both are derived from the MCP tools (`get_network_order` + `list_network_orders` become
one goal-shaped skill, `list_capability_offers` the other), the same curation discipline
as ADR 0002. `get_acknowledgement_report` is deliberately not a skill: it proxies a
second service over REST, and an agent that wants an analytics report should use the
data product, not delegate through this one. The answers are field-for-field the REST and
MCP projections (no ship-to PII exists in them, AGENTS.md hard rule 2; the network's
vocabulary is only the `networkRef` / `networkProductId` the REST and MCP shapes already
expose, nothing from `retailnetwork`'s own wire format).

### Message-only, answered in-band

Both skills are synchronous reads, so the agent is message-only (no Task lifecycle).
Domain rejections and malformed requests are **answered** as RFC 7807-shaped `problem`s
inside the message envelope, never raised as JSON-RPC transport errors, with the same
classification and `type` namespace as the REST adapter: `404` unknown network
reference (`network-order-not-found`), `400` a bad request (unknown skill, unknown
argument, both `networkRef` and `state`, an unknown `state`, an empty value), `500`
anything unrecognised (the cause is logged, never put in the answer), plus one
adapter-owned `503` when a port is not wired. The REST `409`
(`confirm-before-acknowledge`) and `422` (domain validation) classes belong to the
confirmation write and to demand intake, which this agent does not offer, so they cannot
occur.

### Strict input

Unknown argument names are **rejected**, not ignored: a misspelled `stat` must not
silently drop the filter and answer every order. Trailing data after the JSON document
is rejected too. Go's `encoding/json` matches names case-insensitively, so `State`
still reaches `state`; only genuinely unknown names are refused. This context has no
deprecated alias or key to keep off the new surface.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the same
fleet-wide decision as REST and MCP. Schemes are added to the card, not a redesign, if
that decision is ever reversed. A test pins that the card names no write skill.

### Packaging

A fifth binary (`/app/a2a`) in the same image and an `a2a.*` chart block
(`<release>-a2a`, default **off**) with Deployment, Service and a Kong HTTPRoute at
`/api/a2a/network-fulfillment` rewritten to `/`, so the card stays reachable under the
same prefix. `A2A_AGENT_URL` defaults to the in-cluster Service DNS and is overridable
with `a2a.agentUrl`. Port 8093 is the fleet's A2A port (8090 is MCP, 8091 the projector
admin port, 8092 the reports API). An architecture test pins the dependency rule (the
adapter depends only on application and domain, nothing depends on it) and that no
outbound A2A client is imported anywhere in `cmd/` or `internal/`.

## Consequences

- Another context's agent can ask what the network has asked of us and what we would
  advertise, without scraping REST. Who may *ask* is that caller's governance.
- `network_demand_status` without a filter returns every order, as `list_network_orders`
  does; there is no paging yet. A follow-up if the table grows.
- A write skill (for instance confirming a shipment) needs a new ADR that first adds it
  to the MCP surface, then here; `TestA2AAdapterStaysReadOnly` is the tripwire.
- Because the agent holds no in-process session state (unlike MCP's Streamable HTTP
  sessions), it could scale horizontally; the chart leaves `a2a.replicaCount` a plain
  value for now.
- The A2A answers are only as fresh as the stored rows: `advertised_availability` shows
  the last recompute pass of `netfulfil`, and says nothing if that has not run.
