---
id: overview
title: API overview
sidebar_label: API overview
slug: /api-reference
sidebar_position: 1
---

# API overview

:::info Implemented by cmd/netfulfil, cmd/netfulfil-reports, cmd/mcp and cmd/a2a
`cmd/netfulfil` serves the REST routes below (`internal/adapters/inbound/http`)
on `PORT` (default `8080`). `cmd/netfulfil-reports` serves the two report routes
(default `:8092`). `cmd/mcp` and `cmd/a2a` are separate, read-only deployables.
The contracts are `apis/openapi.yaml` and `apis/asyncapi.yaml`, pinned by
[ADR 0001](/docs/adr/0001-network-fulfillment-bounded-context),
[ADR 0014](/docs/adr/0014-explicit-shipment-confirmation-endpoint) and
[ADR 0030](/docs/adr/0030-a2a-inbound-adapter). See the
[Quickstart](/docs/overview/quickstart) for local calls.
:::

| Interface | Source of truth | Notes |
| --- | --- | --- |
| REST | `apis/openapi.yaml` (OpenAPI 3.0.3, version 1.0.0) | The pages under *REST* are generated from the spec with `npm run gen-api-docs`; do not edit them by hand. Servers named in the spec: `http://localhost:8080` (local) and `http://localhost:8000/api/network-fulfillment` (kind cluster through Kong). The reports routes are in the same spec but served by a different binary, through Kong at `/api/network-fulfillment/reports`. |
| Events | `apis/asyncapi.yaml` (AsyncAPI 2.6.0, version 2.0.0) | Summarised on the [event catalogue](/docs/api-reference/events); every field on [Domain events](/docs/ddd/domain-events). |
| MCP | [ADR 0002](/docs/adr/0002-mcp-and-analytics-data-product), `internal/adapters/inbound/mcp` | `cmd/mcp` serves Streamable HTTP on `/` and `/mcp` (`MCP_ADDR`, default `:8090`; through Kong `/api/mcp/network-fulfillment` when `mcp.enabled` and its route are on). Four read-only tools, no write tool: `get_network_order`, `list_network_orders`, `list_capability_offers`, and `get_acknowledgement_report` (only when `REPORTS_BASE_URL` is set); one resource `network-order://network-fulfillment/{networkRef}` and one prompt `answer_network_demand`. See [MCP tools](/docs/mcp/tools). No auth, by fleet decision. |
| A2A | [ADR 0030](/docs/adr/0030-a2a-inbound-adapter), `internal/adapters/inbound/a2a`, `cmd/a2a` | JSON-RPC 2.0 on `/` and `/a2a` (`A2A_ADDR`, default `:8093`; through Kong `/api/a2a/network-fulfillment` when `a2a.enabled` and its route are on), the Agent Card at `/.well-known/agent-card.json`, `GET /healthz`. Two read-only skills, no write skill: `network_demand_status` (`networkRef` or `state`, mutually exclusive) and `advertised_availability` (optional `sku`, `siteId`). Streaming is off. `A2A_AGENT_URL` is required: it is the URL the card advertises. No auth, by fleet decision. |

`/a2a` is not in `apis/openapi.yaml`: it is a JSON-RPC endpoint with its own
protocol, described by the Agent Card and by the A2A rows above and in
[Integration](/docs/ecosystem/integration#a2a-skills).

## Endpoints

| Tag | Endpoint | Operation id | Success |
| --- | --- | --- | --- |
| network-orders | `GET /network-orders` | `listUnansweredNetworkOrders` | `200 {networkOrders: [...]}`, only `NEW` orders, soonest `acknowledgeBy` first |
| network-orders | `GET /network-orders/{networkRef}` | `getNetworkOrder` | `200`, one order in any state, with both vocabularies per line; `404 network-order-not-found` |
| network-orders | `POST /network-orders/{networkRef}/shipment-confirmation` | `confirmNetworkOrderShipment` | `204`, empty body; `404 network-order-not-found`, `409 confirm-before-acknowledge` (ADR 0014) |
| capability-offers | `GET /capability-offers` | `listCapabilityOffers` | `200 {capabilityOffers: [...]}` per SKU and site, `basis` `PHYSICAL` or `THROUGHPUT_CONSTRAINED` |
| operations | `GET /inbound-status` | `getInboundStatus` | `200`, network mode, poller counters, `unanswered`, `overdue` |
| operations | `GET /healthz`, `GET /readyz` | `healthz`, `readyz` | `200`; `/readyz` is `503` once shutdown has started (`{"status":"not_ready"}`) or while Postgres or Kafka is unreachable (`503` problem `dependency-unavailable`, `detail` `not ready: postgres, kafka`) |
| operations | `GET /metrics` | `metrics` | `200` Prometheus text, the `circuit_breaker_state` gauge |
| reports | `GET /reports/acknowledgement` | `getAcknowledgementReport` | `200`, day-bucketed rows for `from` (inclusive) to `to` (exclusive), served by `cmd/netfulfil-reports` from the analytical database; see [ADR 0002](/docs/adr/0002-mcp-and-analytics-data-product) |
| reports | `GET /reports/acknowledgement/freshness` | `getAcknowledgementReportFreshness` | `200`, how far the projection lags the event stream |

The router registers exactly these routes: `GET /capability-offers` and `GET /metrics`
only when their dependency is wired (always, in `cmd/netfulfil`), and the one
`POST` only when the confirm-shipment use case is wired. There is no route in the
code that the spec does not list (`internal/adapters/inbound/http/server.go`).

## Conventions in the contract

- **Errors** are RFC 7807 `application/problem+json` with
  `type = https://errors.network-fulfillment.warehouse-systems.dev/<slug>`
  (`internal/adapters/inbound/http/errors.go`). The slugs: `network-order-not-found` (404),
  `confirm-before-acknowledge` (409), `unknown-product`, `empty-network-ref`,
  `empty-network-line-ref`, `empty-network-product-id`, `non-positive-quantity`,
  `order-without-lines`, `dependency-unavailable` (503, from `/readyz` only) and
  `internal-error`. Remedies are on
  [Troubleshooting](/docs/operations/troubleshooting#rest-errors).
- **One write route.** Demand enters this context only by polling the network
  (ADR 0001 §5); the only write is the explicit shipment confirmation, which is a
  fact this context is told. It is REST only: there is no MCP or A2A write.
- **No ship-to PII** exists anywhere in the contract (ADR 0009 §3).
- **CORS** is enabled on `cmd/netfulfil` for `GET`, `POST`, `OPTIONS` with the
  origins of `CORS_ALLOWED_ORIGINS` (default `http://localhost:5173,http://localhost:5188`).
- **Auth**: none. The fleet reverted REST, MCP and A2A auth, and nothing in this
  repository reintroduces it; the `Authorization` header is only allowed through
  CORS, never checked.
- **The single `POST` is idempotent.** An order already `CONFIRMED` is returned
  unchanged with `204`: nothing is re-submitted to the network and the event is not
  published again (`internal/application/usecases/confirm_network_order_shipment.go`).
  A path carrying a NUL character is answered `404 resource-not-found`
  (`rejectNUL`).
