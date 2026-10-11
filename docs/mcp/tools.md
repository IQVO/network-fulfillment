---
id: tools
title: MCP tools
sidebar_label: MCP tools
---

# MCP tools

`cmd/mcp` is a separate, read-only deployable. It serves the Model Context
Protocol over Streamable HTTP, mounted at both `/` and `/mcp`, on `MCP_ADDR`
(default `:8090`), plus `GET /healthz`. Server name
`network-fulfillment-mcp`, version `1.0.0`. There is no authentication.

It reads the same OLTP Postgres database `cmd/netfulfil` writes (through
`DATABASE_URL`) and, for the report tool, calls `netfulfil-reports` over REST
(`REPORTS_BASE_URL`). Without `DATABASE_URL` it uses a private in-memory store
that is always empty. Configuration: [operations/configuration.md](../operations/configuration.md#cmdmcp-the-mcp-server).

The schemas below were taken from a live `tools/list` call against a local
build of `cmd/mcp` on this branch (in-memory store, `REPORTS_BASE_URL` set),
and match `internal/adapters/inbound/mcp/tools.go` and `report_tool.go`.

## Tools

Every tool is annotated `readOnlyHint: true` (the SDK also reports
`idempotentHint: false`, its default). No write tool exists: demand enters
this context only by polling the network, and the one write action,
shipment confirmation, is REST only.

| Tool | Registered when | Input | Output |
| --- | --- | --- | --- |
| `get_network_order` | always | `networkRef` (string, **required**): the network's own reference, e.g. `po-4711` | one network order (shape below). Unknown ref: tool error `network order not found`. Empty ref: `networkRef is required`. |
| `list_network_orders` | always | `state` (string, optional): `NEW`, `SUBMITTED`, `ACKNOWLEDGED`, `REJECTED` or `CONFIRMED`; omit for every order | `{"networkOrders":[<network order>...]}`. Any other `state` value is a tool error (`unknown state ...`). |
| `list_capability_offers` | always in `cmd/mcp` (the code skips it only when no offer repository is wired, which `cmd/mcp` never does) | none | `{"capabilityOffers":[{"sku","siteId","advertisedQuantity","basis","computedAt"}]}`; `basis` is `PHYSICAL` or `THROUGHPUT_CONSTRAINED`. Empty unless `netfulfil` runs with `CAPABILITY_OFFER_ENABLED=true`. |
| `get_acknowledgement_report` | only when `REPORTS_BASE_URL` is set | `from` (string, **required**, RFC 3339, inclusive), `to` (string, **required**, RFC 3339, exclusive), `granularity` (string, optional, only `day`) | `{"rows":[{"dayBucket","ordersReceived","ordersAcknowledged","ordersRejectedUntranslatableSku","ordersRejectedDomain","acknowledgementDeadlinesMissed","ordersRejectedSubmissionFailed","avgAcknowledgementLatencySeconds"}]}`, proxied from `GET /reports/acknowledgement`. Missing `from`/`to`: `from and to are required (RFC3339)`. A non-2xx reports response: `reports client: unexpected status <n>`. |

Every input schema has `additionalProperties: false`, so an unknown argument
is rejected by the SDK.

### Network order shape

Shared by `get_network_order`, `list_network_orders` and the resource below
(`internal/adapters/inbound/mcp/dto.go`). It mirrors the REST
`GET /network-orders/{networkRef}` response field for field.

| Field | Type | Meaning |
| --- | --- | --- |
| `networkRef` | string | the network's reference |
| `siteId` | string | site the demand is for |
| `state` | string | `NEW`, `SUBMITTED`, `ACKNOWLEDGED`, `REJECTED`, `CONFIRMED` |
| `requiredShipBy` | RFC 3339 | the network's deadline |
| `acknowledgeBy` | RFC 3339 | receipt + 24 h |
| `receivedAt` | RFC 3339 | when the demand was turned into an order |
| `localOrderId` | string, omitted when absent | order-management's held order id, set from `SUBMITTED` on |
| `acknowledgementOverdue` | bool | `true` only for a `NEW` order past `acknowledgeBy` (the sweep's own rule) |
| `lines[]` | `{networkLineRef, networkProductId, sku, quantity}` | both vocabularies; empty for an order rejected as untranslatable |

No ship-to PII exists in this shape (ADR 0009).

## Resource

| URI template | MIME type | Content |
| --- | --- | --- |
| `network-order://network-fulfillment/{networkRef}` | `application/json` | the network order shape above. A URI with another prefix is an error. |

Source: `internal/adapters/inbound/mcp/resources.go`.

## Prompt

`answer_network_demand` (no arguments) returns a standard operating procedure
telling the model to use `get_network_order` for one order,
`list_network_orders` for working sets and `get_acknowledgement_report` for
trends, and how to read each rejection reason. Source:
`internal/adapters/inbound/mcp/prompts.go`. Its step 1 lists the states as
`NEW/ACKNOWLEDGED/REJECTED/CONFIRMED` and leaves out `SUBMITTED`, which the
tools do return.

## Calling it

From a port-forward or inside the cluster (the Service is
`network-fulfillment-mcp:8090`):

```bash
curl -si http://localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'
```

Copy the `Mcp-Session-Id` response header into later calls
(`tools/list`, `tools/call`). Sessions live in the pod's memory, which is why
the chart runs one replica (see the [runbook](../operations/runbook.md#scaling)).

## Operational notes

- `cmd/mcp` runs the OLTP migrations at start, without retry, then opens a
  pool of up to 10 connections.
- It logs JSON to stdout and exports no OpenTelemetry signals.
- Troubleshooting rows: [operations/troubleshooting.md](../operations/troubleshooting.md#mcp).
