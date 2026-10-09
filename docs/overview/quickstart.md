---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
---

# Quickstart

Build, test and run network-fulfillment on a laptop, then reach it in the
local kind cluster. Every command here was taken from the `Makefile`, the
`main.go` files and `web/` on `develop`.

## Prerequisites

| Tool | Version | Needed for |
| --- | --- | --- |
| Go | `1.27.2` (`go.mod`) | everything |
| `golangci-lint` | `v2.14.0` (pinned in the `Makefile`) | `make lint`, `make check` |
| Docker | any recent | `make integration` (testcontainers `postgres:16-alpine`, `confluentinc/confluent-local:7.6.1`) |
| `gremlins` | `v0.6.0` | `make mutation` |
| `govulncheck` | latest | `make vuln` |
| Node 20 | | `web/` only |

No credentials are needed anywhere: the network gateway is a stub and REST
and MCP are unauthenticated.

## Build and test

```bash
make build          # go build ./...
make test           # go test ./... -race (unit + httptest + the BDD suite), no Docker
make bdd            # godog acceptance suite only (features/, ADR 0018)
make arch-test      # hexagonal fitness tests
make check-fast     # fmt-check vet arch-test
make check          # fmt-check vet build lint test (what lefthook pre-push runs)
make check-all      # check + coverage (90 % gate) + arch-test + bdd
make integration    # testcontainers Postgres and Kafka (needs Docker)
```

There is no `make run`. Details of every layer and CI job:
[development/testing.md](../development/testing.md).

## Run the API with zero infrastructure

With no `DATABASE_URL` the service uses in-memory repositories; with the
default `NETWORK_MODE` it uses the stub gateway; with no `EVENT_PUBLISHER`
events go to the log. The stub has nothing to poll unless you seed it.

Create two files (the formats are in
[configuration](../operations/configuration.md#cmdnetfulfil-the-oltp-service)):

`demand.json`, two orders, one with a product we cannot translate:

```json
{
  "demands": [
    { "networkRef": "po-local-1", "siteId": "site-1", "requiredShipBy": "+36h",
      "lines": [ { "networkLineRef": "1", "networkProductId": "ASIN-LOCAL-1", "quantity": 2 } ] },
    { "networkRef": "po-local-2", "siteId": "site-1", "requiredShipBy": "+36h",
      "lines": [ { "networkLineRef": "1", "networkProductId": "ASIN-UNKNOWN", "quantity": 1 } ] }
  ]
}
```

`products.json`, the Anti-Corruption Layer dictionary:

```json
{ "products": [ { "networkProductId": "ASIN-LOCAL-1", "sku": "sku-1" } ] }
```

Run it (port `8088` is where the standalone `web/` dev server looks for the
API, see `web/src/config.ts`):

```bash
PORT=8088 \
NETWORK_SEED_FILE=./demand.json \
PRODUCT_TRANSLATION_FILE=./products.json \
ORDER_MANAGEMENT_URL=http://localhost:8081 \
POLL_INTERVAL=10s \
go run ./cmd/netfulfil
```

First calls:

```bash
curl -s localhost:8088/healthz
curl -s localhost:8088/inbound-status
curl -s localhost:8088/network-orders
curl -s localhost:8088/network-orders/po-local-2
curl -si -X POST localhost:8088/network-orders/po-local-2/shipment-confirmation
```

What you should see (checked against a local build on 2026-10-09):

- `po-local-2` is `REJECTED` with no lines: `ASIN-UNKNOWN` has no mapping,
  so the order is refused as `UNTRANSLATABLE_SKU` without any
  order-management call. The log shows `NetworkOrderReceived`
  (`lineCount: 0`), `NetworkOrderRejected` and
  `stub network acknowledgement ... accepted:false`.
- `po-local-1` stays `NEW` and is the only entry in `GET /network-orders`:
  the held-order call to `ORDER_MANAGEMENT_URL` fails (nothing listens
  there), the log shows `receive network demand failed`, and
  `/inbound-status` reports `"received":1,"failed":1` with no `since`
  (the watermark only moves after a clean pass).
- Confirming shipment of a `REJECTED` order answers
  `409 application/problem+json`, type `.../confirm-before-acknowledge`.
- An OTLP export error for `localhost:4317` appears on shutdown when no
  Collector runs. It never blocks boot.

To take `po-local-1` through `SUBMITTED` and `ACKNOWLEDGED` you need an
order-management that answers `POST /orders`. Point `ORDER_MANAGEMENT_URL`
at the cluster's order-management (port-forward its Service). On `develop`
this call fails with status 400 because order-management requires an
`Idempotency-Key` header this client does not send yet (open PR #63). The
BDD suite (`make bdd`) exercises the full lifecycle with a fake planner.

## Add Postgres

Any Postgres 16 works. The service runs its migrations at startup from
`MIGRATIONS_PATH` (default `/app/migrations`, the image path), so point it at
the repo's directory:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/netfulfil?sslmode=disable \
MIGRATIONS_PATH=migrations \
go run ./cmd/netfulfil
```

If `DATABASE_URL` is set and Postgres cannot be reached after the boot retry
(5 attempts), the process exits with `cannot wire order repository`. It never
falls back to memory.

## Add Kafka

The fleet runs one Kafka broker, in the kind cluster, reachable from the host
at `localhost:9092` through its external access. There is no
docker-compose broker. Publishing:

```bash
EVENT_PUBLISHER=kafka KAFKA_BROKERS=localhost:9092 go run ./cmd/netfulfil
```

With `DATABASE_URL` also set, events go through the `outbox_events` table and
the relay; without it they are written to
`warehouse.network-fulfillment.events` and
`warehouse.network-fulfillment.analytics` directly.

Capability offers (opt-in) also need the broker, plus inventory-storage:

```bash
CAPABILITY_OFFER_ENABLED=true KAFKA_BROKERS=localhost:9092 \
INVENTORY_STORAGE_URL=http://localhost:8082 SITE_ID=site-1 \
go run ./cmd/netfulfil
curl -s localhost:8080/capability-offers
```

Boot waits up to 120 s for both caches to replay their topics before the
listener starts.

## The other binaries

```bash
# MCP server, Streamable HTTP on :8090 (/ and /mcp)
DATABASE_URL=... MIGRATIONS_PATH=migrations go run ./cmd/mcp

# analytics writer: consumes warehouse.network-fulfillment.analytics
ANALYTICS_DATABASE_URL=postgres://... KAFKA_BROKERS=localhost:9092 go run ./cmd/netfulfil-projector

# analytics reader on :8092
ANALYTICS_DATABASE_URL=postgres://... go run ./cmd/netfulfil-reports
curl -s 'localhost:8092/reports/acknowledgement?from=2026-10-01T00:00:00Z&to=2026-10-10T00:00:00Z'
```

The projector's `ANALYTICS_MIGRATIONS_PATH` defaults to the relative
`migrations/analytics`, so run it from the repo root. MCP tool reference:
[mcp/tools.md](../mcp/tools.md).

## The web remote

```bash
cd web
npm ci
npm run dev        # vite on :5188, calls the API at http://localhost:8088
```

`CORS_ALLOWED_ORIGINS` already allows `http://localhost:5188` and
`http://localhost:5173` by default.

## In the kind cluster

`warehouse-infra` deploys the chart (see the
[runbook](../operations/runbook.md#in-the-local-kind-cluster)). Through Kong:

```bash
curl -s http://localhost:8000/api/network-fulfillment/inbound-status
curl -s http://localhost:8000/api/network-fulfillment/network-orders
curl -s 'http://localhost:8000/api/network-fulfillment/reports/acknowledgement/freshness'
```

The kind overlay seeds `po-local-1` (`ASIN-LOCAL-1 -> sku-1`) and does not
enable Kafka publishing; see the runbook for how to turn it on.
