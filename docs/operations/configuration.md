---
id: configuration
title: Configuration reference
sidebar_label: Configuration
---

# Configuration reference

Every environment variable each binary in `cmd/` reads, taken from the code on
`develop`. There is one table per binary. Test-only variables (for example
`BDD_TAGS` in `features_test.go`) are listed in
[development/testing.md](../development/testing.md), not here.

All four binaries ship in the same image (`Dockerfile`: `/app/netfulfil`,
`/app/mcp`, `/app/netfulfil-projector`, `/app/netfulfil-reports`; the
entrypoint is `./netfulfil`). Nothing is authenticated: REST and MCP are open
inside the cluster, like every context in the fleet.

## How values are parsed

- **Unset and empty are the same.** Every reader treats `""` as "not set" and
  falls back to the default.
- **Durations** are Go `time.ParseDuration` strings (`30s`, `1m`, `5m`).
  Two different parsers are in use, and they behave differently on bad input:
  - `durationEnv` (`cmd/netfulfil/config.go`) is used for `RECOMPUTE_INTERVAL`
    and `OUTBOX_RELAY_INTERVAL`. A malformed, zero or negative value falls back
    to the default.
  - `pollInterval` and `sweepInterval` (same file) are used for
    `POLL_INTERVAL` and `SWEEP_INTERVAL`. A malformed value falls back to the
    default, but a **zero or negative value is accepted** and is then handed to
    `time.NewTicker`, which panics. Always set these to a positive duration.
  - `orphanHoldGrace` (same file) is used for `ORPHAN_HOLD_GRACE` and is the
    one duration where `0` (or `0s`) is valid and means "no grace", which is why
    it cannot use `durationEnv`. A malformed or negative value logs a WARN
    (`invalid ORPHAN_HOLD_GRACE, using the default`) and uses `1h`.
- **Integers**: `envInt` (`cmd/netfulfil/config.go`) is used for
  `OUTBOX_MAX_ATTEMPTS`. `0` is valid; a malformed or negative value logs a
  WARN (`invalid integer env var, using default`) and falls back to the
  default.
- **Booleans**: `ORPHAN_HOLD_SWEEP_ENABLED` is **on by default**: unset or empty is on,
  anything `strconv.ParseBool` accepts (`true`, `false`, `1`, `0`, `t`, `f`, ...,
  case-insensitive) is honoured, and an unparseable value logs a WARN
  (`invalid ORPHAN_HOLD_SWEEP_ENABLED, keeping the orphaned-hold sweep on`) and
  keeps it on, so a typo can never switch the safety net off.
- **`EVENT_PUBLISHER`** and **`CAPABILITY_OFFER_ENABLED`** are kept only as
  guards (ADR 0025): unset or empty is the one runtime; `kafka` and `true`
  respectively are accepted (case-insensitive for the second); any other
  value is a boot error naming the variable. They switch nothing off.

## `cmd/netfulfil` (the OLTP service)

The REST API, the inbound poller, the background tickers and the outbox
relay all run in this one process. Its env surface lives in
`cmd/netfulfil/config.go` plus the wiring files named below.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `PORT` | `8080` | no | HTTP listen port. The server listens on `:<PORT>`. | `cmd/netfulfil/config.go` (`addr`) |
| `DATABASE_URL` | none | **yes** | Postgres DSN for the request-serving pool. Missing: boot error `DATABASE_URL is required`. Unreachable: the process exits after the retry budget. There is no in-memory mode (ADR 0025). | `cmd/netfulfil/database.go` (`wireOrders`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct (non-PgBouncer) DSN used only for `golang-migrate` at startup (ADR 0007). The runtime pool always uses `DATABASE_URL`. | `cmd/netfulfil/database.go` (`migrationsURL`) |
| `MIGRATIONS_PATH` | `/app/migrations` | no | Directory of the OLTP `*.up.sql` files. Set it to `migrations` for a local run from the repo root. | `cmd/netfulfil/config.go` (`migrationsPath`) |
| `NETWORK_MODE` | `stub` | no | Which network gateway is wired. `live` (case-insensitive, surrounding blanks ignored) selects the live gateway of ADR 0026, which calls the retail network's Vendor API. Every other value, including the retired `sandbox`, means `stub`: a typo fails closed. The mode in force is logged at startup and echoed by `GET /inbound-status`. | `cmd/netfulfil/config.go` (`loadNetworkConfig`), `cmd/netfulfil/wiring.go` (`wireGateway`), `internal/adapters/outbound/retailnetwork/mode.go` (`ParseMode`) |
| `NETWORK_BASE_URL` | unset | only when `NETWORK_MODE=live` | Base URL of the Vendor API. Read only in live mode. With `live` and no value, boot fails with `NETWORK_MODE=live requires NETWORK_BASE_URL to be set`; if `NETWORK_VENDOR_ID` is also missing the one error names both (`... NETWORK_BASE_URL and NETWORK_VENDOR_ID ...`). Ignored in stub mode. | `cmd/netfulfil/config.go` (`loadNetworkConfig`) |
| `NETWORK_VENDOR_ID` | unset | only when `NETWORK_MODE=live` | The vendor account the service acts as on every Vendor API call. The account must already exist on the network; this service never creates it. Same boot error as `NETWORK_BASE_URL` when missing. Ignored in stub mode. | `cmd/netfulfil/config.go` (`loadNetworkConfig`) |
| `INBOUND_RECEIVING_URL` | unset | only when `NETWORK_MODE=live` | Base URL of inbound-receiving; the inbound loop registers each polled seller shipment there as an ASN (`POST /asns`, ADR 0028). With `live` and no value, boot fails naming it (together with `PRODUCT_MASTER_URL` when both are missing). Ignored in stub mode. | `cmd/netfulfil/inbound.go` (`loadInboundConfig`) |
| `PRODUCT_MASTER_URL` | unset | only when `NETWORK_MODE=live` | Base URL of product-master; the inbound loop resolves each GTIN to a SKU with `GET /gtins/{gtin}` (cached in Postgres). Same boot error as `INBOUND_RECEIVING_URL`. Ignored in stub mode. | `cmd/netfulfil/inbound.go` (`loadInboundConfig`) |
| `INBOUND_INTAKE_INTERVAL` | `30s` | no | Go duration; how often the inbound loop polls shipments, retries the ones not registered yet and reports closed receipts (live only). A malformed or non-positive value uses the default. | `cmd/netfulfil/inbound.go` (`inboundIntakeInterval`) |
| `INBOUND_RECEIPT_CONSUMER_GROUP` | unset | only when `NETWORK_MODE=live` | This service's own, durable Kafka consumer group on inbound-receiving's events topic (`warehouse.inbound-receiving.events`), through which closed receipts come back to be reported to the network (ADR 0028). Boot fails naming it when missing (with the two URLs above in one error). Ignored in stub mode. | `cmd/netfulfil/inbound.go` (`loadInboundConfig`) |
| `NETWORK_NODES_FILE` | unset | no (live only) | JSON file of the node definitions the live gateway registers on the network for a site that is not yet one of its nodes: `{"nodes":[{"siteCode","nodeType","subtype"?,"regionId","timezone","postalCode","countryCode","lat"?,"lon"?}]}`. Strict: an unknown key, a missing required field, a lone `lat` or `lon`, or a site listed twice fails boot (`cannot load network node definitions`). Without it live mode only adopts sites that already are nodes (ADR 0026 Decision 7). Read once at boot; not read in stub mode. | `cmd/netfulfil/wiring.go` (`loadNetworkNodes`), `internal/adapters/outbound/retailnetwork/nodes.go` (`LoadNodesFile`) |
| `NETWORK_SEED_FILE` | unset | no (stub only) | JSON file of stub demand, `{"demands":[{"networkRef","siteId","requiredShipBy","lines":[{"networkLineRef","networkProductId","quantity"}]}]}`. `requiredShipBy` is RFC 3339 or a relative duration such as `+36h`. Unknown keys fail boot. Setting it against the live gateway fails boot (ADR 0012): seeded demand would never be delivered. | `cmd/netfulfil/wiring.go` (`seedStubDemand`), `internal/adapters/outbound/retailnetwork/seed.go` |
| `PRODUCT_TRANSLATION_FILE` | unset | no, but see meaning | The Anti-Corruption Layer dictionary, `{"products":[{"networkProductId","sku","site"}]}`; `site` is optional (an entry without one applies to every site) because the network lists one item at one node, and the inventory feed reads the dictionary the other way, per site (ADR 0027 §10). Unknown keys, empty fields (a blank `site` too), a product mapped twice and a SKU with two listings at one site fail boot. Unset: the dictionary is empty, a `WARN` is logged, and **every** network order is rejected as `UNTRANSLATABLE_SKU` (ADR 0013). | `cmd/netfulfil/wiring.go` (`loadProductTranslation`), `internal/adapters/outbound/producttranslation/file.go` |
| `ORDER_MANAGEMENT_URL` | `http://localhost:8080` | yes in any real deployment | Base URL for the held-order calls `POST /orders`, `POST /orders/{id}/release`, `DELETE /orders/{id}`. | `cmd/netfulfil/wiring.go` (`wirePlanner`) |
| `POLL_INTERVAL` | `1m` | no | Cadence of the inbound poller **and** of `ReconcileSubmittedOrders`. Must be positive (see above). | `cmd/netfulfil/config.go` (`pollInterval`), `cmd/netfulfil/workers.go` |
| `SWEEP_INTERVAL` | `1m` | no | Cadence of `SweepAcknowledgementDeadlines`, of `RejectOverdueOrders` and of `SweepOrphanedHolds` (three separate tickers). Must be positive. The chart sets `5m`. | `cmd/netfulfil/config.go` (`sweepInterval`), `cmd/netfulfil/workers.go` |
| `ORPHAN_HOLD_SWEEP_ENABLED` | `true` | no | Turns the orphaned-hold sweep (ADR 0024) on or off. `false` skips wiring it: no ticker, and a boot WARN `orphaned-hold sweep disabled by ORPHAN_HOLD_SWEEP_ENABLED: a hold nobody decides stays reserved in order-management`. Anything else `strconv.ParseBool` rejects keeps it on. The hold id and outcome are still recorded when it is off. | `cmd/netfulfil/config.go` (`orphanHoldSweepEnabled`), `cmd/netfulfil/wiring.go` (`wireOrphanHoldSweep`) |
| `ORPHAN_HOLD_GRACE` | `1h` | no | Go duration added to each order's `acknowledgeBy` before its undecided hold counts as orphaned (`now > acknowledgeBy + grace`). It lets `RejectOverdueOrders` free the hold first, so the sweep is a backstop and not a second actor. `0` means no grace; a malformed or negative value logs a WARN and uses `1h`. Read once at boot. | `cmd/netfulfil/config.go` (`orphanHoldGrace`) |
| `EVENT_PUBLISHER` | unset | no | Guard only: unset or `kafka`; anything else is a boot error. Every domain event goes to both topics through the transactional outbox and the relay, which is always on (ADR 0003, ADR 0025). | `cmd/netfulfil/publishing.go` (`wireEventPublisher`) |
| `KAFKA_BROKERS` | none | **yes** | Comma-separated broker list for the two publishers and the two capability-offer caches. Missing: boot error `KAFKA_BROKERS is required`; there is no localhost default (ADR 0025). The caches read their start-up offsets from the first broker only. | `cmd/netfulfil/config.go` (`loadBootConfig`, `splitBrokers`), `internal/adapters/outbound/processpathcache/consumer.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Sleep between outbox relay passes that found nothing to publish (outbox mode only). A full batch of 100 is followed immediately by another pass. | `cmd/netfulfil/publishing.go`, `internal/adapters/outbound/postgres/outbox_relay.go` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | Outbox relay attempt cap (ADR 0021, outbox mode only). A row that has failed this many sends is excluded from the relay's claims and logged at ERROR; it stays in `outbox_events` until re-armed with `UPDATE outbox_events SET attempts = 0 WHERE id = ...` ([runbook](./runbook.md#an-outbox-row-hit-the-attempt-cap)). `0` = unlimited (one poison row then blocks every later row forever). An invalid or negative value logs a WARN and uses `20`. | `cmd/netfulfil/config.go` (`outboxMaxAttempts`, `envInt`), `cmd/netfulfil/publishing.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5188` | no | Comma-separated browser origins. Methods `GET`, `POST`, `OPTIONS`; headers `Content-Type`, `Authorization`; no credentials; preflight cached 300 s. | `internal/adapters/inbound/http/server.go` (`corsMiddleware`) |
| `CAPABILITY_OFFER_ENABLED` | unset | no | Guard only: unset or `true`; anything else is a boot error. The CapabilityOffer pipeline is always wired: the two Kafka caches, the inventory-storage client, the `RECOMPUTE_INTERVAL` ticker and `GET /capability-offers`. Boot then blocks until both caches have replayed their topic (one shared wait of 120 s, the sum of the two caches' 60 s `WaitReadyTimeout`s). | `cmd/netfulfil/config.go`, `cmd/netfulfil/capability.go` |
| `NETWORK_PROFILE_INTERVAL` | `5m` | no (live only) | How often the node profile (cutoffs, handling time, throughput) is derived from the local caches and, when its content changed, declared to the network with `PUT /vendor/nodes/{nodeId}/profile` (ADR 0027). A Go duration; `0` (or `0s`) disables the worker with a `WARN`; a negative or malformed value warns and keeps the default. The first pass runs at boot. Not read in stub mode, which never starts the worker. | `cmd/netfulfil/config.go` (`profileInterval`), `cmd/netfulfil/profile.go` (`wireNodeProfiles`) |
| `NETWORK_INVENTORY_INTERVAL` | `5m` | no (live only) | How often the inventory feed is planned from the capability offers the last `RECOMPUTE_INTERVAL` pass persisted and, when its content changed, sent to the network with `POST /vendor/inventory` (ADR 0027 part 2). A Go duration; `0` (or `0s`) disables the worker with a `WARN`; a negative or malformed value warns and keeps the default. The first pass runs at boot. The service itself repeats an unchanged feed at most hourly, so this is the cadence of looking, not of sending. Not read in stub mode, which never starts the worker. | `cmd/netfulfil/config.go` (`inventoryInterval`), `cmd/netfulfil/inventory_feed.go` (`wireInventoryFeed`) |
| `SITE_ID` | `site-1` | no | The single site capability offers are computed for (the CPT schedule is looked up by this id). In live mode it is also a site the gateway converges onto a network node, together with every site of `NETWORK_NODES_FILE`. The chart has no value for it: set it with `extraEnv`. | `cmd/netfulfil/config.go` (`siteId`), `cmd/netfulfil/wiring.go` (`wireGateway`) |
| `RECOMPUTE_INTERVAL` | `1m` | no | Cadence of `RecomputeCapabilityOffers`. Only used when the pipeline is enabled. | `cmd/netfulfil/config.go` (`recomputeInterval`) |
| `INVENTORY_STORAGE_URL` | `http://localhost:8080` | when the pipeline is enabled | Base URL for `GET /inventory/{sku}/usable`. | `cmd/netfulfil/capability.go` (`wireInventoryClient`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC `host:port` of the Collector for traces and metrics (insecure, dialled lazily). An unreachable Collector drops telemetry and never blocks boot (ADR 0019). | `cmd/netfulfil/main.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `OTEL_SERVICE_NAME` | `network-fulfillment` | no | `service.name` resource attribute. | `cmd/netfulfil/main.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. The chart sets it to the image tag. | `cmd/netfulfil/main.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` (`Environment`) |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning` or `error` (case-insensitive); anything else is `info`. Same vocabulary in all four binaries. | `cmd/netfulfil/config.go` (`newLogger`) |

The process logs JSON to stdout at the level `LOG_LEVEL` names (see the row
below).

## `cmd/mcp` (the MCP server)

A separate, read-only deployable that serves MCP over Streamable HTTP at `/`
and `/mcp`, plus `GET /healthz`. See [ecosystem/integration.md](../ecosystem/integration.md#mcp-tools)
for the tool list.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address. | `cmd/mcp/main.go` |
| `DATABASE_URL` | none | **yes** | Postgres DSN of the **same** OLTP database `netfulfil` writes. Missing: boot error `DATABASE_URL is required`. | `cmd/mcp/main.go` (`buildOrders`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step this binary also runs at startup (ADR 0007). Unlike `netfulfil`, this step is **not retried**. | `cmd/mcp/main.go` |
| `MIGRATIONS_PATH` | `/app/migrations` | no | OLTP migrations directory. | `cmd/mcp/main.go` |
| `REPORTS_BASE_URL` | unset | no | Base URL of `netfulfil-reports`. Unset: the `get_acknowledgement_report` tool is not registered. The chart fills it in with the in-cluster reports Service when `analytics.enabled=true`. | `cmd/mcp/main.go` |
| `LOG_LEVEL` | `info` | no | Same values as `netfulfil`. | `cmd/mcp/main.go` (`newLogger`) |

`list_capability_offers` is always registered by this binary: it reads the
`capability_offers` table, which `netfulfil` fills (the pipeline is always on).

## `cmd/a2a` (the A2A agent)

A separate, read-only deployable (ADR 0030) that serves the Agent Card at
`/.well-known/agent-card.json` and the A2A JSON-RPC endpoint at `/` and `/a2a`, plus
`GET /healthz`. It reads the same OLTP database as `cmd/mcp`; it runs no Kafka client,
publisher, outbox or relay, so it needs no `KAFKA_BROKERS`. It has two read skills,
`network_demand_status` and `advertised_availability`; see
[ADR 0030](../adr/0030-a2a-inbound-adapter.md).

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `A2A_ADDR` | `:8093` | no | Listen address. | `cmd/a2a/main.go` |
| `A2A_AGENT_URL` | none | **yes** | The absolute http(s) URL the Agent Card advertises as the JSON-RPC endpoint, one agent clients can dial. Missing or not absolute: boot error naming `A2A_AGENT_URL`, before any dependency is touched. The chart defaults it to `http://<release>-a2a.<ns>.svc.cluster.local:8093/` and `a2a.agentUrl` overrides it. | `cmd/a2a/main.go` (`loadBootConfig`) |
| `DATABASE_URL` | none | **yes** | Postgres DSN of the **same** OLTP database `netfulfil` writes. Missing: boot error `DATABASE_URL is required`. | `cmd/a2a/main.go` (`buildOrders`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step this binary also runs at startup (ADR 0007), not retried. | `cmd/a2a/main.go` |
| `MIGRATIONS_PATH` | `/app/migrations` | no | OLTP migrations directory. | `cmd/a2a/main.go` |
| `LOG_LEVEL` | `info` | no | Same values as `netfulfil`. | `cmd/a2a/main.go` (`newLogger`) |

## `cmd/netfulfil-projector` (analytics writer)

Consumes `warehouse.network-fulfillment.analytics` into the analytical
database. Serves only `GET /healthz` on the admin address.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | DSN of the analytical database. Missing: exits with `ANALYTICS_DATABASE_URL is required`. | `cmd/netfulfil-projector/main.go` |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the health endpoint. | `cmd/netfulfil-projector/main.go` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Comma-separated broker list for the consumer and the DLQ writer. | `cmd/netfulfil-projector/main.go` |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Analytical migrations directory, relative to the working directory (`/app` in the image). | `cmd/netfulfil-projector/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning` or `error`; anything else is `info`. | `cmd/netfulfil-projector/main.go` (`newLogger`) |

## `cmd/netfulfil-reports` (analytics reader)

Serves the acknowledgement report over a read-only pool.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | DSN of the analytical database. Every connection is pinned to `default_transaction_read_only=on`. Missing: exits with `ANALYTICS_DATABASE_URL is required`. | `cmd/netfulfil-reports/main.go`, `internal/adapters/outbound/analyticsstore/pool.go` |
| `HTTP_ADDR` | `:8092` | no | Listen address. | `cmd/netfulfil-reports/main.go` |
| `LOG_LEVEL` | `info` | no | Same values as the projector. | `cmd/netfulfil-reports/main.go` |

## Constants that are not configurable

These look like knobs but are compiled in. Changing them is a code change.

| Constant | Value | Where |
| --- | --- | --- |
| Acknowledgement window | 24 h after receipt | `internal/domain/networkorder/network_order.go` (`AcknowledgementWindow`) |
| Boot retry | 5 attempts, waits 1 s, 2 s, 4 s, 8 s between them | `cmd/netfulfil/retry.go` |
| OLTP pool | `MaxConns` 10, `statement_timeout` 5 s | `internal/adapters/outbound/postgres/pool.go` |
| Projector pool | `MaxConns` 5, `statement_timeout` 10 s | `internal/adapters/outbound/analyticsstore/pool.go` |
| Reports pool | `MaxConns` 5, `statement_timeout` 15 s, read-only | `internal/adapters/outbound/analyticsstore/pool.go` |
| retail-network client | no env knob: 10 s per attempt (bounded by the caller's deadline), at most 5 attempts, jittered backoff 100 ms to 2 s on network errors, timeouts and `5xx`, `429` waits for `Retry-After` (at most 30 s), other `4xx` never retried; own `http.Client`; poll pages of 200, at most 500 pages; poll overlap 2 minutes | `internal/adapters/outbound/retailnetwork/client.go`, `gateway_live.go` (`pollOverlap`) |
| retail-network breaker | the order-management tuning below, one instance for the dependency, reported as `circuit_breaker_state{dependency="retail-network"}` | `internal/adapters/outbound/retailnetwork/client.go`, `internal/resilience/breaker.go` |
| order-management breaker | trips on 5 consecutive failures, or >50 % failures over at least 10 requests in a 30 s window; open for 30 s; 1 half-open probe | `internal/resilience/breaker.go` |
| order-management call timeout | 30 s cap per call (`resilience.DefaultTimeout`), inside a 10 s `http.Client` timeout | `internal/adapters/outbound/ordermanagement/breaker.go`, `planner.go` |
| inventory-storage call timeout | 10 s `http.Client` timeout | `internal/adapters/outbound/inventoryclient/client.go` |
| Outbox relay batch | 100 rows per pass (the attempt cap is configurable: `OUTBOX_MAX_ATTEMPTS`) | `internal/adapters/outbound/postgres/outbox_relay.go` |
| Cache replay wait | 60 s per cache, applied as one 120 s deadline for both | `processpathcache.WaitReadyTimeout`, `pathcapacitycache.WaitReadyTimeout`, `cmd/netfulfil/capability.go` (`waitCachesReady`) |
| OTel metric push | every 30 s | `internal/adapters/outbound/telemetry/telemetry.go` |
| Graceful shutdown budget | 10 s | `cmd/netfulfil/shutdown.go`, and each other `main.go` |

## Helm chart values to environment

`charts/network-fulfillment` sets the variables below. Anything without a
values key (`SITE_ID`, `RECOMPUTE_INTERVAL`, `INVENTORY_STORAGE_URL`,
`OUTBOX_RELAY_INTERVAL`, `LOG_LEVEL`, `MIGRATIONS_PATH`, `CAPABILITY_OFFER_ENABLED`)
can only be set with `extraEnv` (API Deployment) or `mcp.extraEnv`.

| Values key | Env var | Deployment |
| --- | --- | --- |
| `config.port` | `PORT` | api |
| `config.networkMode` | `NETWORK_MODE` | api |
| `config.networkBaseUrl` | `NETWORK_BASE_URL` (live mode only; the render fails without it) | api |
| `config.networkVendorId` | `NETWORK_VENDOR_ID` (live mode only; the render fails without it) | api |
| `config.networkProfileInterval` | `NETWORK_PROFILE_INTERVAL` (live mode only; default `5m`; rendered with `quote`, so `"0"` reaches the process) | api |
| `config.networkInventoryInterval` | `NETWORK_INVENTORY_INTERVAL` (live mode only; default `5m`; rendered with `quote`, so `"0"` reaches the process) | api |
| `networkNodes.nodes` | `NETWORK_NODES_FILE=/etc/network-fulfillment/nodes.json` (live mode only, when non-empty; rendered into the `-data` ConfigMap) | api |
| `config.orderManagementUrl` | `ORDER_MANAGEMENT_URL` | api |
| `config.pollInterval` | `POLL_INTERVAL` (ConfigMap) | api |
| `config.sweepInterval` | `SWEEP_INTERVAL` | api |
| `config.orphanHoldSweepEnabled` | `ORPHAN_HOLD_SWEEP_ENABLED` (default `true`; rendered with `quote` and no `if`, so `false` reaches the process) | api |
| `config.orphanHoldGrace` | `ORPHAN_HOLD_GRACE` (default `"1h"`; `"0"` reaches the process too) | api |
| `config.corsAllowedOrigins` | `CORS_ALLOWED_ORIGINS` | api |
| `config.eventPublisher` | `EVENT_PUBLISHER` | api |
| `config.outboxMaxAttempts` | `OUTBOX_MAX_ATTEMPTS` (ConfigMap and env) | api |
| `kafka.enabled` + `kafka.brokers` | `KAFKA_BROKERS` | api, projector |
| `database.existingSecret` / `database.url` | `DATABASE_URL`, `MIGRATIONS_DATABASE_URL` (optional key) | api, mcp, a2a |
| `productTranslation.mappings` | `PRODUCT_TRANSLATION_FILE=/etc/network-fulfillment/products.json` | api |
| `stubDemand.demands` | `NETWORK_SEED_FILE=/etc/network-fulfillment/demand.json` (stub only; the render fails in live mode) | api |
| `otel.enabled`, `otel.endpoint`, `otel.serviceName`, `environment` | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `SERVICE_VERSION`, `ENVIRONMENT` | api, mcp, a2a, projector, reports |
| `mcp.httpAddr` | `MCP_ADDR` | mcp |
| `mcp.reportsBaseUrl` | `REPORTS_BASE_URL` | mcp |
| `a2a.httpAddr` | `A2A_ADDR` | a2a |
| `a2a.agentUrl` | `A2A_AGENT_URL` (empty: the in-cluster Service DNS) | a2a |
| `analytics.database.*` | `ANALYTICS_DATABASE_URL` | projector, reports |
| `analytics.projector.adminAddr` | `ADMIN_ADDR` | projector |
| `analytics.migrationsPath` | `ANALYTICS_MIGRATIONS_PATH` | projector |
| `analytics.reports.httpAddr` | `HTTP_ADDR` | reports |

The chart refuses to render what the binary would refuse at boot: `live` without
`config.networkBaseUrl` and `config.networkVendorId`, `stubDemand.demands` in live
mode, a `config.networkMode` other than `stub` or `live`, and a
`networkNodes.nodes` entry with an unknown or missing key. There is no
credentials block: the live adapter authenticates with nothing, and the old
`NETWORK_CLIENT_ID`, `NETWORK_CLIENT_SECRET` and `NETWORK_REFRESH_TOKEN`
variables, which no code ever read, are gone (a values file that still sets
`credentials.*` has them ignored). `charts/network-fulfillment/tests/test_env_parity.py`
compares the variables the chart renders with the ones the binary reads, in both
directions, for a stub and a live render, and CI runs it.

One row of that table sets variables only some processes read: the `OTEL_*`,
`SERVICE_VERSION` and `ENVIRONMENT` variables reach the mcp, projector and
reports pods, but only `cmd/netfulfil` calls `telemetry.Setup` (ADR 0019 scopes
the others out), so those three processes ignore them.

The mcp Deployment also sets `MIGRATIONS_PATH` from `config.migrationsPath`,
a key `values.yaml` does not define. It renders as an empty string, which the
binary treats as unset, so `/app/migrations` is used.
