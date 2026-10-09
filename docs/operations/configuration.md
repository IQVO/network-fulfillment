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
- **Booleans**: only `CAPABILITY_OFFER_ENABLED` is boolean. It is `true` only
  for the string `true` (case-insensitive, surrounding spaces trimmed).
- **`EVENT_PUBLISHER`** is compared exactly: only the lower-case string
  `kafka` enables Kafka. Any other value, including `Kafka`, keeps the log
  publisher.

## `cmd/netfulfil` (the OLTP service)

The REST API, the inbound poller, the four background tickers and the outbox
relay all run in this one process. Its env surface lives in
`cmd/netfulfil/config.go` plus the wiring files named below.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `PORT` | `8080` | no | HTTP listen port. The server listens on `:<PORT>`. | `cmd/netfulfil/config.go` (`addr`) |
| `DATABASE_URL` | unset | no | Postgres DSN for the request-serving pool. Unset: in-memory repositories (state is lost on restart). Set: Postgres, or the process **refuses to boot** after the retry budget. It never falls back to memory. | `cmd/netfulfil/database.go` (`wireOrders`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct (non-PgBouncer) DSN used only for `golang-migrate` at startup (ADR 0007). The runtime pool always uses `DATABASE_URL`. | `cmd/netfulfil/database.go` (`migrationsURL`) |
| `MIGRATIONS_PATH` | `/app/migrations` | no | Directory of the OLTP `*.up.sql` files. Set it to `migrations` for a local run from the repo root. | `cmd/netfulfil/config.go` (`migrationsPath`) |
| `NETWORK_MODE` | `stub` | no | Which network gateway is wired. `live` (case-insensitive) selects the live gateway, which **is not implemented**: boot fails with `NETWORK_MODE=live is not implemented yet`. Every other value, including the retired `sandbox`, means `stub`. The mode in force is logged at startup and echoed by `GET /inbound-status`. | `cmd/netfulfil/wiring.go` (`wireGateway`), `internal/adapters/outbound/network/gateway.go` (`ParseMode`) |
| `NETWORK_BASE_URL` | unset | only when `NETWORK_MODE=live` | Live-mode target (ADR 0009 §4). With `live` and no value, boot fails with `NETWORK_MODE=live requires NETWORK_BASE_URL to be set`. Ignored in stub mode. | `cmd/netfulfil/wiring.go` |
| `NETWORK_SEED_FILE` | unset | no | JSON file of stub demand, `{"demands":[{"networkRef","siteId","requiredShipBy","lines":[{"networkLineRef","networkProductId","quantity"}]}]}`. `requiredShipBy` is RFC 3339 or a relative duration such as `+36h`. Unknown keys fail boot. Setting it against a non-stub gateway fails boot (ADR 0012). | `cmd/netfulfil/wiring.go` (`seedStubDemand`), `internal/adapters/outbound/network/seed.go` |
| `PRODUCT_TRANSLATION_FILE` | unset | no, but see meaning | The Anti-Corruption Layer dictionary, `{"products":[{"networkProductId","sku"}]}`. Unknown keys, empty fields and a product mapped twice fail boot. Unset: the dictionary is empty, a `WARN` is logged, and **every** network order is rejected as `UNTRANSLATABLE_SKU` (ADR 0013). | `cmd/netfulfil/wiring.go` (`loadProductTranslation`), `internal/adapters/outbound/memory/translation_file.go` |
| `ORDER_MANAGEMENT_URL` | `http://localhost:8080` | yes in any real deployment | Base URL for the held-order calls `POST /orders`, `POST /orders/{id}/release`, `DELETE /orders/{id}`. | `cmd/netfulfil/wiring.go` (`wirePlanner`) |
| `POLL_INTERVAL` | `1m` | no | Cadence of the inbound poller **and** of `ReconcileSubmittedOrders`. Must be positive (see above). | `cmd/netfulfil/config.go` (`pollInterval`), `cmd/netfulfil/workers.go` |
| `SWEEP_INTERVAL` | `1m` | no | Cadence of `SweepAcknowledgementDeadlines` and of `RejectOverdueOrders` (two separate tickers). Must be positive. The chart sets `5m`. | `cmd/netfulfil/config.go` (`sweepInterval`), `cmd/netfulfil/workers.go` |
| `EVENT_PUBLISHER` | unset (log) | no | `kafka` publishes every domain event to both topics. With `DATABASE_URL` set this goes through the transactional outbox and starts the relay (ADR 0003); without it both topics are written directly. Anything else logs events only. | `cmd/netfulfil/publishing.go` (`wireEventPublisher`) |
| `KAFKA_BROKERS` | `localhost:9092` | when Kafka is used | Comma-separated broker list for the two publishers and the two capability-offer caches. The caches read their start-up offsets from the first broker only. | `cmd/netfulfil/config.go` (`kafkaBrokers`), `internal/adapters/outbound/processpathcache/consumer.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Sleep between outbox relay passes that found nothing to publish (outbox mode only). A full batch of 100 is followed immediately by another pass. | `cmd/netfulfil/publishing.go`, `internal/adapters/outbound/postgres/outbox_relay.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5188` | no | Comma-separated browser origins. Methods `GET`, `POST`, `OPTIONS`; headers `Content-Type`, `Authorization`; no credentials; preflight cached 300 s. | `internal/adapters/inbound/http/server.go` (`corsMiddleware`) |
| `CAPABILITY_OFFER_ENABLED` | `false` | no | `true` wires the CapabilityOffer pipeline: the two Kafka caches, the inventory-storage client, the `RECOMPUTE_INTERVAL` ticker and `GET /capability-offers`. Boot then blocks until both caches have replayed their topic (one shared wait of 120 s, the sum of the two caches' 60 s `WaitReadyTimeout`s). | `cmd/netfulfil/config.go`, `cmd/netfulfil/capability.go` |
| `SITE_ID` | `site-1` | no | The single site capability offers are computed for (the CPT schedule is looked up by this id). | `cmd/netfulfil/config.go` (`siteId`) |
| `RECOMPUTE_INTERVAL` | `1m` | no | Cadence of `RecomputeCapabilityOffers`. Only used when the pipeline is enabled. | `cmd/netfulfil/config.go` (`recomputeInterval`) |
| `INVENTORY_STORAGE_URL` | `http://localhost:8080` | when the pipeline is enabled | Base URL for `GET /inventory/{sku}/usable`. | `cmd/netfulfil/capability.go` (`wireInventoryClient`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC `host:port` of the Collector for traces and metrics (insecure, dialled lazily). An unreachable Collector drops telemetry and never blocks boot (ADR 0019). | `cmd/netfulfil/main.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `OTEL_SERVICE_NAME` | `network-fulfillment` | no | `service.name` resource attribute. | `cmd/netfulfil/main.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. The chart sets it to the image tag. | `cmd/netfulfil/main.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` (`Environment`) |

The process logs JSON to stdout at the default `slog` level (Info). It has no
`LOG_LEVEL` variable.

## `cmd/mcp` (the MCP server)

A separate, read-only deployable that serves MCP over Streamable HTTP at `/`
and `/mcp`, plus `GET /healthz`. See [ecosystem/integration.md](../ecosystem/integration.md#mcp-tools)
for the tool list.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address. | `cmd/mcp/main.go` |
| `DATABASE_URL` | unset | yes in any real deployment | Postgres DSN of the **same** OLTP database `netfulfil` writes. Unset: an in-memory repository private to this process, so every tool sees an empty store. | `cmd/mcp/main.go` (`buildOrders`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step this binary also runs at startup (ADR 0007). Unlike `netfulfil`, this step is **not retried**. | `cmd/mcp/main.go` |
| `MIGRATIONS_PATH` | `/app/migrations` | no | OLTP migrations directory. | `cmd/mcp/main.go` |
| `REPORTS_BASE_URL` | unset | no | Base URL of `netfulfil-reports`. Unset: the `get_acknowledgement_report` tool is not registered. The chart fills it in with the in-cluster reports Service when `analytics.enabled=true`. | `cmd/mcp/main.go` |

`list_capability_offers` is always registered by this binary: it reads the
`capability_offers` table, which stays empty unless `netfulfil` runs with
`CAPABILITY_OFFER_ENABLED=true`.

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
| order-management breaker | trips on 5 consecutive failures, or >50 % failures over at least 10 requests in a 30 s window; open for 30 s; 1 half-open probe | `internal/resilience/breaker.go` |
| order-management call timeout | 30 s cap per call (`resilience.DefaultTimeout`), inside a 10 s `http.Client` timeout | `internal/adapters/outbound/ordermanagement/breaker.go`, `planner.go` |
| inventory-storage call timeout | 10 s `http.Client` timeout | `internal/adapters/outbound/inventoryclient/client.go` |
| Outbox relay batch | 100 rows per pass | `internal/adapters/outbound/postgres/outbox_relay.go` |
| Cache replay wait | 60 s per cache, applied as one 120 s deadline for both | `processpathcache.WaitReadyTimeout`, `pathcapacitycache.WaitReadyTimeout`, `cmd/netfulfil/capability.go` (`waitCachesReady`) |
| OTel metric push | every 30 s | `internal/adapters/outbound/telemetry/telemetry.go` |
| Graceful shutdown budget | 10 s | `cmd/netfulfil/shutdown.go`, and each other `main.go` |

## Helm chart values to environment

`charts/network-fulfillment` sets the variables below. Anything without a
values key (`CAPABILITY_OFFER_ENABLED`, `SITE_ID`, `RECOMPUTE_INTERVAL`,
`INVENTORY_STORAGE_URL`, `OUTBOX_RELAY_INTERVAL`) can only be set with
`extraEnv` (API Deployment) or `mcp.extraEnv`.

| Values key | Env var | Deployment |
| --- | --- | --- |
| `config.port` | `PORT` | api |
| `config.networkMode` | `NETWORK_MODE` | api |
| `config.networkBaseUrl` | `NETWORK_BASE_URL` (only when non-empty) | api |
| `config.orderManagementUrl` | `ORDER_MANAGEMENT_URL` | api |
| `config.pollInterval` | `POLL_INTERVAL` (ConfigMap) | api |
| `config.sweepInterval` | `SWEEP_INTERVAL` | api |
| `config.corsAllowedOrigins` | `CORS_ALLOWED_ORIGINS` | api |
| `config.eventPublisher` | `EVENT_PUBLISHER` | api |
| `kafka.enabled` + `kafka.brokers` | `KAFKA_BROKERS` | api, projector |
| `database.existingSecret` / `database.url` | `DATABASE_URL`, `MIGRATIONS_DATABASE_URL` (optional key) | api, mcp |
| `productTranslation.mappings` | `PRODUCT_TRANSLATION_FILE=/etc/network-fulfillment/products.json` | api |
| `stubDemand.demands` | `NETWORK_SEED_FILE=/etc/network-fulfillment/demand.json` | api |
| `otel.enabled`, `otel.endpoint`, `otel.serviceName`, `environment` | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `SERVICE_VERSION`, `ENVIRONMENT` | api, mcp, projector, reports |
| `mcp.httpAddr` | `MCP_ADDR` | mcp |
| `mcp.reportsBaseUrl` | `REPORTS_BASE_URL` | mcp |
| `analytics.database.*` | `ANALYTICS_DATABASE_URL` | projector, reports |
| `analytics.projector.adminAddr` | `ADMIN_ADDR` | projector |
| `analytics.migrationsPath` | `ANALYTICS_MIGRATIONS_PATH` | projector |
| `analytics.reports.httpAddr` | `HTTP_ADDR` | reports |
| `credentials.*` (non-stub mode only) | `NETWORK_CLIENT_ID`, `NETWORK_CLIENT_SECRET`, `NETWORK_REFRESH_TOKEN` | api |

Two rows in that table set variables no Go code reads today:

- `NETWORK_CLIENT_ID`, `NETWORK_CLIENT_SECRET` and `NETWORK_REFRESH_TOKEN` are
  mounted only outside stub mode, for a live adapter that does not exist yet.
- The `OTEL_*`, `SERVICE_VERSION` and `ENVIRONMENT` variables reach the mcp,
  projector and reports pods, but only `cmd/netfulfil` calls
  `telemetry.Setup` (ADR 0019 scopes the others out), so those three
  processes ignore them.

The mcp Deployment also sets `MIGRATIONS_PATH` from `config.migrationsPath`,
a key `values.yaml` does not define. It renders as an empty string, which the
binary treats as unset, so `/app/migrations` is used.
