---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
---

# Troubleshooting

Symptom, cause, check, fix. Every row comes from a failure path in the code
on `develop` (file named in the cause) or from a scenario tagged
`@known-bug` in `features/`. Commands assume the kind cluster; replace
`<ns>` with the apps namespace.

Quick checks first:

```bash
kubectl -n <ns> logs deploy/network-fulfillment | tail -50
curl -s http://localhost:8000/api/network-fulfillment/inbound-status
curl -s http://localhost:8000/api/network-fulfillment/network-orders
```

## Boot and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod restarts with exit code 143 while booting | The kubelet killed a pod that had not started listening yet. Migrations and the DB ping run before the listener, and the first dial in an Istio pod is reset about 10 s after start (ADR 0011). | `kubectl describe pod`: liveness/startup probe failures; logs show `retrying op="run migrations"` | Keep the chart's `startupProbe` (60 s). Do not shorten it below the boot retry (15 s of waits plus attempt time). |
| Exit 1 with `cannot wire order repository` | `DATABASE_URL` is set but Postgres is unreachable or rejects the login after 5 attempts (`cmd/netfulfil/database.go`). The service never falls back to memory. | The `err` field: `connection refused`, `password authentication failed`, ... | Fix the DSN or the database. In kind, a role added to an already-initialised Postgres was never created: create it by hand as `warehouse-infra`'s `terraform/network-fulfillment.tf` describes. |
| Several replicas crash-loop at the same time with `pq: unnamed prepared statement does not exist` or a statement timeout during migrations | Migrations run through PgBouncer in transaction mode, where `golang-migrate`'s advisory lock does not hold (ADR 0007). | Is `MIGRATIONS_DATABASE_URL` set in the pod? (`optional: true` in the chart, so a Secret without the key is silent.) | Add a direct DSN under `MIGRATIONS_DATABASE_URL` in the database Secret. |
| Exit 1 with `cannot wire network gateway` | `NETWORK_MODE=live`: either `NETWORK_BASE_URL` is empty, or the live adapter (`not implemented yet`). `internal/adapters/outbound/network/gateway.go` | The log's `mode` and `err` | Use `stub`. No live adapter exists on `develop`. |
| Exit 1 with `cannot load stub demand` | `NETWORK_SEED_FILE` unreadable, has an unknown key, a bad `requiredShipBy`, or is set while the gateway is not the stub (`internal/adapters/outbound/network/seed.go`) | The `err` names the field or demand index | Fix the file or `stubDemand.demands`. |
| Exit 1 with `cannot load product translation` | `PRODUCT_TRANSLATION_FILE` has an unknown key, an empty field, or the same `networkProductId` twice (`internal/adapters/outbound/memory/translation_file.go`) | The `err` names the product index | Fix the mapping. |
| Pod never Ready, logs stop after `waiting for capability-offer caches to replay ...`, then exit 1 with `... cache did not become ready within 1m0s` | `CAPABILITY_OFFER_ENABLED=true` and a cache could not replay its topic within the shared 120 s wait (`cmd/netfulfil/capability.go`). The listener, and so `/readyz`, starts only after that. | Can the pod reach the **first** broker in `KAFKA_BROKERS`? The readiness target is read from it alone. | Fix `KAFKA_BROKERS` or the broker. Unset `CAPABILITY_OFFER_ENABLED` to boot without the pipeline. |
| Process panics `non-positive interval for NewTicker` right after start | `POLL_INTERVAL` or `SWEEP_INTERVAL` is `0` or negative. Those two parsers accept any valid duration (`cmd/netfulfil/config.go`). | The env values | Use a positive duration. |
| `/readyz` returns 503 | Only happens after `SIGTERM`: shutdown flips it first (`internal/adapters/inbound/http/readiness.go`). | Is the pod terminating? | Expected. |
| Exit 1 with `ANALYTICS_DATABASE_URL is required` | projector or reports started without the analytical DSN | `analytics.database.existingSecret` / `url` | Set it. |

## Network orders

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Every order ends `REJECTED`, report shows `ordersRejectedUntranslatableSku` ≈ `ordersReceived` | No product dictionary (empty `productTranslation.mappings`), or the product id is not in it. One unknown product rejects the whole order (`ReceiveNetworkDemand.translate`). | Boot log `no PRODUCT_TRANSLATION_FILE set ...`; the order's lines via `GET /network-orders/{ref}` (an untranslatable order has none) | Add the mapping and restart the pod (the file is read once at boot, see the runbook). |
| Orders stay `NEW`, `/inbound-status` `failed` grows, logs `receive network demand failed ... raise held order: order-management POST /orders: status 400` | order-management's `POST /orders` requires an `Idempotency-Key` header (its PR #105) and this client sends none on `develop` (`internal/adapters/outbound/ordermanagement/planner.go`). | Response status 400 in the error; order-management logs `idempotency-key-required` | Fix pending in this repo's PR #63, which sends `netful-raise-<networkRef>`. |
| An order that failed while order-management was down stays `NEW` after order-management recovers, then is rejected 24 h later as `ACKNOWLEDGEMENT_DEADLINE_MISSED` | The order was saved `NEW` before the hold call failed. On the next poll `ReceiveNetworkDemand` finds it and returns early, so it is never answered (`receive_network_demand.go`). Recorded as `@known-bug` in `features/network_order_intake.feature`. | `GET /network-orders/{ref}`: `state` `NEW`, no `localOrderId` | No automatic recovery on `develop`. `RejectOverdueOrders` frees it after the window. |
| `failed` grows with `order-management: circuit breaker open, call not attempted` | Five consecutive order-management failures (or >50 % of at least 10 calls in 30 s) opened the breaker (`internal/resilience/breaker.go`). No fallback: the call fails. | `GET /metrics`: `circuit_breaker_state{dependency="order-management"} 2` | Fix order-management. After 30 s one probe call is let through; success closes the breaker. |
| `polls` grows, `received` stays 0, `since` advances | The network has nothing for us. In stub mode the gateway only returns what `NETWORK_SEED_FILE` seeded, and drops each unit once it is answered. | `networkMode` in `/inbound-status`; boot log `stub demand seeded` | Expected. Seed more demand to test. |
| `since` absent minutes after start | No poll pass has fully succeeded: either `PollDemand` fails (`poll demand failed`) or at least one unit fails every pass. | ERROR lines `poll demand failed` / `receive network demand failed` | Fix the failing unit or gateway; the watermark advances on the first clean pass. |
| Orders stuck in `SUBMITTED` after a pod restart (stub mode, Postgres on) | The stub gateway records submissions in process memory. After a restart it answers `PENDING` for every earlier submission, and nothing else moves a `SUBMITTED` order: the sweep and the overdue rejection only look at `NEW` (`internal/adapters/outbound/network/gateway.go`, `reject_overdue_orders.go`). | `list_network_orders` with `state=SUBMITTED`; reconcile log `pending` | No automatic recovery in stub mode. A live gateway would answer from the network's own record. |
| Order is `ACKNOWLEDGED` but the work never reached the floor | `ReconcileSubmittedOrders.confirm` commits `ACKNOWLEDGED` first, then calls `POST /orders/{id}/release`. If that call fails, the error is dropped without a log line and the order is no longer `SUBMITTED`, so it is not retried. | order-management: the order with the network order's `localOrderId` is still held (allocated, not released) | Release it in order-management: `POST /orders/{localOrderId}/release`. |
| Many `AcknowledgementDeadlineAtRisk` events for the same order | By design: the sweep re-publishes it on every pass while the order stays overdue and `NEW`. | | Consumers must treat it as level-triggered. `RejectOverdueOrders` rejects the order on its own ticker. |

## REST errors

All errors are RFC 7807 `application/problem+json` with `type`
`https://errors.network-fulfillment.warehouse-systems.dev/<slug>`
(`internal/adapters/inbound/http/errors.go`).

| Status and slug | When | Fix |
| --- | --- | --- |
| 404 `network-order-not-found` | unknown `networkRef` on `GET /network-orders/{networkRef}` or the shipment confirmation | check the ref (`GET /network-orders` lists only `NEW` ones; use the MCP `list_network_orders` for every state) |
| 409 `confirm-before-acknowledge` | `POST .../shipment-confirmation` on an order that is not `ACKNOWLEDGED` | wait for reconciliation to settle it; `REJECTED` orders can never be confirmed |
| 500 `internal-error` on shipment confirmation | the confirmation committed (`CONFIRMED`, event enqueued) but the gateway call failed afterwards (`confirm_network_order_shipment.go`) | a retry returns 204 without calling the gateway again, because the order is already `CONFIRMED`; with a live gateway, re-submit out of band |
| 422 `unknown-product`, `empty-network-ref`, `empty-network-line-ref`, `empty-network-product-id`, `non-positive-quantity`, `order-without-lines` | mapped in `errors.go`, but no current route takes demand over HTTP. `empty-network-ref` is unreachable: `ServeMux` answers `//` with a 307 redirect first (`@known-bug` in `features/shipment_confirmation.feature`). | none needed |
| 400 `invalid-report-query` | reports API: `from`/`to` missing or not RFC 3339, or `granularity` other than `day` | fix the query |
| 500 `report-store-error` | reports API: the analytical database failed | check the reports pod and `ANALYTICS_DATABASE_URL` |
| plain-text 404 on `GET /capability-offers` | `CAPABILITY_OFFER_ENABLED` is not `true`, so the route is not registered | enable the pipeline |

There is no `412` (no `ETag`/`If-Match` on any route) and no `Idempotency-Key`
handling on this service's own routes.

## Events, outbox and analytics

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| No events on `warehouse.network-fulfillment.*` | `EVENT_PUBLISHER` is not exactly `kafka`. The kind overlay on `warehouse-infra` `develop` sets neither `kafka.enabled` nor `config.eventPublisher`. | Boot log: no `event publisher configured` line | Set `kafka.enabled=true` and `config.eventPublisher=kafka`. |
| Outbox backlog: `outbox_events` rows with `published_at IS NULL` grow, `last_error` set | The relay cannot send (broker down, auth, topic). It stops each pass at the first failing row to keep order. | `SELECT id, topic, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 20;` and ERROR `outbox relay pass failed` | Fix Kafka; the relay drains on its own. |
| Analytics report empty | Nothing reaches the analytics topic (row above), or the projector's consumer stopped. | Projector log `analytics consumer stopped`; the process keeps answering `/healthz` 200 after that | Restart the projector pod. |
| `warehouse.network-fulfillment.analytics.dlq` grows | Non-CloudEvents messages, or the projection failed 3 times (analytical DB down, statement timeout 10 s) | DLQ headers `x-dlq-error`, projector WARN lines with `phase` | Fix the cause, then replay as the runbook describes. |
| `lagSeconds` keeps growing | No event has been projected since that time: either nothing was published (quiet network, or log publisher) or the projector stopped | Compare with `/inbound-status` `received` | Only act when orders are arriving. |
| Report counts look low after an upgrade | `orders_rejected_submission_failed` (migration `0002`) is not back-filled | | Rebuild the read model (runbook). |

## Capability offers

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Every offer is `PHYSICAL` | No CPT schedule cached for `SITE_ID`, or no capacity figure observed for any eligible path at that exact `cutoffAt` (`recompute_capability_offers.go`). Unknown capacity falls back to physical stock. | Is `SITE_ID` the site process-path-management publishes `CPTScheduleChanged` for? Does wes-work-planning publish `PathCapacityChanged` for the same `cutoff_at`? | Align `SITE_ID`; capacity is matched on the exact cutoff instant. |
| Offers `THROUGHPUT_CONSTRAINED` with quantity 0 | Every eligible path's `CycleTimeP95` exceeds the time left to the cutoff (ADR 0017), which is a proven zero. | path cycle times on `warehouse.process-path-management.events` | Expected near a cutoff. |
| Cutoffs look shifted by the site's UTC offset | The cache treats a cutoff's `local_time` as UTC; the schedule `timezone` is not applied (documented simplification in `processpathcache/consumer.go`). | | Known limitation. |
| SKU missing from the offers | Only SKUs in the product dictionary are recomputed, and a failed inventory-storage call skips that SKU for the pass. | ERROR `capability offer: usable inventory lookup failed` | Fix `INVENTORY_STORAGE_URL` or inventory-storage. |

## MCP

| Symptom | Cause | Fix |
| --- | --- | --- |
| `get_acknowledgement_report` not in `tools/list` | `REPORTS_BASE_URL` unset (the chart sets it only with `analytics.enabled` or `mcp.reportsBaseUrl`) | set one of them |
| Tools return no orders while the API has some | the mcp pod has no `DATABASE_URL` and reads its own empty in-memory store | give it the OLTP Secret (`database.existingSecret`) |
| `session not found` style errors with more than one mcp replica | sessions live in one pod's memory and the Service has no affinity | keep `mcp.replicaCount: 1` |
