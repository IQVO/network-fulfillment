---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
---

# Troubleshooting

Symptom, cause, check, fix. Every row comes from a failure path in the code
on `develop` (file named in the cause) or from a scenario tagged
`@known-bug` in `features/` (none is tagged today). Commands assume the kind cluster; replace
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
| Exit 1 with `cannot wire order repository` | Postgres is unreachable or rejects the login after 5 attempts (`cmd/netfulfil/database.go`). There is no in-memory fallback (ADR 0025). | The `err` field: `connection refused`, `password authentication failed`, ... | Fix the DSN or the database. In kind, a role added to an already-initialised Postgres was never created: create it by hand as `warehouse-infra`'s `terraform/network-fulfillment.tf` describes. |
| Several replicas crash-loop at the same time with `pq: unnamed prepared statement does not exist` or a statement timeout during migrations | Migrations run through PgBouncer in transaction mode, where `golang-migrate`'s advisory lock does not hold (ADR 0007). | Is `MIGRATIONS_DATABASE_URL` set in the pod? (`optional: true` in the chart, so a Secret without the key is silent.) | Add a direct DSN under `MIGRATIONS_DATABASE_URL` in the database Secret. |
| Exit 1 with `invalid configuration` naming `NETWORK_BASE_URL` and/or `NETWORK_VENDOR_ID` | `NETWORK_MODE=live` without them (one error names every missing one; `cmd/netfulfil/config.go`) | The log's `error` | Set both, or use `stub`. |
| Exit 1 with `cannot wire network gateway` | `NETWORK_MODE=live` and the gateway refused its configuration (for example a malformed `NETWORK_BASE_URL`). `internal/adapters/outbound/retailnetwork/client.go` | The log's `mode` and `err` | Fix the value. |
| Exit 1 with `cannot load network node definitions` | `NETWORK_NODES_FILE` (live only) is unreadable, has an unknown key, a missing field, a lone `lat`/`lon` or a site listed twice (`internal/adapters/outbound/retailnetwork/nodes.go`) | The `err` names the node and field | Fix the file or `networkNodes.nodes`. |
| `WARN purchase order skipped, its node is not mapped to any of our sites` (live) | The network issued a purchase order for a node that is neither adopted nor registered for one of our sites. The order is read again only on the next boot and is otherwise expired by the network at `ackBy` | `nodeId`, `poNumber` | [Purchase orders that are skipped](runbook.md#the-live-retail-network-adapter) |
| `WARN node profile: no cutoff schedule in the cache` or `no eligible path has a cycle time that fits the schedule` / `no capacity figure is known` (live) | the node profile is built only from what the local caches know, so the site is skipped (`skipped_*` on `network_fulfillment.node_profile.declarations`) and nothing is declared. The network keeps whatever profile it had | The `site` and the reason in the line | [The node profile](runbook.md#the-node-profile) |
| `ERROR the network failed a node profile declaration` (live) | the profile transaction failed; `errors` is the network's own list | `site`, `revision`, `errors` | [The node profile](runbook.md#the-node-profile) |
| Exit 1 with `cannot load stub demand` | `NETWORK_SEED_FILE` unreadable, has an unknown key, a bad `requiredShipBy`, or is set while the gateway is not the stub (`internal/adapters/outbound/retailnetwork/seed.go`) | The `err` names the field or demand index | Fix the file or `stubDemand.demands`. |
| Exit 1 with `cannot load product translation` | `PRODUCT_TRANSLATION_FILE` has an unknown key, an empty field, or the same `networkProductId` twice (`internal/adapters/outbound/memory/translation_file.go`) | The `err` names the product index | Fix the mapping. |
| Pod never Ready, logs stop after `waiting for capability-offer caches to replay ...`, then exit 1 with `... cache did not become ready within 1m0s` | a capability-offer cache could not replay its topic within the shared 120 s wait (`cmd/netfulfil/capability.go`). The listener, and so `/readyz`, starts only after that. | Can the pod reach the **first** broker in `KAFKA_BROKERS`? The readiness target is read from it alone. | Fix `KAFKA_BROKERS` or the broker. |
| Process panics `non-positive interval for NewTicker` right after start | `POLL_INTERVAL` or `SWEEP_INTERVAL` is `0` or negative. Those two parsers accept any valid duration (`cmd/netfulfil/config.go`). | The env values | Use a positive duration. |
| `/readyz` returns 503 | Only happens after `SIGTERM`: shutdown flips it first (`internal/adapters/inbound/http/readiness.go`). | Is the pod terminating? | Expected. |
| Exit 1 with `ANALYTICS_DATABASE_URL is required` | projector or reports started without the analytical DSN | `analytics.database.existingSecret` / `url` | Set it. |

## Network orders

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Every order ends `REJECTED`, report shows `ordersRejectedUntranslatableSku` ≈ `ordersReceived` | No product dictionary (empty `productTranslation.mappings`), or the product id is not in it. One unknown product rejects the whole order (`ReceiveNetworkDemand.translate`). | Boot log `no PRODUCT_TRANSLATION_FILE set ...`; the order's lines via `GET /network-orders/{ref}` (an untranslatable order has none) | Add the mapping and restart the pod (the file is read once at boot, see the runbook). |
| Orders stay `NEW`, `/inbound-status` `failed` grows, logs `receive network demand failed ... raise held order: order-management POST /orders: status <n>` | order-management refused or failed the raise (`n` >= 300), or the breaker is open (next row). Nothing was held by a refused raise, so nothing is leaked. The poll finds the order still `NEW` inside its window and runs the raise again (`ReceiveNetworkDemand` resume); the raise carries the stable `Idempotency-Key` `netful-raise-<networkRef>`, so order-management replays the same order instead of reserving stock twice (`internal/adapters/outbound/ordermanagement/planner.go`). | The status `n` in the error; order-management's own logs | Fix order-management. The next poll answers the order, as long as its 24 h window is open. |
| Stock stays reserved in order-management for a network order that was rejected, or never answered | The hold leaked: the cancel failed, or the order stayed `NEW` past `acknowledgeBy`. Since ADR 0024 the hold id is recorded the moment it is raised and `SweepOrphanedHolds` cancels it once `acknowledgeBy + ORPHAN_HOLD_GRACE` has passed. | The open-holds query in [A held order is stuck in order-management](./runbook.md#a-held-order-is-stuck-in-order-management); `GET /network-orders/{ref}` does not show the hold id of a `NEW` or `REJECTED` order | Wait `ORPHAN_HOLD_GRACE + SWEEP_INTERVAL` after the window closes, or follow the runbook. |
| WARN `orphaned holds swept` repeats with `failed` > 0, counter `network_fulfillment.orphaned_holds.swept{outcome="failed"}` grows | order-management answered the cancel (`DELETE /orders/{id}`) with something other than 2xx, 404, 409 or 422, did not answer, or the breaker is open. The hold stays open and is retried every `SWEEP_INTERVAL`; the sweep logs counts, not the cause. | `GET /metrics`: `circuit_breaker_state{dependency="order-management"}`; order-management logs for `DELETE /orders/{localOrderId}` | Fix order-management; no action here. For one stubborn hold, cancel it there by hand. |
| An order is past `acknowledgeBy + ORPHAN_HOLD_GRACE + SWEEP_INTERVAL`, its hold is still held, and no `orphaned holds swept` line appears | The sweep is not acting on it: switched off (boot WARN `orphaned-hold sweep disabled by ORPHAN_HOLD_SWEEP_ENABLED`), the order is `SUBMITTED`, `ACKNOWLEDGED` or `CONFIRMED` (never swept), or the row has no recorded id (a raise lost before it was recorded, or an order rejected before migration `0005`). | `SELECT state, local_order_id, hold_outcome FROM network_orders WHERE network_ref = '<ref>';` | See the runbook's "Holds the sweep cannot see". |
| `orphaned holds swept` shows `alreadyReleased` > 0 | order-management answered the cancel with 409 or 422: the order had been released, so its stock is committed to the floor. Not an orphan: the outcome is recorded as `ALREADY_RELEASED`, no event is published and it is never retried. | `hold_outcome = 'ALREADY_RELEASED'` on the row | Decide in order-management whether that order should ship. |
| `failed` grows with `order-management: circuit breaker open, call not attempted` | Five consecutive order-management failures (or >50 % of at least 10 calls in 30 s) opened the breaker (`internal/resilience/breaker.go`). No fallback: the call fails. | `GET /metrics`: `circuit_breaker_state{dependency="order-management"} 2` | Fix order-management. After 30 s one probe call is let through; success closes the breaker. |
| `polls` grows, `received` stays 0, `since` advances | The network has nothing for us. In stub mode the gateway only returns what `NETWORK_SEED_FILE` seeded, and drops each unit once it is answered. | `networkMode` in `/inbound-status`; boot log `stub demand seeded` | Expected. Seed more demand to test. |
| `since` absent minutes after start | No poll pass has fully succeeded: either `PollDemand` fails (`poll demand failed`) or at least one unit fails every pass. | ERROR lines `poll demand failed` / `receive network demand failed` | Fix the failing unit or gateway; the watermark advances on the first clean pass. |
| Orders stuck in `SUBMITTED` after a pod restart (stub mode, Postgres on) | The stub gateway records submissions in process memory. After a restart it answers `PENDING` for every earlier submission, and nothing else moves a `SUBMITTED` order: the sweeps and the overdue rejection only look at `NEW` (and the orphaned-hold sweep at `NEW` or `REJECTED`, never `SUBMITTED`, because the network is the only judge of its own record) (`internal/adapters/outbound/retailnetwork/stub.go`, `reject_overdue_orders.go`, `networkorder/hold.go`). | `list_network_orders` with `state=SUBMITTED`; reconcile log `pending` | No automatic recovery in stub mode. A live gateway would answer from the network's own record. |
| Order is `ACKNOWLEDGED` but the work never reached the floor | `ReconcileSubmittedOrders.confirm` commits `ACKNOWLEDGED` first, then calls `POST /orders/{id}/release`. If that call fails, the error is dropped without a log line and the order is no longer `SUBMITTED`, so it is not retried. The orphaned-hold sweep never touches an `ACKNOWLEDGED` order (a cancel would break the commitment made to the network), so this is the residual hold of ADR 0024. | order-management: the order with the network order's `localOrderId` is still held (allocated, not released) | Release it in order-management: `POST /orders/{localOrderId}/release`. |
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
| 422 `unknown-product`, `empty-network-ref`, `empty-network-line-ref`, `empty-network-product-id`, `non-positive-quantity`, `order-without-lines` | mapped in `errors.go`. Only `empty-network-ref` is reachable: `POST /network-orders//shipment-confirmation` answers it (`features/shipment_confirmation.feature`). The others have no route today, because no route takes demand over HTTP. | none needed |
| 400 `invalid-report-query` | reports API: `from`/`to` missing or not RFC 3339, or `granularity` other than `day` | fix the query |
| 500 `report-store-error` | reports API: the analytical database failed | check the reports pod and `ANALYTICS_DATABASE_URL` |

There is no `412` (no `ETag`/`If-Match` on any route) and no `Idempotency-Key`
handling on this service's own routes.

## Events, outbox and analytics

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| No events on `warehouse.network-fulfillment.*` | The outbox relay is not draining (`outbox_events` rows pile up unsent) or the broker is unreachable (`/readyz` 503 `dependency-unavailable` naming kafka). | `SELECT count(*) FROM outbox_events WHERE sent_at IS NULL`; relay ERROR logs; `/readyz` | Fix `KAFKA_BROKERS` or the broker. |
| Outbox backlog: `outbox_events` rows with `published_at IS NULL` grow, `last_error` set | The relay cannot send (broker down, auth, topic). It stops each pass at the first failing row to keep order, until that row reaches `OUTBOX_MAX_ATTEMPTS` (default 20; `0` = forever). | `SELECT id, topic, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 20;` and WARN `outbox relay: send failed, stopping this pass at the failed row` | Fix Kafka; the relay drains on its own. |
| ERROR `outbox relay: row reached the attempt cap and is excluded from future claims` | A row failed `OUTBOX_MAX_ATTEMPTS` sends ([ADR 0021](../adr/0021-outbox-relay-attempt-cap.md)); later rows, including ones for the same `networkRef`, now overtake it and consumers never got it. | `SELECT id, topic, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= 20;` | Fix the cause, then `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` ([runbook](./runbook.md#an-outbox-row-hit-the-attempt-cap)). |
| Analytics report empty | Nothing reaches the analytics topic (row above), or the projector's consumer stopped. | Projector log `analytics consumer stopped`; the process keeps answering `/healthz` 200 after that | Restart the projector pod. |
| `warehouse.network-fulfillment.analytics.dlq` grows | Non-CloudEvents messages, or the projection failed 3 times (analytical DB down, statement timeout 10 s) | DLQ headers `x-dlq-error`, projector WARN lines with `phase` | Fix the cause, then replay as the runbook describes. |
| `lagSeconds` keeps growing | No event has been projected since that time: either nothing was published (quiet network) or the projector stopped | Compare with `/inbound-status` `received` | Only act when orders are arriving. |
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
| Tools return no orders while the API has some | the mcp pod points at a different database than the API | give it the OLTP Secret (`database.existingSecret`) |
| `session not found` style errors with more than one mcp replica | sessions live in one pod's memory and the Service has no affinity | keep `mcp.replicaCount: 1` |
