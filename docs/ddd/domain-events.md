# Domain events

Every event this context publishes and consumes, read from code
(`internal/domain/shared/events.go`, `internal/adapters/outbound/kafka/`,
`internal/adapters/kafka/cloudevents/`, the three consumers) and checked
against `apis/asyncapi.yaml`.

## Envelope

Every Kafka message is a **CloudEvents 1.0 event in structured content
mode** (ADR 0008), built only by `internal/adapters/kafka/cloudevents.New`:

| Attribute | Value |
| --- | --- |
| `specversion` | `1.0` |
| `id` | a UUID minted once per encoded event. In outbox mode the encoded bytes are persisted, so a redelivery carries the same `id` |
| `source` | `/warehouse/network-fulfillment` |
| `type` | `com.warehouse.wes.network-fulfillment.networkorder.<EventName>` (entity `networkorder` for every event); a breaking change adds a `.v2` suffix: `...NetworkOrderAcknowledged.v2` (ADR 0016) |
| `subject` | the `networkRef` |
| `time` | the event's own `At`, in UTC |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:network-fulfillment:<events\|analytics>:<EventName>:v<N>`: `v1` for every event except `NetworkOrderAcknowledged`, which is `v2` |
| Kafka header | `content-type: application/cloudevents+json; charset=UTF-8` |
| Kafka key | `networkRef`, with the `kafkago.Hash` balancer, so one order's events share a partition (ADR 0005) |

## Published

Publishing is active only with `EVENT_PUBLISHER=kafka`; otherwise a
log-only publisher is used. `fanOutPublisher` (`cmd/netfulfil/main.go`)
sends **every** event to **both** topics:

- `warehouse.network-fulfillment.events`, the integration topic
  (dataschema stream `events`);
- `warehouse.network-fulfillment.analytics`, the analytics topic (stream
  `analytics`). The same `type` is used on both, but the `id` differs per
  topic.

With `DATABASE_URL` set, both are written through `outbox_events` in the
same transaction as the aggregate (ADR 0003), and the relay drains them.

| Event | Full `type` | Payload fields (`data`) | Producer use case | Known consumers |
| --- | --- | --- | --- | --- |
| `NetworkOrderReceived` | `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived` | `networkRef`, `siteId`, `requiredShipBy`, `acknowledgeBy`, `lineCount` (0 for untranslatable demand), `at` | `ReceiveNetworkDemand` (both paths) | own analytics projector (`orders_received`) |
| `NetworkOrderSubmitted` | `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderSubmitted` (v1) | `networkRef`, `siteId`, `localOrderId`, `receivedAt`, `at` | `ReceiveNetworkDemand.acknowledge`, when the order moves to `SUBMITTED` (the fact the old Acknowledged announced, ADR 0016) | own analytics projector (claimed, no report effect) |
| `NetworkOrderAcknowledged` | `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged.v2` (dataschema `...:NetworkOrderAcknowledged:v2`) | `networkRef`, `siteId`, `localOrderId`, `receivedAt`, `at` | `ReconcileSubmittedOrders.confirm`, only when the order **settles** `ACKNOWLEDGED`, atomically with the save (ADR 0016) | own analytics projector (`orders_acknowledged`, latency `at - receivedAt`) |
| `NetworkOrderRejected` | `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderRejected` | `networkRef`, `siteId`, `reason` (`UNTRANSLATABLE_SKU` \| `INFEASIBLE_DEADLINE` \| `ACKNOWLEDGEMENT_DEADLINE_MISSED` \| `SUBMISSION_FAILED`), `at` | `ReceiveNetworkDemand.reject` (first two reasons), `RejectOverdueOrders`, `ReconcileSubmittedOrders.fail` | own analytics projector (counters by reason, including `SUBMISSION_FAILED` -> `orders_rejected_submission_failed`) |
| `NetworkOrderShipmentConfirmed` | `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderShipmentConfirmed` | `networkRef`, `siteId`, `localOrderId`, `at` | `ConfirmNetworkOrderShipment` | none (the projector ignores it) |
| `AcknowledgementDeadlineAtRisk` | `com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk` | `networkRef`, `siteId`, `acknowledgeBy`, `at` | `SweepAcknowledgementDeadlines`, re-fired every pass while the order is overdue | none (the projector ignores it) |

**Fleet consumers:** none found. A search of the local fleet checkouts
found no other context referencing `warehouse.network-fulfillment.events`.
The integration topic is a published language that nobody subscribes to
yet.

Partition key and `subject` are always `networkRef`
(`kafka.aggregateKey`). The default branch, which keys by event name, is
never reached by these six types.

`CapabilityOffer` raises no events.

## Consumed

| Topic | Full `type` | Fields used | Consumer | Group id |
| --- | --- | --- | --- | --- |
| `warehouse.network-fulfillment.analytics` (own) | `...networkorder.NetworkOrderReceived`, `...NetworkOrderSubmitted`, `...NetworkOrderAcknowledged.v2`, the historic `...NetworkOrderAcknowledged` (v1, published before ADR 0016, counted as before so a replay keeps the report numbers), `...NetworkOrderRejected`; other types ignored | `reason`, `receivedAt`; `time`, `id` from the envelope | `internal/adapters/inbound/kafka/analytics_consumer.go` (`cmd/netfulfil-projector`) | `network-fulfillment-analytics-<host>-<pid>-<ts>` |
| `warehouse.process-path-management.events` | `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated`, `...processpath.ProcessPathUpdated`, `...processpath.ProcessPathDeactivated`, `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged` | `path_id`, `cycle_time_p95`; schedule `site_id`, `timezone`, `cutoffs[].cpt_id/local_time/days_of_week/ship_method/eligible_path_ids` | `internal/adapters/outbound/processpathcache/consumer.go` (opt-in) | `network-fulfillment-process-path-capability-cache-<host>-<pid>-<ts>` |
| `warehouse.work-planning.events` | `com.warehouse.wes.work-planning.workpool.PathCapacityChanged` | `path_id`, `cutoff_at`, `remaining_units`, `known` | `internal/adapters/outbound/pathcapacitycache/consumer.go` (opt-in) | `network-fulfillment-path-capacity-cache-<host>-<pid>-<ts>` |

Consumer behaviour:

- **Analytics consumer**: dedupes on the CloudEvents `id`
  (`analytics_consumed_events`, then `analytics_processed_events`). A
  non-CloudEvent is dead-lettered immediately to
  `warehouse.network-fulfillment.analytics.dlq`. Infrastructure failures
  are retried up to 3 times per phase, then dead-lettered (ADR 0004).
- **The two capability caches** replay from the first offset under a
  per-process group and gate boot on `WaitReady` (60s each). A
  non-CloudEvent or a handling error is **logged and skipped**: no DLQ.

## Discrepancies found (spec vs code)

Fixed on 2026-10-06 (docs-audit PR, [ADR 0015](../adr/0015-docs-audit-contract-corrections.md)):

1. ~~`apis/asyncapi.yaml`'s analytics channel lists only four messages~~:
   `AcknowledgementDeadlineAtRisk` is now on the analytics channel
   (`AcknowledgementDeadlineAtRiskAnalytics`; the projector still ignores it).
2. ~~`info.description` says four events~~: now lists all five.
3. ~~`info.contact.url` points at `github.com/claudioed/network-fulfillment`~~:
   now `github.com/IQVO/network-fulfillment`. The Go module path is
   unchanged.
4. ~~The `NetworkOrderShipmentConfirmed` doc comment says no use case calls
   `ConfirmShipment`~~: corrected (it is `ConfirmNetworkOrderShipment`, ADR
   0014), here, in `events.go` and in the AsyncAPI message descriptions.
5. ~~`ApplyNetworkOrderRejected` has no `SUBMISSION_FAILED` counter~~: the
   rollup now has `orders_rejected_submission_failed`, surfaced as
   `ordersRejectedSubmissionFailed` in the report (history before the
   migration reads 0).

Resolved on 2026-10-06 (decisions PR):

- **`NetworkOrderAcknowledged` timing (ex-hotspot H6/H7).** Decided
  2026-10-06: `NetworkOrderSubmitted` is raised at `SUBMITTED`,
  `NetworkOrderAcknowledged` only when the order settles `ACKNOWLEDGED`,
  published as the `.v2` type/dataschema (ADR 0016). Nothing is left open here.
- **`contract.EligiblePath.CycleTimeP95` unused (ex-hotspot).** Decided
  2026-10-06: a path is eligible for a cutoff only if
  `CycleTimeP95 <= cutoff - now`; missing/zero cycle time stays eligible
  (fail-open); no capacity scaling (ADR 0017). Nothing is left open here.
