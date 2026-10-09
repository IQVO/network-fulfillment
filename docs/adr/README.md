# Architecture Decision Records

Every ADR in this directory, with the Status each record states in its own
`## Status` section. ADR bodies are immutable; this index is the only page
to update when a record is added or its Status line changes.

| # | Title | Status (as stated in the ADR) |
| --- | --- | --- |
| [0001](0001-network-fulfillment-bounded-context.md) | Network Fulfillment as a bounded context — conformist to the network, anti-corruption layer for the fleet | Accepted (2026-09-23). Amended by 0009 (counterpart, PII rule, no `sandbox`) and 0014 (shipment-confirmation trigger) |
| [0002](0002-mcp-and-analytics-data-product.md) | An MCP server and an analytics data product, on the same terms as the other eight bounded contexts | Accepted (2026-09-26) |
| [0003](0003-transactional-outbox.md) | Transactional outbox for the network-fulfillment Published Language | Accepted; envelope description superseded by 0008 |
| [0004](0004-resilience-circuit-breakers-retry-dlq-shutdown.md) | Circuit breaker, Kafka DLQ, and graceful shutdown hardening | Accepted |
| [0005](0005-kafka-hash-balancer-for-partition-affinity.md) | Hash balancer for per-aggregate Kafka partition affinity | Accepted |
| [0006](0006-horizontal-autoscaling-and-pgxpool-tuning.md) | Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning | Accepted |
| [0007](0007-migrations-direct-postgres-connection.md) | Run golang-migrate against a direct Postgres connection, not PgBouncer | Accepted |
| [0008](0008-cloudevents-mandatory-event-envelope.md) | CloudEvents 1.0 as the mandatory event envelope | Accepted (2026-09-30) |
| [0009](0009-retail-network-not-amazon-counterpart.md) | The counterpart is retail-network, our own ecosystem service — not a real external network's Selling Partner API | Accepted (2026-09-26); amends 0001 |
| [0010](0010-web-mfe-remote-and-frontend-chart.md) | web/ is a Module Federation remote, served by its own chart Deployment | Accepted |
| [0011](0011-boot-retry-for-istio-first-dial-reset.md) | Retry the first outbound dial at boot to survive the Istio sidecar race | Accepted |
| [0012](0012-network-seed-file-stub-demand-seeding.md) | NETWORK_SEED_FILE seeds demand into the stub gateway only | Accepted |
| [0013](0013-product-translation-file-acl-dictionary.md) | PRODUCT_TRANSLATION_FILE loads the ACL's product dictionary | Accepted |
| [0014](0014-explicit-shipment-confirmation-endpoint.md) | Shipment confirmation is an explicit endpoint, not a PackageManifested correlation | Accepted (2026-10); amends 0001 Rollout step 6 |
| [0015](0015-docs-audit-contract-corrections.md) | Contract corrections from the 2026-10-05 docs audit: 409 for confirm-before-acknowledge, CORS POST, SUBMISSION_FAILED report counter | Accepted (2026-10-06); additive to 0001 and 0014 |
| [0016](0016-cloudevents-submitted-and-settle-time-acknowledged.md) | `NetworkOrderSubmitted` at SUBMITTED; `NetworkOrderAcknowledged` only when the order settles (v2) | Accepted (2026-10-06); resolves a design note of 0015, versions the event per 0008 |
| [0017](0017-cycle-time-p95-path-eligibility.md) | A path is eligible for a cutoff only if its `CycleTimeP95` fits the time left | Accepted (2026-10-06); resolves a design note of 0015, implements 0001 §8 |
| [0018](0018-godog-bdd-acceptance-tests.md) | godog/Gherkin acceptance tests as executable specification | Accepted (2026-10-08); mirrors inventory-storage ADR 0007 |

[`0002-retail-network-not-amazon-counterpart.md`](0002-retail-network-not-amazon-counterpart.md)
is a redirect stub ("Moved to ADR 0009"), kept so old links resolve; it is
not a separate decision. ADR 0009 was renumbered from 0002 on 2026-10-05
to remove the duplicate number.

Related: [`docs/planning/retail-network-adr-0001-DRAFT-for-new-repo.md`](../planning/retail-network-adr-0001-DRAFT-for-new-repo.md)
(draft ADR for the `retail-network` repository, not a decision of this
one) and the [DDD artifact pack](../ddd/README.md).
