---
id: index
title: Architecture decision records
sidebar_label: ADR index
slug: /
sidebar_position: 0
---

# Architecture decision records

The decisions of `network-fulfillment`, in the order they were taken. The ADR bodies are
immutable; a changed decision gets a new ADR that supersedes the old one. The
*Status* column is copied from each ADR's own status line.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](/docs/adr/0001-network-fulfillment-bounded-context) | Network Fulfillment as a bounded context — conformist to the network, anti-corruption layer for the fleet | Accepted (2026-09-23). Companion to `order-management` ADR 0020 (network-originated |
| [0002](/docs/adr/0002-mcp-and-analytics-data-product) | An MCP server and an analytics data product, on the same terms as the other eight bounded contexts | > Status pointer: ADR 0025 made `DATABASE_URL` (mcp) and `KAFKA_BROKERS` (projector) mandatory and removed the in-memory store and the opt-in `EVENT_PUBLISHER=kafka` run described below. |
| [0003](/docs/adr/0003-transactional-outbox) | Transactional outbox for the network-fulfillment Published Language | Accepted — implemented in the same change that introduced this record. |
| [0004](/docs/adr/0004-resilience-circuit-breakers-retry-dlq-shutdown) | Circuit breaker, Kafka DLQ, and graceful shutdown hardening | Accepted — implemented in the same change that introduced this record. §6 (separate per-phase retries) amended by [ADR 0023](./0023-analytics-claim-and-apply-in-one-transaction.md): the claim and the projection write now commit in one transaction and are retried as one unit. |
| [0005](/docs/adr/0005-kafka-hash-balancer-for-partition-affinity) | Hash balancer for per-aggregate Kafka partition affinity | Accepted — implemented in the same change that introduced this record. |
| [0006](/docs/adr/0006-horizontal-autoscaling-and-pgxpool-tuning) | Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning | Accepted — implemented in the same change that introduces this record. |
| [0007](/docs/adr/0007-migrations-direct-postgres-connection) | Run golang-migrate against a direct Postgres connection, not PgBouncer | Accepted — implemented in the same change that introduces this record. |
| [0008](/docs/adr/0008-cloudevents-mandatory-event-envelope) | CloudEvents 1.0 as the mandatory event envelope | Accepted (2026-09-30) — implemented in the same change that introduces this |
| [0009](/docs/adr/0009-retail-network-not-a-real-network-counterpart) | The counterpart is retail-network, our own ecosystem service — not a real external network's vendor platform API | **Accepted** (2026-09-26). Companion to `retail-network` ADR 0001 (drafted at |
| [0010](/docs/adr/0010-web-mfe-remote-and-frontend-chart) | web/ is a Module Federation remote, served by its own chart Deployment | Accepted — implemented in the same change that introduced this record |
| [0011](/docs/adr/0011-boot-retry-for-istio-first-dial-reset) | Retry the first outbound dial at boot to survive the Istio sidecar race | Accepted — implemented in the same change that introduced this record |
| [0012](/docs/adr/0012-network-seed-file-stub-demand-seeding) | NETWORK_SEED_FILE seeds demand into the stub gateway only | Accepted — implemented in the same change that introduced this record |
| [0013](/docs/adr/0013-product-translation-file-acl-dictionary) | PRODUCT_TRANSLATION_FILE loads the ACL's product dictionary | Accepted — implemented in the same change that introduced this record |
| [0014](/docs/adr/0014-explicit-shipment-confirmation-endpoint) | Shipment confirmation is an explicit endpoint, not a PackageManifested correlation | Accepted (2026-10). Amends [ADR 0001](./0001-network-fulfillment-bounded-context.md)'s |
| [0015](/docs/adr/0015-docs-audit-contract-corrections) | Contract corrections from the 2026-10-05 docs audit | Accepted (2026-10-06). Additive to [ADR 0001](./0001-network-fulfillment-bounded-context.md) |
| [0016](/docs/adr/0016-cloudevents-submitted-and-settle-time-acknowledged) | `NetworkOrderSubmitted` at SUBMITTED; `NetworkOrderAcknowledged` only when the order settles (v2) | Accepted (2026-10-06). Resolves the "`NetworkOrderAcknowledged` timing" design |
| [0017](/docs/adr/0017-cycle-time-p95-path-eligibility) | A path is eligible for a cutoff only if its `CycleTimeP95` fits the time left | Accepted (2026-10-06). Resolves the "`contract.EligiblePath.CycleTimeP95`" |
| [0018](/docs/adr/0018-godog-bdd-acceptance-tests) | godog/Gherkin acceptance tests as executable specification | Accepted (2026-10-08). Mirrors `inventory-storage`'s ADR 0007 (the fleet's |
| [0019](/docs/adr/0019-otlp-telemetry-export) | OTLP trace and metric export for the API process | Accepted (2026-10-08) |
| [0020](/docs/adr/0020-adr-0013-standard-metrics-convention-adoption) | Adopt ADR-0013 (fleet "Standard metrics convention"): otelchi HTTP RED metrics everywhere | Accepted (2026-10-12) |
| [0021](/docs/adr/0021-outbox-relay-attempt-cap) | Outbox relay attempt cap | **Accepted** (2026-10-13, ecosystem gap fix D12). Amends the per-key ordering |
| [0022](/docs/adr/0022-trace-context-crosses-the-outbox) | The W3C trace context crosses the outbox to Kafka | > Status pointer: ADR 0025 removed the "direct mode" (no database) described in this record; the outbox is the only path. |
| [0023](/docs/adr/0023-analytics-claim-and-apply-in-one-transaction) | The analytics consumer's dedupe claim and projection write commit in one transaction | **Accepted** (2026-10-13). Amends [ADR 0004](./0004-resilience-circuit-breakers-retry-dlq-shutdown.md) |
| [0024](/docs/adr/0024-orphaned-order-management-hold-sweep) | Sweep orphaned order-management holds past the acknowledgement window | **Accepted** (2026-10-17). Builds on [ADR 0001](./0001-network-fulfillment-bounded-context.md) |
| [0025](/docs/adr/0025-postgres-and-kafka-are-the-only-runtime) | Postgres and Kafka are the only runtime | **Accepted** (2026-10-19). Supersedes the optional-runtime parts of |
| [0026](/docs/adr/0026-live-retail-network-adapter-demand-loop) | Live retail-network adapter: the demand loop | **Accepted** (2026-10-24). Implements the part of [ADR 0009](./0009-retail-network-not-a-real-network-counterpart.md) |
| [0027](/docs/adr/0027-supply-loop-node-profile-and-inventory-feed) | Supply loop: node profile and inventory feed | **Accepted** (2026-10-24). Completes the live adapter that |
| [0028](/docs/adr/0028-inbound-loop-seller-shipments-and-receipts) | The inbound loop: seller shipments in, receipts out | **Accepted** (2026-10-24). Builds on [ADR 0026](./0026-live-retail-network-adapter-demand-loop.md) |
| [0029](/docs/adr/0029-fulfillment-site-on-the-held-order) | The fulfilling site rides on the held order | **Accepted** (2026-10-25). Builds on [ADR 0026](./0026-live-retail-network-adapter-demand-loop.md) |
| [0030](/docs/adr/0030-a2a-inbound-adapter) | A2A inbound adapter (a read-only third driving adapter) | **Accepted** (2026-10-29). Mirrors the reference rollout in |
| [0031](/docs/adr/0031-advertise-against-the-next-feasible-cutoff) | Advertise against the next feasible cutoff, read in the site's timezone | **Accepted** (2026-10-30). Two root causes of the same live symptom, decided |
