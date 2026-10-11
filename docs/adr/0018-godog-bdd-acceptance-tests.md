---
id: 0018-godog-bdd-acceptance-tests
slug: /adr/0018-godog-bdd-acceptance-tests
title: "18. godog/Gherkin acceptance tests as executable specification"
sidebar_label: "18. godog BDD tests"
sidebar_position: 18
description: "ADR 0018 expresses this context's behaviour as black-box Gherkin scenarios driven through the real HTTP routers, over in-memory adapters and the real CloudEvents encoders, and gates them in CI as a bdd job; scenarios that describe a found defect carry @known-bug and are excluded until it is fixed."
---

# 18. godog/Gherkin acceptance tests as executable specification

## Status

Accepted (2026-10-08). Mirrors `inventory-storage`'s ADR 0007 (the fleet's
canonical BDD pattern) and fills the gap
[`docs/operations-notes.md`](../operations-notes.md) recorded ("`bdd` dropped,
no `features/`"). No REST, MCP or event contract changes.

## Context

By now this context has deep layer-by-layer tests: table-driven aggregate
tests, use-case tests against in-memory adapters, `httptest` tests per
handler, wire goldens for every CloudEvent, and testcontainers integration
tests for Postgres and Kafka. What it did not have was a statement of its
behaviour **in the domain's own vocabulary**, nor anything that exercised the
whole stack as a black box:

- A `TestReceiveNetworkDemand_...` test is precise but says nothing a
  non-Go reader can review about *why* an untranslatable product rejects the
  whole order, or why `NetworkOrderAcknowledged` is only published once the
  network's transaction-status record settles.
- No test drove the poller, the use cases, the event encoders, the analytics
  projection and both HTTP routers *together*, the way an operator sees them.

This is a Supporting context whose correctness is defined by the counterparty's
protocol (24-hour window, answered exactly once, in full or not at all, no
partial acknowledgement) and by fleet rules (CloudEvents 1.0 only, no auth, no
customer PII). Those are exactly the things worth reading as prose and
failing the build on when they stop being true.

One constraint shapes the suite: **demand never arrives over HTTP** (ADR 0001
§5; `apis/openapi.yaml` is read-only apart from one write). "Creating" a
network order therefore means seeding the stub network and running the real
poller, then observing the result through REST.

## Decision

**We write executable specifications in Gherkin under `features/`, run them
with [godog](https://github.com/cucumber/godog) v0.16.0, and gate them in CI
as a blocking `bdd` job.**

1. **One feature file per concept**, each starting with a `# Derived from:`
   comment citing the `apis/openapi.yaml` operations and
   `.claude/rules/domain-model.md` sections it specifies, and each scenario
   tagged `@bdd`:

   | File | Covers |
   | --- | --- |
   | `network_order_intake.feature` | polling demand, ACL translation, accept/reject, idempotency, malformed demand |
   | `network_order_queries.feature` | `GET /network-orders[/{ref}]`, PII rule, unanswered list order, overdue boundary |
   | `acknowledgement_lifecycle.feature` | reconciliation (SUBMITTED to ACKNOWLEDGED / REJECTED), deadline sweep, reject-overdue |
   | `shipment_confirmation.feature` | `POST /network-orders/{ref}/shipment-confirmation`: 204/404/409/500, idempotency |
   | `capability_offers.feature` | `GET /capability-offers`, `min(physical, throughput)`, ADR 0017 cycle-time eligibility |
   | `inbound_status.feature` | `GET /inbound-status`: counters, watermark, unanswered/overdue |
   | `operations.feature` | `/healthz`, `/readyz`, `/metrics`, read-only surface, no authentication |
   | `integration_events.feature` | CloudEvents 1.0 envelope, `type`/`dataschema` per event (`NetworkOrderAcknowledged` is `.v2`), partition key |
   | `acknowledgement_report.feature` | `GET /reports/acknowledgement[/freshness]`, rejection causes, window, 400s |

2. **True black-box tests of the real routers.** `features_test.go` (plus
   sibling `features_*_test.go` step files in the same `main_test` package)
   builds the composition root the way `cmd/netfulfil` and
   `cmd/netfulfil-reports` do: `inboundhttp.Server.Routes()` for the OLTP API
   and the chi `inboundhttp.NewReportsRouter` for the report, both served by
   `httptest.NewServer` and driven by plain `net/http` with **no
   `Authorization` header**. Redirects are not followed, because a router
   redirect is itself behaviour.

3. **Real adapters wherever they exist, doubles only for siblings.** The
   scenarios use the real poller, use cases, in-memory repositories, the real
   `StubGateway` (wrapped only to record what the network was told), the real
   Kafka *encoders* (`Publisher`, `AnalyticsPublisher`, writing into a
   recording `Writer`, so no broker) and the real `AnalyticsConsumer` feeding
   the real `MemoryStore` read model. The suite makes **no REST or MCP call
   to any sibling context**: order-management, inventory-storage,
   process-path-management and wes-work-planning are in-process doubles of
   the `ports` interfaces.

4. **Fresh state and a fixed clock per scenario.** A `Before` hook rebuilds
   everything, and the clock only moves with "N hours pass", so the 24-hour
   window and freshness lag are exact, not slept.

5. **Real behaviour, not bugs.** Where the implementation contradicts the
   spec, the scenario states the **spec's** behaviour. If the defect cannot
   be fixed safely in the same change, the scenario is tagged `@known-bug`
   and excluded from the default run (`Tags: "~@known-bug"`); deleting the
   tag when the defect is fixed turns it into a regression test.
   `BDD_TAGS=@known-bug make bdd` runs only those scenarios and must fail.

6. **Blocking in CI** as the `bdd` job (`go test ./... -run TestFeatures -v`,
   the same invocation as `make bdd`), added to `docker-publish`'s `needs`.
   The branch-protection required-check list is *not* changed here: adding
   `bdd` to it is a repository-settings change for a maintainer, recorded in
   [`operations-notes.md`](../operations-notes.md).

## Findings recorded by the first run

Writing the scenarios from the spec surfaced three defects. They are listed
here so the `@known-bug` tags have a home; each is re-verified by
`BDD_TAGS=@known-bug make bdd`.

1. **A demand that fails during an order-management outage is never answered
   on retry.** `ReceiveNetworkDemand` saves the order as `NEW` *before*
   asking order-management for a verdict, then its idempotency guard
   (`FindByRef != nil` returns the existing order) short-circuits every
   retry. The poller counts the re-fetched demand as `received`, the order
   stays `NEW`, and the sweep eventually rejects it as
   `ACKNOWLEDGEMENT_DEADLINE_MISSED`. `apis/openapi.yaml` (`failed`) says a
   failed unit "will be re-fetched ... processing is idempotent on
   `networkRef`, which is what makes the retry safe". Not fixed here: a
   correct resume must also avoid raising a second held order when the first
   attempt got past `RaiseHeldOrder`, which needs its own design (and ADR).
   Scenario: *Demand that failed during an order-management outage is
   answered on a later pass* (`@known-bug`).
2. **`422 empty-network-ref` on shipment confirmation is unreachable.** The
   spec documents it, but `net/http`'s `ServeMux` cleans `//` and answers a
   307 redirect before the handler runs; the handler's `ref == ""` branch is
   dead over HTTP. Fix is a spec edit (drop the 422) or an explicit
   pre-routing check, both outside this change. Scenario: *An empty
   networkRef is a 422 problem document* (`@known-bug`).
3. **A failed shipment submission is never retried.** By design (and as
   `apis/openapi.yaml` states for `500`), the order is saved `CONFIRMED`
   before the network is told, so a retry after a failed
   `SubmitShipmentConfirmation` answers 204 *without re-submitting*: the
   network is never told about that shipment. The scenario encodes the
   documented behaviour and asserts the consequence (one attempt, no second
   one); it is flagged here because the consequence is an operational hazard,
   not because it contradicts the spec.

One further defect **was** fixed in this change because it is trivial and
safe: the in-memory `NetworkOrderRepo.ListUnanswered` returned Go's
randomised map order, contradicting `apis/openapi.yaml` ("soonest deadline
first") and the Postgres adapter (`ORDER BY acknowledge_by`). It now sorts by
`acknowledgeBy`, then `networkRef`; the scenario *Unanswered orders are
listed soonest deadline first* failed roughly one run in ten before the fix,
and `memory/network_order_repo_test.go` pins it at unit level.

Two documentation drifts were also found and are *not* fixed here (the files
are outside this change's remit): `.claude/rules/domain-model.md` still says
there is no `SUBMITTED` state, "no typed domain events", `CapabilityOffer` is
"planned" and `ConfirmShipment` has no use case; and the
`ReceiveNetworkDemand` comment on untranslatable demand says the order is
built with "a synthetic single line", while `ReceiveUntranslatable` builds it
lineless (the REST response has `lines: []`).

## Consequences

### Easier

- **The behaviour is reviewable by someone who has never opened a Go file**,
  in the ubiquitous language (NetworkOrder, acknowledgeBy, held order,
  requiredShipBy, CapabilityOffer).
- **The fleet rules are executable here.** Every message is asserted to be a
  CloudEvents 1.0 envelope with the right `type`, `dataschema`, `subject`,
  key and content-type header on both streams; every endpoint is asserted to
  answer without credentials; the order response is asserted to carry no
  property beyond the documented schema (the PII rule).
- **The seams between layers are covered**: poller to use case to event
  encoder to analytics consumer to report router, which no single-layer
  test sees.
- **Defects found by writing prose from the spec are recorded where they
  cannot be forgotten**, without turning the suite red or encoding the bug.

### Harder

- **A second vocabulary to maintain.** Step definitions are glue; a DTO or
  route change updates `features_*_test.go` as well as the handler.
- **Doubles can drift from siblings.** The planner, inventory, path-capability
  and capacity doubles encode what this context *asks* of them; they are
  checked against the port interfaces by the compiler, not against the real
  sibling services (by design: no sibling calls).
- **`@known-bug` is an escape hatch.** It must stay rare and each tag must
  point at a recorded finding; a tag with no finding is a silenced test.
- **Overlap with the `httptest` suite.** Some assertions now exist twice.
  Accepted: the `httptest` tests are exhaustive per endpoint, the scenarios
  are the readable specification of the journeys.
