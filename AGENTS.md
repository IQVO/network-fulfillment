# Network Fulfillment

harness-template: v1

Supporting bounded context: the anti-corruption layer between the
`warehouse-systems` fleet and an external retail fulfillment network
(a major e-commerce retailer's Selling Partner API, Vendor Direct Fulfillment). **Conformist**
to the network upstream, **Anti-Corruption Layer** for everything
downstream of it in this fleet. Owns **NetworkOrder** and
**CapabilityOffer** as first-class aggregates.

Study project — not a production system, not affiliated with any real-world
company (see README.md banner).

## CURRENT STATE: persisted, deployed, stub-only

This repo holds the `NetworkOrder` aggregate, the ACL translation
boundary, the two use cases (`ReceiveNetworkDemand`,
`SweepAcknowledgementDeadlines`), in-memory AND Postgres repositories, the
stub network gateway, a Dockerfile, a Helm chart, and the `netfulfil`
composition root. It is deployed to the kind cluster.

Persistence is opt-in by `DATABASE_URL`: unset means the in-memory repo
(zero-config local run, hermetic unit suite), set means Postgres or a
REFUSAL to boot — never a silent fallback, because the fallback costs the
acknowledgement deadlines this context owes the network.

**Startup is RETRIED (~31s budget) and gated by a `startupProbe`.** Both
are required by this cluster, not defensive padding: every injected pod's
first outbound dial is reset ~10s after the app starts (Istio native
sidecars, so `holdApplicationUntilProxyStarts` is a no-op), and this
service runs migrations before it starts listening. Without the retry that
reset is fatal; without the probe the kubelet SIGTERMs a pod that is still
booting (exit 143, which reads like an app crash and is not one). Both
were observed live as CrashLoopBackOff. The retry does not weaken
fail-closed — after the budget it still refuses to boot.

The inbound leg is WIRED: a poller (`internal/adapters/inbound/poller`)
drives `ReceiveNetworkDemand` on an interval, and a read-only REST surface
(`internal/adapters/inbound/http`, contract in `apis/openapi.yaml`)
exposes what we told the network plus whether the poller is alive.
Previously `ReceiveNetworkDemand` was constructed and DISCARDED
(`_ = receive`), so nothing deployed could create a NetworkOrder at all.

**The REST surface is read-only on purpose.** Demand arrives by polling
and by nothing else (ADR 0001 section 5), so an HTTP intake endpoint would
be a second, fictional inbound path; the only thing it could honestly do
is inject test demand, which `NETWORK_SEED_FILE` does declaratively
instead. Do not add a write endpoint here without changing that record.

Two curated files, both following the fleet's `PATH_CATALOGUE_FILE`
precedent: `PRODUCT_TRANSLATION_FILE` (the ACL dictionary — **without it
every order is rejected as untranslatable**, and the service logs a
warning at startup saying so) and `NETWORK_SEED_FILE` (stub demand, which
refuses to boot if set against a non-stub gateway).

There is still no `web/` and no live network call anywhere. Read
`docs/adr/0001-network-fulfillment-bounded-context.md` before writing any
code here — the boundary is **Accepted (2026-09-23)** and is a companion
to `order-management` ADR 0020. Neither is meaningful without the other.

**`docs/adr/0002-retail-network-not-amazon-counterpart.md` (Proposed)
amends ADR 0001**: the counterpart this context is Conformist to is
`retail-network`, a new bounded context inside this same fleet's orbit
built to play that structural role — not a real e-commerce retailer's
SP-API. Read ADR 0002 alongside ADR 0001 before writing any network-
facing code; it renames the ACL adapter package and tightens the PII rule
(see rules 1 and 2 below, both already updated to match ADR 0002's
decision even though the ADR itself is still Proposed pending the
companion `retail-network` ADR 0001's acceptance).

**CI is ACTIVE** (`.github/workflows/ci.yml`), with eleven jobs: `lint`,
`test`, `integration`, `api-lint`, `mutation-fast`, `vuln`, `arch-test`,
`helm-lint` (`helm lint` plus the chart wiring tests in
`charts/network-fulfillment/tests/`), plus the packaging/release trio
`trivy-scan`, `docker-publish`, `release` — added on parity with
`order-management` once `main` existed to gate them on.
`integration` runs the Postgres adapter against a real Postgres started by
testcontainers INSIDE the test — never a `DATABASE_URL` service container
with a skip gate, which would report success while asserting nothing. The
template's remaining jobs are still **dropped, not disabled**: `bdd` (no
`features/`), `docs-api-drift` (no docs site), `web` (no `web/`), `drift`
(no `warehouse-ui-kit` frontend dependency here). Add each back in the PR
that creates or first needs its surface. A job that fails because a
directory is missing is noise, not signal.

`trivy-scan` builds the image (no push) and blocks on CRITICAL/HIGH with a
known fix, but only for a PR targeting `main` — same fleet convention as
`order-management` (validated pre-merge, not on every `develop` push).
`docker-publish` pushes `ghcr.io/claudioed/network-fulfillment` on every
push to `main`, cosign-signs the image keylessly (GitHub OIDC), and
attests it with an SPDX SBOM. `release` runs after `docker-publish`
succeeds: auto-bumps semver from the latest `vX.Y.Z` tag (starting
`v0.1.0`), re-tags the image with that version, packages and pushes the
Helm chart to `oci://ghcr.io/claudioed` as an OCI artifact, cuts the git
tag, and creates a GitHub Release with the chart `.tgz` attached.

**Branch protection on `develop` requires the eight non-packaging
contexts** with `strict: true`:

```
lint  test  integration  api-lint  mutation-fast  vuln  arch-test  helm-lint
```

**Branch protection on `main` requires `lint`, `test`, `helm-lint`,
`trivy-scan`** with `strict: true` and `enforce_admins: true` — same shape
as `order-management`'s `main`. `docker-publish`/`release` are
push-triggered, not PR checks, so they are deliberately not in the
required-status-checks list. The earlier deferral on `develop` (protection
live with `required_status_checks: null`, because requiring contexts that
could never report would have made every PR permanently unmergeable) is
resolved: the contexts report, so they are required. Add `bdd` when a
`features/` directory lands.

`.gremlins.yaml` thresholds are MEASURED, not copied: `efficacy: 99`,
`mutant-coverage: 92`, set strictly below a real `gremlins unleash
./internal/domain` run of this repo's own code (100.00% efficacy, 93.33%
mutant coverage, 14 killed / 0 lived / 1 not covered). Re-measure and
re-set them when the domain grows; never copy a sibling's numbers.

`.claude/rules/*.md` describe the code as it is: `domain-model.md`
(NetworkOrder, use cases, ports), `rest-api.md` (the four read-only
routes, RFC 7807), and `integration-events.md` (no Kafka yet; the rules
for when it arrives). Keep them in sync with the code. Do not write planned
concepts into them as if they exist.

## Why this context exists

`process-path-management` owns per-path `cycleTimeP95` and a site-scoped
CPT schedule; `wes-work-planning` publishes remaining capacity per
`(path, CPT)`; `order-management` derives a real delivery promise from
both. Offering that capability to an external retail network turns out
**not** to be a matter of publishing it:

> **No Selling Partner API operation accepts a declaration of capability.**
> Cutoffs and lead times are Vendor Central configuration, not an API
> surface.

Capability reaches a network only through three *consequence* signals —
inventory update, order acknowledgement, shipment confirmation — and the
network infers our capability from the gap between the last two. So the
domain decision is **what to advertise and what to commit to**.

The one genuinely new concept here is **throughput-constrained advertised
availability**:

```
advertisedQuantity = min(
    physicalAvailable,                       inventory-storage
    throughputFeasibleBefore(nextCutoff)     PPM cycleTimeP95 + CPTSchedule
                                             × WES remaining PathCapacity
)
```

Everything else in this context is plumbing that makes that deliverable
and honest.

## Architecture (NON-NEGOTIABLE — identical shape to the fleet)

Hexagonal / Ports & Adapters, enforced by the `arch-go` fitness tests in
`internal/architecture/`. Strict dependency rule: **domain depends on
nothing; application depends on domain; adapters depend on
application/domain.**

```
cmd/netfulfil/          composition root (the only binary)
internal/
  domain/
    networkorder/       NetworkOrder aggregate, acknowledgement invariants
    shared/             NetworkRef, NetworkLineRef, NetworkProductId, SKU,
                        LocalOrderId, SiteId, validation errors
  application/
    contract/           InboundDemand, HeldOrderRequest/Result
    ports/              NetworkOrderRepo, NetworkGateway, FulfillmentPlanner,
                        ProductTranslation, EventPublisher, Clock
    usecases/           ReceiveNetworkDemand, SweepAcknowledgementDeadlines
  adapters/
    inbound/http/       read-only REST (apis/openapi.yaml)
    inbound/poller/     the inbound leg: polls the gateway, feeds Receive...
    outbound/network/   NETWORK_MODE switch + stub gateway + seed file — the
                        ONLY place network vocabulary may exist TODAY.
                        Renamed to outbound/retailnetwork/ in the Phase 3
                        implementation PR per ADR 0002, once a live
                        gateway calling retail-network's real Vendor API
                        is added alongside the stub.
    outbound/ordermanagement/  held order, release, cancel (OM REST)
    outbound/postgres/  NetworkOrderRepo + migrations runner
    outbound/memory/    in-memory repo + product-translation file loader
  architecture/         arch-go + fleet fitness tests
migrations/             0001_init (network_orders, network_order_lines)
charts/network-fulfillment/   Helm chart + Python wiring tests
```

Still planned (ADR 0001 + ADR 0002), not in the tree: `CapabilityOffer`
and its Kafka-fed caches, `outbound/kafka`, the live `retailnetwork`
gateway calling `retail-network`'s real Vendor API, shipment
confirmation, transaction-status reconciliation, and an MCP adapter.

`MUTATION_FAST_PKG` is `./internal/domain/networkorder`, the only aggregate.

## Hard rules for this context

1. **The network's vocabulary stops at `adapters/outbound/network/`**
   (today the stub gateway; per ADR 0002 this package is renamed to
   `adapters/outbound/retailnetwork/` in the Phase 3 implementation PR,
   when the live gateway calling `retail-network`'s real Vendor API is
   added alongside the stub).
   `purchaseOrderNumber`, `itemSequenceNumber`, `buyerProductIdentifier`
   (ASIN) and other real-e-commerce-retailer field names must never
   appear in `internal/domain` or in anything published to the fleet —
   nor must `retail-network`'s own vocabulary (`poNumber`, `listingId`,
   `nodeId`, its reason codes) leak past this one package once the live
   gateway lands. `arch-go` cannot catch a *vocabulary* leak — that check
   is human.
2. **No ship-to PII reaches this context at all** (ADR 0002, amending ADR
   0001's original "Customer PII stops here"). With `retail-network` as
   the counterpart, ship-to name/address/phone live in `retail-network`'s
   own `CustomerOrder` aggregate and are never sent to
   `network-fulfillment` in either direction — this context receives only
   a `poNumber`, line items, quantities and a `requiredShipBy`, and a
   shipping-label request returns only `{labelRef, trackingNumber,
   carrier}`. This context therefore does **not** need to run
   authenticated for PII reasons (it holds none); it stays unauthenticated
   for the same fleet-wide reason every other context does.
3. **Never recompute promise math here.** Deadline feasibility is asked of
   `order-management`'s `PromisePolicy.FeasibleBy` (its ADR 0020). This
   context has the deadline and *could* consume the same caches — doing so
   guarantees the two copies drift.
4. **Correlation is a persisted mapping, never a string convention.**
   `NetworkOrder.localOrderId` is stored explicitly. The fleet already got
   burned once: FE's `order_ref` carries WES's per-LINE `WorkUnitId`, not
   OM's `OrderId` (OM ADR 0018).
5. **Network demand is ship-complete.** The network confirms or rejects a
   purchase order in its entirety; partial acknowledgements are rejected.
   OM ADR 0017's per-shipment-group promising must not apply to it.
6. **`NETWORK_MODE=live|stub`, default `stub`.** Per ADR 0002, `sandbox` is
   removed — it only ever meant a real vendor program's own sandbox tier,
   which `retail-network` has no equivalent of (its one deployed instance
   in the kind cluster is well-behaved by default and stressed only via
   its own simulation knobs). The kind cluster, `e2e-tests` and CI must
   never need a credential. Log the chosen mode at startup so the running
   value can be verified, not assumed — every `*_MODE` in this fleet
   defaults permissive and several sat wrong in the cluster for weeks.
7. **Any FirstOffset-replay Kafka cache needs a consumer group id unique
   per process instance** (hostname+PID+timestamp). A shared group means a
   fresh process resumes from an earlier instance's committed offset and
   is marked ready holding an empty cache. This has bitten this fleet in
   two separate services.
8. **Submissions are asynchronous.** `submitAcknowledgement` /
   `submitShipmentConfirmations` return accepted-for-processing; reconcile
   via `getTransactionStatus` (~15 min for acks, ~10 for shipments). A 200
   is not a completed commitment — model submitted-but-unreconciled as its
   own state. Per ADR 0002 D5, `network-fulfillment`/`order-management`
   release work only once the acknowledgement submission has *reconciled*
   to `SUCCESS`, never on the 202 alone.

## Key commands (harness v1 — see HARNESS.md for what each sensor costs)

```bash
make check          # fmt-check + vet + build + lint + test
make check-all      # check + coverage (90% gate) + arch-test + bdd (no features/ yet)
make arch-test      # arch-go hexagonal + fleet fitness tests
make integration    # Postgres via testcontainers — never a skip-gated check
make mutation       # gremlins on MUTATION_FAST_PKG (CI job: mutation-fast)
make mutation-full  # measure real thresholds before editing .gremlins.yaml
make vuln           # govulncheck ./...
lefthook install    # pre-commit fmt/lint/vet, pre-push make check
```

## Git workflow

GitFlow: `feature/*` branches off `develop`, PR into `develop`
(`gh pr create --base develop`); `develop` promotes to `main` for release.
This repo has never been released: there is no `main` branch yet.
Do not merge your own PR — leave it open for independent review.

## Related decisions elsewhere in the fleet

| Repo | ADR | Why it matters here |
| --- | --- | --- |
| `retail-network` | 0001 (drafted at `docs/planning/retail-network-adr-0001-DRAFT-for-new-repo.md` in this repo, pending that repo's creation) | Defines the organization this context is Conformist to — the companion to this repo's own ADR 0002 |
| `order-management` | 0020 | The companion: `releaseOnAllocation` hold, `PromisePolicy.FeasibleBy`, `Network` promise basis |
| `order-management` | 0014 | The promise is a CPT window derived from fulfillment capability |
| `order-management` | 0004 | Release is the cancellation boundary — why network demand is held, not optimistically released |
| `order-management` | 0017 | Per-shipment-group promising — the rule network demand must *not* use |
| `process-path-management` | 0010 | The fulfillment capability contract this context advertises against |
| `fulfillment-execution` | 0025 | The sweep pattern the acknowledgement-deadline sweep mirrors |

Note: `warehouse-infra` does **not** auto-discover repos. This context is
deployed by its own `terraform/network-fulfillment.tf`, not through
`local.services`. It gets its own database Secret (`network-fulfillment-db`)
and a Kong route at `/api/network-fulfillment`. That file leaves
`config.networkMode` at the chart's `stub` default on purpose.
