---
id: 0027-supply-loop-node-profile-and-inventory-feed
slug: /adr/0027-supply-loop-node-profile-and-inventory-feed
title: "27. Supply loop: node profile and inventory feed"
sidebar_label: "27. Supply loop: profile and feed"
sidebar_position: 27
description: "ADR 0027 (Accepted) designs the supply half of the live NetworkGateway: the node operational profile (cutoffs, handling time, throughput, a monotonic persisted revision, declared by a background use case) and the inventory feed (one full feed per site, listings from the dictionary, basis carried). Part 1 builds the profile; part 2 builds the feed against this same ADR."
---

# 27. Supply loop: node profile and inventory feed

## Status

**Accepted** (2026-10-24). Completes the live adapter that
[ADR 0026](./0026-live-retail-network-adapter-demand-loop.md) started: that ADR built the
demand loop and reserved this number for the supply loop. Amends the
`NetworkGateway` port of [ADR 0001](./0001-network-fulfillment-bounded-context.md) a
second time (Decision 2) and replaces the "documented no-op" status of
`DeclareCapability` that ADR 0001, [ADR 0009](./0009-retail-network-not-a-real-network-counterpart.md)
and ADR 0026 (Decision 1, As built 2) gave it.

**This ADR decides both loops. Delivery is split:**

- **Part 1 (built, see "As built (part 1)"):** the node profile.
- **Part 2 (designed here, not built):** the inventory feed. It is implemented later
  against Decisions 9 to 12 without a new ADR, and appends its own "As built (part 2)".

## Context

The network knows our building only through what we declare and what we feed. The
network's capability model (its ADR 0003, "Capability is declared and measured", in
the `retail-network` docs) is a versioned operational profile per node, corrected by
measurement. The fleet
already holds every number such a profile needs, in local caches: the CPT schedule
and each path's cycle time (`ProcessPathCapability`, ADR 0001 §8 and
[ADR 0017](./0017-cycle-time-p95-path-eligibility.md)) and remaining admission capacity
(`PathCapacity`). Today none of it leaves the building: the live gateway's
`DeclareCapability` and `SubmitAvailability` log and return.

`DeclareCapability(SiteId, SKU, AdvertisedQuantity)` is also a shape the network never
had. A capability is declared per node, not per SKU, and the quantity of a SKU belongs
to the inventory feed. The Vendor API facts the design rests on, each read from the
vendored contract (`testdata/vendor-connectivity-openapi.yaml`, unchanged by this work:
blob `7393aeb57374eb7e38d98272a6851a5ee4ba36a1`) or from `vendor-connectivity` and
`node-topology` on their `develop`:

- `PUT /vendor/nodes/{nodeId}/profile` is an asynchronous write: `202 {transactionId}`,
  a `NODE_PROFILE` transaction, relayed to node-topology. Like every write it takes an
  `Idempotency-Key` (24 hours, scoped to method and path; same body replays the first
  answer; same key with another body is `422 idempotency-key-reused`).
- The body requires `vendorId`, `profileRevision` (integer, at least 1, "strictly
  increases per node"), `timezone`, `operatingHoursByDay`, `handlingTimeMinutes` (at
  least 1), `cutoffs[]` and `throughputCap {value >= 1, timeUnit HOUR|DAY, unit ORDER}`;
  `holidays` and `sortDwellMinutes` are optional; a cutoff is
  `{localTime "HH:mm", days[] (absent = every operating day), maxOrders >= 1 (optional)}`.
- **A stale revision is not a `409`/`422` on the `PUT`.** The `202` is given and
  node-topology refuses later: the transaction ends `FAILURE` with `errors[0].code =
  ErrStaleProfileRevision` (node-topology answered `409`, "revision 2 is not newer than
  3"). The same transaction mechanism carries `ErrIncompleteOperationalProfile` and
  `ErrCutoffOutsideOperatingHours`. A `404` is an unknown node or one that belongs to
  another vendor; a `422` is a malformed body.
- node-topology checks that every cutoff lies inside the operating hours of each of its
  days (end inclusive), refuses two cutoffs at the same time on a shared day, and treats
  a profile as complete (a fulfilment centre may be `ACTIVE`) only with at least one
  cutoff and a handling time above zero.
- `GET /vendor/nodes/{nodeId}` and the listing carry `profileRevision` and
  `handlingTimeMinutes` "from the latest operational profile the network knows; absent
  before the first one". The view is fed by node-topology events, so it can lag a
  just-applied revision. `ackBy = min(issuedAt + ackWindow, requiredShipBy - handlingTime)`
  uses that handling time: **what we declare shortens or lengthens the deadline we are
  then judged by (ADR 0026, Context).**
- `POST /vendor/inventory` is asynchronous (`INVENTORY` transaction) with
  `{vendorId, nodeId, fullUpdate, asOf, lines[{listingId, availableQty >= 0, basis
  PHYSICAL|THROUGHPUT_CONSTRAINED|UNKNOWN}]}`, at most 5000 lines. "A full feed zeroes
  every listing of the node it omits." Downstream refusals (`ErrListingNotOnNode`,
  `ErrFeedAlreadyApplied`) appear as `errors` on the transaction, with a JSON `path` of
  the offending field when known. A listing is one catalogue item at one node; an
  unknown listing passes the network's own pre-check, one known to belong to another
  node does not.

## Decision

### 1. Scope and delivery

One ADR, two loops, two deliveries. Part 1 implements Decisions 2 to 8. Part 2 implements
Decisions 9 to 12. Until part 2 lands, `SubmitAvailability` stays the logged no-op of
ADR 0026 and **no inventory leaves the building**; nothing in part 1 weakens that.

### 2. The port keeps our vocabulary; three changes

No `nodeId`, `profileRevision`, `operatingHoursByDay`, transaction id or network error
code appears in `ports`, `contract`, `usecases` or the domain.

1. `DeclareCapability` and `contract.CapabilityDeclaration` are **removed**. No use case
   called them, and the shape (an advertised quantity per SKU) is the inventory feed's,
   not a capability's.
2. `DeclareNodeProfile(ctx, contract.NodeProfile) (contract.ProfileResult, error)`
   replaces it. `NodeProfile` is fleet terms only: `SiteId`, `Timezone`, `Cutoffs` (local
   `HH:mm`, weekdays, and a max orders figure that may be unknown), `HandlingTime`,
   `ThroughputPerDay` and an opaque `Fingerprint` of the content. `ProfileResult` says
   what happened (`declared`, `pending`, `confirmed`, `unchanged`, `failed`) and under
   which revision, so the use case can count and log without ever seeing a wire shape.
   `contract.ErrSiteNotRegistered` is returned for a site with no node yet.
3. `ProcessPathCapability` gains `Schedule(siteId, after) (contract.CutoffSchedule,
   bool)`: the site's whole weekly cutoff schedule (timezone, and per cutoff its local
   time, weekdays, next occurrence and eligible paths with cycle times). `NextCutoff`
   answers only the next one, which cannot describe a week. A small `ports.ProfileHistory`
   (`LastDeclared`) lets the use case read what was last declared, for Decision 3.

`SubmitAvailability` is untouched in part 1 and replaced in part 2 (Decision 9).

### 3. Where each number comes from

The profile is derived only from the local caches. **There is never a synchronous call
to `process-path-management` or `wes-work-planning`** (the fleet rule: local and
declarative). A site with no CPT schedule in the cache is skipped and logged, not
declared empty.

| Profile field | Source and rule |
| --- | --- |
| `timezone` | the schedule's `timezone` as `process-path-management` publishes it |
| `cutoffs[].localTime`, `days` | the schedule's cutoffs. Cutoffs with the same local time are one slot (days united), because node-topology refuses two at the same time on a shared day |
| `handlingTimeMinutes` | the **longest known, positive `cycleTimeP95`** among the paths eligible for any cutoff, rounded up to whole minutes. ADR 0017's rule, applied without a clock: a path counts only if its cycle time fits the longest window the schedule gives (the largest gap between two cutoff occurrences in the week). A path with no cycle time stays out ("no data is not evidence"); none known means the profile cannot be declared yet |
| `cutoffs[].maxOrders` | for the slot's next occurrence, the sum of `RemainingCapacity` over its eligible paths (ADR 0017 fit as of now) whose figure is known; unknown (omitted) when no path's is. **A ceiling is never lowered by an observation**: remaining capacity falls as work is admitted, so the declared figure for a slot is the larger of the one last declared for the same slot and the one observed now |
| `throughputCap` | `ORDER`s per `DAY`: for the busiest weekday, the sum of the known `maxOrders` of its slots. No known figure means the profile cannot be declared yet |
| `operatingHoursByDay` | **not a fleet fact.** The Vendor API requires it and node-topology refuses a cutoff outside it. The adapter declares `00:00` to `23:59` on every weekday that has a cutoff and omits (closed) every other day, so the cutoffs and the handling time carry the real constraint and no hours of our own are invented |
| `holidays`, `sortDwellMinutes` | omitted: the fleet holds neither (`sortDwellMinutes` is for sortation nodes) |

A remaining-capacity unit is counted as one order. An order has at least one unit, so
this can over-declare; the network's measured scorecard is what corrects an
over-declaring vendor (its ADR 0003), and the ceiling rule keeps the figure from
sliding the other way on every admitted order. Inherited and recorded: the cache's
`NextCutoff` resolves a cutoff's instant without interpreting the schedule's timezone
(its own doc comment); the profile declares the zone, so the network does interpret it.

### 4. The revision is monotonic and persisted

`profileRevision` is assigned by this service, per node, from a row in the adapter's
own table (migration 0007, Decision 7):

- It is bumped **only when the content fingerprint changes**. The fingerprint is a hash
  of the canonical profile (zone, slots with days and ceilings, handling time,
  throughput), computed by the use case; the gateway treats it as opaque.
- It never goes down and is never reused for different content. The row is advanced by a
  guarded write (`revision` only grows), before the first HTTP call. A restart reads it
  back and continues.
- The idempotency key is deterministic per `(node, revision, attempt)`:
  `nf-profile-<site>-<revision>` for the first attempt and `...-<revision>-<attempt>`
  for a retry (Decision 5), so a retry after a crash replays and never duplicates.
- **Stale recovery.** When the transaction fails with `ErrStaleProfileRevision` (the
  network already holds a revision at or above ours: a wiped table, a second
  declarer), the gateway reads the node's current `profileRevision` from
  `GET /vendor/nodes/{nodeId}` and declares the same content at `max(ours, theirs) + 1`.
  Once per call: a still-stale answer is reported and retried on the next tick. If the
  view shows no revision the climb is one per tick until it is above the network's.

### 5. Writing and reconciling

The ledger of ADR 0026 (Decision 4) is reused: kind `NODE_PROFILE`, subject
`<site>#<revision>#<attempt>`; the exact body is stored before the call. The
transaction is reconciled by its persisted transaction id with the same status mapping
as acknowledgements. A `FAILURE` is logged at **ERROR** with the network's error list.
A failure is retried on a later tick **with the same revision and the same content** (a
new attempt, hence a new key, because the network replays a failed transaction for the
same key) only when it is retryable. Not retryable, and never re-sent until the content
changes: `ErrIncompleteOperationalProfile`, `ErrCutoffOutsideOperatingHours`, a `422`
`validation-failed` on the `PUT` itself (our body is wrong). `ErrStaleProfileRevision`
is recovery (Decision 4). Everything else is retryable.

### 6. When it runs

A use case `DeclareNodeProfiles` is driven by a background worker every
`NETWORK_PROFILE_INTERVAL` (default `5m`; `0` disables; live mode only; stub mode never
starts it) and once immediately at boot. It declares only for sites that have a
persisted node id (ADR 0026 registers them, at each poll), so the first declaration
follows registration by up to one interval. Per site it builds the profile from the
caches, and one site's failure never stops the others. The worker stops on the
process context and shutdown waits for it, inside the existing 10 second window
(ADR 0004), so the termination grace floor is unchanged.

### 7. Persistence (migration 0007)

`network_node_profiles (site_code PK, revision, attempt, fingerprint, content, updated_at)`
holds the revision state and the content last declared in fleet terms (for Decision 3's
ceiling). `network_submissions.kind` accepts `NODE_PROFILE`. Both are adapter-owned, as
in 0006. A profile that the network never learned of is therefore not lost across
restarts, and a stale row cannot lower anything.

### 8. Observability

An OTel counter `network_fulfillment.node_profile.declarations` by `outcome` (`declared`,
`confirmed`, `pending`, `unchanged`, `failed`, `skipped_no_node`, `skipped_no_schedule`,
`skipped_incomplete`) and a gauge `network_fulfillment.node_profile.revision` by `site`
(ADR 0020's naming and unit conventions). A skipped site is logged with the reason.

### 9. The inventory feed: shape (part 2)

`SubmitAvailability(update)` (one SKU) is replaced by `SubmitInventoryFeed(ctx,
contract.InventoryFeed)`: `SiteId`, `AsOf`, `Full` and lines of `{SKU, Quantity, Basis}`.
The lines are the `CapabilityOffer`s `RecomputeCapabilityOffers` already persists
(`advertisedQuantity`, `basis`); `Basis` maps to the wire's `PHYSICAL` and
`THROUGHPUT_CONSTRAINED`. `UNKNOWN` is never sent. `AsOf` is the clock reading of the
recompute pass that produced the offers. One feed per site per pass, never one request
per SKU.

### 10. The inventory feed: listings and fullness (part 2)

The network keys stock by `listingId`, one per catalogue item **at one node**. The
product dictionary (ADR 0013) maps `networkProductId` (which the live adapter already
sets to the listing id, ADR 0026 Decision 4) to a SKU; part 2 inverts it per site and
gives its file an optional `site` per entry (an entry without one applies to every
site). A SKU with no listing for the site is not fed, and logged once.
`fullUpdate = true` only when **every** SKU the site has a listing for has a fresh
offer in the pass; if any offer is missing (an inventory lookup failed), the feed is
partial (`false`), because a full feed would zero that listing on the network. Over
5000 lines the feed is split into partial feeds.

### 11. The inventory feed: writing and failure (part 2)

Ledger kind `INVENTORY`, subject `<site>#<asOf>`, key `nf-inventory-<site>-<asOf>`,
body stored before the call, transaction reconciled like Decision 5. A feed is sent
when its line fingerprint changes, and an unchanged feed is repeated at most hourly so
the network's view carries a recent `asOf`. `ErrFeedAlreadyApplied` is success.
`ErrListingNotOnNode` fails the whole transaction: the offending line (its JSON path
is in the error) is dropped from the next feed and logged at ERROR, so one wrong
listing cannot silence the rest. The worker, interval and `0` disables rule follow
Decision 6 (its own variable, named in part 2).

### 12. What part 2 may rely on from part 1

The node map and its guard (`ErrSiteNotRegistered`), `send` and the ledger with a
`PUT`/`POST` choice, the transaction reconciliation helper, the stale-revision
vocabulary, the metrics file and the worker pattern in `cmd/netfulfil` are all built
and tested in part 1 and are meant to be reused as they are.

## Consequences

- The network can show and use a real handling time and cutoffs for our building, and
  `ackBy` starts to reflect them. A wrong declaration is worse than none: it shortens
  our own acknowledgement window. That is why an incomplete profile (no schedule, no
  cycle time, no capacity figure) is skipped, never filled with a guess.
- One more migration (0007), one more env variable (`NETWORK_PROFILE_INTERVAL`), no new
  dependency, one more worker. `NETWORK_MODE` stays `stub` in the cluster; nothing here
  changes a stub deployment.
- Known residuals: the declared ceilings only rise until the slot itself changes, so a
  real capacity cut is invisible to the network until measurement catches it; a
  remaining-capacity unit stands for an order; a revision that the network applied but
  reported as failed is recovered through the stale path, at the cost of one extra
  revision; two replicas declaring at once converge on the first persisted revision and
  the second waits for its next tick.

## Alternatives considered

- **Keep `DeclareCapability` per SKU.** The network has no such operation; it would stay a
  no-op forever. Removed.
- **Declare `RemainingCapacity` directly as `maxOrders`.** It falls with every admitted
  order, so the fingerprint would change on every tick and every tick would burn a
  revision. The ceiling rule exists for this.
- **Bump the revision on a clock** (a timestamp or a counter per tick). It would give the
  network a new revision with identical content every time; "only when the content
  changes" is what makes the revision mean something.
- **Burn the revision on every failure instead of retrying it.** Simpler, but a
  transient failure would then change a number that should change only with content;
  stale recovery already covers the case where reuse is wrong.
- **Declare 24 hours a day on all seven days.** Claims availability on days with no
  cutoff; the days-with-a-cutoff rule claims no more than the schedule says.
- **Synchronous reads of the neighbours at declaration time.** Violates the fleet rule
  and makes a neighbour's outage stop a declaration.
- **Send the inventory per SKU.** The API takes a feed per node and a full feed has
  meaning (zeroing) a stream of single lines cannot have.

## As built (part 1)

Filled in when part 1 is complete (see the section below once the code lands).
