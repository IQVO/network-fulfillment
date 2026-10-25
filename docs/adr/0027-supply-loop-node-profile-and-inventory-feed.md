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
- **Part 2 (built, see "As built (part 2)"):** the inventory feed, implemented against
  Decisions 9 to 12 without a new ADR.

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
own table (migration 0010, Decision 7):

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

### 7. Persistence (migration 0010)

`network_node_profiles (site_code PK, revision, attempt, fingerprint, content, updated_at)`
holds the revision state and the content last declared in fleet terms (for Decision 3's
ceiling). `network_submissions.kind` accepts `NODE_PROFILE`: that widening was made by
migration 0008 of [ADR 0028](./0028-inbound-loop-seller-shipments-and-receipts.md), which
admits it ahead of this migration, so 0010 creates the table and leaves the kind check
alone (re-creating the check here would drop `INBOUND_RECEIPT` from it). The table is
adapter-owned, as in 0006. This migration was first numbered 0007; develop had taken
0008 and 0009 for the inbound loop before it landed, and golang-migrate never runs a
file numbered below a database's current version, so it was renumbered to 0010. A profile that the network never learned of is therefore not lost across
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
- One more migration (0010), one more env variable (`NETWORK_PROFILE_INTERVAL`), no new
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

Part 1 is on `develop` as this ADR's first deliverable; part 2 (Decisions 9 to 12) is
not built and `SubmitAvailability` is still the documented, non-failing, log-once
no-op of ADR 0026. What the code does, where it differs from or adds to the Decisions:

1. **The port.** `NetworkGateway.DeclareNodeProfile(ctx, contract.NodeProfile)
   (contract.ProfileResult, error)` replaces `DeclareCapability`; `CapabilityDeclaration`
   is gone. `ProcessPathCapability` gained `Schedule(siteId, after)`, answered by the
   process-path cache from the weekly `CPTScheduleChanged` data (a cutoff it cannot
   read, for example a time that is not `HH:mm`, is left out; a schedule with none
   left is "not found"; the next occurrence is looked for within 8 days). Three narrow
   ports serve the use case: `ProfileDeclarer`, `ProfileHistory` (`LastDeclared`,
   implemented by the live gateway from its persisted content) and `ProfileMetrics`.
   The stub records the last profile per site, returns `unchanged` and sends nothing.
2. **Results.** `ProfileResult.Outcome` is one of `declared` (written in this call,
   outcome not read yet), `confirmed` (settled `SUCCESS` in this call), `pending` (an
   earlier write still processing, or another declarer holds the revision),
   `unchanged` (the content is already settled), `failed`, and the three `skipped_*`
   the use case itself reports. A refusal by the network is a result; a call that could
   not be made is an error and the next pass repeats it.
3. **Derivation (Decision 3).** Cutoffs that share a local time are one cutoff
   whatever the ship method (days united, earliest next occurrence, paths united). A
   path counts for the handling time only with a known, positive `cycleTimeP95` that
   fits the longest gap of the schedule; for a slot's ceiling it counts by ADR 0017's
   rule against the time left to that slot's next occurrence (so an unknown cycle time
   stays eligible there, as it does for the offer). A slot with no occurrence in the
   look-ahead has an unknown ceiling. The ceiling memory is keyed by the slot's local
   time and days, so a slot that changed shape starts from what is observed. The
   fingerprint hashes the zone, the handling time in whole minutes, the throughput and
   the slots (sorted), and nothing else. A `LastDeclared` that cannot be read leaves the
   site for the next pass (reported `failed`), because declaring without it could lower
   a ceiling.
4. **The body.** `buildNodeProfileBody` is deterministic byte for byte (the weekday map
   is encoded sorted) and is built and validated before any I/O and before a revision
   is taken: no zone, no cutoff, a time that is not `HH:mm`, a day outside Sunday to
   Saturday, a negative ceiling, no handling time, no throughput, or two cutoffs at one
   time on a shared day is `ErrBadProfile`, never a request. `operatingHoursByDay` is
   `00:00` to `23:59` on each weekday that has a cutoff and absent on the others (the end
   is inclusive in node-topology, which is why `23:59` holds a cutoff at `23:59`);
   `holidays` and `sortDwellMinutes` are not sent; an unknown ceiling omits `maxOrders`.
   The handling time is rounded up to whole minutes in both the use case and the body.
5. **The revision (Decision 4).** `network_node_profiles.site_code` references
   `network_nodes`, so a site with no node has no revision and `DeclareNodeProfile`
   answers `ErrSiteNotRegistered` (the use case reports `skipped_no_node` at `INFO`).
   `AdvanceNodeProfile` is one guarded upsert on `(revision, attempt)` and returns the
   state in force: when another replica has already taken that revision for other
   content, the call sends nothing and reports `pending` at that revision. Stale
   recovery is bounded per call: the first call can write twice (the stale write, then
   the recovered one); a later pass that finds a stale `FAILURE` row recovers with one
   write. The recovered revision is `max(ours, the network's) + 1`.
6. **Failures (Decision 5).** A failure learned in a call is logged at `ERROR` with the
   network's list; the same standing refusal on later passes is logged at `WARN`
   (`stays failed until the profile changes`), so an operator sees it once loudly. A
   write the network refused outright (a `422` on the `PUT`) leaves a `FAILURE` row
   with no transaction id and is final for that content. A retryable failure is
   retried on the next pass at the same revision, as attempt 2, 3, ...: the key is
   `nf-profile-<site>-<revision>-<attempt>` from attempt 2.
7. **The worker (Decision 6).** `cmd/netfulfil/profile.go`: live mode only,
   one pass at boot and then every `NETWORK_PROFILE_INTERVAL` (a malformed or negative
   value warns and keeps `5m`; `0` disables with a `WARN`). The sites are the live
   gateway's own (`LiveGateway.Sites`: `SITE_ID` plus the node file's). Shutdown waits
   for the worker inside the existing 10 second budget. The chart renders
   `NETWORK_PROFILE_INTERVAL` from `config.networkProfileInterval`, in live mode only.
8. **Metrics (Decision 8).** The counter has the unit `{declaration}` and the gauge
   `{revision}`; the gauge is only recorded when an attempt concerned a revision.

**Evidence.** The request body and every route are checked against the vendored
`testdata/vendor-connectivity-openapi.yaml`, whose blob is identical to the
`apis/openapi.yaml` of vendor-connectivity on its `develop` (checked at the time of
writing; the file is the contract and is vendored on purpose). The gateway runs
against a stateful fake Vendor API that judges a revision the way node-topology does (a
revision not above the one held fails the transaction with `ErrStaleProfileRevision`,
and the node view shows the revision), covering the first declaration, the unchanged
content, a restart, a call that never got an answer, both stale paths, a lagging node
view, retryable and final failures, a refused `PUT`, and a lost revision race. The
store runs against a real Postgres (testcontainers, build tag `integration`): the
guarded write, 16 racing writers, a reopen, and migration 0010 up, down and up with
data in it, and a fresh database migrated through every migration, down one step and up
again.

**Not verified.** There is no test against a running node-topology or a real network:
the fake models their documented rule, not their code. No deployment was changed (the
cluster stays `NETWORK_MODE=stub`), so the worker has not run against a real
`CPTScheduleChanged` stream. `ErrIncompleteOperationalProfile` and
`ErrCutoffOutsideOperatingHours` are treated as final from the vendor-connectivity and
node-topology sources, not from a sample of the live service.

## As built (part 2)

Part 2 is on `develop` as this ADR's second deliverable: Decisions 9 to 12 are built and
`SubmitAvailability` is gone. This section supersedes the "not built" notes of the Context
and of "As built (part 1)". The Decisions above are unchanged; what the code does, where it
differs from or adds to them:

1. **The port.** `NetworkGateway.SubmitInventoryFeed(ctx, contract.InventoryFeed)
   (contract.FeedResult, error)` **replaces** `SubmitAvailability`; `AvailabilityUpdate` is
   gone. The feed is `SiteId`, `AsOf`, `Complete` (the wire's full-update flag stays in the
   adapter), `Lines` (`SKU`, `Quantity`, `Basis`, sorted, one per SKU), `Part`/`Parts` and a
   `Fingerprint`. Two narrow ports serve the use case: `InventoryFeeder` and
   `FeedCatalogue` (`ListedSKUs(ctx, site)`, implemented by the product dictionary). The
   stub logs the feed and sends nothing.
2. **The use case** (`FeedInventory`, `feed_plan.go`, pure planning). A site's *pass* is the
   set of its offers sharing the newest `ComputedAt`; that is the feed's `AsOf`, and an offer
   of an earlier pass is not fresh (it is not fed, and it makes the feed incomplete). `Complete`
   is true only when every listed SKU has a fresh offer **and** the feed fits one part: above
   `contract.MaxFeedLines` (5000) it is split into partial parts, all with the same `AsOf`
   (an omitted listing is zeroed by a complete feed and the other parts carry it). The
   fingerprint hashes the complete flag and the lines, never the moment. A SKU with an offer and
   no listing at the site is **logged once per process at `INFO`** and is not an error.
3. **The gateway** (`retailnetwork/inventory.go`). Ledger kind `INVENTORY`, subject
   `<site>#<asOf>` (RFC 3339 with nanoseconds; `#<part>` from the second part), key
   `nf-inventory-<site>-<asOf>` (`-<part>` likewise), the exact body stored before the call and
   the transaction reconciled with Decision 5's helper. A pass is fed once (a second look at the
   same `asOf` reports where the first stands); a feed whose fingerprint changed is sent; an
   unchanged one is sent again once **`inventoryRepeatEvery` = 1 h** has passed since the pass
   it last described (a feed the network *failed* waits the same hour unless its content
   changes). `ErrFeedAlreadyApplied` alone is a success. `ErrListingNotOnNode` is recorded
   **before** the failed outcome (a crash in between records it again), the listing is taken
   from the body we stored by the JSON path of the error, it is logged at `ERROR`, and it stays
   out of the following feeds for **`rejectedListingTTL` = 24 h**, after which it is tried once
   more: a listing the network learns of later is not silenced for good, and one that is still
   wrong costs one failed feed and one `ERROR` a day. Leaving a line out changes the feed's
   fingerprint, so the reduced feed is sent. When every line is held back nothing is sent
   (`skipped_no_listings`, `WARN`). A SKU the dictionary lists no more (a dictionary that
   changed under the use case) is left out and makes the feed incomplete.
4. **Migration 0011** (`inventory_feed`). The ledger's `kind` check is **rebuilt**, not
   widened in place: migration 0008 (ADR 0028) fixed the list at five kinds and `INVENTORY` was
   not among them, so the constraint is dropped and recreated with all six. The new list must
   stay a superset of 0008's (dropping `INBOUND_RECEIPT` or `NODE_PROFILE` would fail on a ledger
   that already holds such a row), and the down migration deletes the `INVENTORY` rows before it
   restores 0008's list. It also creates `network_inventory_feeds (site_code, part, subject,
   fingerprint, as_of, updated_at)`, which is what lets a restart skip an unchanged feed, and
   `network_rejected_listings (site_code, listing_id, reason, rejected_at)`; both reference
   `network_nodes`, like `network_node_profiles`, so a site with no node has neither a feed state
   nor a rejection and `SubmitInventoryFeed` answers `ErrSiteNotRegistered` (the use case reports
   `skipped_no_node` at `INFO`). 0011 was the next free number on `develop` when this was built.
5. **The dictionary reaches the gateway.** The live gateway finds the listing of a SKU through
   `LiveConfig.Listings`, which `wireGateway` now takes: part 1 built the gateway without it, and
   a gateway without a dictionary answers `ErrNoDictionary` rather than guessing. The one
   `*producttranslation.Dictionary` serves three readers through their own ports (inbound
   translation, the feed's catalogue, the gateway's listing lookup). `ListedSKUs` returns fleet
   SKUs only, so the use case names no network identifier and no adapter was needed between
   them; the composition root hands the dictionary over.
6. **The worker (Decision 11).** `cmd/netfulfil/inventory_feed.go`, a copy of the profile worker:
   live mode only, one pass at boot and then every `NETWORK_INVENTORY_INTERVAL` (default `5m`,
   its own variable; `0` disables with a `WARN`; a malformed or negative value warns and keeps
   `5m`), the sites are the gateway's own, and shutdown waits for the worker inside the existing
   10 second budget (the shutdown semantics are unchanged). `5m` is a cadence of *looking*: the
   offers change at the `RECOMPUTE_INTERVAL` (`1m`) and the gateway decides what is *sent*. The
   chart renders `NETWORK_INVENTORY_INTERVAL` from `config.networkInventoryInterval`, in live
   mode only. There is no metric for the feed: its signals are the logs and the `INVENTORY`
   ledger rows (runbook, "The inventory feed").

**Evidence.** The request body, the route, the two bases and the transaction kind are
checked against the vendored `testdata/vendor-connectivity-openapi.yaml` (contract test:
a complete feed, a one-line partial feed, bodies the schema must reject, the DTOs against
the schema's properties and required fields, and the failed-transaction samples that carry
`ErrListingNotOnNode` with its line path and `ErrFeedAlreadyApplied`). The gateway runs
against the stateful fake Vendor API of part 1 extended with the inventory route
(`gateway_live_inventory_test.go`): the ledger row and key, a restart, the hourly repeat,
a split feed, a refused listing left out and tried again after its 24 hours, a crash between the
ledger row and the call. The use case and
its planning are table tests. Against a real Postgres (testcontainers, build tag
`integration`) the feed state and the refused listings are written, refreshed and read
after a reopen, the ledger accepts `INVENTORY`, and migration 0011 goes up, down and up
with a ledger row of each earlier kind, a node and a profile in it (the rebuilt check must
keep admitting all five), next to a fresh database migrated through every migration, down one
step and up again. Through the real entry point (`run(ctx)`, a real Postgres and Kafka, the
fake Vendor API judging a feed as the network does, a fake inventory-storage):
both workers switched off with `0` send nothing; one run declares the profile and feeds the
offers of a pass, complete and with listed SKUs only; later passes, and a restart, send
nothing while the feed is unchanged and under an hour old; after an hour the same feed is
repeated; a changed figure is sent at once; and a listing the node does not carry fails the
first feed at its line, applies nothing, is left out of the next feed and stays out after a
restart.

**Not verified.** Nothing ran against the real network or offer-availability: the fake models
the documented rules (the listing check, the zeroing of a full feed), not their code, and
`ErrFeedAlreadyApplied` is taken from the spec's description, not from a sample of the live
service. No deployment was changed (the cluster stays `NETWORK_MODE=stub`), so the worker has
not fed a real site. The feed reads the offers of the last recompute pass without a lock
against the next one: a read that falls in the middle of a pass finds some SKUs of the new pass
and some of the old, and sends a partial feed (never a complete one with a stale figure); the
next pass of the worker completes it. A feed above 5000 lines is split and tested as a plan
and in the gateway, not through the real entry point.
