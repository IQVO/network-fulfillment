---
id: 0031-advertise-against-the-next-feasible-cutoff
slug: /adr/0031-advertise-against-the-next-feasible-cutoff
title: "31. Advertise against the next feasible cutoff, read in the site's timezone"
sidebar_label: "31. Next feasible cutoff, in the site's zone"
sidebar_position: 31
description: "ADR 0031 amends ADR 0017 and resolves a recorded simplification of ADR 0027: when no path's CycleTimeP95 fits the next CPT cutoff, RecomputeCapabilityOffers walks forward to the first cutoff some path can make (bounded to 7 days); and the schedule cache resolves every cutoff's local time in the schedule's IANA zone (DST-correct, UTC instants), not as UTC."
---

# 31. Advertise against the next feasible cutoff, read in the site's timezone

## Status

**Accepted** (2026-10-30). Two root causes of the same live symptom, decided
together:

- §1 amends the "every path ruled out by cycle time is a proven zero" bullet of
  [ADR 0017](./0017-cycle-time-p95-path-eligibility.md). The rest of ADR 0017 is
  unchanged: the `CycleTimeP95 <= cutoff - now` fit, fail-open on a missing or zero
  cycle time, and no capacity scaling. It implements
  [ADR 0001](./0001-network-fulfillment-bounded-context.md) §8's
  `throughputFeasibleBefore(nextCutoff)` with "next" read as "the next one that can
  be made".
- §2 removes the "cutoffs are read as UTC" simplification that the schedule
  cache's doc comment carried and that
  [ADR 0027](./0027-supply-loop-node-profile-and-inventory-feed.md) Decision 3
  recorded as inherited.

No REST, MCP, A2A or event contract changes. Only the *value* of an advertised
quantity and the instant of a cutoff change. The derivation of the node-profile
ceiling is not changed by §1 (see Consequences); it is corrected by §2, because it
reads the same cache.

## Context

The live symptom (2026-10-30): every listing on the network showed `0` with basis
`THROUGHPUT_CONSTRAINED` for hours each day, and the storefront showed the whole
catalogue as unavailable. Two defects combined to cause it.

**1. A missed truck was treated as "cannot ship".** ADR 0017 made
`RecomputeCapabilityOffers.throughputFeasible` look only at the site's next cutoff
(`ProcessPathCapability.NextCutoff(site, now)`). It returned a proven zero
(`0, known=true`) when every eligible path's `CycleTimeP95` was longer than the time
left to that cutoff. Site-1's PICK path has a p95 of 2 hours, so the offer dropped
to 0 for the 2 hours before **every** cutoff, even though the same units could ship
on the following cutoff, later that day or the next. ADR 0017's Consequences had
called this "the intended signal". It is not.

**2. The zone was ignored.** `processpathcache` resolved each cutoff's `local_time`
(`HH:mm`) and `days_of_week` against `after` as if they were UTC. The schedule's
`timezone` was carried but never applied. Site-1's schedule is `America/Chicago`
with cutoffs at 11:00 and 17:00, and the cache read them as 11:00Z and 17:00Z. At
09:56Z the next cutoff looked 64 minutes away instead of six hours (11:00 CDT =
16:00Z). PICK was excluded and every offer became a proven zero. It also put the
capacity lookups on the wrong instants: wes-work-planning keys
`PathCapacityChanged` by the real `cutoff_at`.

Alternatives weighed:

- **Advertise physical stock when the next cutoff is infeasible.** Rejected: it
  discards the capacity figure of the cutoff the units would actually ship on, and
  it can advertise more than the paths can move.
- **Sum capacity over every cutoff in a window (a day, a week).** Rejected: the
  network asks what can ship on the next truck we can make. A weekly sum would
  present capacity spread over many cutoffs as if it were available now.
- **Walk to the next feasible cutoff (chosen for §1).** It is the smallest change
  that keeps ADR 0017's per-cutoff fit and ADR 0001's single-cutoff formula.
- **Interpret the zone in the use cases instead of the cache.** Rejected: the cache
  is the one place that turns the published schedule into instants. Both of its
  readers (the offer and the node profile) need the same instants, and those must
  match the capacity cache's keys.
- **Fixed UTC offsets per site.** Rejected: an offset is wrong for half of every
  year in a DST zone.

## Decision

### §1. The offer is computed against the first feasible cutoff

1. Start from `c = NextCutoff(site, now)`. While no path of `c` fits
   (`CycleTimeP95 > c.CutoffAt - now` for every path), set
   `c = NextCutoff(site, c.CutoffAt + 1ns)`. `NextCutoff`'s `after` is
   **inclusive**: it returns a cutoff exactly at `after`, hence the nanosecond.
   `TestNextCutoff_AfterIsInclusiveAndAdvancesToTheFollowingCutoff` pins this
   contract of the cache.
2. **The capacity sum is ADR 0017's, taken at that cutoff.** The eligible paths of
   the feasible cutoff contribute `RemainingCapacity(pathId, feasibleCutoffAt)`
   when it is known. A path is eligible when its cycle time fits the time left to
   *that* cutoff; a missing or zero cycle time stays eligible. If no eligible
   path's figure is known, the offer falls back to `PHYSICAL`, not zero.
3. **The walk is bounded to 7 days from now** (`feasibleCutoffHorizon`). A CPT
   schedule repeats weekly, so if no cutoff in a week can be made, none can until
   a cycle time changes. A cutoff exactly 7 days ahead is inside the bound.
4. **A proven zero now means "no feasible cutoff inside the bound".** That is the
   case when the walk runs past the bound, when the cache answers no further
   cutoff, or when the cache does not advance (a defensive stop, so a misbehaving
   cache cannot make the pass spin).
5. **The cutoff is resolved once per pass.** The feasible cutoff and its capacity
   do not depend on the SKU, so every SKU of a pass is advertised against the same
   cutoff.
6. **The other fallbacks are unchanged.** No schedule, an empty path list, or no
   observed capacity figure still fall back to `PHYSICAL`, as in ADR 0017.

### §2. Cutoffs are read in the schedule's IANA timezone

1. **The zone is resolved when the schedule is applied.** When a
   `CPTScheduleChanged` is applied, its `timezone` is loaded with
   `time.LoadLocation` and kept with the schedule.
2. **An unknown or missing zone falls back to UTC.** This covers an empty zone,
   one `LoadLocation` rejects, and the process-local `Local`. Such a zone is read
   as UTC (the old reading), and one WARN per schedule received names the site
   and the zone: `the CPT schedule's timezone is unknown or missing`. It is not
   repeated on every lookup.
3. **The day walk uses the zone's local calendar.** The walk starts from the
   local date of `after` in the zone and advances one local day at a time.
   `days_of_week` is evaluated on that local date: at 2026-10-31T00:30Z it is
   still Friday 19:30 in Chicago. Each instant is built with
   `time.Date(y, m, d, hh, mm, 0, 0, zone)`, so 11:00 stays 11:00 local across a
   DST change and only the UTC instant moves. For a local time that falls inside
   a DST gap or overlap, the instant is whatever Go's `time.Date` resolves.
4. **Instants are returned in UTC**, the form the capacity cache keys `cutoff_at`
   by. This holds for both `NextCutoff` and `Schedule` (a slot's `NextAt`).
   `Schedule` still returns each slot's local time, its local days and the zone
   name unchanged, because that is what the node profile declares.
5. **Every binary that runs the cache embeds the zone database.** It does this
   with a blank import of `time/tzdata` (`cmd/netfulfil/tzdata.go`), so a zone
   resolves on any base image. `TestEveryCmdRunningTheScheduleCacheEmbedsTZData`
   (`internal/architecture`) fails for a cmd that imports `processpathcache`
   without it.
6. **A UTC schedule resolves exactly as before.**

## Consequences

**Easier:**

- The catalogue no longer goes dark before every cutoff.
- An offer reflects the next truck the units can actually make, at the instant
  that truck leaves in the site's own time.
- The capacity lookups for those instants now hit the figures wes-work-planning
  publishes.
- The rule for §1 stays in the application use case, and the rule for §2 stays in
  the adapter that owns the schedule's wire form. `CapabilityOffer.Compute` and
  the ports are unchanged.

**Harder / to watch:**

- An offer computed against a later cutoff reads that cutoff's capacity figure,
  which wes-work-planning may not have published yet. That case is the `PHYSICAL`
  fallback, not a zero.
- A pass makes up to one `NextCutoff` call per infeasible cutoff in the week
  instead of one. The cache is in-process, so this is cheap.
- The offer does not say which cutoff it was computed against. If the network
  ever needs a ship date with the quantity, that is a contract change and needs a
  new ADR.
- A schedule published with a wrong zone is now read in that wrong zone, and the
  WARN only covers zones that cannot be resolved at all.
- The binary is about 450 KB larger because of the embedded zone database.

**Node-profile derivation (ADR 0027).**

- **§1 is deliberately not applied to it.** `slotCeiling` has the same "fit
  against the time left to the next occurrence" shape, but not the defect:
  - Each schedule slot is declared with its own recurring ceiling. A slot whose
    paths cannot make its next occurrence yields `0`, which in a profile means
    *unknown*: the declaration says nothing about that slot. It is never a proven
    zero.
  - `DeclareNodeProfiles` keeps the larger of the observed and the last declared
    ceiling (`History`), so the dip before a cutoff does not lower what was
    declared.
  - Walking to another slot there would be wrong, because it would give one slot
    another slot's capacity.
- **§2 does apply to it, through the shared cache.** A slot's `NextAt`, and so the
  window and the capacity key behind its ceiling, is now the real instant of the
  next local occurrence.
  - The declared local times, days, handling time (a longest-gap figure computed
    on local wall-clock minutes) and zone were already local and do not change.
  - The profile derivation makes no UTC assumption of its own.
