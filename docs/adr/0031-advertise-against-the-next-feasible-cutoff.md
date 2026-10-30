---
id: 0031-advertise-against-the-next-feasible-cutoff
slug: /adr/0031-advertise-against-the-next-feasible-cutoff
title: "31. Advertise against the next feasible cutoff, not the next cutoff"
sidebar_label: "31. Next feasible cutoff"
sidebar_position: 31
description: "ADR 0031 amends ADR 0017: when no path's CycleTimeP95 fits the time left to the next CPT cutoff, RecomputeCapabilityOffers walks forward to the first cutoff some path can still make (bounded to 7 days) and advertises against it; only no feasible cutoff in the bound is a proven zero."
---

# 31. Advertise against the next feasible cutoff, not the next cutoff

## Status

**Accepted** (2026-10-30). Amends the "every path ruled out by cycle time is a
proven zero" bullet of [ADR 0017](./0017-cycle-time-p95-path-eligibility.md);
the rest of ADR 0017 (the `CycleTimeP95 <= cutoff - now` fit, fail-open on a
missing or zero cycle time, no capacity scaling) is unchanged. It implements
[ADR 0001](./0001-network-fulfillment-bounded-context.md) §8's
`throughputFeasibleBefore(nextCutoff)` with "next" read as "next one that can be
made". No REST, MCP, A2A or event contract changes; only the *value* of an
advertised quantity changes. The node-profile derivation of
[ADR 0027](./0027-supply-loop-node-profile-and-inventory-feed.md) is not changed
(see Consequences).

## Context

ADR 0017 made `RecomputeCapabilityOffers.throughputFeasible` look at the site's
next cutoff only (`ProcessPathCapability.NextCutoff(site, now)`) and return a
proven zero (`0, known=true`) when every eligible path's `CycleTimeP95` was longer
than the time left to it.

On the live cluster (2026-10-30) the PICK path of site-1 has a p95 of 2 hours.
For the 2 hours before **every** cutoff, every listing on the network therefore
dropped to 0 with basis `THROUGHPUT_CONSTRAINED`, although the same units could
ship on the following cutoff, later the same day or the next. The storefront
showed the whole catalogue as unavailable for hours each day. ADR 0017's own
Consequences had called this "the intended signal"; it is not: missing the next
truck is not "cannot ship".

Alternatives weighed:

- **Advertise physical stock when the next cutoff is infeasible.** Rejected: it
  throws away the capacity figure of the cutoff the units would actually ship on,
  and can advertise more than the paths can move.
- **Sum capacity over every cutoff in a window (a day, a week).** Rejected: the
  network asks what can ship on the next truck we can make, and a weekly sum
  would advertise capacity spread over many cutoffs as if it were available now.
- **Walk to the next feasible cutoff (chosen).** The smallest change that keeps
  ADR 0017's per-cutoff fit and ADR 0001's single-cutoff formula.

## Decision

1. **The offer is computed against the first feasible cutoff.** Starting from
   `c = NextCutoff(site, now)`: while no path of `c` fits (`CycleTimeP95 >
   c.CutoffAt - now` for every path), `c = NextCutoff(site, c.CutoffAt + 1ns)`.
   `NextCutoff`'s `after` is **inclusive** (it returns a cutoff exactly at
   `after`), hence the nanosecond; `TestNextCutoff_AfterIsInclusiveAndAdvancesToTheFollowingCutoff`
   pins that contract of the cache.
2. **The capacity sum is ADR 0017's, at that cutoff.** The eligible paths of the
   feasible cutoff (those whose cycle time fits the time left to *it*, missing or
   zero cycle time staying eligible) contribute `RemainingCapacity(pathId,
   feasibleCutoffAt)` when known. No known figure among them is still the
   `PHYSICAL` fallback; it is not a zero.
3. **The walk is bounded to 7 days from now** (`feasibleCutoffHorizon`). A CPT
   schedule repeats weekly, so if no cutoff in a week can be made none can until a
   cycle time changes. A cutoff exactly 7 days ahead is inside the bound.
4. **A proven zero is now "no feasible cutoff inside the bound"**: the walk ran
   past the bound, the cache answered no further cutoff, or the cache did not
   advance (a defensive stop so a misbehaving cache cannot make the pass spin).
5. **Resolved once per pass.** The feasible cutoff and its capacity do not depend
   on the SKU; every SKU of a pass is advertised against the same cutoff.
6. The unchanged fallbacks: no schedule, an empty path list, or no observed
   capacity figure still fall back to `PHYSICAL`, as in ADR 0017.

## Consequences

**Easier:** the catalogue no longer goes dark before every cutoff; an offer
reflects the next truck the units can actually make. The rule stays in the
application use case: `CapabilityOffer.Compute` and the ports are unchanged.

**Harder / to watch:**

- An offer computed against a later cutoff reads that cutoff's capacity figure,
  which wes-work-planning may not have published yet; that is the `PHYSICAL`
  fallback, not a zero. Capacity is still matched on the exact cutoff instant.
- A pass makes up to one `NextCutoff` call per infeasible cutoff in the week
  instead of one; the cache is in-process, so this is cheap.
- The offer does not say which cutoff it was computed against; the read surface
  is unchanged. If the network ever needs a ship date with the quantity, that is a
  contract change and a new ADR.

**Node-profile derivation (ADR 0027) is deliberately unchanged.** `slotCeiling`
has the same "fit against the time left to the next occurrence" shape, but not the
defect: each schedule slot is declared with its own recurring ceiling, a slot whose
paths cannot make its next occurrence yields `0`, which in a profile means
*unknown* (the declaration says nothing about that slot), never a proven zero; and
`DeclareNodeProfiles` keeps the larger of the observed and the last declared
ceiling (`History`), so the dip before a cutoff does not lower what was declared.
Walking to another slot there would be wrong: it would give one slot another
slot's capacity.
