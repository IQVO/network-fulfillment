---
id: 0017-cycle-time-p95-path-eligibility
slug: /adr/0017-cycle-time-p95-path-eligibility
title: "17. A path is eligible for a cutoff only if its CycleTimeP95 fits the time left"
sidebar_label: "17. CycleTimeP95 path eligibility"
sidebar_position: 17
description: "ADR 0017 makes RecomputeCapabilityOffers honour the cycle-time input ADR 0001 names: a path counts toward throughputFeasible only if CycleTimeP95 <= cutoff - now; missing or zero cycle time stays eligible (fail-open); remaining capacity is not scaled."
---

# 17. A path is eligible for a cutoff only if its `CycleTimeP95` fits the time left

## Status

Accepted (2026-10-06). Resolves the "`contract.EligiblePath.CycleTimeP95`"
design note that [ADR 0015](./0015-docs-audit-contract-corrections.md) left
open. Implements the cycle-time input that
[ADR 0001](./0001-network-fulfillment-bounded-context.md) §8 already names for
`throughputFeasibleBefore`; it does not amend ADR 0001. No REST, MCP or event
contract changes; only the *value* of an advertised quantity can change.

## Context

ADR 0001 lists `cycleTimeP95 + CPTSchedule` as the inputs of
`throughputFeasibleBefore(nextCutoff)`. The cache already carried each eligible
path's `CycleTimeP95` (`contract.EligiblePath`), but
`RecomputeCapabilityOffers.throughputFeasible` summed `RemainingCapacity` over
every path in `NextCutoff.Paths` and never looked at it. A path that cannot
complete work inside the remaining time to the cutoff was therefore still
counted as capacity the network could be promised.

## Decision

A path is eligible for a cutoff **only if `CycleTimeP95 <= cutoff - now`**
(`now` is the recompute pass's clock reading; equality is eligible).

- **Missing or zero cycle time stays eligible (fail-open).** `CycleTimeKnown ==
  false`, or a known value `<= 0`, is no data, and no data is not evidence that
  a path is too slow. Any stale value attached to an unknown cycle time is
  ignored.
- **No capacity scaling.** An eligible path contributes its full
  `RemainingCapacity`; an ineligible one contributes nothing. Cycle time is a
  yes/no filter, not a multiplier.
- **Every path ruled out by cycle time is a proven zero**, not the "no capacity
  figure observed" fallback: `throughputFeasible` returns `(0, known=true)` and
  the offer is `THROUGHPUT_CONSTRAINED` at 0 (when physical stock is positive),
  because nothing can make the cutoff. The existing fallbacks are unchanged: no
  schedule, an empty path list, or no observed capacity figure among the
  eligible paths still falls back to the `PHYSICAL` basis.
- The rule lives in the application use case; `CapabilityOffer.Compute` and the
  `ProcessPathCapability` / `PathCapacity` ports are unchanged.

## Consequences

**Easier:** the advertised throughput honours every input ADR 0001 names;
the rule is minimal, conservative and reversible (delete the filter to restore
the previous behaviour).

**Harder / to watch:** when a cutoff is close, offers can drop to 0 until the
next cutoff, which is the intended signal. A path whose published
`cycle_time_p95` is overstated will be excluded; the fail-open rule only covers
*missing* data, not wrong data. The comparison uses the cache's last observed
figure at recompute time (`CAPABILITY_OFFER` recompute interval), so it can lag a
changed cycle time by one pass.
