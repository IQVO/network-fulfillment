---
id: 0033-a-full-cutoff-misses-only-its-own-truck
slug: /adr/0033-a-full-cutoff-misses-only-its-own-truck
title: "33. A full cutoff misses only its own truck"
sidebar_label: "33. A full cutoff"
sidebar_position: 33
description: "ADR 0033 amends ADR 0031: RecomputeCapabilityOffers also walks past a cutoff whose observed path capacity is used up, instead of advertising a proven zero for every SKU of the site."
---

# 33. A full cutoff misses only its own truck

## Status

**Accepted** (2026-11-05). Amends §1 of
[ADR 0031](./0031-advertise-against-the-next-feasible-cutoff.md). Nothing else in
ADR 0031 or [ADR 0017](./0017-cycle-time-p95-path-eligibility.md) changes. No REST,
MCP, A2A or event contract changes: only the value of an advertised quantity.

## Context

ADR 0031 stopped a cutoff that no path can *make in time* from zeroing the
catalogue: such a cutoff is "misses this truck", and the offer is computed against
the next one. A cutoff whose paths can make it but have **used up their capacity**
still ended the walk: `capacityBefore` answered `total=0, known=true`, which
`CapabilityOffer.Compute` turns into a proven zero with basis
`THROUGHPUT_CONSTRAINED` for **every SKU of the pass**. Downstream,
`ITEM_QUANTITY_NOT_AVAILABLE` then fails the quote for every tier, including an
order that could leave on the following truck. One full truck made the whole site
unsellable: the same "dark catalogue" symptom ADR 0031 fixed for cycle time.

## Decision

- `throughputFeasible` skips a cutoff whose eligible paths have **observed**
  capacity figures that sum to zero, exactly as it skips a cutoff no path can make
  (`anyPathFits` false): it asks the cache for the cutoff strictly after it and
  repeats, bounded by `feasibleCutoffHorizon` (7 days).
- A cutoff whose capacity was never observed is **not** full: it still falls back
  to `PHYSICAL`, as ADR 0017 and ADR 0031 §1.6 say.
- A proven zero now means: no cutoff inside the bound can be made **and** has room.
- The blast radius of a full truck is therefore the units that cannot ship before
  the next truck with room, not the whole site's catalogue.

## Consequences

- A site whose next truck is full keeps advertising the capacity of the next truck
  that has room. Quantities for a customer who needs the full truck itself are
  refused downstream by `delivery-promise`, whose `PromiseBook` and departures
  are per cutoff.
- Same trade-off as ADR 0031: the offer does not say which cutoff it was computed
  against.
- A pass makes up to one more `NextCutoff` call per full cutoff in the week.
