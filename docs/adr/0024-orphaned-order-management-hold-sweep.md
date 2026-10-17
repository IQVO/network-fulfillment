---
id: 0024-orphaned-order-management-hold-sweep
slug: /adr/0024-orphaned-order-management-hold-sweep
title: "24. Sweep orphaned order-management holds past the acknowledgement window"
sidebar_label: "24. Orphaned-hold sweep"
description: "ADR 0024 — network-fulfillment records the order-management hold id as soon as the hold exists, and a new SweepOrphanedHolds pass (same SWEEP_INTERVAL ticker, ORPHAN_HOLD_GRACE after acknowledgeBy) cancels a hold nobody decided, classifying order-management's answer (2xx/404 done, 409/422 already released, 5xx/timeout/open breaker retry). Reports OrphanedHoldCancelled. Closes order-management ADR 0020's accepted deferral (gap OM-0020)."
---

# 24. Sweep orphaned order-management holds past the acknowledgement window

## Status

**Accepted** (2026-10-17). Builds on [ADR 0001](./0001-network-fulfillment-bounded-context.md)
§6 (the acknowledgement clock and its sweep) and closes the open gap its
Consequences name ("orphaned-hold sweep as an open gap"). It is the
follow-up that `order-management` ADR 0020 ("Accepted deferral") asks for:
*"sweep orphaned holds past the acknowledgement window by calling
order-management's cancel"*. Migration `0005` and one additive event
(`OrphanedHoldCancelled`); no existing contract changes.

## Context

A network-originated order reaches `order-management` as a HELD order
(`POST /orders`, `releaseOnAllocation=false`, a `requiredShipBy`). That call
makes real inventory reservations. If nobody ever decides (release with
`POST /orders/{id}/release`, or cancel with `DELETE /orders/{id}`) the hold
keeps its reservations forever. `order-management` deliberately does not sweep
them: a hold's lifetime is bounded by the acknowledgement clock this context
owns, so the trigger and the timeout belong here and a second TTL there would
race this one.

Reading the code (origin/develop, 2026-10-16) shows the premise of that
deferral — "this context can cancel what it holds" — was not true:

1. **The hold id was recorded too late.** `ReceiveNetworkDemand` saves the
   order `NEW`, then calls `RaiseHeldOrder` (the hold now exists), and only
   `acknowledge()` stored the OM order id, in the same write that moves the
   order to `SUBMITTED`. The infeasible branch never stored it at all.
2. **The database forbade storing it earlier.** `network_orders` carries
   `CHECK local_order_id IS NULL OR state IN ('SUBMITTED','ACKNOWLEDGED','CONFIRMED')`
   (migrations `0001`, `0003`). `NEW` and `REJECTED` could not hold an id.
3. **`RejectOverdueOrders` therefore cannot free a hold.** It cancels only
   `if o.LocalOrderId() != nil`, but it only ever sees `NEW` orders, which never
   carry an id. The branch is dead: an overdue order whose raise succeeded is
   rejected and its hold stays.
4. **A rejection after a failed submission cannot persist.** `fail()` in
   `ReconcileSubmittedOrders` cancels the hold and calls `Reject()`, which keeps
   the id on a `REJECTED` aggregate; `REJECTED` is not in the CHECK above, so
   that `Save` is refused, the order stays `SUBMITTED` and the pass retries
   forever (proved red by the repository integration test added with this ADR).

So the sweep this ADR adds has nothing to sweep unless the hold is recorded the
moment it exists. Recording it is part of the decision.

### Every code path that can leave a hold behind

"Record" is NF's own row. "OM" is order-management. `Idempotency-Key` is
`netful-raise-<networkRef>`, which `RaiseHeldOrder` always sends and which OM
replays for 24 hours (OM ADR 0032, `IDEMPOTENCY_KEY_TTL`); since the raise
happens after receipt, the replay window always covers the whole
acknowledgement window.

| # | Path | NF record when the hold is left | Disposition |
|---|---|---|---|
| P1 | Raise succeeded; NF crashed, lost the response or failed to save before the id was written (the gap between OM's commit and NF's write) | `NEW`, no id | **Residual, narrowed to that gap.** Before the window closes the poller re-delivers the demand, `resume` repeats the raise with the same `Idempotency-Key`, OM replays the same order id, and it is recorded then: no second hold. After the window nothing can name the hold: NF never reads OM state (only the `DELETE` answer is used) and replaying the raise after OM's key TTL would create a second hold. Runbook: cancel by hand with the OM order id. |
| P2 | Infeasible demand: raise succeeded, `CancelHeldOrder` failed | `NEW`, id (recorded at raise) | **Covered by `RejectOverdueOrders`** (its cancel now fires because the id exists) and re-tried by the poller inside the window; **backstopped by the sweep** after `acknowledgeBy + grace`. |
| P3 | Infeasible demand: cancel succeeded, saving `REJECTED` or its event failed | `NEW`, id, hold already gone in OM | **Covered.** OM's cancel is idempotent (204 on a cancelled order), so the re-cancel by the poller, `RejectOverdueOrders` or the sweep is harmless. |
| P4 | Accepted: `SUBMITTED` saved with the id, then `SubmitAcknowledgement` to the network failed | `SUBMITTED`, id | **Not an orphan by state.** The order is still being decided by reconciliation; `ReconcileSubmittedOrders` owns it. |
| P5 | `SUBMITTED` and the network's transaction status stays `PENDING` (or unreachable) | `SUBMITTED`, id | **Residual by design.** The network is the only judge of its own record; cancelling could contradict a later `SUCCESS` and leave a committed order with no hold. Visible as `list_network_orders state=SUBMITTED`; runbook step. |
| P6 | Network reports `FAILURE`: `fail()` cancels, or fails to cancel, then rejects | `SUBMITTED`, id (cancel failed) / `REJECTED`, id (cancel done) | **Covered.** A failed cancel leaves `SUBMITTED` and every reconcile pass retries; the successful path now persists because migration `0005` removes the CHECK that refused `REJECTED` with an id (finding 4). |
| P7 | Network reports `SUCCESS`: `ACKNOWLEDGED` saved, then `ReleaseHeldOrder` failed | `ACKNOWLEDGED`, id | **Residual, and not a cancel case.** NF told the network it will ship; cancelling would destroy a commitment. The right recovery is retrying the (idempotent) release, but nothing records "release confirmed", and `ListSubmitted` no longer lists the order, so no pass retries. Recorded as a follow-up (release-first ordering or a release marker) because it reopens ADR 0016's record-before-release choice; until then runbook step 3 releases by hand. |
| P8 | Overdue `NEW` order and OM answers the cancel with a code `RejectOverdueOrders` reads as a failure (404 gone, 409 released) | `NEW`, id | **Covered by the sweep.** `RejectOverdueOrders` still treats any non-2xx as a failure and retries; the sweep classifies the answer, records the outcome, and `RejectOverdueOrders` then skips the cancel and rejects. |
| P9 | A hold NF recorded and then cancelled itself (`reject`, `rejectOne`, `fail`) | any, outcome `CANCELLED` | **Closed.** Each of those paths records the outcome in the same save as the rejection, so the sweep never re-cancels it. |
| P10 | Rows rejected before this ADR whose raise had succeeded (P1/P2 era) | `REJECTED`, no id | **Residual, unfindable** (same reason as P1). Runbook step 4. |
| P11 | A hold with no NF record at all | none | **Cannot arise from this code**: `NEW` is saved before the raise (`ReceiveNetworkDemand` step 2 precedes step 3), so an NF row exists before any hold. A hold created by someone else directly on OM is out of scope. |

## Decision

### 1. Record the hold the moment it exists

`NetworkOrder.RecordHold(id)` stores the OM order id on a `NEW` order
(idempotent for the same id, `ErrLocalOrderAlreadyLinked` for another) and
`ReceiveNetworkDemand.answer` saves it immediately after `RaiseHeldOrder`
returns, before it decides. `LinkLocalOrder` (used at `SUBMITTED`) becomes
idempotent for the same id. The id still lives in the existing
`local_order_id` column: one correlation, one column (AGENTS.md rule 4).

### 2. A hold has an outcome

`NetworkOrder` gains `HoldOutcome` (`CANCELLED` | `ALREADY_RELEASED`, unset while
the hold is open) and its settle instant. `reject`, `rejectOne` and `fail`
record `CANCELLED` right after their successful cancel, in the same save as
the rejection; `rejectOne` skips the cancel when the outcome is already set.

### 3. The orphan predicate

A hold is **orphaned** when all of these are true:

- NF's record has the OM order id (`localOrderId != nil`);
- the hold has no outcome (neither a cancel nor a found-released was confirmed);
- the state is `NEW` or `REJECTED`;
- `now > acknowledgeBy + ORPHAN_HOLD_GRACE`.

`NEW` before the window closes is being decided and is never touched. `NEW`
after the window is not being decided any more: an answer after the SLA is worse
than none (`RejectOverdueOrders`). `REJECTED` is a made decision.
`SUBMITTED`, `ACKNOWLEDGED` and `CONFIRMED` are never swept: NF committed to
them or is waiting for the network's verdict (P4, P5, P7). The grace
(default `1h`) lets `RejectOverdueOrders` do its normal work first, so the sweep
is a backstop, never a second actor racing it. The predicate is a domain method
(`NetworkOrder.HoldOrphaned(now, grace)`), the repository only narrows the
listing (`ListOpenHolds`: id recorded, no outcome, `NEW` or `REJECTED`).

### 4. The sweep and order-management's answers

`SweepOrphanedHolds` (new use case, own file, independent of the two existing
sweeps) calls the existing `CancelHeldOrder` (`DELETE /orders/{id}`) for each
orphaned hold and classifies the answer; the Planner adapter turns the OM
status into two sentinel errors in `internal/application/contract`:

| OM answer | Meaning | Sweep action |
|---|---|---|
| 2xx (204) | cancelled | outcome `CANCELLED`, publish `OrphanedHoldCancelled` (`answer=CANCELLED`) |
| 404 | the hold does not exist | outcome `CANCELLED`, publish `OrphanedHoldCancelled` (`answer=NOT_FOUND`) |
| 409 / 422 | already released (BR6) | **not an orphan**: outcome `ALREADY_RELEASED`, counted in the pass's `orphaned holds swept` WARN line (`alreadyReleased`) and the `already_released` metric outcome, no event, never retried |
| 5xx, timeout, transport error, open breaker | unknown | outcome untouched, retried on the next pass, never marked done |

The circuit breaker counts 404 and 409/422 as successful calls (they are
answers, not dependency failures) and refuses to call when open, which the sweep
treats as "retry later". Save and `OrphanedHoldCancelled` commit in one
transaction (the outbox, ADR 0003), so the settle and the report cannot diverge,
and a failed save is retried safely because OM's cancel is idempotent. The
second pass finds nothing: the order has an outcome and is no longer listed.

### 5. Scheduling and configuration

The sweep runs on the same in-process ticker mechanism and the same
`SWEEP_INTERVAL` as the other two passes (`runSweepOrphanedHolds` next to
`runSweep` and `runRejectOverdue` in `cmd/netfulfil/workers.go`); there is no
second scheduler. Two new knobs, rendered by the chart into the ConfigMap and
mounted through `envFrom` (fleet rule R10):

| Env var | Chart value | Default | Meaning |
|---|---|---|---|
| `ORPHAN_HOLD_SWEEP_ENABLED` | `config.orphanHoldSweepEnabled` | `true` | `false` turns the pass off (a leaked hold is the worse failure, so on by default) |
| `ORPHAN_HOLD_GRACE` | `config.orphanHoldGrace` | `1h` | Go duration added to `acknowledgeBy`; `0` means no grace; a malformed or negative value logs a warning and uses the default |

### 6. The reported fact

`OrphanedHoldCancelled`, raised by the `NetworkOrder` aggregate, CloudEvents type
`com.warehouse.wes.network-fulfillment.networkorder.OrphanedHoldCancelled`,
dataschema `urn:warehouse:network-fulfillment:<events|analytics>:OrphanedHoldCancelled:v1`,
subject and partition key `networkRef`, data
`{networkRef, siteId, localOrderId, acknowledgeBy, answer, at}`. It is published
through the outbox on the integration and analytics streams like every other
event. The analytics consumer ignores unknown types by design. No existing type
or dataschema changes. It is not edge-triggered and is published once per hold
(the outcome stops the next pass), except for the multi-replica case below.

### 7. Metric

One counter, following the fleet's Tier 2 convention (wes-work-planning ADR
0013, reused here as this repo's ADR 0020 adopts that convention):
`network_fulfillment.orphaned_holds.swept`, unit `{hold}`, attribute `outcome` =
`cancelled` | `already_released` | `failed`. The use case calls a port
(`ports.OrphanHoldMetrics`), the instrument lives in the `telemetry` adapter.

### 8. Schema (migration `0005_hold_outcome`)

Additive columns `hold_outcome` and `hold_settled_at` (both NULL), a CHECK that
an outcome needs an id and that the pair is set together, a partial index for the
listing, and **removal of `local_order_requires_answer`** (findings 2 and 4).
No row, column or table is dropped; every existing row stays valid and an older
binary keeps working against the new schema. The down migration clears ids that
the old CHECK would refuse (`NEW`, `REJECTED`), drops the new objects and
restores the old CHECK.

## Consequences

**Easier.** A hold NF knows about is freed within `grace + SWEEP_INTERVAL` of the
window closing, or its release by someone else is recorded; operators get one
event, one counter and a runbook procedure. `RejectOverdueOrders`'s hold branch
finally fires. A rejection after a failed network submission persists.

**Harder.** One more write per received order (the id is saved before the
decision), and one more column pair to keep in step with the aggregate.
`RejectOverdueOrders` and the sweep both cancel an overdue `NEW` order's hold;
they cannot conflict because OM's cancel is idempotent and the outcome is
recorded, but a reader must know there are two actors (the sweep only after
`grace`).

**Multi-replica.** Like the two existing sweeps (ADR 0006), every API replica
runs the pass. Two replicas can both cancel the same hold (idempotent) and both
publish `OrphanedHoldCancelled`; consumers must tolerate a rare duplicate. Row
locking is not added.

**Residual (stated, not hidden).** P1 after the window, P5, P7 and P10 above.
P7 (`ACKNOWLEDGED` whose release never succeeded) is the one that matters: it is
a leaked hold of a committed order and needs its own decision (release-first
ordering or a release marker, amending ADR 0016).

**Alternatives rejected.**

- *Cancel `SUBMITTED` orders past the window.* They are still being decided by
  reconciliation; a later `SUCCESS` would commit an order with no hold.
- *A TTL inside order-management.* Rejected by OM ADR 0020: it would race the
  clock that lives here.
- *Read OM (`GET /orders/{id}`) to discover held orders or their state.* NF
  reaches OM only through the existing REST client (context map R11) and uses
  nothing but the `DELETE` answer.
- *Replay `RaiseHeldOrder` to recover the id.* After OM's 24h key TTL it creates
  a second hold.
- *A new `NetworkOrder` state for "hold cancelled".* The hold outcome is
  orthogonal to the protocol state (`REJECTED` and `NEW` both occur); a state
  would multiply the lifecycle.
