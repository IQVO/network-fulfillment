
# How to write an ADR

> **In network-fulfillment:** ADRs live in **`docs/adr/`** (not
> `docs/docs/adr/`) — currently only
> `docs/adr/0001-network-fulfillment-bounded-context.md`, whose companion is
> `order-management` ADR 0020. There is no Docusaurus site here, so the
> `about.md` index, `sidebar_position` and the `npm run build` verification
> below do not apply; keep the existing frontmatter shape (`id`, `slug`,
> `title`, `sidebar_label`, `description`) so the ADR can be aggregated
> later. ADR-0013/0015 cited below are other repos' records.

Use when a change is architecturally significant — a new bounded-context
integration, a reversal of a prior decision, a cross-repo contract change,
or anything a future reader would otherwise have to reverse-engineer from
the diff. Not every change needs one: a bug fix or a routine feature
addition inside an already-decided architecture doesn't.

## Numbering and location

`docs/docs/adr/NNNN-kebab-case-title.md`, four-digit zero-padded,
sequential — check the highest existing number
(`git ls-tree --name-only origin/develop -- docs/docs/adr/` and pick the
next integer, never reuse or guess). `docs/docs/adr/about.md` explains the
format to readers; you don't need to touch it when adding a new ADR.

## Frontmatter (Docusaurus needs all five fields)

```yaml
---
id: NNNN-kebab-case-title
slug: /adr/NNNN-kebab-case-title
title: "NN. Title (a short noun phrase, matching the heading)"
sidebar_label: "NN. Short label for the nav sidebar"
sidebar_position: NN
description: "One or two sentences — this shows up in search and link
  previews, so make it stand alone without the rest of the doc."
---
```

`id`/`slug` are the full kebab-case filename (minus `.md`); `title`/
`sidebar_label` repeat the number as plain text (`"13. ..."`, not `#13`);
`sidebar_position` is the bare integer. Getting these inconsistent is the
most common cause of a broken sidebar entry or 404 after merge — verify
by running the docs build (see below) before opening the PR.

## Format: Michael Nygard's template

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play — technical, business, constraints — that make this
decision necessary. Write in the past tense, as if explaining to someone
who wasn't there. State the alternatives seriously considered, not just
the one chosen; a reader six months from now needs to know a simpler
option was weighed and rejected, not assume nobody thought of it.

## Decision
What was actually decided, stated as an active, present-tense
declaration ("we will...", not "we might..."). Be specific about the
mechanism, not just the intent — this section should let a reader
implement the same decision from scratch without asking follow-up
questions.

## Consequences
What becomes easier, what becomes harder, and what future work this
creates or forecloses. Be honest about the downsides — an ADR that only
lists benefits reads as marketing, not a decision record.
```

The `## Decision` section is the part worth the most editing effort: see
ADR-0013 (`docs/docs/adr/0013-location-classification-via-facility-events.md`)
for a model example — it states the exact mechanism (event-fed local
cache replacing a synchronous HTTP read), names the readiness-gate design,
and is specific enough that Task "how-to-add-an-integration-event"'s
consumer-group guidance can point straight at it.

## Superseding an earlier ADR

Don't edit the old ADR's Decision section. Add a `## Status` line noting
`Superseded by ADR-XXXX` on the OLD one (a one-line patch), and open the
new ADR referencing it: `**Accepted.** <date>. Supersedes [NN. Old title](./NNNN-old-slug.md).`
— see ADR-0015 (`0015-remove-rest-identity-layer.md`) for the exact
wording pattern superseding ADR-0014.

## Cross-repo decisions: use a companion ADR, not one repo's private opinion

When a decision genuinely spans two bounded-context repos (e.g.
facility-layout's functional-location roles enabling wes-work-planning's
travel-graph feature), write ONE ADR per repo, each referencing the other
explicitly as "the companion ADR" with a one-line description of the
split of responsibility — see facility-layout's ADR-0016/0017 pair. Don't
write the decision once in one repo and expect the other repo's readers
to find it; each bounded context's docs site is read independently.

## After writing: regenerate and verify the docs build

```bash
cd docs
npm ci
npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw' — this
                 # WILL fail if the frontmatter/slug is wrong or a
                 # cross-reference link is broken
```

A broken ADR link or malformed frontmatter fails the build with a clear
Docusaurus error, not a silent 404 — always run this locally before
opening the PR; several repos in this fleet gate this in CI
(`docs-api-drift`/dedicated docs build job) but not all yet.
