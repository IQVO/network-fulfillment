---
name: how-to-write-an-adr
description: Write an Architecture Decision Record in this repo's numbering and format, including companion ADRs for cross-repo changes. Use when a design decision should be recorded or a change contradicts an existing ADR.
---


# How to write an ADR

> **In network-fulfillment:** ADRs live in **`docs/adr/`** (0001-0008). There
> is no Docusaurus site here, so there is no `about.md` index and no
> `npm run build` check; keep the frontmatter shape of the existing records
> (`id`, `slug`, `title`, `sidebar_label`, `description`, plus
> `sidebar_position` as in ADR 0002 and 0008) so they can be aggregated
> later. Two records are numbered 0002 (`0002-mcp-and-analytics-data-product.md`
> and `0002-retail-network-not-amazon-counterpart.md`); do not reuse a
> number, take the next free one (0009).

Use when a change is architecturally significant — a new bounded-context
integration, a reversal of a prior decision, a cross-repo contract change,
or anything a future reader would otherwise have to reverse-engineer from
the diff. Not every change needs one: a bug fix or a routine feature
addition inside an already-decided architecture doesn't.

## Numbering and location

`docs/adr/NNNN-kebab-case-title.md`, four-digit zero-padded, sequential —
check the highest existing number
(`git ls-tree --name-only origin/develop -- docs/adr/` and pick the next
integer, never reuse or guess).

## Frontmatter (keep all fields the existing ADRs carry)

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
`docs/adr/0008-cloudevents-mandatory-event-envelope.md` for a model
example — it states the exact mechanism (attributes, type naming, removal
of the flat envelope with no coexistence) and is specific enough that the
"how-to-add-an-integration-event" skill can point straight at it.

## Superseding an earlier ADR

Don't edit the old ADR's Decision section. Add a `## Status` line noting
`Superseded by ADR-XXXX` on the OLD one (a one-line patch), and open the
new ADR referencing it: `**Accepted.** <date>. Supersedes [NN. Old title](./NNNN-old-slug.md).`
— for wording, see how ADR 0003 notes that its envelope description is
superseded by ADR 0008 (`docs/adr/0003-transactional-outbox.md`, Status),
and how `docs/adr/0009-retail-network-not-amazon-counterpart.md` amends
ADR 0001.

## Cross-repo decisions: use a companion ADR, not one repo's private opinion

When a decision genuinely spans two bounded-context repos (e.g.
this repo's `network-fulfillment` ADR 0001 and `order-management` ADR
0020), write ONE ADR per repo, each referencing the other
explicitly as "the companion ADR" with a one-line description of the
split of responsibility — `docs/adr/0001-network-fulfillment-bounded-context.md`
is the model, and `docs/planning/retail-network-adr-0001-DRAFT-for-new-repo.md`
holds the not-yet-filed companion for ADR 0002. Don't
write the decision once in one repo and expect the other repo's readers
to find it; each bounded context's docs site is read independently.

## After writing: check the links by hand

There is no docs build in this repo. Open the ADR and confirm every
relative link (`./NNNN-old-slug.md`) resolves, that `id`/`slug` match the
filename, and that the Status line is consistent with any ADR you
supersede or amend. If a docs site is added later, run its build before
opening the PR.
