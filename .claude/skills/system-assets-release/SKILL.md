---
name: system-assets-release
description: Build this repo's system/<version>/ folder (OpenAPI, AsyncAPI, MCP server card + primitives, A2A agent card, application descriptor, interaction map) for a release, and keep system/interactions.overlay.yaml current. Use when a release is tagged, when an API, event, MCP tool, A2A skill or integration changes, or when harness-health reports a release without its system/<version>/ folder.
---

# System assets per release (architecture map)

## The rule

**Every contract change produces a new asset.** A contract change is any change to what this context
exposes or how it integrates: `apis/openapi*.yaml`, `apis/asyncapi*.yaml` (channels, event types,
payloads), an MCP tool / resource / prompt (name, schemas, annotations, authority), an A2A skill or
card, a new or removed outbound call, a new or removed consumed event.

The change is not finished until the release that carries it has its folder `system/<version>/` on
`develop`. The sequence is fixed:

1. Merge the contract change (the usual PR flow; spec, code and `system/interactions.overlay.yaml` together).
2. Release it (tag + Helm chart) and deploy it to k3s.
3. Build `system/<version>/` from that release (below) and PR it into `develop`.

A release with no contract change still gets its folder, so the version history has no gaps.
The fleet `harness-health` report flags a repo whose latest release has no folder.

## What the folder contains (plain files, never an archive)

```
system/<version>/            <version> = release tag without "v" = Helm chart version
  application.yaml           application + subdomain + bounded context + tags (from the ecosystem repo)
  system-assets.yaml         the four assets, release tag/commit/chart/images, release.drift
  interactions.yaml          every call this context makes + every event it consumes (technology,
                             operation, business context)
  openapi/ asyncapi/         the contracts at the release tag
  mcp/server-card.json       MCP Server Card (bex.co / SEP-2127); version == chart version
  mcp/primitives.json        live tools/resources/prompts of the deployed release
  a2a/agent-card.json        live agent card of the deployed release
  SHA256SUMS
system/interactions.overlay.yaml   hand-written business facets; lives next to the version folders
```

Never hand-edit a committed `system/<version>/`: rebuild the SAME version from the same tag instead.
Never edit application code in this PR: it carries only `system/`.

## Build a release

Work in a fresh worktree off `origin/develop` (never in a dirty main checkout), then run the builder
from an up-to-date warehouse-harness-template develop:

```bash
T=~/warehouse-systems/warehouse-harness-template      # must be at origin/develop
git fetch origin && git worktree add -b feature/system-<version> ../.worktrees/<repo>-system-<version> origin/develop
python3 $T/tools/system-assets/build_system_bundle.py --app <repo> --version <version> \
        --repo ../.worktrees/<repo>-system-<version>
cd ../.worktrees/<repo>-system-<version>/system/<version> && shasum -a 256 -c SHA256SUMS
```

The builder refuses unless: the GitHub release and the OCI chart (same version) exist, every pod
behind the MCP and A2A Services runs the tag's commit, and every reference in the overlay resolves
against the release. Fix a failure in the overlay or deploy the release; never loosen the check.

Then `git add system` (only that), commit, open the PR into `develop`, merge on green CI.
Report the `release.drift` list from `system-assets.yaml` to the user: those are runtime mismatches
with the standard (served version != chart version, protocol version, wrong chart deployed). They are
recorded, never rewritten; `--strict` makes them fatal.

## Keep the overlay current (same PR as the code)

`system/interactions.overlay.yaml` holds what no machine artifact knows: capability, process, use case,
triggers, purpose. The builder fails on any published or consumed CloudEvents type that is not mapped.
Update it in the SAME change that adds or removes an integration.

- `interactions[]`: one entry per (use case, event type, channel); technology is exactly
  `rest | event.publish | mcp | a2a`; `operation` names the asset, operation id and `effect`;
  `business` has `capability`, `process`, `useCase`, `triggers[]`, `purpose`, `criticality`, `onFailure`.
- `subscriptions[]`: one per consumed channel; `operation` carries the producer asset and op id from the
  pinned producer contract, the own `consumerOperation` and the event `types`; `business` lists
  `useCases` or a read-model `projection` port.
- `triggers[]`: `{kind: api, via: rest|mcp|a2a, ref: <operationId|tool|skill>, actor}` or
  `{kind: event, ref: <consumed CE type>, actor: system}`.
- A context that may not call siblings (see `.claude/rules/fleet/context-boundaries.md` and this repo's
  guide) has NO rest/mcp/a2a interactions, only `event.publish` plus subscriptions.
- Names come from this repo's ubiquitous language and real use-case types of the application layer.

Full standard: warehouse-harness-template `tools/system-assets/STANDARD.md` (Parts 0, A, B, D, E).

## Pitfalls

- Building from a deployment that is not the release: the builder checks pod images; deploy first.
- Reading `develop` instead of the tag: contracts are read with `git show <tag>:<path>`.
- A new AsyncAPI channel or event type with no overlay entry fails coverage: add it to the overlay.
- The ecosystem repo lags the release (tags, purpose): the descriptor reflects ecosystem `origin/develop`;
  fix the source there, never invent values here.
