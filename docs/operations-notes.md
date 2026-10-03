# Operations and CI notes

Detail moved out of `AGENTS.md` to keep that file a short map. Everything
here was true when written; re-verify against the files named before
relying on it.

## Persistence and startup

Persistence is opt-in by `DATABASE_URL`: unset means the in-memory repo
(zero-config local run, hermetic unit suite); set means Postgres or a
REFUSAL to boot, never a silent fallback, because the fallback costs the
acknowledgement deadlines this context owes the network.

Startup is RETRIED (~31s budget) and gated by a `startupProbe`
(`cmd/netfulfil/main.go`, `cmd/netfulfil/retry_test.go`, the Helm chart).
Both are required by this cluster: every injected pod's first outbound
dial is reset ~10s after the app starts (Istio native sidecars, so
`holdApplicationUntilProxyStarts` is a no-op) and this service runs
migrations before listening. Without the retry that reset is fatal;
without the probe the kubelet SIGTERMs a booting pod (exit 143, which
reads like an app crash and is not one). The retry does not weaken
fail-closed: after the budget it still refuses to boot.

## Inbound leg and curated files

A poller (`internal/adapters/inbound/poller`) drives `ReceiveNetworkDemand`
on an interval; the read-only REST surface (`internal/adapters/inbound/http`,
contract `apis/openapi.yaml`) exposes what we told the network and whether
the poller is alive. Demand arrives by polling and nothing else (ADR 0001
section 5); do not add a write endpoint without changing that record.

Two curated files, following the fleet's `PATH_CATALOGUE_FILE` precedent:
`PRODUCT_TRANSLATION_FILE` (the ACL dictionary: without it every order is
rejected as untranslatable, and startup logs a warning) and
`NETWORK_SEED_FILE` (stub demand; refuses to boot against a non-stub
gateway).

## CI (`.github/workflows/ci.yml`)

Jobs: `lint`, `guide-lint`, `complexity`, `test`, `integration`,
`api-lint`, `mutation-fast`, `vuln`, `arch-test`, `helm-lint` (`helm lint`
plus `charts/network-fulfillment/tests/`), then `trivy-scan`,
`docker-publish`, `release`.

- `integration` runs the Postgres adapter against a real Postgres started
  by testcontainers INSIDE the test, never a `DATABASE_URL` service
  container with a skip gate (it would report success while asserting
  nothing).
- Template jobs still dropped, not disabled: `bdd` (no `features/`),
  `docs-api-drift` (no docs site), `web` (a `web/` remote exists but has no
  CI job yet), `drift`. Add each in the PR that first needs it. A job that
  fails because a directory is missing is noise, not signal.
- `trivy-scan` builds the image (no push) and blocks on CRITICAL/HIGH with
  a known fix, only for PRs targeting `main`.
- `docker-publish` (push to `main`) pushes `ghcr.io/claudioed/network-fulfillment`,
  cosign-signs it keylessly and attests an SPDX SBOM. `release` runs after
  it: bumps semver from the latest `vX.Y.Z` tag, re-tags the image,
  pushes the Helm chart to `oci://ghcr.io/claudioed`, cuts the git tag and
  a GitHub Release with the chart `.tgz`.
- `guide-lint` (harness v3) runs `scripts/harness/guide_lint.py` and
  `scripts/harness/test_hook.py`; it is blocking as a job but is not in
  the required-status-checks lists below.

## Branch protection (as read from the GitHub API)

- `develop` requires `lint test integration api-lint mutation-fast vuln arch-test helm-lint`
  with `strict: true`.
- `main` requires `lint test helm-lint trivy-scan` with `strict: true` and
  `enforce_admins: true`. `docker-publish`/`release` are push-triggered, not
  PR checks, so they are not required.
- Add `bdd` to the lists when a `features/` directory lands.

## Mutation thresholds

`.gremlins.yaml` thresholds are MEASURED, not copied: `efficacy: 99`,
`mutant-coverage: 92`, set below a real `gremlins unleash ./internal/domain`
run of this repo's own code (100.00% efficacy, 93.33% mutant coverage,
14 killed / 0 lived / 1 not covered). Re-measure when the domain grows;
never copy a sibling's numbers. `MUTATION_FAST_PKG` is
`./internal/domain/networkorder`, the only aggregate.

## Deployment

`warehouse-infra` does not auto-discover repos. This context is deployed
by its own Terraform file in the `warehouse-infra` repo (not in this
one), not through `local.services`. It has its own
database Secret (`network-fulfillment-db`) and a Kong route at
`/api/network-fulfillment`; `config.networkMode` stays at the chart's
`stub` default on purpose.

## Related fleet decisions

`order-management` 0020 (companion: `FeasibleBy`), 0014, 0004 (release is
the cancellation boundary), 0017; `process-path-management` 0010 (the
capability contract advertised against); `fulfillment-execution` 0025 (the
sweep pattern); `retail-network` 0001 (draft in `docs/planning/`).
