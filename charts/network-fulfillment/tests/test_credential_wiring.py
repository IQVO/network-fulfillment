#!/usr/bin/env python3
"""Assert this chart never wires a Secret it did not create, and no credential.

Why this exists: an earlier draft of this chart rendered three NETWORK_*
secretKeyRefs outside stub mode but only created the Secret when inline
credential values were supplied, so the Deployment referenced a Secret that did
not exist and Kubernetes left the pod in CreateContainerConfigError, an opaque
failure that looks like a cluster problem. The live adapter (ADR 0026) has no
credential at all: no code reads NETWORK_CLIENT_ID, NETWORK_CLIENT_SECRET or
NETWORK_REFRESH_TOKEN, so the chart no longer renders them in any mode, and
these tests pin that.

The guard that remains is a template-time `fail`: live mode needs
config.networkBaseUrl and config.networkVendorId (the binary refuses to boot
without them, naming each). The stub assertions matter just as much: stub mode is
what lets the kind cluster and the e2e suite run with nothing configured, and a
chart that started demanding a Secret or a live value by default would break
both.

The same invariant covers the DATABASE_URL Secret: a chart that references a
database Secret it never creates fails the identical opaque way, and a chart
with no database source must refuse to render (there is no in-memory mode).

Run: python3 charts/network-fulfillment/tests/test_credential_wiring.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]

# What live mode needs and nothing else: no credential exists (ADR 0026).
LIVE = [
    "--set", "config.networkMode=live",
    "--set", "config.networkBaseUrl=https://vendor.invalid",
    "--set", "config.networkVendorId=V-1",
    "--set", "config.inboundReceivingUrl=http://inbound-receiving.invalid",
    "--set", "config.productMasterUrl=http://product-master.invalid",
]
CREDENTIAL_ENV_PREFIXES = ("NETWORK_CLIENT", "NETWORK_REFRESH")

# Postgres and Kafka are the only runtime: the chart refuses to render without
# a database source and kafka.brokers. Every render here gets both, so these
# tests keep asserting the credential and database wiring rather than the
# refusal (tests/render_test.sh owns the refusals). The database comes from an
# existingSecret, which the chart references but never creates; secret_refs()
# drops it so "no Secret referenced" still means no credentials Secret. A test
# that sets any database.* value itself replaces this default.
BASE_DB_SECRET = "base-db"
REAL_RUNTIME = [
    "--set", f"database.existingSecret={BASE_DB_SECRET}",
    "--set", "kafka.brokers=kafka.invalid:9092",
]


def with_real_runtime(extra_args: list[str]) -> list[str]:
    base = list(REAL_RUNTIME)
    if any(a.startswith("database.") for a in extra_args):
        base = base[2:]
    return [*base, *extra_args]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "network-fulfillment", str(CHART_DIR), *with_real_runtime(extra_args)],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert wiring", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def render_expecting_failure(extra_args: list[str]) -> str:
    proc = subprocess.run(
        ["helm", "template", "network-fulfillment", str(CHART_DIR), *with_real_runtime(extra_args)],
        capture_output=True, text=True,
    )
    if proc.returncode == 0:
        raise AssertionError(
            f"helm template SUCCEEDED for {extra_args}, expected it to fail closed"
        )
    return proc.stderr


def secret_names(docs: list[dict]) -> set[str]:
    return {
        d["metadata"]["name"]
        for d in docs
        if d.get("kind") == "Secret"
    }


def secret_refs(docs: list[dict]) -> set[str]:
    """Every Secret name referenced by a container env var."""
    refs: set[str] = set()
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            for env in c.get("env", []):
                ref = env.get("valueFrom", {}).get("secretKeyRef")
                if ref:
                    refs.add(ref["name"])
    refs.discard(BASE_DB_SECRET)
    return refs


def env_names(docs: list[dict]) -> set[str]:
    names: set[str] = set()
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            names.update(e["name"] for e in c.get("env", []))
    return names


def check_stub_default_needs_no_secret() -> None:
    docs = render([])

    assert not secret_names(docs), (
        "the default (stub) install created a Secret; stub mode must need no "
        "external configuration at all, which is what keeps the kind cluster "
        "and e2e suite free of it"
    )
    assert not secret_refs(docs), (
        "the default (stub) install referenced a Secret; nothing should be "
        "mounted when no external network is contacted"
    )

    leaked = {n for n in env_names(docs) if n.startswith(CREDENTIAL_ENV_PREFIXES)}
    assert not leaked, f"stub mode exposed credential env vars: {sorted(leaked)}"

    print("PASS: stub default creates and references no Secret")


def check_live_needs_no_credential_and_no_secret() -> None:
    """Live mode is configured by values alone: no credential, no extra Secret."""
    docs = render(LIVE)

    assert not secret_names(docs), (
        f"live mode created Secret(s) {sorted(secret_names(docs))}; the Vendor API "
        f"has no credential, so the only Secret is the database one"
    )
    assert not secret_refs(docs), (
        f"live mode referenced Secret(s) {sorted(secret_refs(docs))} beyond the "
        f"database one; there is no credential to mount"
    )
    leaked = {n for n in env_names(docs) if n.startswith(CREDENTIAL_ENV_PREFIXES)}
    assert not leaked, f"live mode exposed credential env vars: {sorted(leaked)}"
    print("PASS: live mode needs no credential and mounts no Secret beyond the database one")


def check_credentials_values_are_inert() -> None:
    """A values file written for the retired credentials block must not wire anything."""
    docs = render([
        *LIVE,
        "--set", "credentials.existingSecret=legacy",
        "--set", "credentials.clientId=x",
        "--set", "credentials.clientSecret=y",
        "--set", "credentials.refreshToken=z",
    ])
    assert not secret_names(docs), f"retired credentials.* created {sorted(secret_names(docs))}"
    assert not secret_refs(docs), f"retired credentials.* wired {sorted(secret_refs(docs))}"
    assert not {n for n in env_names(docs) if n.startswith(CREDENTIAL_ENV_PREFIXES)}
    print("PASS: the retired credentials.* values wire nothing")


def check_live_without_its_values_fails_closed() -> None:
    """Live names every missing value, as the binary does, instead of rendering a pod that cannot boot."""
    stderr = render_expecting_failure(["--set", "config.networkMode=live"])
    for needle in ("config.networkBaseUrl", "NETWORK_BASE_URL", "config.networkVendorId", "NETWORK_VENDOR_ID"):
        assert needle in stderr, f"live with no values: the refusal does not name {needle}. stderr was:\n{stderr}"

    only_url = render_expecting_failure(
        ["--set", "config.networkMode=live", "--set", "config.networkBaseUrl=https://vendor.invalid"])
    assert "config.networkVendorId" in only_url and "config.networkBaseUrl" not in only_url, (
        f"live without a vendor id must name only the vendor id. stderr was:\n{only_url}")

    only_vendor = render_expecting_failure(
        ["--set", "config.networkMode=live", "--set", "config.networkVendorId=V-1"])
    assert "config.networkBaseUrl" in only_vendor and "config.networkVendorId" not in only_vendor, (
        f"live without a base URL must name only the base URL. stderr was:\n{only_vendor}")
    print("PASS: live without a base URL and/or a vendor id fails at template time, naming each")


def check_unknown_mode_is_refused() -> None:
    """The binary reads anything but live as stub, so the chart must not let a typo through."""
    for mode in ("sandbox", "lve", ""):
        stderr = render_expecting_failure(["--set", f"config.networkMode={mode}"])
        assert "config.networkMode" in stderr and "stub" in stderr, (
            f"networkMode={mode!r} was refused without explaining the allowed values. stderr was:\n{stderr}")
    docs = render(["--set", "config.networkMode=LIVE ", "--set", "config.networkBaseUrl=https://vendor.invalid",
                   "--set", "config.networkVendorId=V-1",
                   "--set", "config.inboundReceivingUrl=http://inbound-receiving.invalid",
                   "--set", "config.productMasterUrl=http://product-master.invalid"])
    assert docs, "a case/whitespace variant of live must render like the binary reads it"
    print("PASS: a mode other than stub or live is refused; LIVE (any case) is live")


def check_live_with_seeded_demand_is_refused() -> None:
    """NETWORK_SEED_FILE against the live gateway is a boot error in the binary."""
    stderr = render_expecting_failure([
        *LIVE,
        "--set", "stubDemand.demands[0].networkRef=po-1",
        "--set", "stubDemand.demands[0].siteId=site-1",
        "--set", "stubDemand.demands[0].requiredShipBy=+36h",
    ])
    assert "stubDemand.demands" in stderr and "NETWORK_SEED_FILE" in stderr, (
        f"seeded demand in live mode was refused without naming the cause. stderr was:\n{stderr}")
    print("PASS: seeded stub demand in live mode is refused at template time")


def check_default_has_no_database() -> None:
    """No in-memory mode: without a database source the chart refuses to render."""
    proc = subprocess.run(
        ["helm", "template", "network-fulfillment", str(CHART_DIR),
         "--set", "kafka.brokers=kafka.invalid:9092"],
        capture_output=True, text=True,
    )
    assert proc.returncode != 0, (
        "the chart rendered with no database source; there is no in-memory "
        "mode, so it must refuse"
    )
    assert "database.url or database.existingSecret" in proc.stderr, (
        f"the refusal does not name the missing value. stderr was:\n{proc.stderr}"
    )
    print("PASS: no database source -- the chart refuses to render, naming the value")


def check_database_url_creates_the_secret_it_references() -> None:
    """The same dangling-reference invariant, for the database Secret."""
    docs = render(["--set", "database.url=postgres://u:p@pg:5432/nf?sslmode=disable"])

    assert "DATABASE_URL" in env_names(docs), (
        "database.url was set but the container has no DATABASE_URL env var, "
        "so the binary would silently run on its in-memory repo and forget "
        "every acknowledgement deadline on restart"
    )
    dangling = secret_refs(docs) - secret_names(docs)
    assert not dangling, (
        f"database.url references Secret(s) {sorted(dangling)} that this chart "
        f"never creates (it creates {sorted(secret_names(docs))})"
    )
    print("PASS: database.url -- every referenced Secret is created")


def check_database_existing_secret_is_trusted() -> None:
    """existingSecret is the user's promise that the Secret exists."""
    docs = render([
        "--set", "database.existingSecret=my-own-pg",
        "--set", "database.existingSecretKey=url",
    ])

    assert not any(n.endswith("-database") for n in secret_names(docs)), (
        "database.existingSecret was set but the chart created its own "
        "database Secret anyway, which would overwrite externally-managed "
        "credentials"
    )
    assert "my-own-pg" in secret_refs(docs), (
        f"expected the container to reference my-own-pg, got {secret_refs(docs)}"
    )
    # The key is configurable and a mismatch is invisible until the pod
    # cannot start, so assert the override actually reaches the ref.
    keys = {
        e["valueFrom"]["secretKeyRef"]["key"]
        for d in docs if d.get("kind") == "Deployment"
        for c in d["spec"]["template"]["spec"]["containers"]
        for e in c.get("env", [])
        if e.get("name") == "DATABASE_URL"
    }
    assert keys == {"url"}, f"expected the overridden key 'url', got {keys}"
    print("PASS: database.existingSecret is referenced, not overwritten, with its key")


def main() -> int:
    check_stub_default_needs_no_secret()
    check_live_needs_no_credential_and_no_secret()
    check_credentials_values_are_inert()
    check_live_without_its_values_fails_closed()
    check_unknown_mode_is_refused()
    check_live_with_seeded_demand_is_refused()
    check_default_has_no_database()
    check_database_url_creates_the_secret_it_references()
    check_database_existing_secret_is_trusted()
    print("\nAll secret-, network- and database-wiring assertions passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
