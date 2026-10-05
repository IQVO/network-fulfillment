#!/usr/bin/env python3
"""Assert this chart never wires credentials it did not create.

Why this exists: an earlier draft of this chart rendered the three
NETWORK_* secretKeyRefs whenever networkMode was not "stub", but only
created the Secret when inline credential values were supplied. Asking
for sandbox mode without credentials therefore produced a Deployment
referencing a Secret that does not exist -- Kubernetes leaves that pod in
CreateContainerConfigError, an opaque failure that looks like a cluster
problem rather than the binary's own clear fail-closed message.

The guard is a template-time `fail`, and these are its regression tests.
The stub assertions matter just as much: stub mode is what lets the kind
cluster and the e2e suite run with no credentials anywhere, and a chart
that started demanding a Secret by default would break both.

The same invariant now also covers the DATABASE_URL Secret, for the same
reason: a chart that references a database Secret it never creates fails
the identical opaque way, and the default (no database at all, in-memory
repo) must keep needing no Secret whatsoever.

Run: python3 charts/network-fulfillment/tests/test_credential_wiring.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]

INLINE_CREDS = [
    "--set", "credentials.clientId=test-id",
    "--set", "credentials.clientSecret=test-secret",
    "--set", "credentials.refreshToken=test-token",
]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "network-fulfillment", str(CHART_DIR), *extra_args],
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
        ["helm", "template", "network-fulfillment", str(CHART_DIR), *extra_args],
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
        "credentials at all, which is what keeps the kind cluster and e2e "
        "suite credential-free"
    )
    assert not secret_refs(docs), (
        "the default (stub) install referenced a Secret; nothing should be "
        "mounted when no external network is contacted"
    )

    leaked = {n for n in env_names(docs) if n.startswith("NETWORK_CLIENT") or n.startswith("NETWORK_REFRESH")}
    assert not leaked, f"stub mode exposed credential env vars: {sorted(leaked)}"

    print("PASS: stub default creates and references no Secret")


def check_every_ref_is_created() -> None:
    """The actual invariant: never reference a Secret nothing creates."""
    for mode in ("live",):
        docs = render(["--set", f"config.networkMode={mode}", *INLINE_CREDS])
        created, referenced = secret_names(docs), secret_refs(docs)

        dangling = referenced - created
        assert not dangling, (
            f"{mode} mode references Secret(s) {sorted(dangling)} that this "
            f"chart never creates (it creates {sorted(created)}); the pod "
            f"would land in CreateContainerConfigError"
        )
        print(f"PASS: {mode} + inline credentials -- every referenced Secret is created")


def check_existing_secret_is_trusted() -> None:
    """existingSecret is the user's promise that the Secret exists."""
    docs = render([
        "--set", "config.networkMode=live",
        "--set", "credentials.existingSecret=my-own-creds",
    ])

    assert not secret_names(docs), (
        "existingSecret was set but the chart created its own Secret anyway, "
        "which would overwrite externally-managed credentials"
    )
    assert secret_refs(docs) == {"my-own-creds"}, (
        f"expected the container to reference only my-own-creds, got {secret_refs(docs)}"
    )
    print("PASS: existingSecret is referenced and not overwritten")


def check_non_stub_without_credentials_fails_closed() -> None:
    """The regression this file exists for."""
    for mode in ("live",):
        stderr = render_expecting_failure(["--set", f"config.networkMode={mode}"])
        assert "no credentials are set" in stderr, (
            f"{mode} without credentials failed, but not with the explanatory "
            f"message a reader needs. stderr was:\n{stderr}"
        )
        print(f"PASS: {mode} without credentials fails at template time, with a reason")


def check_default_has_no_database() -> None:
    """The zero-config default: in-memory repo, so no DATABASE_URL anywhere.

    This is what keeps `helm install` with no values working against a
    cluster that has no Postgres for this context.
    """
    docs = render([])
    assert "DATABASE_URL" not in env_names(docs), (
        "the default render set DATABASE_URL; the zero-config install must "
        "run on the in-memory repository"
    )
    assert not any(n.endswith("-database") for n in secret_names(docs)), (
        f"the default render created a database Secret: {sorted(secret_names(docs))}"
    )
    print("PASS: default render has no database wiring at all")


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
    check_every_ref_is_created()
    check_existing_secret_is_trusted()
    check_non_stub_without_credentials_fails_closed()
    check_default_has_no_database()
    check_database_url_creates_the_secret_it_references()
    check_database_existing_secret_is_trusted()
    print("\nAll credential- and database-wiring assertions passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
