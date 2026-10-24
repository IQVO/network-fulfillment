#!/usr/bin/env python3
"""Assert the chart renders EXACTLY the environment the netfulfil binary reads.

Why this exists: the chart and the binary are two hand-kept lists of the same
names, and they drifted before (the chart mounted NETWORK_CLIENT_ID,
NETWORK_CLIENT_SECRET and NETWORK_REFRESH_TOKEN for an adapter that no code
reads). ADR 0026 added NETWORK_VENDOR_ID and NETWORK_NODES_FILE;
this test makes the next addition, rename or removal fail here instead of in a
cluster.

The binary's side is a scan of the source, not a list kept in this file: every
non-test Go file under cmd/netfulfil and internal/ is searched for the calls that
read the environment (os.Getenv, the local getenv, LookupEnv, durationEnv, envInt
and the `get` accessor of loadBootConfig/loadNetworkConfig). The chart's side is
what the api Deployment's container sees: the keys of the main ConfigMap
(mounted with envFrom) plus its inline env entries.

Three assertions, for a stub render and for a live render:

  1. Nothing is rendered that the binary does not read.
  2. Every variable the binary reads is rendered, except the ones listed in
     EXTRA_ENV_ONLY below: knobs with deliberately no chart value, set (when
     needed) through extraEnv. A new variable in the binary that is neither
     rendered nor listed there fails the test; that is the point.
  3. The mode-specific variables appear only in their mode: NETWORK_BASE_URL,
     NETWORK_VENDOR_ID and NETWORK_NODES_FILE only in live, NETWORK_SEED_FILE only
     in stub (the binary refuses it against the live gateway).

Run: python3 charts/network-fulfillment/tests/test_env_parity.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = CHART_DIR.parents[1]

# Variables the binary reads that the chart has no value for. SITE_ID is the one
# to watch in live mode: the sites the live gateway converges onto network nodes
# are SITE_ID (default site-1) plus every site of the node file, and warehouse-infra
# sets SITE_ID through extraEnv. The chart deliberately does not also render it: a
# second entry for the same name breaks ArgoCD's diff.
EXTRA_ENV_ONLY = {
    "CAPABILITY_OFFER_ENABLED",  # guard only; warehouse-infra sets it to true through extraEnv
    "INVENTORY_STORAGE_URL",
    "LOG_LEVEL",
    "MIGRATIONS_PATH",  # the image sets /app/migrations; a local run overrides it
    "OUTBOX_RELAY_INTERVAL",
    "RECOMPUTE_INTERVAL",
    "SITE_ID",
}

LIVE_ONLY = {"NETWORK_BASE_URL", "NETWORK_VENDOR_ID", "NETWORK_NODES_FILE",
             "INBOUND_RECEIVING_URL", "PRODUCT_MASTER_URL", "INBOUND_INTAKE_INTERVAL"}
STUB_ONLY = {"NETWORK_SEED_FILE"}

# The calls that read the environment. `get(` is the accessor loadBootConfig and
# loadNetworkConfig are handed (os.Getenv in production, a map in the tests).
_READERS = [
    re.compile(r'\b(?:os\.Getenv|os\.LookupEnv|getenv|durationEnv)\(\s*"([A-Z][A-Z0-9_]*)"'),
    re.compile(r'\benvInt\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*"([A-Z][A-Z0-9_]*)"'),
    re.compile(r'(?<![A-Za-z0-9_.])get\(\s*"([A-Z][A-Z0-9_]*)"'),
]

STUB_VALUES = """
productTranslation:
  mappings:
    - networkProductId: LST-1
      sku: sku-1
stubDemand:
  demands:
    - networkRef: po-1
      siteId: site-1
      requiredShipBy: "+36h"
      lines:
        - networkLineRef: "1"
          networkProductId: LST-1
          quantity: 1
"""

LIVE_VALUES = """
config:
  networkMode: live
  networkBaseUrl: https://vendor.invalid
  networkVendorId: V-1
  inboundReceivingUrl: http://inbound-receiving.invalid
  productMasterUrl: http://product-master.invalid
productTranslation:
  mappings:
    - networkProductId: LST-1
      sku: sku-1
networkNodes:
  nodes:
    - siteCode: site-1
      nodeType: FULFILLMENT_CENTER
      regionId: SP
      timezone: America/Sao_Paulo
      postalCode: 01000-000
      countryCode: BR
"""


def binary_env() -> set[str]:
    sources = sorted((REPO_DIR / "cmd" / "netfulfil").glob("*.go")) + sorted((REPO_DIR / "internal").rglob("*.go"))
    names: set[str] = set()
    scanned = 0
    for path in sources:
        if path.name.endswith("_test.go"):
            continue
        scanned += 1
        text = path.read_text()
        for pattern in _READERS:
            names.update(pattern.findall(text))
    assert scanned > 10, f"scanned only {scanned} Go files; is the repository layout what this test expects?"
    return names


def render(values: str) -> list[dict]:
    import yaml  # PyYAML is installed by the chart-tests job

    values_path = CHART_DIR / "tests" / ".parity-values.yaml"
    values_path.write_text(values)
    try:
        out = subprocess.run(
            ["helm", "template", "network-fulfillment", str(CHART_DIR),
             "--set", "database.existingSecret=base-db", "--set", "kafka.brokers=kafka.invalid:9092",
             "-f", str(values_path)],
            capture_output=True, text=True, check=True,
        ).stdout
    finally:
        values_path.unlink(missing_ok=True)
    return [d for d in yaml.safe_load_all(out) if d]


def rendered_env(docs: list[dict]) -> tuple[set[str], list[str]]:
    """(names the container sees, inline env names in order)."""
    deployment = next(d for d in docs if d.get("kind") == "Deployment"
                      and d["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "api")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    mounted = {r["configMapRef"]["name"] for r in container.get("envFrom", []) if "configMapRef" in r}
    names: set[str] = set()
    for d in docs:
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"] in mounted:
            names.update(d.get("data", {}))
    inline = [e["name"] for e in container.get("env", [])]
    names.update(inline)
    return names, inline


def check_mode(label: str, values: str, binary: set[str], only_here: set[str], not_here: set[str]) -> None:
    names, inline = rendered_env(render(values))

    dups = sorted({n for n in inline if inline.count(n) > 1})
    assert not dups, f"{label}: duplicate inline env entries (breaks ArgoCD's diff): {dups}"

    unread = sorted(names - binary)
    assert not unread, (
        f"{label}: the chart renders {unread}, which the binary never reads "
        f"(remove them from the chart, or the binary's reader is not recognised by this scan)"
    )

    unrendered = sorted(binary - names)
    allowed = EXTRA_ENV_ONLY | not_here
    surprise = sorted(set(unrendered) - allowed)
    assert not surprise, (
        f"{label}: the binary reads {surprise} but the chart renders no value for it and it is not "
        f"in EXTRA_ENV_ONLY: add a chart value, or list it there on purpose"
    )
    stale = sorted(EXTRA_ENV_ONLY - binary)
    assert not stale, f"EXTRA_ENV_ONLY lists {stale}, which the binary no longer reads: remove them"
    leaked_in = sorted(EXTRA_ENV_ONLY & names)
    assert not leaked_in, (
        f"{label}: {leaked_in} is rendered but EXTRA_ENV_ONLY says the chart has no value for it: "
        f"remove it from the list"
    )

    missing_here = sorted(only_here - names)
    assert not missing_here, f"{label}: {missing_here} must be rendered in this mode"
    present_wrong = sorted(not_here & names)
    assert not present_wrong, f"{label}: {present_wrong} must not be rendered in this mode"
    print(f"PASS: {label} renders {len(names)} variables, all read by the binary; "
          f"{len(unrendered)} binary variables have no chart value ({', '.join(sorted(allowed & set(unrendered)))})")


def main() -> int:
    binary = binary_env()
    expected_core = {"NETWORK_MODE", "NETWORK_BASE_URL", "NETWORK_VENDOR_ID", "NETWORK_NODES_FILE", "NETWORK_SEED_FILE",
                     "DATABASE_URL", "KAFKA_BROKERS", "PORT", "SITE_ID", "ENVIRONMENT", "CORS_ALLOWED_ORIGINS"}
    missed = sorted(expected_core - binary)
    assert not missed, f"the source scan did not find {missed}: it no longer recognises how the binary reads them"

    check_mode("stub", STUB_VALUES, binary, only_here=STUB_ONLY, not_here=LIVE_ONLY)
    check_mode("live", LIVE_VALUES, binary, only_here=LIVE_ONLY, not_here=STUB_ONLY)
    print("\nAll environment parity assertions passed.")
    return 0


if __name__ == "__main__":
    try:
        import yaml  # noqa: F401
    except ModuleNotFoundError:
        print("SKIP: PyYAML not available; cannot assert parity", file=sys.stderr)
        raise SystemExit(0)
    raise SystemExit(main())
