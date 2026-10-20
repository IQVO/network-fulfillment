#!/usr/bin/env python3
"""Assert the chart's hand-built JSON files are actually valid JSON.

Why this exists: `configmap-data.yaml` renders products.json and
demand.json by string-concatenating Helm template output, with `range`
loops placing commas between entries. That is the fragile way to produce
JSON, and the failure mode is silent at install time -- Kubernetes happily
mounts a malformed file, and the binary then refuses to boot with a parse
error, which reads like a code bug rather than a chart bug.

`helm lint` cannot catch this: the ConfigMap is valid YAML either way. So
these tests parse the rendered payloads with json.loads and check the
values survived the round trip.

They also pin the empty-by-default posture: no mappings means no data
ConfigMap, no volume and no *_FILE env var, because the zero-config
install must keep working on a cluster with nothing configured.

Run: python3 charts/network-fulfillment/tests/test_data_files.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]

VALUES = """
productTranslation:
  mappings:
    - networkProductId: ASIN-ONE
      sku: sku-one
    - networkProductId: ASIN-TWO
      sku: sku-two
stubDemand:
  demands:
    - networkRef: po-1
      siteId: site-1
      requiredShipBy: "+36h"
      lines:
        - networkLineRef: "1"
          networkProductId: ASIN-ONE
          quantity: 3
        - networkLineRef: "2"
          networkProductId: ASIN-TWO
          quantity: 1
    - networkRef: po-2
      siteId: site-2
      requiredShipBy: "2026-12-01T10:00:00Z"
      lines:
        - networkLineRef: "1"
          networkProductId: ASIN-TWO
          quantity: 7
"""


def render(values: str | None) -> list[dict]:
    # Postgres and Kafka are the only runtime: the chart refuses to render
    # without a database source and kafka.brokers (tests/render_test.sh owns
    # the refusals), so every render here carries both.
    args = ["helm", "template", "network-fulfillment", str(CHART_DIR),
            "--set", "database.existingSecret=base-db",
            "--set", "kafka.brokers=kafka.invalid:9092"]
    if values is not None:
        values_path = CHART_DIR / "tests" / ".rendered-values.yaml"
        values_path.write_text(values)
        args += ["-f", str(values_path)]
    try:
        out = subprocess.run(args, capture_output=True, text=True, check=True).stdout
    finally:
        if values is not None:
            (CHART_DIR / "tests" / ".rendered-values.yaml").unlink(missing_ok=True)
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert rendered data", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def data_configmap(docs: list[dict]) -> dict | None:
    for d in docs:
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"].endswith("-data"):
            return d
    return None


def container(docs: list[dict]) -> dict:
    for d in docs:
        if d.get("kind") == "Deployment":
            return d["spec"]["template"]["spec"]["containers"][0]
    raise AssertionError("no Deployment rendered")


def pod_spec(docs: list[dict]) -> dict:
    for d in docs:
        if d.get("kind") == "Deployment":
            return d["spec"]["template"]["spec"]
    raise AssertionError("no Deployment rendered")


def main_configmap(docs: list[dict]) -> dict:
    """The non-data ConfigMap, mounted wholesale via envFrom."""
    for d in docs:
        if d.get("kind") == "ConfigMap" and not d["metadata"]["name"].endswith("-data"):
            return d
    raise AssertionError("no main ConfigMap rendered")


def env_map(docs: list[dict]) -> dict[str, str]:
    """Config as the CONTAINER sees it.

    The chart delivers configuration two ways: `envFrom` mounts every key
    of the main ConfigMap, and inline `env` entries repeat a few of them
    (taking precedence, per Kubernetes merge rules). A test that read only
    inline `env` would report a variable missing when it is in fact
    delivered — so resolve both, in the same order Kubernetes does.
    """
    resolved: dict[str, str] = {}

    c = container(docs)
    mounted = {ref["configMapRef"]["name"] for ref in c.get("envFrom", []) if "configMapRef" in ref}
    cm = main_configmap(docs)
    if cm["metadata"]["name"] in mounted:
        resolved.update({k: str(v) for k, v in cm.get("data", {}).items()})

    for e in c.get("env", []):
        if "value" in e:
            resolved[e["name"]] = e["value"]
    return resolved


def check_default_has_no_data_files() -> None:
    docs = render(None)

    assert data_configmap(docs) is None, "the default render created a data ConfigMap"
    assert not pod_spec(docs).get("volumes"), (
        f"the default render mounted volumes: {pod_spec(docs).get('volumes')}"
    )
    env = env_map(docs)
    for key in ("PRODUCT_TRANSLATION_FILE", "NETWORK_SEED_FILE"):
        assert key not in env, f"the default render set {key} with no file to back it"
    # The poll interval is NOT optional: it is the only thing that brings
    # demand into this context.
    assert env.get("POLL_INTERVAL"), "POLL_INTERVAL must always be set"
    print("PASS: default render has no data files, and still sets POLL_INTERVAL")


def check_rendered_json_is_parseable() -> None:
    docs = render(VALUES)
    cm = data_configmap(docs)
    assert cm is not None, "mappings were set but no data ConfigMap was rendered"

    # The actual point: these are hand-built by template loops, so parse
    # them rather than grepping for substrings.
    products = json.loads(cm["data"]["products.json"])
    demand = json.loads(cm["data"]["demand.json"])

    got = {p["networkProductId"]: p["sku"] for p in products["products"]}
    assert got == {"ASIN-ONE": "sku-one", "ASIN-TWO": "sku-two"}, got

    refs = [d["networkRef"] for d in demand["demands"]]
    assert refs == ["po-1", "po-2"], refs

    first = demand["demands"][0]
    assert first["siteId"] == "site-1", first
    assert first["requiredShipBy"] == "+36h", first
    assert [(l["networkLineRef"], l["networkProductId"], l["quantity"]) for l in first["lines"]] == [
        ("1", "ASIN-ONE", 3),
        ("2", "ASIN-TWO", 1),
    ], first["lines"]

    # quantity must survive as a NUMBER: quoting it would make the
    # service's decoder reject the file at startup.
    assert isinstance(first["lines"][0]["quantity"], int), (
        f"quantity rendered as {type(first['lines'][0]['quantity'])}, must be a JSON number"
    )

    # An absolute RFC 3339 deadline must pass through untouched.
    assert demand["demands"][1]["requiredShipBy"] == "2026-12-01T10:00:00Z", demand["demands"][1]
    print("PASS: rendered products.json and demand.json are valid JSON with the right values")


def check_files_are_wired_where_the_binary_looks() -> None:
    docs = render(VALUES)
    env = env_map(docs)
    mounts = {m["mountPath"] for m in container(docs).get("volumeMounts", [])}

    # An env var pointing outside the mounted directory is the silent
    # failure this check exists for: the pod starts, the file is absent,
    # and the binary exits with a read error.
    for key in ("PRODUCT_TRANSLATION_FILE", "NETWORK_SEED_FILE"):
        assert key in env, f"{key} was not set despite values being provided"
        parent = str(Path(env[key]).parent)
        assert parent in mounts, (
            f"{key}={env[key]} is not inside a mounted path ({sorted(mounts)}); "
            f"the file would not exist in the container"
        )

    cm = data_configmap(docs)
    assert cm is not None, "values were provided but no data ConfigMap was rendered"
    cm_name = cm["metadata"]["name"]
    volumes = pod_spec(docs)["volumes"]
    assert any(v.get("configMap", {}).get("name") == cm_name for v in volumes), (
        f"no volume references the data ConfigMap {cm_name}; the mount would fail"
    )
    print("PASS: both file env vars point inside the mounted ConfigMap volume")


def check_translation_only_needs_no_demand() -> None:
    """A real deployment has mappings and NO seeded demand."""
    docs = render("productTranslation:\n  mappings:\n    - networkProductId: ASIN-ONE\n      sku: sku-one\n")
    cm = data_configmap(docs)
    assert cm is not None and "products.json" in cm["data"], "mappings produced no products.json"
    assert "demand.json" not in cm["data"], (
        "a deployment with no stubDemand still rendered demand.json; it would seed demand nobody asked for"
    )
    env = env_map(docs)
    assert "PRODUCT_TRANSLATION_FILE" in env
    assert "NETWORK_SEED_FILE" not in env, (
        "NETWORK_SEED_FILE was set with no demand.json to back it; the binary would fail to read it"
    )
    print("PASS: mappings without stub demand render only products.json")


def check_startup_probe_gates_boot_work() -> None:
    """Boot work must not race the liveness probe.

    This service runs migrations BEFORE it starts listening, and in this
    cluster every injected pod's first outbound dial is reset ~10s after
    start. Without a startupProbe the kubelet SIGTERMs a pod that is still
    booting — observed live as CrashLoopBackOff with exit code 143, which
    reads like an app crash and is not one.
    """
    docs = render(None)
    c = container(docs)

    probe = c.get("startupProbe")
    assert probe, (
        "no startupProbe: boot work (migrations, plus a retried first dial) "
        "would race the liveness probe and lose"
    )

    grace = probe.get("periodSeconds", 10) * probe.get("failureThreshold", 3)
    assert grace >= 45, (
        f"startupProbe allows only {grace}s; it must outlast the 15 s "
        f"(1+2+4+8) database retry backoff in cmd/netfulfil/retry.go"
    )
    # Liveness must still exist — the startup probe defers it, never
    # replaces it.
    assert c.get("livenessProbe"), "startupProbe replaced livenessProbe instead of deferring it"
    print(f"PASS: startupProbe gates boot for {grace}s, liveness still present")


def check_outbox_max_attempts_reaches_the_container() -> None:
    """The outbox relay attempt cap (ADR 0021, gap fix D12) is a chart value.

    Without OUTBOX_MAX_ATTEMPTS the binary still defaults to 20, but an
    operator who sets config.outboxMaxAttempts must see it reach the
    container, including 0 (unlimited), which is a valid value and must not
    be dropped as "empty".
    """
    env = env_map(render(None))
    assert env.get("OUTBOX_MAX_ATTEMPTS") == "20", (
        f"OUTBOX_MAX_ATTEMPTS = {env.get('OUTBOX_MAX_ATTEMPTS')!r}, want the fleet default '20'"
    )
    env = env_map(render("config:\n  outboxMaxAttempts: 0\n"))
    assert env.get("OUTBOX_MAX_ATTEMPTS") == "0", (
        f"config.outboxMaxAttempts=0 rendered OUTBOX_MAX_ATTEMPTS={env.get('OUTBOX_MAX_ATTEMPTS')!r}, want '0'"
    )
    print("PASS: config.outboxMaxAttempts reaches the container as OUTBOX_MAX_ATTEMPTS (default 20, 0 kept)")


def check_orphan_hold_sweep_knobs_reach_the_container() -> None:
    """The orphaned-hold sweep's two knobs (ADR 0024 §5, gap OM-0020) are chart values.

    Fleet rule R10 (configmap-mount): a knob the binary reads must be rendered
    into the main ConfigMap AND that ConfigMap must be mounted with envFrom, or
    the value an operator sets never reaches the process. The sweep is ON by
    default (a leaked hold is the worse failure) and both "off" and "no grace"
    are valid values that must not be dropped as empty.
    """
    docs = render(None)
    cm = main_configmap(docs)
    for key in ("ORPHAN_HOLD_SWEEP_ENABLED", "ORPHAN_HOLD_GRACE"):
        assert key in cm.get("data", {}), f"{key} is not rendered into the main ConfigMap"
    mounted = {ref["configMapRef"]["name"] for ref in container(docs).get("envFrom", []) if "configMapRef" in ref}
    assert cm["metadata"]["name"] in mounted, (
        f"the main ConfigMap {cm['metadata']['name']} is not mounted with envFrom ({sorted(mounted)})"
    )
    env = env_map(docs)
    assert env.get("ORPHAN_HOLD_SWEEP_ENABLED") == "true", (
        f"ORPHAN_HOLD_SWEEP_ENABLED = {env.get('ORPHAN_HOLD_SWEEP_ENABLED')!r}, want the default 'true'"
    )
    assert env.get("ORPHAN_HOLD_GRACE") == "1h", (
        f"ORPHAN_HOLD_GRACE = {env.get('ORPHAN_HOLD_GRACE')!r}, want the default '1h'"
    )
    env = env_map(render('config:\n  orphanHoldSweepEnabled: false\n  orphanHoldGrace: "0"\n'))
    assert env.get("ORPHAN_HOLD_SWEEP_ENABLED") == "false", (
        f"config.orphanHoldSweepEnabled=false rendered {env.get('ORPHAN_HOLD_SWEEP_ENABLED')!r}, want 'false'"
    )
    assert env.get("ORPHAN_HOLD_GRACE") == "0", (
        f"config.orphanHoldGrace=0 rendered {env.get('ORPHAN_HOLD_GRACE')!r}, want '0' kept"
    )
    print("PASS: orphan-hold sweep knobs are in the ConfigMap, mounted via envFrom, default on/1h, off and 0 kept")


def main() -> int:
    check_startup_probe_gates_boot_work()
    check_outbox_max_attempts_reaches_the_container()
    check_orphan_hold_sweep_knobs_reach_the_container()
    check_default_has_no_data_files()
    check_rendered_json_is_parseable()
    check_files_are_wired_where_the_binary_looks()
    check_translation_only_needs_no_demand()
    print("\nAll data-file assertions passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
