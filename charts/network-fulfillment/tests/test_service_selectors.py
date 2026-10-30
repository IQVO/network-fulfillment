#!/usr/bin/env python3
"""Assert this chart's Service selectors isolate each component.

Why this test exists (fleet-wide): `app.kubernetes.io/name` + `instance` are
identical on every pod a release creates. A Service that selects on those two
alone selects ALL components. Every Deployment here carries
`app.kubernetes.io/component` in selector.matchLabels and its pod labels, and
every Service selects on it, so each Service selects EXACTLY ONE Deployment.
Components: api (always), projector and reports (analytics read side, gated on
analytics.enabled), mcp (read-only MCP server, default off), a2a (read-only
A2A agent, default off) and frontend (the
nginx pod serving the netfulfil_mfe remote, default off), all rendered
TOGETHER here. The frontend is never routed by this chart. Env parity between
the chart and the binaries is asserted by test_env_parity.py next to this
file; this file owns the Service/Deployment wiring only.

Run: python3 charts/network-fulfillment/tests/test_service_selectors.py
Needs: helm, PyYAML.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
RELEASE = "network-fulfillment"

# Dummy values, no password: the chart refuses to render without a database
# source and without Kafka brokers.
BASE = [
    "--set", "database.url=postgres://u@example.invalid:5432/db",
    "--set", "kafka.brokers=kafka.example.invalid:9092",
]
ENABLE_EVERYTHING = BASE + [
    "--set", "analytics.enabled=true",
    "--set", "analytics.database.url=postgres://a@example.invalid:5432/analytics",
    "--set", "mcp.enabled=true",
    "--set", "a2a.enabled=true",
    "--set", "frontend.enabled=true",
    "--set", "autoscaling.api.enabled=true",
    "--set", "gatewayApi.enabled=true",
    "--set", "ingress.enabled=true",
]

COMPONENTS = ("mcp", "a2a", "frontend", "projector", "reports")
SERVED = ("mcp", "a2a", "frontend", "reports")  # components with a Service of their own


def render(extra_args: list[str]) -> list[dict]:
    result = subprocess.run(
        ["helm", "template", RELEASE, str(CHART_DIR), *extra_args],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"FAIL: helm template {' '.join(extra_args)}:\n{result.stderr}")
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert selectors", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def selector_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("selector") or {}


def pod_labels_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("template", {}).get("metadata", {}).get("labels") or {}


def matches(selector: dict, labels: dict) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def env_names(dep: dict) -> list[str]:
    env = dep["spec"]["template"]["spec"]["containers"][0].get("env", [])
    return [e["name"] for e in env]


def command_of(dep: dict) -> list[str] | None:
    return dep["spec"]["template"]["spec"]["containers"][0].get("command")


def pins(svc: dict | None, component: str) -> bool:
    return svc is not None and selector_of(svc).get("app.kubernetes.io/component") == component


def main() -> int:
    failures: list[str] = []
    docs = render(ENABLE_EVERYTHING)
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}

    want_d = sorted([RELEASE] + [f"{RELEASE}-{c}" for c in COMPONENTS])
    want_s = sorted([RELEASE] + [f"{RELEASE}-{c}" for c in SERVED])
    if sorted(deployments) != want_d:
        failures.append(f"expected the Deployments {want_d}, got {sorted(deployments)}")
    if sorted(services) != want_s:
        failures.append(f"expected the Services {want_s}, got {sorted(services)}")

    if not pins(services.get(RELEASE), "api"):
        failures.append("the api Service selector must pin component=api")
    if not pins(services.get(f"{RELEASE}-mcp"), "mcp"):
        failures.append("the MCP Service must exist and pin component=mcp")
    if not pins(services.get(f"{RELEASE}-a2a"), "a2a"):
        failures.append("the A2A Service must exist and pin component=a2a")
    if not pins(services.get(f"{RELEASE}-frontend"), "frontend"):
        failures.append("the frontend Service must exist and pin component=frontend")
    elif services[f"{RELEASE}-frontend"]["spec"].get("type") != "ClusterIP":
        failures.append("the frontend Service must be ClusterIP")
    if not pins(services.get(f"{RELEASE}-reports"), "analytics-reports"):
        failures.append("the reports Service must exist and pin component=analytics-reports")

    # Every Deployment's own selector must pin a component, and be satisfied
    # by its pod labels.
    for name, dep in deployments.items():
        match_labels = dep["spec"]["selector"].get("matchLabels") or {}
        if "app.kubernetes.io/component" not in match_labels:
            failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component")
        if not matches(match_labels, pod_labels_of(dep)):
            failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, s in services.items():
        sel = selector_of(s)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1")

    # The Service's named target port exists on every container a Service
    # targets (the projector is a background consumer: no Service, no port).
    for name, dep in deployments.items():
        if name.endswith("projector"):
            continue
        if name.endswith("frontend"):
            continue  # nginx serves its own port map
        ports = {p["name"] for p in dep["spec"]["template"]["spec"]["containers"][0].get("ports", [])}
        if "http" not in ports:
            failures.append(f"Deployment {name} does not expose the named port 'http'")

    # Read-side binaries run their own commands; the MCP server is /app/mcp.
    for name, cmd in (
        (f"{RELEASE}-mcp", ["/app/mcp"]),
        (f"{RELEASE}-a2a", ["/app/a2a"]),
        (f"{RELEASE}-projector", ["/app/netfulfil-projector"]),
        (f"{RELEASE}-reports", ["/app/netfulfil-reports"]),
    ):
        if deployments.get(name) is not None and command_of(deployments[name]) != cmd:
            failures.append(f"Deployment {name} must run {cmd}")
    # The A2A agent is read-only: no publisher, no outbox, so no Kafka env.
    a2a = deployments.get(f"{RELEASE}-a2a")
    if a2a is not None and {"KAFKA_BROKERS", "EVENT_PUBLISHER"} & set(env_names(a2a)):
        failures.append("the a2a Deployment is read-only: it must not get KAFKA_BROKERS / EVENT_PUBLISHER")
    # The frontend serves static bytes: no database, no Kafka.
    fe = deployments.get(f"{RELEASE}-frontend")
    if fe is not None and {"DATABASE_URL", "KAFKA_BROKERS"} & set(env_names(fe)):
        failures.append("the frontend Deployment must not get database/Kafka env (it serves static bytes)")
    # The frontend is routed by the platform layer, never by this chart.
    for d in docs:
        if d.get("kind") in {"Ingress", "HTTPRoute"} and "frontend" in d["metadata"]["name"]:
            failures.append(f"{d['kind']} {d['metadata']['name']}: frontend routing must not live in this chart")

    # The api HPA owns replicas when enabled.
    hpa = next((d for d in docs if d.get("kind") == "HorizontalPodAutoscaler"), None)
    if hpa is None or hpa["spec"]["scaleTargetRef"]["name"] != RELEASE:
        failures.append("the api HPA was not rendered (or does not scale the api Deployment)")
    elif "replicas" in deployments.get(RELEASE, {}).get("spec", {}):
        failures.append("the api Deployment must omit replicas when its HPA owns them")

    # Defaults: the api only. No optional component, no Service beyond api,
    # no HPA, no route.
    defaults = render(BASE)
    d_dep = sorted(d["metadata"]["name"] for d in defaults if d.get("kind") == "Deployment")
    d_svc = sorted(d["metadata"]["name"] for d in defaults if d.get("kind") == "Service")
    if d_dep != [RELEASE]:
        failures.append(f"defaults must deploy the api only, got {d_dep}")
    if d_svc != [RELEASE]:
        failures.append(f"defaults must expose the api Service only, got {d_svc}")
    if any(d.get("kind") in {"HorizontalPodAutoscaler", "HTTPRoute", "Ingress"} for d in defaults):
        failures.append("defaults must not render an HPA, HTTPRoute or Ingress")

    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1
    print("PASS: network-fulfillment Service selectors isolate every component")
    return 0


if __name__ == "__main__":
    sys.exit(main())
