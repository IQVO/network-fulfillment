#!/usr/bin/env bash
# Chart render assertions for charts/network-fulfillment (run in CI by the
# chart-tests job, next to the python tests in this directory).
#
# Postgres and Kafka are the only runtime: the binaries exit at boot without
# DATABASE_URL and KAFKA_BROKERS (no in-memory mode, no log sink, no localhost
# broker default), so the chart refuses to render without a source for each,
# refuses a non-kafka publisher and a disabled capability-offer pipeline, and
# still renders with the values warehouse-infra really passes
# (tests/warehouse-infra-*-values.yaml, copied from warehouse-infra
# origin/develop helm-values/).
#
# Run: bash charts/network-fulfillment/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=network-fulfillment
# Dummy DSN (no password) and broker: the chart refuses to render without them.
base=(--set database.url=postgres://u@example.invalid:5432/db
  --set kafka.brokers=kafka.invalid:9092)
everything=(--set analytics.enabled=true
  --set analytics.database.url=postgres://a@example.invalid:5432/analytics
  --set mcp.enabled=true)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() { helm template "$release" "$chart_dir" "${base[@]}" "$@"; }

# env_value DOC NAME: the value of the first `- name: NAME` entry in DOC.
env_value() {
  awk -v n="$2" '$0 ~ "- name: " n "$" {found=1; next} found && /value:/ {gsub(/^ *value: *"?|"?$/, ""); print; exit} found {exit}' <<<"$1"
}

# --- the chart renders a real runtime ------------------------------------
api_deployment="$(render --show-only templates/deployment.yaml)"
all_docs="$(render "${everything[@]}")"

[[ "$(env_value "$api_deployment" EVENT_PUBLISHER)" == "kafka" ]] || fail "EVENT_PUBLISHER must default to kafka"
[[ "$(env_value "$api_deployment" KAFKA_BROKERS)" == "kafka.invalid:9092" ]] || fail "KAFKA_BROKERS is not rendered from kafka.brokers"
grep -A4 'name: DATABASE_URL$' <<<"$api_deployment" | grep -q 'secretKeyRef:' || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
grep -q 'terminationGracePeriodSeconds: 30' <<<"$api_deployment" || fail "terminationGracePeriodSeconds must default to 30"
if grep -qiE 'value: "?(log|permissive|off)"?$' <<<"$all_docs"; then fail "the render carries a log/permissive/off value, which the binaries reject at boot"; fi
if grep -q 'DATABASE_URL: ""' <<<"$all_docs"; then fail "an empty DATABASE_URL must never be rendered"; fi
if grep -qi 'in-memory' <<<"$all_docs"; then fail "the render mentions an in-memory mode"; fi

# Every optional component renders its mandatory variable.
mcp_doc="$(render "${everything[@]}" --show-only templates/mcp-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$mcp_doc" | grep -q 'secretKeyRef:' || fail "mcp must render DATABASE_URL from a Secret"
proj_doc="$(render "${everything[@]}" --show-only templates/projector-deployment.yaml)"
grep -q 'name: ANALYTICS_DATABASE_URL$' <<<"$proj_doc" || fail "projector must render ANALYTICS_DATABASE_URL"
[[ "$(env_value "$proj_doc" KAFKA_BROKERS)" == "kafka.invalid:9092" ]] || fail "projector must render KAFKA_BROKERS"
reports_doc="$(render "${everything[@]}" --show-only templates/reports-deployment.yaml)"
grep -q 'name: ANALYTICS_DATABASE_URL$' <<<"$reports_doc" || fail "reports must render ANALYTICS_DATABASE_URL"

# --- the chart refuses to render without its mandatory inputs -----------
expect_refusal() {
  local why="$1" needle="$2"
  shift 2
  local err
  if err="$(helm template "$release" "$chart_dir" "$@" 2>&1 >/dev/null)"; then
    fail "the chart must refuse to render: $why"
  fi
  grep -qF -- "$needle" <<<"$err" || fail "the refusal for '$why' does not name '$needle': $err"
}
db=(--set database.url=postgres://u@example.invalid:5432/db)
kafka=(--set kafka.brokers=kafka.invalid:9092)
expect_refusal "no database source" "database.url or database.existingSecret" "${kafka[@]}"
expect_refusal "mcp without a database" "database.url or database.existingSecret" "${kafka[@]}" --set mcp.enabled=true
expect_refusal "no kafka brokers" "kafka.brokers is required" "${db[@]}"
expect_refusal "empty kafka.brokers" "kafka.brokers is required" "${db[@]}" --set kafka.brokers=
expect_refusal "kafka.enabled=false" "kafka.enabled=false is not supported" "${base[@]}" --set kafka.enabled=false
expect_refusal "log publisher" "config.eventPublisher" "${base[@]}" --set config.eventPublisher=log
expect_refusal "EVENT_PUBLISHER=log in extraEnv" "EVENT_PUBLISHER" "${base[@]}" \
  --set 'extraEnv[0].name=EVENT_PUBLISHER' --set 'extraEnv[0].value=log'
expect_refusal "CAPABILITY_OFFER_ENABLED=false in extraEnv" "CAPABILITY_OFFER_ENABLED" "${base[@]}" \
  --set 'extraEnv[0].name=CAPABILITY_OFFER_ENABLED' --set 'extraEnv[0].value=false'
expect_refusal "EVENT_PUBLISHER=log in mcp.extraEnv" "mcp.extraEnv" "${base[@]}" \
  --set 'mcp.extraEnv[0].name=EVENT_PUBLISHER' --set 'mcp.extraEnv[0].value=log'
expect_refusal "CAPABILITY_OFFER_ENABLED=false in mcp.extraEnv" "CAPABILITY_OFFER_ENABLED" "${base[@]}" \
  --set 'mcp.extraEnv[0].name=CAPABILITY_OFFER_ENABLED' --set 'mcp.extraEnv[0].value=false'
expect_refusal "grace period under 20" "terminationGracePeriodSeconds" "${base[@]}" --set terminationGracePeriodSeconds=10
expect_refusal "analytics without a DSN source" "analytics.database.url or analytics.database.existingSecret" \
  "${base[@]}" --set analytics.enabled=true

# --- NETWORK_MODE=live (ADR 0026) ------------------------------------------
# Live takes a base URL and a vendor id and no credential; the chart refuses what
# the binary refuses at boot, and nothing else (tests/test_env_parity.py owns the
# exact variable list, tests/test_credential_wiring.py the Secret wiring).
live=(--set config.networkMode=live
  --set config.networkBaseUrl=https://vendor.invalid
  --set config.networkVendorId=V-1
  --set config.inboundReceivingUrl=http://inbound-receiving.invalid
  --set config.productMasterUrl=http://product-master.invalid)
expect_refusal "live without a base URL and a vendor id" "config.networkBaseUrl" "${base[@]}" --set config.networkMode=live
expect_refusal "live without a vendor id" "config.networkVendorId" "${base[@]}" \
  --set config.networkMode=live --set config.networkBaseUrl=https://vendor.invalid
expect_refusal "live without a database" "database.url or database.existingSecret" "${kafka[@]}" "${live[@]}"
expect_refusal "an unsupported network mode" "config.networkMode" "${base[@]}" --set config.networkMode=sandbox

live_cm="$(render "${live[@]}" --show-only templates/configmap.yaml)" || fail "live mode with its values must render"
grep -q 'NETWORK_BASE_URL: "https://vendor.invalid"' <<<"$live_cm" || fail "live: NETWORK_BASE_URL is not rendered"
grep -q 'NETWORK_VENDOR_ID: "V-1"' <<<"$live_cm" || fail "live: NETWORK_VENDOR_ID is not rendered"
grep -q 'INBOUND_RECEIVING_URL: "http://inbound-receiving.invalid"' <<<"$live_cm" || fail "live: INBOUND_RECEIVING_URL is not rendered"
grep -q 'PRODUCT_MASTER_URL: "http://product-master.invalid"' <<<"$live_cm" || fail "live: PRODUCT_MASTER_URL is not rendered"
grep -q 'INBOUND_INTAKE_INTERVAL: "30s"' <<<"$live_cm" || fail "live: INBOUND_INTAKE_INTERVAL is not rendered"
expect_refusal "live without the inbound loop's sibling URLs" "config.inboundReceivingUrl" "${base[@]}" \
  --set config.networkMode=live --set config.networkBaseUrl=https://vendor.invalid --set config.networkVendorId=V-1
expect_refusal "live without product-master" "config.productMasterUrl" "${base[@]}" \
  --set config.networkMode=live --set config.networkBaseUrl=https://vendor.invalid --set config.networkVendorId=V-1 \
  --set config.inboundReceivingUrl=http://inbound-receiving.invalid
live_api="$(render "${live[@]}" --show-only templates/deployment.yaml)"
[[ "$(env_value "$live_api" NETWORK_MODE)" == "live" ]] || fail "live: NETWORK_MODE must reach the api Deployment"
if grep -qE '(^  |- name: )NETWORK_(CLIENT|REFRESH)' <<<"$(render "${live[@]}")"; then fail "live mode renders a credential variable that no code reads"; fi
stub_cm="$(render --show-only templates/configmap.yaml)"
if grep -qE '^  (NETWORK_(BASE_URL|VENDOR_ID|NODES_FILE)|INBOUND_RECEIVING_URL|PRODUCT_MASTER_URL|INBOUND_INTAKE_INTERVAL):' <<<"$stub_cm"; then fail "stub mode renders a live-only variable"; fi

# Tolerated: a legacy kafka.enabled: true, and the explicit real values.
render --set kafka.enabled=true >/dev/null || fail "kafka.enabled=true (legacy) must still render"
render --set config.eventPublisher=kafka \
  --set 'extraEnv[0].name=CAPABILITY_OFFER_ENABLED' --set 'extraEnv[0].value=true' >/dev/null \
  || fail "EVENT_PUBLISHER=kafka and CAPABILITY_OFFER_ENABLED=true must render"

# --- the values warehouse-infra really passes still render --------------
# Same layering as the ArgoCD Application: _platform, the app file, then the
# kind CPU overlay.
infra=(--values "$chart_dir/tests/warehouse-infra-platform-values.yaml"
  --values "$chart_dir/tests/warehouse-infra-values.yaml"
  --values "$chart_dir/tests/warehouse-infra-kind-resources-values.yaml")
infra_all="$(helm template "$release" "$chart_dir" "${infra[@]}")" || fail "the warehouse-infra values no longer render"
infra_api="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/deployment.yaml)"
[[ "$(env_value "$infra_api" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: KAFKA_BROKERS"
[[ "$(env_value "$infra_api" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: EVENT_PUBLISHER"
[[ "$(env_value "$infra_api" CAPABILITY_OFFER_ENABLED)" == "true" ]] || fail "infra values: CAPABILITY_OFFER_ENABLED"
grep -A4 'name: DATABASE_URL$' <<<"$infra_api" | grep -q 'network-fulfillment-db' || fail "infra values: DATABASE_URL must come from the network-fulfillment-db secret"
# A duplicate env entry breaks ArgoCD's diff (extraEnv names KAFKA_BROKERS too).
dups="$(grep -E '^ {12}- name: ' <<<"$infra_api" | sort | uniq -d)"
[[ -z "$dups" ]] || fail "infra values: duplicate env entries in the api Deployment: $dups"
infra_mcp="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/mcp-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$infra_mcp" | grep -q 'network-fulfillment-db' || fail "infra values: mcp DATABASE_URL"
infra_proj="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/projector-deployment.yaml)"
grep -A4 'name: ANALYTICS_DATABASE_URL$' <<<"$infra_proj" | grep -q 'network-fulfillment-analytics-db' || fail "infra values: projector ANALYTICS_DATABASE_URL"
[[ "$(env_value "$infra_proj" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: projector KAFKA_BROKERS"
for component in api mcp analytics-projector analytics-reports frontend; do
  grep -q "app.kubernetes.io/component: $component" <<<"$infra_all" || fail "infra values: component $component not rendered"
done

# The chart's own readinessProbe is /readyz (a real dependency check).
grep -A3 'readinessProbe:' <<<"$api_deployment" | grep -A2 'httpGet:' | grep -q 'path: /readyz' || fail "readinessProbe must hit /readyz"

echo "PASS: network-fulfillment chart render assertions"
