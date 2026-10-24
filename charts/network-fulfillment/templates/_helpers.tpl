{{- define "network-fulfillment.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "network-fulfillment.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "network-fulfillment.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "network-fulfillment.labels" -}}
helm.sh/chart: {{ include "network-fulfillment.chart" . }}
{{ include "network-fulfillment.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "network-fulfillment.selectorLabels" -}}
app.kubernetes.io/name: {{ include "network-fulfillment.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "network-fulfillment.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "network-fulfillment.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The adapter the binary wires from NETWORK_MODE, read the way
retailnetwork.ParseMode reads it: trimmed and lower-cased. Only "live" selects the
live gateway; the binary reads everything else as stub.
*/}}
{{- define "network-fulfillment.networkMode" -}}
{{- lower (trim (toString .Values.config.networkMode)) -}}
{{- end }}

{{/*
Non-empty when the data ConfigMap is rendered and mounted: the product dictionary,
the stub demand, or (live mode only) the node definitions of NETWORK_NODES_FILE.
*/}}
{{- define "network-fulfillment.hasDataFiles" -}}
{{- if or .Values.productTranslation.mappings .Values.stubDemand.demands (and (eq (include "network-fulfillment.networkMode" .) "live") .Values.networkNodes.nodes) -}}
true
{{- end -}}
{{- end }}

{{- define "network-fulfillment.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "network-fulfillment.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote is served by its own nginx pod and reached through warehouse-infra's
Nginx web gateway at /mfes/network-fulfillment/. It is deliberately a separate
workload from the API: Kong never routes to it, and the OLTP Service must never
select it.
*/}}
{{- define "network-fulfillment.frontendFullname" -}}
{{- include "network-fulfillment.fullname" . }}-frontend
{{- end }}

{{- define "network-fulfillment.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "network-fulfillment.fullname" . }}-analytics
{{- end }}
{{- end }}

{{- define "network-fulfillment.mcpFullname" -}}
{{- include "network-fulfillment.fullname" . }}-mcp
{{- end }}

{{- define "network-fulfillment.projectorFullname" -}}
{{- include "network-fulfillment.fullname" . }}-projector
{{- end }}

{{- define "network-fulfillment.reportsFullname" -}}
{{- include "network-fulfillment.fullname" . }}-reports
{{- end }}

{{/*
Postgres and Kafka are the only runtime. The binaries exit at boot without
DATABASE_URL and KAFKA_BROKERS (cmd/netfulfil loadBootConfig, cmd/mcp,
cmd/netfulfil-projector, cmd/netfulfil-reports): there is no in-memory mode, no
log sink and no localhost broker default. The chart therefore refuses to render
without a source for each, naming the value.
*/}}
{{- define "network-fulfillment.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "database.url or database.existingSecret is required: DATABASE_URL is mandatory, this service runs only on a real Postgres" -}}
{{- end -}}
{{- end }}

{{/*
kafka.brokers is required (KAFKA_BROKERS) and kafka.enabled=false is refused.
`kafka.enabled: true` is tolerated: warehouse-infra's values may still set it.
*/}}
{{- define "network-fulfillment.requireKafka" -}}
{{- if and (hasKey .Values.kafka "enabled") (not .Values.kafka.enabled) -}}
{{- fail "kafka.enabled=false is not supported: this service runs only on a real Kafka; remove the value (kafka.enabled: true is still tolerated)" -}}
{{- end -}}
{{- $_ := required "kafka.brokers is required (KAFKA_BROKERS): every binary of this chart runs on a real Kafka" .Values.kafka.brokers -}}
{{- end }}

{{/*
EVENT_PUBLISHER is kept because warehouse-infra sets it; the only value is kafka.
*/}}
{{- define "network-fulfillment.requireEventPublisher" -}}
{{- if ne (lower (toString .Values.config.eventPublisher)) "kafka" -}}
{{- fail (printf "config.eventPublisher=%q is not supported: this service publishes through the outbox to a real Kafka, the only value is kafka" (toString .Values.config.eventPublisher)) -}}
{{- end -}}
{{- end }}

{{/*
extraEnv must not turn the publisher or the capability-offer pipeline off. The
binary accepts only EVENT_PUBLISHER unset/kafka and CAPABILITY_OFFER_ENABLED
unset/true. Call with (dict "env" <list> "where" <name>).
*/}}
{{- define "network-fulfillment.requireRealEnv" -}}
{{- $where := .where -}}
{{- range .env -}}
{{- if hasKey . "value" -}}
{{- $v := lower (toString .value) -}}
{{- if and (eq .name "EVENT_PUBLISHER") (ne $v "") (ne $v "kafka") -}}
{{- fail (printf "%s sets EVENT_PUBLISHER=%s: not supported, this service runs only on a real Kafka and the binary rejects it at boot" $where (toString .value)) -}}
{{- end -}}
{{- if and (eq .name "CAPABILITY_OFFER_ENABLED") (ne $v "") (ne $v "true") -}}
{{- fail (printf "%s sets CAPABILITY_OFFER_ENABLED=%s: not supported, the capability-offer pipeline is always on and the binary rejects it at boot" $where (toString .value)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
The binary's graceful shutdown flips readiness, stops the consumers, drains HTTP
and waits for the outbox relay under a short deadline; the readiness probe needs
a period to see the flip. Under 20s a SIGKILL can cut that short.
*/}}
{{- define "network-fulfillment.requireGracePeriod" -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) 20 -}}
{{- fail (printf "terminationGracePeriodSeconds is %v, want at least 20: readiness flip + HTTP drain + consumers + outbox relay" .Values.terminationGracePeriodSeconds) -}}
{{- end -}}
{{- end }}

{{/*
netfulfil-projector and netfulfil-reports exit at boot without
ANALYTICS_DATABASE_URL; the projector also needs KAFKA_BROKERS (requireKafka).
*/}}
{{- define "network-fulfillment.requireAnalytics" -}}
{{- if and .Values.analytics.enabled (not (or .Values.analytics.database.url .Values.analytics.database.existingSecret)) -}}
{{- fail "analytics.database.url or analytics.database.existingSecret is required while analytics.enabled: ANALYTICS_DATABASE_URL is mandatory for the projector and the reports API" -}}
{{- end -}}
{{- end }}

{{/*
NETWORK_MODE is stub (the default, no external call) or live (ADR 0026). The binary
reads anything but live as stub, so the chart refuses any other value instead of
deploying a stub that looks configured for a real network. Live needs a base URL
and a vendor id (the binary exits at boot without them, naming each), takes no
credential, and refuses seeded stub demand (NETWORK_SEED_FILE is refused against
the live gateway). The node definitions are checked against the file's strict
schema (retailnetwork.LoadNodesFile) so a typo fails the render, not the boot.
*/}}
{{- define "network-fulfillment.requireNetwork" -}}
{{- $mode := include "network-fulfillment.networkMode" . -}}
{{- if not (or (eq $mode "stub") (eq $mode "live")) -}}
{{- fail (printf "config.networkMode=%q is not supported: use stub (the default) or live. The binary reads anything but live as stub, so another value would deploy a stub that looks configured for a real network" (toString .Values.config.networkMode)) -}}
{{- end -}}
{{- if eq $mode "live" -}}
{{- $missing := list -}}
{{- if not .Values.config.networkBaseUrl -}}
{{- $missing = append $missing "config.networkBaseUrl (NETWORK_BASE_URL)" -}}
{{- end -}}
{{- if not .Values.config.networkVendorId -}}
{{- $missing = append $missing "config.networkVendorId (NETWORK_VENDOR_ID)" -}}
{{- end -}}
{{- if not .Values.config.inboundReceivingUrl -}}
{{- $missing = append $missing "config.inboundReceivingUrl (INBOUND_RECEIVING_URL)" -}}
{{- end -}}
{{- if not .Values.config.productMasterUrl -}}
{{- $missing = append $missing "config.productMasterUrl (PRODUCT_MASTER_URL)" -}}
{{- end -}}
{{- if $missing -}}
{{- fail (printf "config.networkMode=live requires %s to be set: the binary refuses to boot in live mode without them" (join " and " $missing)) -}}
{{- end -}}
{{- if .Values.stubDemand.demands -}}
{{- fail "stubDemand.demands is set while config.networkMode is live: the binary refuses to boot with NETWORK_SEED_FILE against the live gateway, because seeded demand would never be delivered" -}}
{{- end -}}
{{- range $i, $node := .Values.networkNodes.nodes -}}
{{- range $key, $ignored := $node -}}
{{- if not (has $key (list "siteCode" "nodeType" "subtype" "regionId" "timezone" "postalCode" "countryCode" "lat" "lon")) -}}
{{- fail (printf "networkNodes.nodes[%d] has the unknown key %q: the binary rejects it at boot (allowed: siteCode, nodeType, subtype, regionId, timezone, postalCode, countryCode, lat, lon)" $i $key) -}}
{{- end -}}
{{- end -}}
{{- range $field := list "siteCode" "nodeType" "regionId" "timezone" "postalCode" "countryCode" -}}
{{- if not (get $node $field) -}}
{{- fail (printf "networkNodes.nodes[%d].%s is required: the binary rejects the node file at boot without it" $i $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
One entry point for the api Deployment, which every release renders, so any
release fails whichever optional component is enabled.
*/}}
{{- define "network-fulfillment.requireRealRuntime" -}}
{{- include "network-fulfillment.requireNetwork" . -}}
{{- include "network-fulfillment.requireDatabase" . -}}
{{- include "network-fulfillment.requireKafka" . -}}
{{- include "network-fulfillment.requireEventPublisher" . -}}
{{- include "network-fulfillment.requireGracePeriod" . -}}
{{- include "network-fulfillment.requireAnalytics" . -}}
{{- include "network-fulfillment.requireRealEnv" (dict "env" .Values.extraEnv "where" "extraEnv") -}}
{{- include "network-fulfillment.requireRealEnv" (dict "env" .Values.mcp.extraEnv "where" "mcp.extraEnv") -}}
{{- end }}


{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "network-fulfillment.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "network-fulfillment.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end -}}
