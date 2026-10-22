{{/* Chart name. */}}
{{- define "jobrun.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name. */}}
{{- define "jobrun.fullname" -}}
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

{{- define "jobrun.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "jobrun.labels" -}}
helm.sh/chart: {{ include "jobrun.chart" . }}
{{ include "jobrun.selectorLabels" . }}
app.kubernetes.io/version: {{ include "jobrun.imageTag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: jobrun
{{- end }}

{{- define "jobrun.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jobrun.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Component selector labels: pass (dict "ctx" $ "component" "server"). */}}
{{- define "jobrun.componentSelectorLabels" -}}
{{ include "jobrun.selectorLabels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "jobrun.componentLabels" -}}
{{ include "jobrun.labels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "jobrun.serverName" -}}
{{- printf "%s-server" (include "jobrun.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "jobrun.workerName" -}}
{{- printf "%s-worker" (include "jobrun.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "jobrun.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "jobrun.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "jobrun.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{- define "jobrun.image" -}}
{{- printf "%s:%s" .Values.image.repository (include "jobrun.imageTag" .) }}
{{- end }}

{{/* Name of the Secret holding DATABASE_URL. */}}
{{- define "jobrun.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- printf "%s-db" (include "jobrun.fullname" .) }}
{{- end }}
{{- end }}

{{/* In-cluster gRPC address of the server Service. */}}
{{- define "jobrun.grpcAddr" -}}
{{- printf "%s.%s.svc:%d" (include "jobrun.serverName" .) .Release.Namespace (int .Values.service.grpcPort) }}
{{- end }}

{{/* Render-time validation of values whose mistakes would only show up at runtime. */}}
{{- define "jobrun.validate" -}}
{{- if and (not .Values.database.existingSecret) (not .Values.database.url) }}
{{- fail "jobrun: set database.existingSecret (production) or database.url (local clusters only)" }}
{{- end }}
{{- $s := .Values.server }}
{{- $minGrace := add $s.preStopSleepSeconds $s.shutdownTimeoutSeconds 50 }}
{{- if lt (int $s.terminationGracePeriodSeconds) (int $minGrace) }}
{{- fail (printf "jobrun: server.terminationGracePeriodSeconds (%d) must be >= preStopSleepSeconds + shutdownTimeoutSeconds + 50 (%d) to cover jobrund's staged teardown" (int $s.terminationGracePeriodSeconds) (int $minGrace)) }}
{{- end }}
{{- if and (gt (int $s.pgMaxConns) 0) (lt (int $s.pgMaxConns) (add $s.concurrency 2 | int)) }}
{{- fail "jobrun: server.pgMaxConns must be 0 (auto) or >= server.concurrency + 2" }}
{{- end }}
{{- if .Values.worker.enabled }}
{{- $w := .Values.worker }}
{{- if lt (int $w.terminationGracePeriodSeconds) (add $w.drainSeconds 10 | int) }}
{{- fail (printf "jobrun: worker.terminationGracePeriodSeconds (%d) must be >= worker.drainSeconds + 10" (int $w.terminationGracePeriodSeconds)) }}
{{- end }}
{{- end }}
{{- end }}

{{/* Default soft topology spread for a component: pass (dict "ctx" $ "component" "server"). */}}
{{- define "jobrun.defaultSpread" -}}
- maxSkew: 1
  topologyKey: topology.kubernetes.io/zone
  whenUnsatisfiable: ScheduleAnyway
  labelSelector:
    matchLabels:
      {{- include "jobrun.componentSelectorLabels" . | nindent 6 }}
- maxSkew: 1
  topologyKey: kubernetes.io/hostname
  whenUnsatisfiable: ScheduleAnyway
  labelSelector:
    matchLabels:
      {{- include "jobrun.componentSelectorLabels" . | nindent 6 }}
{{- end }}
