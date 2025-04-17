{{/*
Create a list of telemetry related env vars.
*/}}
{{- define "telemetry.envVars" -}}
{{- with .Values.telemetry }}
{{- if not .consoleLog.enabled }}
- name: CONSOLE_LOG_DISABLED
  value: "true"
{{- end }}
{{- if and .consoleLog.enabled .consoleLog.level }}
- name: CONSOLE_LOG_LEVEL
  value: {{ .consoleLog.level }}
{{ end }}
{{- if and .consoleLog.enabled .consoleLog.format }}
- name: CONSOLE_LOG_FORMAT
  value: {{ .consoleLog.format }}
{{ end }}
{{- if .fileLog.enabled }}
- name: FILE_LOG_DIRECTORY
  value: /stackable/logs/{{ include "operator.appname" $ }}
{{- end }}
{{- if and .fileLog.enabled .fileLog.level }}
- name: FILE_LOG_LEVEL
  value: {{ .fileLog.level }}
{{- end }}
{{- if and .fileLog.enabled .fileLog.rotationPeriod }}
- name: FILE_LOG_ROTATION_PERIOD
  value: {{ .fileLog.rotationPeriod }}
{{- end }}
{{- if and .fileLog.enabled .fileLog.maxFiles }}
- name: FILE_LOG_MAX_FILES
  value: {{ quote .fileLog.maxFiles }}
{{- end }}
{{- if .otelLogExporter.enabled }}
- name: OTEL_LOG_EXPORTER_ENABLED
  value: "true"
{{- end }}
{{- if and .otelLogExporter.enabled .otelLogExporter.level }}
- name: OTEL_LOG_EXPORTER_LEVEL
  value: {{ .otelLogExporter.level }}
{{- end }}
{{- if and .otelLogExporter.enabled .otelLogExporter.endpoint }}
- name: OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
  value: {{ .otelLogExporter.endpoint }}
{{- end }}
{{- if .otelTraceExporter.enabled }}
- name: OTEL_TRACE_EXPORTER_ENABLED
  value: "true"
{{- end }}
{{- if and .otelTraceExporter.enabled .otelTraceExporter.level }}
- name: OTEL_TRACE_EXPORTER_LEVEL
  value: {{ .otelTraceExporter.level }}
{{- end }}
{{- if and .otelTraceExporter.enabled .otelTraceExporter.endpoint }}
- name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  value: {{ .otelTraceExporter.endpoint }}
{{- end }}
{{- end }}
{{- end }}
