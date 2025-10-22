{{/*
Create a list of maintenance related env vars.
*/}}
{{- define "maintenance.envVars" -}}
{{- with .Values.maintenance }}
{{- if not .endOfSupportCheck.enabled }}
- name: EOS_DISABLED
  value: "true"
{{- end }}
{{- if and .endOfSupportCheck.enabled .endOfSupportCheck.mode }}
- name: EOS_CHECK_MODE
  value: {{ .endOfSupportCheck.mode }}
{{ end }}
{{- if and .endOfSupportCheck.enabled .endOfSupportCheck.interval }}
- name: EOS_INTERVAL
  value: {{ .endOfSupportCheck.interval }}
{{ end }}
{{- if not .customResourceDefinitions.maintain }}
- name: DISABLE_CRD_MAINTENANCE
  value: "true"
{{- end }}
{{- end }}
{{- end }}
