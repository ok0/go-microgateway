{{- define "microgateway.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "microgateway.fullname" -}}
{{- $name := .Chart.Name -}}
{{- if or (eq .Release.Name $name) (contains $name .Release.Name) -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "microgateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "microgateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "microgateway.labels" -}}
{{ include "microgateway.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "microgateway.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "microgateway.millis" -}}
{{- $v := .value -}}
{{- if or (not (kindIs "string" $v)) (not (regexMatch "^([0-9]+(ms|h|m|s))+$" $v)) -}}
{{- fail (printf "%s must be a duration string made of whole h, m, s or ms parts, such as 10s or 1m30s; got %v" .key $v) -}}
{{- end -}}
{{- $total := 0 -}}
{{- range regexFindAll "[0-9]+(ms|h|m|s)" $v -1 -}}
{{- if hasSuffix "ms" . -}}
{{- $total = add $total (trimSuffix "ms" . | atoi) -}}
{{- else if hasSuffix "h" . -}}
{{- $total = add $total (mul (trimSuffix "h" . | atoi) 3600000) -}}
{{- else if hasSuffix "m" . -}}
{{- $total = add $total (mul (trimSuffix "m" . | atoi) 60000) -}}
{{- else -}}
{{- $total = add $total (mul (trimSuffix "s" . | atoi) 1000) -}}
{{- end -}}
{{- end -}}
{{- $total -}}
{{- end -}}

{{- define "microgateway.validate" -}}
{{- $delay := atoi (include "microgateway.millis" (dict "key" "shutdown.delay" "value" .Values.shutdown.delay)) -}}
{{- $drain := atoi (include "microgateway.millis" (dict "key" "shutdown.drainTimeout" "value" .Values.shutdown.drainTimeout)) -}}
{{- $grace := .Values.shutdown.terminationGracePeriodSeconds -}}
{{- if not (or (kindIs "float64" $grace) (kindIs "int" $grace) (kindIs "int64" $grace)) -}}
{{- fail "shutdown.terminationGracePeriodSeconds must be a whole number of seconds" -}}
{{- end -}}
{{- $graceMs := mul (int $grace) 1000 -}}
{{- if eq $delay 0 -}}
{{- fail (printf "shutdown.delay must be greater than 0s so endpoints are removed while the pod still serves; the chart requires delay > 0 and delay + drainTimeout + 5s <= terminationGracePeriodSeconds (got delay %s)" .Values.shutdown.delay) -}}
{{- end -}}
{{- if eq $drain 0 -}}
{{- fail "shutdown.drainTimeout must be greater than 0s" -}}
{{- end -}}
{{- if gt (add $delay $drain 5000) $graceMs -}}
{{- fail (printf "shutdown: delay + drainTimeout + 5s <= terminationGracePeriodSeconds is not met: %s + %s + 5s > %ds; raise terminationGracePeriodSeconds or lower delay or drainTimeout" .Values.shutdown.delay .Values.shutdown.drainTimeout (int $grace)) -}}
{{- end -}}
{{- if not (kindIs "bool" .Values.metrics.enabled) -}}
{{- fail "metrics.enabled must be true or false" -}}
{{- end -}}
{{- if .Values.redis.enabled -}}
{{- $_ := required "redis.address is required when redis.enabled is true" .Values.redis.address -}}
{{- $_ := required "redis.existingSecret is required when redis.enabled is true (a Secret holding the Redis password)" .Values.redis.existingSecret -}}
{{- $_ := required "redis.passwordKey is required when redis.enabled is true" .Values.redis.passwordKey -}}
{{- $_ := required "redis.keyPrefix is required when redis.enabled is true" .Values.redis.keyPrefix -}}
{{- range .Values.env -}}
{{- if eq .name "REDIS_PASSWORD" -}}
{{- fail "env must not set REDIS_PASSWORD when redis.enabled is true; the chart sets it from redis.existingSecret" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "microgateway.config" -}}
{{- $cfg := mergeOverwrite (dict "listeners" (list (dict "name" "http" "address" (printf ":%d" (int .Values.ports.http))))) (deepCopy (.Values.config | default dict)) -}}
{{- $_ := set $cfg "version" 1 -}}
{{- $g := dict -}}
{{- if hasKey $cfg "global" -}}
{{- if not (kindIs "map" (get $cfg "global")) -}}
{{- fail "config.global must be a map" -}}
{{- end -}}
{{- $g = get $cfg "global" -}}
{{- end -}}
{{- $_ := set $g "shutdown_delay" .Values.shutdown.delay -}}
{{- $_ := set $g "drain_timeout" .Values.shutdown.drainTimeout -}}
{{- $_ := set $cfg "global" $g -}}
{{- $a := dict -}}
{{- if hasKey $cfg "admin" -}}
{{- if not (kindIs "map" (get $cfg "admin")) -}}
{{- fail "config.admin must be a map" -}}
{{- end -}}
{{- $a = get $cfg "admin" -}}
{{- end -}}
{{- $p := dict -}}
{{- if hasKey $a "probes" -}}
{{- if not (kindIs "map" (get $a "probes")) -}}
{{- fail "config.admin.probes must be a map" -}}
{{- end -}}
{{- $p = get $a "probes" -}}
{{- end -}}
{{- $_ := set $a "address" "127.0.0.1:9901" -}}
{{- $_ := set $p "enabled" true -}}
{{- $_ := set $p "address" (printf ":%d" (int .Values.ports.probe)) -}}
{{- $_ := set $p "metrics" .Values.metrics.enabled -}}
{{- $_ := set $a "probes" $p -}}
{{- $_ := set $cfg "admin" $a -}}
{{- if .Values.redis.enabled -}}
{{- $_ := set $cfg "stores" (list (dict "name" "shared" "type" "redis" "redis" (dict "mode" "standalone" "addresses" (list .Values.redis.address) "password" "${REDIS_PASSWORD}" "key_prefix" .Values.redis.keyPrefix))) -}}
{{- $c := dict -}}
{{- if hasKey $cfg "cache" -}}
{{- if not (kindIs "map" (get $cfg "cache")) -}}
{{- fail "config.cache must be a map" -}}
{{- end -}}
{{- $c = get $cfg "cache" -}}
{{- end -}}
{{- $_ := set $c "store" "shared" -}}
{{- $_ := set $cfg "cache" $c -}}
{{- end -}}
{{- toYaml $cfg -}}
{{- end -}}
