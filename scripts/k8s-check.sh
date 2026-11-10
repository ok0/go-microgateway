#!/bin/sh
# Usage: sh scripts/k8s-check.sh render|behavior <dir>
# render lints the Helm chart with HELM, renders it with the default values and with the Redis values into <dir>, and validates both renders with KUBECONFORM against the Kubernetes schemas.
# behavior checks the render failures, the default manifest, the merged configuration, metrics and the install notes with HELM, and compares the renders with HELM3 when it is set.
# HELM, HELM3 and KUBECONFORM are commands, not paths. The tool versions come from the Makefile.
set -eu

chart=deploy/helm/microgateway
redis_values=$chart/ci/redis-values.yaml
formula='delay + drainTimeout + 5s <= terminationGracePeriodSeconds'

fail() { echo "k8s-check: $*" >&2; exit 1; }

require_version() {
	got=$($1 version --short) || fail "$1 version failed"
	case $got in
	"$2" | "$2"+*) echo "k8s-check: $1 is $got" ;;
	*) fail "$1 is $got, want $2" ;;
	esac
}

has_line() { grep -qxF -- "$2" "$1" || fail "$1 lacks the line \"$2\""; }

lacks() {
	if grep -qF -- "$2" "$1"; then
		fail "$1 contains \"$2\""
	fi
}

expect_fail() {
	name=$1
	want=$2
	shift 2
	if $HELM template gw "$chart" "$@" >"$out/$name.out" 2>"$out/$name.err"; then
		fail "$name: the render succeeded, want a failure"
	fi
	grep -qF -- "$want" "$out/$name.err" || fail "$name: the error lacks \"$want\": $(cat "$out/$name.err")"
	echo "k8s-check: $name fails with \"$want\""
}

render() {
	out=$1
	require_version "$HELM" "$HELM_VERSION"
	rm -rf "$out"
	mkdir -p "$out"
	$HELM lint --strict "$chart"
	$HELM lint --strict "$chart" -f "$redis_values"
	$HELM template gw "$chart" >"$out/default.yaml"
	$HELM template gw "$chart" -f "$redis_values" >"$out/redis.yaml"
	$KUBECONFORM -strict -summary -schema-location default -kubernetes-version "$K8S_SCHEMA_VERSION" "$out/default.yaml" "$out/redis.yaml"
}

behavior() {
	out=$1
	require_version "$HELM" "$HELM_VERSION"
	kinds=$(sed -n 's/^kind: //p' "$out/default.yaml" | sort | tr '\n' ' ')
	[ "$kinds" = "ConfigMap Deployment HorizontalPodAutoscaler PodDisruptionBudget Service " ] || fail "the default render has the kinds $kinds"
	expect_fail grace-30 "$formula is not met: 10s + 20s + 5s > 30s" --set shutdown.terminationGracePeriodSeconds=30
	expect_fail delay-0 "$formula (got delay 0s)" --set shutdown.delay=0s
	expect_fail redis-no-address "redis.address is required when redis.enabled is true" --set redis.enabled=true --set redis.existingSecret=gateway-redis
	expect_fail redis-no-secret "redis.existingSecret is required when redis.enabled is true" --set redis.enabled=true --set redis.address=redis:6379
	if grep -inE 'redis|^ *stores:|^ *cache:' "$out/default.yaml"; then
		fail "the default render has Redis, store or cache settings"
	fi
	cat >"$out/user-values.yaml" <<'EOF'
config:
  listeners:
    - name: public
      address: ":8080"
  global:
    shutdown_delay: 1s
    log:
      level: debug
  admin:
    address: "0.0.0.0:9901"
    probes:
      address: ":7000"
      metrics: true
  upstreams:
    - name: api
      hosts:
        - url: "http://api:8080"
  routes:
    - name: api
      match:
        path: /api/
      upstream: api
EOF
	$HELM template gw "$chart" -f "$out/user-values.yaml" --show-only templates/configmap.yaml >"$out/user.yaml"
	has_line "$out/user.yaml" '    - address: :8080'
	has_line "$out/user.yaml" '      name: public'
	has_line "$out/user.yaml" '      upstream: api'
	has_line "$out/user.yaml" '      - url: http://api:8080'
	has_line "$out/user.yaml" '      address: 127.0.0.1:9901'
	has_line "$out/user.yaml" '        address: :9902'
	has_line "$out/user.yaml" '        metrics: false'
	has_line "$out/user.yaml" '      shutdown_delay: 10s'
	has_line "$out/user.yaml" '      drain_timeout: 20s'
	has_line "$out/user.yaml" '        level: debug'
	lacks "$out/user.yaml" 'name: http'
	lacks "$out/user.yaml" '0.0.0.0'
	lacks "$out/user.yaml" ':7000'
	lacks "$out/user.yaml" 'shutdown_delay: 1s'
	$HELM template gw "$chart" --set metrics.enabled=true --show-only templates/configmap.yaml >"$out/metrics.yaml"
	has_line "$out/metrics.yaml" '        metrics: true'
	$HELM install gw "$chart" --dry-run=client --namespace default >"$out/notes-default.txt"
	$HELM install gw "$chart" --dry-run=client --namespace default -f "$redis_values" >"$out/notes-redis.txt"
	has_line "$out/notes-default.txt" '  kubectl --namespace default port-forward deployment/gw-microgateway 9901:9901'
	has_line "$out/notes-redis.txt" '  kubectl --namespace default port-forward deployment/gw-microgateway 9901:9901'
	grep -qF 'WARNING: rate limits and the response cache are kept per pod' "$out/notes-default.txt" || fail "the default install notes lack the per-pod warning"
	lacks "$out/notes-redis.txt" 'WARNING:'
	if [ -z "${HELM3:-}" ]; then
		echo "k8s-check: HELM3 is not set, so the Helm 3 comparison is skipped; set HELM3 to a helm $HELM3_VERSION command to run it"
		return 0
	fi
	require_version "$HELM3" "$HELM3_VERSION"
	$HELM3 lint --strict "$chart"
	$HELM3 lint --strict "$chart" -f "$redis_values"
	$HELM3 template gw "$chart" >"$out/helm3-default.yaml"
	$HELM3 template gw "$chart" -f "$redis_values" >"$out/helm3-redis.yaml"
	cmp "$out/default.yaml" "$out/helm3-default.yaml" || fail "Helm 3 renders the default values differently"
	cmp "$out/redis.yaml" "$out/helm3-redis.yaml" || fail "Helm 3 renders the Redis values differently"
	echo "k8s-check: Helm 3 lints the chart and renders the same bytes as Helm 4"
}

[ $# -eq 2 ] || fail "usage: sh scripts/k8s-check.sh render|behavior <dir>"
case $1 in
render) render "$2" ;;
behavior) behavior "$2" ;;
*) fail "usage: sh scripts/k8s-check.sh render|behavior <dir>" ;;
esac
