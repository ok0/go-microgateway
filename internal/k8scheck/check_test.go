package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
)

func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mutate(t *testing.T, src, old, with string) string {
	t.Helper()
	if n := strings.Count(src, old); n != 1 {
		t.Fatalf("anchor %q occurs %d times, want 1", old, n)
	}
	return strings.Replace(src, old, with, 1)
}

func drop(t *testing.T, src, kind string) string {
	t.Helper()
	var keep []string
	removed := 0
	for _, doc := range strings.Split(src, "\n---\n") {
		if strings.Contains(doc, "\nkind: "+kind+"\n") {
			removed++
			continue
		}
		keep = append(keep, doc)
	}
	if removed != 1 {
		t.Fatalf("%d documents of kind %s, want 1", removed, kind)
	}
	return strings.Join(keep, "\n---\n")
}

func findingsOf(t *testing.T, src string) []Finding {
	t.Helper()
	docs, err := Decode([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	var cfg *config.Config
	if _, text, ok := ConfigText(docs); ok {
		parsed, err := config.Parse([]byte(text), config.WithEnvLookup(lookupEnv))
		if err != nil {
			t.Fatal(err)
		}
		cfg = parsed
	}
	return Check(docs, cfg)
}

type edit func(t *testing.T, src string) string

func replaceEdit(old, with string) edit {
	return func(t *testing.T, src string) string {
		return mutate(t, src, old, with)
	}
}

func dropEdit(kind string) edit {
	return func(t *testing.T, src string) string {
		return drop(t, src, kind)
	}
}

func appendEdit(extra string) edit {
	return func(t *testing.T, src string) string {
		return src + extra
	}
}

func noEdit(_ *testing.T, src string) string {
	return src
}

func TestCheck(t *testing.T) {
	const (
		D  = "Deployment"
		g  = "gw-microgateway"
		c  = "container microgateway: "
		cm = "ConfigMap"
	)
	const graceRule = "shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds is not met: 10s + 20s + 5s > 30s"
	const spreadBlock = "      topologySpreadConstraints:\n" +
		"        - maxSkew: 1\n" +
		"          topologyKey: kubernetes.io/hostname\n" +
		"          whenUnsatisfiable: ScheduleAnyway\n" +
		"          labelSelector:\n" +
		"            matchLabels:\n" +
		"              app.kubernetes.io/name: microgateway\n" +
		"              app.kubernetes.io/instance: gw\n"
	const secretBlock = "              valueFrom:\n" +
		"                secretKeyRef:\n" +
		"                  name: \"microgateway-redis\"\n" +
		"                  key: \"password\"\n"
	const serviceDoc = "kind: Service\nmetadata:\n  name: gw-microgateway\n"

	tests := []struct {
		name string
		base string
		edit edit
		want []Finding
	}{
		{"pass-default", "default", noEdit, nil},
		{"pass-redis", "redis", noEdit, nil},
		{"grace-30", "default", replaceEdit("terminationGracePeriodSeconds: 40", "terminationGracePeriodSeconds: 30"),
			[]Finding{{D, g, graceRule}}},
		{"grace-absent", "default", replaceEdit("      terminationGracePeriodSeconds: 40\n", ""),
			[]Finding{{D, g, graceRule}}},
		{"grace-null", "default", replaceEdit("terminationGracePeriodSeconds: 40", "terminationGracePeriodSeconds: null"),
			[]Finding{{D, g, graceRule}}},
		{"grace-string", "default", replaceEdit("terminationGracePeriodSeconds: 40", "terminationGracePeriodSeconds: \"40\""),
			[]Finding{{D, g, "terminationGracePeriodSeconds is not an integer: 40"}}},
		{"zero-delay", "default", replaceEdit("      shutdown_delay: 10s", "      shutdown_delay: 0s"),
			[]Finding{{D, g, "shutdown_delay 0s is not greater than 0"}}},
		{"exec-prestop", "default", replaceEdit("          imagePullPolicy: IfNotPresent\n",
			"          imagePullPolicy: IfNotPresent\n          lifecycle:\n            preStop:\n              exec:\n                command: [\"sleep\", \"5\"]\n"),
			[]Finding{{D, g, c + "lifecycle.preStop has an exec handler"}}},
		{"exec-poststart", "default", replaceEdit("          imagePullPolicy: IfNotPresent\n",
			"          imagePullPolicy: IfNotPresent\n          lifecycle:\n            postStart:\n              exec:\n                command: [\"sleep\", \"5\"]\n"),
			[]Finding{{D, g, c + "lifecycle.postStart has an exec handler"}}},
		{"subpath", "default", replaceEdit("              readOnly: true\n", "              readOnly: true\n              subPath: config.yaml\n"),
			[]Finding{{D, g, c + "config volume config is mounted with subPath"}}},
		{"replicas-one", "default", replaceEdit("  strategy:\n", "  replicas: 1\n  strategy:\n"),
			[]Finding{{D, g, "replicas 1 is below 2"}}},
		{"hpa-min-one", "default", replaceEdit("  minReplicas: 2", "  minReplicas: 1"),
			[]Finding{{D, g, "replicas is unset and no HorizontalPodAutoscaler with minReplicas >= 2 targets it"}}},
		{"no-hpa", "default", dropEdit("HorizontalPodAutoscaler"),
			[]Finding{
				{"HorizontalPodAutoscaler", "-", "want exactly one HorizontalPodAutoscaler, found 0"},
				{D, g, "replicas is unset and no HorizontalPodAutoscaler with minReplicas >= 2 targets it"},
			}},
		{"max-unavailable", "default", replaceEdit("      maxUnavailable: 0", "      maxUnavailable: 1"),
			[]Finding{{D, g, "strategy is not a rolling update with maxUnavailable: 0"}}},
		{"no-spread", "default", replaceEdit(spreadBlock, ""),
			[]Finding{{D, g, "no topology spread constraint or pod anti-affinity"}}},
		{"run-as-non-root", "default", replaceEdit("runAsNonRoot: true", "runAsNonRoot: false"),
			[]Finding{{D, g, "pod securityContext.runAsNonRoot is not true"}}},
		{"run-as-user", "default", replaceEdit("runAsUser: 65532", "runAsUser: 1000"),
			[]Finding{{D, g, "pod securityContext.runAsUser is not 65532"}}},
		{"run-as-group", "default", replaceEdit("runAsGroup: 65532", "runAsGroup: 1000"),
			[]Finding{{D, g, "pod securityContext.runAsGroup is not 65532"}}},
		{"seccomp", "default", replaceEdit("type: RuntimeDefault", "type: Unconfined"),
			[]Finding{{D, g, "pod securityContext.seccompProfile.type is not RuntimeDefault"}}},
		{"privilege-escalation", "default", replaceEdit("allowPrivilegeEscalation: false", "allowPrivilegeEscalation: true"),
			[]Finding{{D, g, c + "securityContext.allowPrivilegeEscalation is not false"}}},
		{"read-only-root", "default", replaceEdit("readOnlyRootFilesystem: true", "readOnlyRootFilesystem: false"),
			[]Finding{{D, g, c + "securityContext.readOnlyRootFilesystem is not true"}}},
		{"drop-all", "default", replaceEdit("drop: [ALL]", "drop: [NET_RAW]"),
			[]Finding{{D, g, c + "securityContext.capabilities.drop does not contain ALL"}}},
		{"probe-port", "default", replaceEdit("              path: /readyz\n              port: probe", "              path: /readyz\n              port: http"),
			[]Finding{{D, g, c + "readinessProbe port http is not the container port 9902 of admin.probes.address"}}},
		{"liveness-path", "default", replaceEdit("path: /livez", "path: /healthz"),
			[]Finding{{D, g, c + "livenessProbe is not httpGet on /livez"}}},
		{"management-open", "default", replaceEdit("      address: 127.0.0.1:9901", "      address: 0.0.0.0:9901\n      auth:\n        management: none"),
			[]Finding{{cm, g, "admin.address 0.0.0.0:9901 is not loopback and management endpoints do not require a key"}}},
		{"probes-loopback", "default", replaceEdit("        address: :9902", "        address: 127.0.0.1:9902"),
			[]Finding{{cm, g, "admin.probes is not enabled on a non-loopback address"}}},
		{"missing-kind", "default", dropEdit("PodDisruptionBudget"),
			[]Finding{{"PodDisruptionBudget", "-", "want exactly one PodDisruptionBudget, found 0"}}},
		{"duplicate-kind", "default", appendEdit("---\napiVersion: v1\nkind: Service\nmetadata:\n  name: gw-microgateway-extra\nspec:\n  ports:\n    - name: http\n      port: 81\n"),
			[]Finding{{"Service", "-", "want exactly one Service, found 2"}}},
		{"unexpected-kind", "default", appendEdit("---\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: gw-microgateway\n"),
			[]Finding{{"ServiceAccount", g, "unexpected object kind"}}},
		{"literal-secret", "redis", replaceEdit(secretBlock, "              value: literal-value\n"),
			[]Finding{{D, g, c + "env REDIS_PASSWORD is not from a secretKeyRef"}}},
		{"example-password", "default", replaceEdit(serviceDoc, serviceDoc+"  annotations:\n    note: example-redis-password\n"),
			[]Finding{{"Service", g, "contains the example Redis password"}}},
		{"no-cache-store", "redis", replaceEdit("    cache:\n      store: shared\n", ""),
			[]Finding{{cm, g, "cache.store does not name a configured store"}}},
		{"no-config-key", "default", replaceEdit("  config.yaml: |", "  gateway.yaml: |"),
			[]Finding{{cm, g, "no config.yaml in data"}}},
		{"config-not-volume", "default", replaceEdit("          configMap:\n            name: gw-microgateway\n", "          configMap:\n            name: other\n"),
			[]Finding{{D, g, "config ConfigMap gw-microgateway is not a pod volume"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := findingsOf(t, tc.edit(t, fixture(t, tc.base)))
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("findings = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCheckDefaults(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want []Finding
	}{
		{"defaults", defaultsDocument, nil},
		{"drain-30s", defaultsDocument + "global:\n  drain_timeout: 30s\n",
			[]Finding{{"Config", "defaults", "default shutdown_delay + drain_timeout + 5s <= 30s is not met: 5s + 30s + 5s > 30s"}}},
		{"delay-0", defaultsDocument + "global:\n  shutdown_delay: 0s\n",
			[]Finding{{"Config", "defaults", "default shutdown_delay 0s is not greater than 0"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.Parse([]byte(tc.doc), config.WithEnvLookup(lookupEnv))
			if err != nil {
				t.Fatal(err)
			}
			got := CheckDefaults(cfg)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("findings = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	got, err := Decode([]byte("---\n# only a comment\n---\n" + "kind: Service\nmetadata:\n  name: a\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != "Service" || got[0].Name != "a" {
		t.Fatalf("Decode = %+v, want one Service a", got)
	}
	if _, err := Decode([]byte("- a\n")); err == nil {
		t.Error("a sequence document decoded without error")
	}
	if _, err := Decode([]byte("metadata:\n  name: a\n")); err == nil || !strings.Contains(err.Error(), "has no kind") {
		t.Errorf("Decode error = %v, want one containing has no kind", err)
	}
}

func TestFindingLine(t *testing.T) {
	got := Finding{"Deployment", "gw", "x"}.Line("a.yaml")
	if want := "a.yaml: Deployment/gw: x"; got != want {
		t.Errorf("Line = %q, want %q", got, want)
	}
}
