package main

import (
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ok0/go-microgateway/config"
)

const (
	examplePassword     = "example-redis-password"
	nonrootID           = 65532
	margin              = 5 * time.Second
	defaultGraceSeconds = 30
	configKey           = "config.yaml"
)

var requiredKinds = []string{"ConfigMap", "Deployment", "HorizontalPodAutoscaler", "PodDisruptionBudget", "Service"}

// Finding is one rule violation of an object.
type Finding struct {
	Kind, Name, Rule string
}

// Line formats f for file as "<file>: <kind>/<name>: <rule>".
func (f Finding) Line(file string) string {
	return fmt.Sprintf("%s: %s/%s: %s", file, f.Kind, f.Name, f.Rule)
}

// ConfigText returns the name of the single ConfigMap in docs and its config.yaml entry; ok is false unless docs hold exactly one ConfigMap and it has a string data entry config.yaml.
func ConfigText(docs []Manifest) (name, text string, ok bool) {
	cm := singleConfigMap(docs)
	if cm == nil {
		return "", "", false
	}
	text, ok = mapAt(cm.Obj, "data")[configKey].(string)
	if !ok {
		return "", "", false
	}
	return cm.Name, text, true
}

// Check returns the rule violations of one rendered variant. cfg is the parsed config.yaml of its ConfigMap with defaults applied, or nil when it could not be parsed; the rules that need it are skipped then.
func Check(docs []Manifest, cfg *config.Config) []Finding {
	out := kindFindings(docs)
	out = append(out, secretFindings(docs)...)
	cm := singleConfigMap(docs)
	_, text, hasText := ConfigText(docs)
	if cm != nil {
		out = append(out, configMapFindings(cm, hasText, cfg)...)
	}
	for _, d := range docs {
		if d.Kind == "Deployment" {
			out = append(out, deploymentFindings(d, docs, cm, cfg, text)...)
		}
	}
	return out
}

// CheckDefaults returns the violations of the shutdown rules by cfg, the binary's defaults: shutdown_delay > 0 and shutdown_delay + drain_timeout + 5s <= 30s.
func CheckDefaults(cfg *config.Config) []Finding {
	var out []Finding
	delay := shutdownDelay(cfg.Global)
	drain := time.Duration(cfg.Global.DrainTimeout)
	if delay <= 0 {
		out = append(out, Finding{"Config", "defaults", fmt.Sprintf("default shutdown_delay %s is not greater than 0", delay)})
	}
	if delay+drain+margin > defaultGraceSeconds*time.Second {
		out = append(out, Finding{"Config", "defaults", fmt.Sprintf("default shutdown_delay + drain_timeout + 5s <= 30s is not met: %s + %s + 5s > 30s", delay, drain)})
	}
	return out
}

func kindFindings(docs []Manifest) []Finding {
	var out []Finding
	for _, k := range requiredKinds {
		if n := countKind(docs, k); n != 1 {
			out = append(out, Finding{k, "-", fmt.Sprintf("want exactly one %s, found %d", k, n)})
		}
	}
	for _, d := range docs {
		if !slices.Contains(requiredKinds, d.Kind) {
			out = append(out, Finding{d.Kind, d.Name, "unexpected object kind"})
		}
	}
	return out
}

func secretFindings(docs []Manifest) []Finding {
	var out []Finding
	for _, d := range docs {
		if containsText(d.Obj, examplePassword) {
			out = append(out, Finding{d.Kind, d.Name, "contains the example Redis password"})
		}
	}
	return out
}

func configMapFindings(cm *Manifest, hasText bool, cfg *config.Config) []Finding {
	var out []Finding
	if !hasText {
		out = append(out, Finding{"ConfigMap", cm.Name, "no config.yaml in data"})
	}
	if cfg == nil {
		return out
	}
	probes := cfg.Admin.Probes
	if probes.Enabled == nil || !*probes.Enabled || config.IsLoopbackAddress(probes.Address) {
		out = append(out, Finding{"ConfigMap", cm.Name, "admin.probes is not enabled on a non-loopback address"})
	}
	adm := &cfg.Admin
	if adm.Enabled == nil || *adm.Enabled {
		if !config.IsLoopbackAddress(adm.Address) && (!adm.ManagementAuthRequired() || len(adm.Auth.Keys) == 0) {
			out = append(out, Finding{"ConfigMap", cm.Name, fmt.Sprintf("admin.address %s is not loopback and management endpoints do not require a key", adm.Address)})
		}
	}
	if hasRedisStore(cfg) && !storeNamed(cfg, cfg.Cache.Store) {
		out = append(out, Finding{"ConfigMap", cm.Name, "cache.store does not name a configured store"})
	}
	return out
}

func deploymentFindings(d Manifest, docs []Manifest, cm *Manifest, cfg *config.Config, text string) []Finding {
	var out []Finding
	add := func(rule string) {
		out = append(out, Finding{d.Kind, d.Name, rule})
	}
	pod := mapAt(d.Obj, "spec", "template", "spec")

	if rv, ok := valueAt(d.Obj, "spec", "replicas"); ok {
		if n, isInt := intValue(rv); !isInt || n < 2 {
			add(fmt.Sprintf("replicas %v is below 2", rv))
		}
	} else if !hpaTargets(docs, d.Name) {
		add("replicas is unset and no HorizontalPodAutoscaler with minReplicas >= 2 targets it")
	}

	strategy := mapAt(d.Obj, "spec", "strategy")
	typ, hasType := strategy["type"]
	maxUnavailable, _ := valueAt(d.Obj, "spec", "strategy", "rollingUpdate", "maxUnavailable")
	if (hasType && typ != "RollingUpdate") || !isZeroUnavailable(maxUnavailable) {
		add("strategy is not a rolling update with maxUnavailable: 0")
	}

	if len(listAt(pod, "topologySpreadConstraints")) == 0 && len(mapAt(pod, "affinity", "podAntiAffinity")) == 0 {
		add("no topology spread constraint or pod anti-affinity")
	}

	psc := mapAt(pod, "securityContext")
	if psc["runAsNonRoot"] != true {
		add("pod securityContext.runAsNonRoot is not true")
	}
	if !isInt(psc["runAsUser"], nonrootID) {
		add("pod securityContext.runAsUser is not 65532")
	}
	if !isInt(psc["runAsGroup"], nonrootID) {
		add("pod securityContext.runAsGroup is not 65532")
	}
	if stringAt(psc, "seccompProfile", "type") != "RuntimeDefault" {
		add("pod securityContext.seccompProfile.type is not RuntimeDefault")
	}

	all := append(containers(pod, true), containers(pod, false)...)
	for _, c := range all {
		name := stringAt(c, "name")
		sc := mapAt(c, "securityContext")
		if sc["allowPrivilegeEscalation"] != false {
			add(fmt.Sprintf("container %s: securityContext.allowPrivilegeEscalation is not false", name))
		}
		if sc["readOnlyRootFilesystem"] != true {
			add(fmt.Sprintf("container %s: securityContext.readOnlyRootFilesystem is not true", name))
		}
		if !listHas(listAt(sc, "capabilities", "drop"), "ALL") {
			add(fmt.Sprintf("container %s: securityContext.capabilities.drop does not contain ALL", name))
		}
		lc := mapAt(c, "lifecycle")
		for _, h := range slices.Sorted(maps.Keys(lc)) {
			if _, ok := mapAt(lc, h)["exec"]; ok {
				add(fmt.Sprintf("container %s: lifecycle.%s has an exec handler", name, h))
			}
		}
	}

	if cm != nil {
		vols := configVolumes(pod, cm.Name)
		if len(vols) == 0 {
			add(fmt.Sprintf("config ConfigMap %s is not a pod volume", cm.Name))
		}
		for _, v := range vols {
			vn := stringAt(v, "name")
			mounted := false
			for _, c := range all {
				for _, m := range mapList(listAt(c, "volumeMounts")) {
					if stringAt(m, "name") != vn {
						continue
					}
					mounted = true
					if stringAt(m, "subPath") != "" || stringAt(m, "subPathExpr") != "" {
						add(fmt.Sprintf("container %s: config volume %s is mounted with subPath", stringAt(c, "name"), vn))
					}
				}
			}
			if !mounted {
				add(fmt.Sprintf("config volume %s is not mounted", vn))
			}
		}
	}

	for _, c := range containers(pod, false) {
		name := stringAt(c, "name")
		for _, p := range []struct{ field, path string }{{"livenessProbe", "/livez"}, {"readinessProbe", "/readyz"}} {
			if stringAt(c, p.field, "httpGet", "path") != p.path {
				add(fmt.Sprintf("container %s: %s is not httpGet on %s", name, p.field, p.path))
			}
		}
	}
	if cfg != nil {
		if want, ok := adminProbePort(cfg); ok {
			for _, c := range containers(pod, false) {
				for _, probe := range []string{"livenessProbe", "readinessProbe"} {
					if _, ok := valueAt(c, probe, "httpGet"); !ok {
						continue
					}
					port, _ := valueAt(c, probe, "httpGet", "port")
					if n, resolved := resolvePort(c, port); !resolved || n != want {
						add(fmt.Sprintf("container %s: %s port %v is not the container port %d of admin.probes.address", stringAt(c, "name"), probe, port, want))
					}
				}
			}
		}
	}

	if strings.Contains(text, "${REDIS_PASSWORD}") {
		for _, c := range containers(pod, false) {
			if !redisPasswordFromSecret(c) {
				add(fmt.Sprintf("container %s: env REDIS_PASSWORD is not from a secretKeyRef", stringAt(c, "name")))
			}
		}
	}

	if cfg != nil {
		delay := shutdownDelay(cfg.Global)
		drain := time.Duration(cfg.Global.DrainTimeout)
		grace := defaultGraceSeconds
		graceKnown := true
		if raw, present := pod["terminationGracePeriodSeconds"]; present && raw != nil {
			if n, ok := intValue(raw); ok {
				grace = n
			} else {
				add(fmt.Sprintf("terminationGracePeriodSeconds is not an integer: %v", raw))
				graceKnown = false
			}
		}
		if delay <= 0 {
			add(fmt.Sprintf("shutdown_delay %s is not greater than 0", delay))
		}
		if graceKnown && delay+drain+margin > time.Duration(grace)*time.Second {
			add(fmt.Sprintf("shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds is not met: %s + %s + 5s > %ds", delay, drain, grace))
		}
	}
	return out
}

func singleConfigMap(docs []Manifest) *Manifest {
	var cm *Manifest
	n := 0
	for i := range docs {
		if docs[i].Kind == "ConfigMap" {
			cm = &docs[i]
			n++
		}
	}
	if n != 1 {
		return nil
	}
	return cm
}

func countKind(docs []Manifest, kind string) int {
	n := 0
	for _, d := range docs {
		if d.Kind == kind {
			n++
		}
	}
	return n
}

func hpaTargets(docs []Manifest, name string) bool {
	for _, d := range docs {
		if d.Kind != "HorizontalPodAutoscaler" {
			continue
		}
		if stringAt(d.Obj, "spec", "scaleTargetRef", "kind") != "Deployment" || stringAt(d.Obj, "spec", "scaleTargetRef", "name") != name {
			continue
		}
		minV, _ := valueAt(d.Obj, "spec", "minReplicas")
		if n, ok := intValue(minV); ok && n >= 2 {
			return true
		}
	}
	return false
}

func hasRedisStore(cfg *config.Config) bool {
	for _, s := range cfg.Stores {
		if s.Type == "redis" {
			return true
		}
	}
	return false
}

func storeNamed(cfg *config.Config, name string) bool {
	for _, s := range cfg.Stores {
		if s.Name == name {
			return true
		}
	}
	return false
}

func shutdownDelay(g config.Global) time.Duration {
	if g.ShutdownDelay == nil {
		return 0
	}
	return g.ShutdownDelay.Std()
}

func adminProbePort(cfg *config.Config) (int, bool) {
	_, ps, err := net.SplitHostPort(cfg.Admin.Probes.Address)
	if err != nil {
		return 0, false
	}
	p, err := strconv.Atoi(ps)
	if err != nil {
		return 0, false
	}
	return p, true
}

func resolvePort(c map[string]any, port any) (int, bool) {
	if n, ok := intValue(port); ok {
		return n, true
	}
	name, ok := port.(string)
	if !ok {
		return 0, false
	}
	for _, p := range mapList(listAt(c, "ports")) {
		if stringAt(p, "name") == name {
			return intValue(p["containerPort"])
		}
	}
	return 0, false
}

func redisPasswordFromSecret(c map[string]any) bool {
	found := false
	for _, e := range mapList(listAt(c, "env")) {
		if stringAt(e, "name") != "REDIS_PASSWORD" {
			continue
		}
		found = true
		if _, ok := e["value"]; ok || stringAt(e, "valueFrom", "secretKeyRef", "name") == "" || stringAt(e, "valueFrom", "secretKeyRef", "key") == "" {
			return false
		}
	}
	return found
}

func configVolumes(pod map[string]any, cmName string) []map[string]any {
	var out []map[string]any
	for _, v := range mapList(listAt(pod, "volumes")) {
		if stringAt(v, "configMap", "name") == cmName {
			out = append(out, v)
		}
	}
	return out
}

func mapList(l []any) []map[string]any {
	var out []map[string]any
	for _, v := range l {
		if m, ok := v.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func listHas(list []any, want string) bool {
	for _, v := range list {
		if s, ok := v.(string); ok && s == want {
			return true
		}
	}
	return false
}

func containsText(v any, sub string) bool {
	switch x := v.(type) {
	case string:
		return strings.Contains(x, sub)
	case map[string]any:
		for _, e := range x {
			if containsText(e, sub) {
				return true
			}
		}
	case []any:
		for _, e := range x {
			if containsText(e, sub) {
				return true
			}
		}
	}
	return false
}

func isInt(v any, want int) bool {
	n, ok := intValue(v)
	return ok && n == want
}

func isZeroUnavailable(v any) bool {
	if n, ok := intValue(v); ok {
		return n == 0
	}
	s, ok := v.(string)
	return ok && (s == "0" || s == "0%")
}
