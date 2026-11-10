# Kubernetes

The Helm chart in `deploy/helm/microgateway` runs the gateway as two or more stateless replicas. It deploys a Deployment, a Service, a ConfigMap that holds the gateway configuration, a PodDisruptionBudget and a HorizontalPodAutoscaler, and nothing else: there is no Ingress and no Redis. The defaults of every value are in the [values file](../deploy/helm/microgateway/values.yaml).

## Install

From a checkout of the repository:

```sh
helm install gateway deploy/helm/microgateway -f values.yaml
```

To render the manifests without installing them:

```sh
helm template gateway deploy/helm/microgateway -f values.yaml > gateway.yaml
```

The chart needs Helm 4, or Helm 3.22 or later. The default image is `ghcr.io/ok0/go-microgateway:<appVersion>`. That image exists only after the release tag with the same version is published. Before then, set `values.image.repository` and `values.image.tag` to an image you built with `make docker`.

## Values

| Key | Default | Meaning |
|-----|---------|---------|
| `values.image.repository` | `ghcr.io/ok0/go-microgateway` | Image repository. |
| `values.image.tag` | `""` | Image tag. Empty means the chart `appVersion`. |
| `values.image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `values.replicaCount` | `2` | Replicas. Used only when autoscaling is off. |
| `values.autoscaling.enabled` | `true` | Creates a HorizontalPodAutoscaler and leaves the replica count to it. |
| `values.autoscaling.minReplicas` | `2` | Lowest replica count the autoscaler sets. |
| `values.autoscaling.maxReplicas` | `10` | Highest replica count the autoscaler sets. |
| `values.autoscaling.targetCPUUtilizationPercentage` | `70` | Average CPU utilization, in percent of the request, that the autoscaler aims for. |
| `values.podDisruptionBudget.enabled` | `true` | Creates a PodDisruptionBudget. |
| `values.podDisruptionBudget.maxUnavailable` | `1` | Pods that voluntary disruptions may take down at once. |
| `values.resources.requests.cpu` | `100m` | CPU request of the gateway container. |
| `values.resources.requests.memory` | `64Mi` | Memory request of the gateway container. |
| `values.resources.limits.memory` | `256Mi` | Memory limit of the gateway container. The chart sets no CPU limit. |
| `values.shutdown.delay` | `10s` | Value of `global.shutdown_delay`. Must be greater than `0s`. |
| `values.shutdown.drainTimeout` | `20s` | Value of `global.drain_timeout`. Must be greater than `0s`. |
| `values.shutdown.terminationGracePeriodSeconds` | `40` | Pod termination grace period. Must be at least `values.shutdown.delay` + `values.shutdown.drainTimeout` + 5s; see [Shutdown timing](#shutdown-timing). |
| `values.ports.http` | `8080` | Container port of the data listeners, and the port that the Service targets. |
| `values.ports.probe` | `9902` | Container port of the probe listener. |
| `values.service.type` | `ClusterIP` | Service type. |
| `values.service.port` | `80` | Service port for the `http` port. |
| `values.metrics.enabled` | `false` | Serves `/metrics` on the probe listener, without a token. |
| `values.redis.enabled` | `false` | Uses an external Redis for rate limits and the response cache. |
| `values.redis.address` | `""` | Address of the Redis server, as `host:port`. Required when Redis is enabled. |
| `values.redis.keyPrefix` | `microgateway` | Prefix of the keys that the gateway writes to Redis. |
| `values.redis.existingSecret` | `""` | Secret that holds the Redis password. Required when Redis is enabled. |
| `values.redis.passwordKey` | `password` | Key of the Secret that holds the Redis password. |
| `values.env` | `[]` | Extra container environment entries, appended after the chart's own entries. |
| `values.config` | `{}` | Gateway configuration, merged with the chart defaults as described in [Gateway configuration](#gateway-configuration). |

## Gateway configuration

Write the gateway settings under `values.config`, using the keys of the [configuration reference](configuration.md). The values below define one upstream and one route:

```yaml
config:
  global:
    log:
      level: debug
  upstreams:
    - name: api
      hosts:
        - url: "http://api.default.svc.cluster.local:8080"
  routes:
    - name: api
      match:
        path: /api/
      upstream: api
```

Valid configuration:

```yaml
version: 1

global:
  log:
    level: debug
  shutdown_delay: 10s
  drain_timeout: 20s
listeners:
  - name: http
    address: ":8080"
admin:
  address: 127.0.0.1:9901
  probes:
    enabled: true
    address: ":9902"
    metrics: false
upstreams:
  - name: api
    hosts:
      - url: "http://api.default.svc.cluster.local:8080"
routes:
  - name: api
    match:
      path: /api/
    upstream: api
```

The chart merges the values as follows:

- The chart default for `listeners` is replaced when `values.config` sets `listeners`. A user listener must listen on `values.ports.http`, because the container port and the Service target that port.
- The chart always sets `version`, `global.shutdown_delay`, `global.drain_timeout`, `admin.address` and `admin.probes`. These values win over the same keys in `values.config`.
- Other keys under `global` and `admin` are kept, for example `global.log` and `admin.auth`.
- Lists are replaced, not merged.
- With Redis enabled, `stores` and `cache.store` are set by the chart. A user `stores` list is dropped. Other keys under `cache` are kept.
- A change to a listener address or to an admin address needs a restart. See [Hot reload behavior](operations.md#hot-reload-behavior).

## Shared state with Redis

The gateway keeps rate-limit counters and the response cache in Redis when a store is configured. Redis is external to the chart and is not bundled with it. Create a Secret that holds the password first. The Secret must be in the same namespace as the release:

```sh
kubectl create secret generic gateway-redis --from-literal=password='<password>'
```

Set the Redis values and the configuration:

```yaml
redis:
  enabled: true
  address: redis.data.svc.cluster.local:6379
  existingSecret: gateway-redis
config:
  upstreams:
    - name: api
      hosts:
        - url: "http://api.default.svc.cluster.local:8080"
  routes:
    - name: api
      match:
        path: /api/
      upstream: api
      rate_limit:
        rate: 100
        per: 1s
        burst: 100
        store: shared
        on_store_error: local
```

Valid configuration:

```yaml
version: 1

global:
  shutdown_delay: 10s
  drain_timeout: 20s
listeners:
  - name: http
    address: ":8080"
admin:
  address: 127.0.0.1:9901
  probes:
    enabled: true
    address: ":9902"
    metrics: false
stores:
  - name: shared
    type: redis
    redis:
      mode: standalone
      addresses: ["redis.data.svc.cluster.local:6379"]
      password: "${REDIS_PASSWORD}"
      key_prefix: microgateway
cache:
  store: shared
upstreams:
  - name: api
    hosts:
      - url: "http://api.default.svc.cluster.local:8080"
routes:
  - name: api
    match:
      path: /api/
    upstream: api
    rate_limit:
      rate: 100
      per: 1s
      burst: 100
      store: shared
      on_store_error: local
```

The password reaches the pod as the environment variable `REDIS_PASSWORD`, read from the Secret. The configuration holds only the reference `${REDIS_PASSWORD}`, so the password never appears in the ConfigMap. The response cache uses the store as soon as `cache.store` is set. Rate limits share their counters only on routes that set `store: shared`.

The chart supports standalone Redis only. Sentinel and cluster modes are not set by values. To use them, leave `values.redis.enabled` false, write the `stores` entry in `values.config` with the mode you need, and pass the password through `values.env` with a `valueFrom.secretKeyRef` entry.

### Per-pod state without Redis

Without a shared store, every rate limit, cache entry and cache purge applies to one pod only. With the default `values.redis.enabled` false, the install notes print the same warning. See the [multi-instance guide](multi-instance.md).

## Probes and the management listener

The kubelet probes `/livez` and `/readyz` with `httpGet` on the container port named `probe`. The management listener stays on `127.0.0.1:9901` inside the pod. It is not a container port and is not part of the Service. To reach it, forward the port from a deployment and then query it from your machine:

```sh
kubectl port-forward deployment/gateway-microgateway 9901:9901
```

Setting `values.metrics.enabled` to `true` serves `/metrics` on the probe port without a token, so that Prometheus can scrape the pod IP. Enable it only on a pod network that you trust. See the [probe listener](operations.md#probe-listener) for the details.

## Shutdown timing

The chart keeps this inequality true:

```text
shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds
```

`shutdown_delay` and `drain_timeout` are the `global` keys of the rendered configuration, set from `values.shutdown.delay` and `values.shutdown.drainTimeout`, and `terminationGracePeriodSeconds` comes from `values.shutdown.terminationGracePeriodSeconds`. A render that breaks the rule fails with an error that names it by the values: `delay + drainTimeout + 5s <= terminationGracePeriodSeconds`.

When a pod is deleted, the kubelet sends SIGTERM while the endpoint removal is still spreading to the load balancers. During `shutdown_delay` the gateway reports not ready and keeps serving requests, so traffic moves away from the pod before the listeners stop. After that, in-flight requests get up to `drain_timeout` to finish. The 5 s margin covers closing the gateway and the process exit. Keep at least that margin.

The image is distroless and has no shell, so the chart has no exec `preStop` hook and no `lifecycle` block.

Worked check with the defaults: `10s + 20s + 5s = 35s`, which is at most the default grace period of `40s`.

The render fails when `values.shutdown.delay` is `0s`, or when the sum is greater than `values.shutdown.terminationGracePeriodSeconds`. Durations are written as whole `h`, `m`, `s` or `ms` parts, such as `10s` or `1m30s`.

## Configuration updates

Edit `values.config` and run `helm upgrade`. The ConfigMap changes, and the gateway reloads the new file when the kubelet updates the mounted volume. This can take a minute or more, and no rollout is needed. An invalid configuration is logged, and the previous configuration keeps serving.

Some settings need a restart, such as listener and admin addresses. For those, run `kubectl rollout restart deployment/<name>`.

### Why the chart does not use subPath

The chart mounts the directory, not a `subPath`, because a `subPath` mount never receives updates.

Changes to `values.env`, including a rotated value in the Secret, need a restart as well.

## Security context

The pod and the container run with these settings:

- The pod runs as non-root, with user and group ID `65532`.
- The pod sets the seccomp profile to `RuntimeDefault`.
- The container disallows privilege escalation.
- The container uses a read-only root filesystem.
- The container drops all Linux capabilities.

## Sizing

The defaults request `100m` of CPU and `64Mi` of memory, and limit memory to `256Mi`. The chart sets no CPU limit. The Go runtime sets `GOMAXPROCS` from the container CPU limit when one is set, and from the CPUs of the node otherwise.

To keep the Go heap below the memory limit, set `GOMEMLIMIT` a little under `values.resources.limits.memory` through `values.env`:

```yaml
env:
  - name: GOMEMLIMIT
    value: 230MiB
```
