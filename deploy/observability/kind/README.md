# Kind Grafana OSS platform

This is a Kubernetes-native composition, not a Helm umbrella dependency.
Helm dependencies always render into the parent's release namespace, so each
observability component is instead installed as its own normally named release:

| Release | Namespace | Service used by Cogito |
| --- | --- | --- |
| `alloy` | `alloy` | `alloy.alloy.svc.cluster.local:4318` |
| `loki` | `loki` | `loki.loki.svc.cluster.local:3100` |
| `tempo` | `tempo` | `tempo.tempo.svc.cluster.local:4317` |
| `mimir` | `mimir` | `mimir-gateway.mimir.svc.cluster.local` |
| `grafana` | `grafana` | `grafana.grafana.svc.cluster.local` |

The profile is ClusterIP-only: it requires neither an Ingress nor a gateway.
It uses the existing local `cogito-minio` Service for S3-compatible storage.
The logs phase creates its `loki`, `tempo`, `mimir-blocks`, `mimir-ruler`, and
`mimir-alertmanager` buckets idempotently through the local MinIO pod.

First deploy Cogito with its Kind connection values, then install the platform:

```sh
helm upgrade --install cogito charts/ \
  -f charts/values.yaml \
  -f charts/values-kind-observability.yaml \
  --namespace cogito --create-namespace

KUBE_CONTEXT=kind-cogito-observability \
  deploy/observability/kind/install.sh install logs

KUBE_CONTEXT=kind-cogito-observability \
  deploy/observability/kind/install.sh install traces

KUBE_CONTEXT=kind-cogito-observability \
  deploy/observability/kind/install.sh install metrics
```

Use `all` in place of a phase to install or render the complete platform.
For a render-only validation, run `deploy/observability/kind/install.sh render all`.
Open Grafana locally without an Ingress controller:

```sh
kubectl -n grafana port-forward service/grafana 3000:80
```
