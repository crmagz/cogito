#!/usr/bin/env bash
# Install the local Grafana OSS platform as separate native Helm releases.
set -euo pipefail

mode="${1:-install}"
phase="${2:-all}"
if [[ "$mode" != "install" && "$mode" != "render" ]]; then
  echo "usage: $0 [install|render] [logs|traces|metrics|all]" >&2
  exit 2
fi
case "$phase" in logs|traces|metrics|all) ;; *) echo "unknown phase: $phase" >&2; exit 2;; esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
values="$root/deploy/observability/kind/values"
context_args=()
kubectl_context_args=()
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  context_args=(--kube-context "$KUBE_CONTEXT")
  kubectl_context_args=(--context "$KUBE_CONTEXT")
fi

install_component() {
  local release="$1" namespace="$2" chart="$3" version="$4" values_file="$5"
  if [[ "$mode" == "render" ]]; then
    helm template "$release" "$chart" --version "$version" --namespace "$namespace" -f "$values_file"
    return
  fi
  helm upgrade --install "$release" "$chart" --version "$version" \
    --namespace "$namespace" --create-namespace -f "$values_file" \
    --wait --timeout 10m "${context_args[@]}"
}

wait_for_loki_ingestion() {
  if [[ "$mode" == "render" ]]; then return; fi
  # The Loki image is intentionally distroless, so use Kubernetes readiness
  # rather than trying to exec an HTTP client in the container.  Alloy starts
  # only after the single-binary ingester advertises itself Ready; otherwise
  # startup batches can be rejected while the ingester is shutting down.
  kubectl "${kubectl_context_args[@]}" -n loki wait \
    --for=condition=Ready pod -l app.kubernetes.io/name=loki --timeout=10m
}

bootstrap_kind_buckets() {
  local minio_pod
  minio_pod="$(kubectl "${kubectl_context_args[@]}" -n cogito get pod -l app=minio,release=cogito -o jsonpath='{.items[0].metadata.name}')"
  if [[ -z "$minio_pod" ]]; then
    echo "Cogito MinIO pod is required before installing observability" >&2
    exit 1
  fi
  kubectl "${kubectl_context_args[@]}" -n cogito exec "$minio_pod" -- sh -ec '
    export MC_CONFIG_DIR=/tmp/cogito-observability-mc
    trap "rm -rf \"$MC_CONFIG_DIR\"" EXIT
    mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
    for bucket in loki tempo mimir-blocks mimir-ruler mimir-alertmanager; do
      mc mb --ignore-existing "local/$bucket" >/dev/null
    done
  '
}

# These are intentionally distinct releases. Helm dependencies always inherit
# their parent's namespace, while this composition preserves component names
# and gives each service its own namespace without name overrides.
if [[ "$phase" == "logs" || "$phase" == "all" ]]; then
  if [[ "$mode" == "install" ]]; then bootstrap_kind_buckets; fi
  install_component loki loki grafana-community/loki 18.11.2 "$values/loki.yaml"
  wait_for_loki_ingestion
  install_component alloy alloy grafana/alloy 1.11.1 "$values/alloy.yaml"
fi
if [[ "$phase" == "traces" || "$phase" == "all" ]]; then
  install_component tempo tempo grafana-community/tempo 2.2.4 "$values/tempo.yaml"
fi
if [[ "$phase" == "metrics" || "$phase" == "all" ]]; then
  install_component mimir mimir grafana/mimir-distributed 6.2.0 "$values/mimir.yaml"
  install_component grafana grafana grafana/grafana 10.5.15 "$values/grafana.yaml"
fi
