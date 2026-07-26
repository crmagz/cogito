#!/usr/bin/env bash
# Exercise the approval-gated registry-native flow against a disposable Kind fixture.

set -euo pipefail

context="${COGITO_E2E_CONTEXT:-kind-cogito-observability}"
namespace="${COGITO_E2E_NAMESPACE:-cogito}"
execution_namespace="${COGITO_E2E_EXECUTION_NAMESPACE:-cogito-executions}"
release="${COGITO_E2E_RELEASE:-cogito}"
target_repo="${COGITO_E2E_TARGET_REPO:-https://github.com/crmagz/cogito-kind-e2e-fixture.git#7d1ddc14c1cbaf666641c7235c89fa937bb1bd50}"
spec_ref="${COGITO_E2E_SPEC_REF:?Set COGITO_E2E_SPEC_REF to an immutable spec-set reference.}"
timeout_seconds="${COGITO_E2E_TIMEOUT_SECONDS:-900}"

require_command() {
  command -v "$1" >/dev/null || { echo "required command is unavailable: $1" >&2; exit 2; }
}

require_command jq
require_command kubectl
require_command shasum

command kubectl config get-contexts -o name | grep -qx "$context" || {
  echo "configured Kubernetes context not found: ${context}" >&2
  exit 2
}

kubectl() {
  command kubectl --context "$context" "$@"
}

api_post() {
  local path="$1"
  local authorize="$2"
  local code='import os,sys,urllib.request; payload=sys.stdin.buffer.read(); headers={"Content-Type":"application/json"}; headers.update({"Authorization":"Bearer "+os.environ["COGITO_AUTH_STATIC_TOKEN"],"Idempotency-Key":"phase12"+sys.argv[1]} if sys.argv[2] == "true" else {}); req=urllib.request.Request("http://127.0.0.1:8000"+sys.argv[1], data=payload, headers=headers, method="POST"); print(urllib.request.urlopen(req, timeout=90).read().decode())'
  kubectl -n "$namespace" exec -i "deployment/${release}-api" -- python -c "$code" "$path" "$authorize"
}

api_get() {
  local path="$1"
  local code='import sys,urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8000"+sys.argv[1], timeout=30).read().decode())'
  kubectl -n "$namespace" exec "deployment/${release}-api" -- python -c "$code" "$path"
}

read_snapshot() {
  local reference="$1"
  local code='import os,sys
from minio import Minio
from urllib.parse import urlparse
parsed=urlparse(sys.argv[1])
client=Minio(os.environ["MINIO_ENDPOINT"], access_key=os.environ["MINIO_ACCESS_KEY"], secret_key=os.environ["MINIO_SECRET_KEY"], secure=os.environ.get("MINIO_SECURE", "false").lower()=="true")
response=client.get_object(parsed.netloc, parsed.path.lstrip("/"))
try: print(response.read().decode())
finally: response.close(); response.release_conn()'
  kubectl -n "$namespace" exec "deployment/${release}-api" -- python -c "$code" "$reference"
}

registry_roles() {
  local run_id="$1"
  local code='PGPASSWORD="$(cat "$POSTGRES_PASSWORD_FILE")" psql -U postgres -d cogito -At -F "|" -c "SELECT role, registration_id, registration_version, manifest_sha256 FROM run_registration_resolutions WHERE run_id = '\''$1'\'' ORDER BY role"'
  kubectl -n "$namespace" exec statefulset/cogito-postgresql -- sh -ec "$code" -- "$run_id"
}

wait_for_planning_status() {
  local run_id="$1"
  local expected="$2"
  local deadline=$((SECONDS + timeout_seconds))
  local response
  while (( SECONDS < deadline )); do
    response="$(api_get "/api/v1/planning-runs/${run_id}")"
    if test "$(printf '%s' "$response" | jq -r '.status')" = "$expected"; then
      printf '%s\n' "$response"
      return
    fi
    sleep 5
  done
  printf '%s\n' "${response:-no planning status response}" >&2
  echo "timed out waiting for planning status ${expected}" >&2
  exit 1
}

kubectl -n "$namespace" rollout status "deployment/${release}-api" --timeout=180s
kubectl -n "$namespace" rollout status "deployment/${release}-worker" --timeout=180s
kubectl -n "$namespace" rollout status "deployment/${release}-litellm" --timeout=240s

marker=".cogito-phase12-${RANDOM}-${RANDOM}"
submission="$(jq -n --arg repo "$target_repo" --arg spec_ref "$spec_ref" --arg marker "$marker" '{
  initial_specification: ("Create exactly one implementation phase. In the pinned target repository, create a file named " + $marker + " containing exactly phase-12. Commit it to the feature branch. Use exactly one verification command: test -f " + $marker + ". Do not create more phases."),
  target_repos: [$repo],
  spec_set: $spec_ref,
  constraints: {max_wall_clock_minutes: 8, max_cost_usd: 3.0, max_review_rounds: 1, max_turns_per_phase: 50, backup_reserve_turns: 20},
  priority: "normal"
}')"

planning="$(printf '%s' "$submission" | api_post /api/v1/planning-runs false)"
run_id="$(printf '%s' "$planning" | jq -er '.run_id')"
generated="$(printf '{}' | api_post "/api/v1/planning-runs/${run_id}/generate-plan" false)"
plan_sha256="$(printf '%s' "$generated" | jq -er '.plan_artifact.sha256')"
plan_ref="$(printf '%s' "$generated" | jq -er '.plan_artifact.ref')"

awaiting_plan="$(wait_for_planning_status "$run_id" awaiting_plan_approval)"
test "$(printf '%s' "$awaiting_plan" | jq -r '.plan_artifact.sha256')" = "$plan_sha256"
plan="$(read_snapshot "$plan_ref")"
printf '%s' "$plan" | jq -e --arg marker "$marker" '
  (.phases | length) == 1 and
  (.phases[0].tasks | join(" ") | contains($marker)) and
  (.phases[0].verification | length) == 1
' >/dev/null

roles_before_approval="$(registry_roles "$run_id")"
test "$(printf '%s\n' "$roles_before_approval" | wc -l | tr -d ' ')" = 6
printf '%s\n' "$roles_before_approval" | awk -F '|' 'NF != 4 || length($4) != 64 { exit 1 }'

plan_approval="$(jq -n --arg sha "$plan_sha256" '{decision:"approve", artifact_sha256:$sha}')"
printf '%s' "$plan_approval" | api_post "/api/v1/runs/${run_id}/approvals/plan" true >/dev/null
awaiting_implementation="$(wait_for_planning_status "$run_id" awaiting_implementation_approval)"
implementation_sha256="$(printf '%s' "$awaiting_implementation" | jq -er '.implementation_artifact.sha256')"
implementation_ref="$(printf '%s' "$awaiting_implementation" | jq -er '.implementation_artifact.ref')"
implementation="$(read_snapshot "$implementation_ref")"
printf '%s' "$implementation" | jq -e '
  .validation.status == "passed" and
  (.registry_resolutions | map(.role) | sort) == ["developer", "ephemeral_environment_tester", "planner", "pull_request_publisher", "reviewer", "validator"]
' >/dev/null

implementation_approval="$(jq -n --arg sha "$implementation_sha256" '{decision:"approve", artifact_sha256:$sha}')"
first_approval="$(printf '%s' "$implementation_approval" | api_post "/api/v1/runs/${run_id}/approvals/implementation" true)"
second_approval="$(printf '%s' "$implementation_approval" | api_post "/api/v1/runs/${run_id}/approvals/implementation" true)"
test "$(printf '%s' "$first_approval" | jq -r '.decision_id')" = "$(printf '%s' "$second_approval" | jq -r '.decision_id')"
completed="$(wait_for_planning_status "$run_id" completed)"
status="$(api_get "/api/v1/runs/${run_id}/status")"
printf '%s' "$status" | jq -e '
  .execution_status == "completed" and
  (.pull_request.number | type == "number") and
  (.pull_request.url | strings | startswith("https://github.com/"))
' >/dev/null

run_hash="$(printf %s "$run_id" | shasum -a 256 | cut -c1-20)"
if kubectl -n "$execution_namespace" get jobs,pods,secrets -l "cogito.dev/run-hash=${run_hash}" -o name | grep -q .; then
  echo "execution resources leaked for run ${run_id}" >&2
  exit 1
fi

printf 'Phase 12 Kind E2E passed: run_id=%s plan_sha256=%s implementation_sha256=%s pr=%s marker=%s\n' \
  "$run_id" "$plan_sha256" "$implementation_sha256" "$(printf '%s' "$status" | jq -r '.pull_request.url')" "$marker"
