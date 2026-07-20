#!/usr/bin/env bash
# Run the Phase 8 acceptance path against an already deployed local kind release.
# The target repository must be disposable and pinned to an immutable commit.

set -euo pipefail

namespace="${COGITO_E2E_NAMESPACE:-cogito}"
execution_namespace="${COGITO_E2E_EXECUTION_NAMESPACE:-cogito-executions}"
release="${COGITO_E2E_RELEASE:-cogito}"
target_repo="${COGITO_E2E_TARGET_REPO:?Set COGITO_E2E_TARGET_REPO to an HTTPS repository pinned with #<commit-sha>.}"
spec_ref="${COGITO_E2E_SPEC_REF:?Set COGITO_E2E_SPEC_REF to an existing immutable spec-set reference.}"
expected_status="${COGITO_E2E_EXPECTED_STATUS:-completed}"
timeout_seconds="${COGITO_E2E_TIMEOUT_SECONDS:-300}"

case "$expected_status" in
  completed|stopped_with_backup|failed) ;;
  *) echo "COGITO_E2E_EXPECTED_STATUS must be completed, stopped_with_backup, or failed" >&2; exit 2 ;;
esac

require_command() {
  command -v "$1" >/dev/null || { echo "required command is unavailable: $1" >&2; exit 2; }
}

require_command jq
require_command kubectl
require_command shasum

kubectl config current-context | grep -qx 'kind-cogito' || {
  echo "refusing to run outside the kind-cogito context" >&2
  exit 2
}

kubectl -n "$namespace" rollout status "deployment/${release}-api" --timeout=120s
kubectl -n "$namespace" rollout status "deployment/${release}-worker" --timeout=120s
kubectl auth can-i get pods --subresource=exec \
  --as="system:serviceaccount:${namespace}:${release}-worker" -n "$execution_namespace" | grep -qx yes
kubectl auth can-i create secrets \
  --as="system:serviceaccount:${namespace}:${release}-worker" -n "$execution_namespace" | grep -qx yes

# The source credential belongs only in the control namespace. The worker
# creates an individually labelled, short-lived copy for each execution run.
source_git_secret="$(kubectl -n "$namespace" get configmap "${release}-worker-config" -o jsonpath='{.data.COGITO_EXECUTION_GIT_CREDENTIALS_SECRET}')"
test -n "$source_git_secret" || {
  echo "worker config does not name the source Git credential Secret" >&2
  exit 1
}
if kubectl -n "$execution_namespace" get secret "$source_git_secret" >/dev/null 2>&1; then
  echo "refusing E2E: a long-lived Git credential exists in the execution namespace" >&2
  exit 1
fi

marker=".cogito-kind-e2e-${RANDOM}-${RANDOM}"
payload="$(jq -n \
  --arg target_repo "$target_repo" \
  --arg spec_ref "$spec_ref" \
  --arg marker "$marker" \
  '{plan: {
    title: "Kind Phase 8 E2E",
    summary: "Validate ordered multi-phase execution, limits, durable status, and cleanup.",
    target_repos: [$target_repo],
    spec_set: $spec_ref,
    phases: [
      {
        id: "phase-1",
        name: "Create first E2E marker",
        description: "Create and commit the first requested marker file on the feature branch.",
        tasks: [("Create " + $marker + " containing phase-1, then commit it on the feature branch.")],
        acceptance_criteria: [("The committed feature branch contains " + $marker + ".")],
        verification: [("test -f " + $marker)],
        depends_on: []
      },
      {
        id: "phase-2",
        name: "Create dependent E2E marker",
        description: "After phase 1, update the same marker file and commit the change.",
        tasks: [("Append phase-2 to " + $marker + ", then commit it on the feature branch.")],
        acceptance_criteria: [("The committed feature branch contains phase-1 and phase-2 in " + $marker + ".")],
        verification: [("grep -qx phase-1 " + $marker), ("grep -qx phase-2 " + $marker)],
        depends_on: ["phase-1"]
      }
    ],
    constraints: {
      max_wall_clock_minutes: 5,
      max_cost_usd: 1.0,
      max_review_rounds: 1,
      max_turns_per_phase: 50,
      backup_reserve_turns: 20
    },
    review_profile: "minimal"
  }}')"

submit_command='import os,sys,urllib.request; payload=sys.stdin.buffer.read(); request=urllib.request.Request("http://127.0.0.1:8000/api/v1/runs", data=payload, headers={"Authorization":"Bearer "+os.environ["COGITO_AUTH_STATIC_TOKEN"],"Content-Type":"application/json"}); print(urllib.request.urlopen(request, timeout=30).read().decode())'
response="$(printf '%s' "$payload" | kubectl -n "$namespace" exec -i "deployment/${release}-api" -- python -c "$submit_command")"
run_id="$(printf '%s' "$response" | jq -er '.run_id')"
deadline=$((SECONDS + timeout_seconds))
status=""

while (( SECONDS < deadline )); do
  status_response="$(kubectl -n "$namespace" exec "deployment/${release}-api" -- python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/api/v1/runs/${run_id}/status', timeout=15).read().decode())")"
  status="$(printf '%s' "$status_response" | jq -er '.status')"
  case "$status" in
    completed|stopped_with_backup|failed) break ;;
  esac
  sleep 5
done

test "$status" = "$expected_status" || {
  printf '%s\n' "$status_response" >&2
  echo "expected terminal status ${expected_status}, got ${status}" >&2
  exit 1
}

case "$expected_status" in
  completed)
    test "$(printf '%s' "$status_response" | jq -c '.completed_phase_ids')" = '["phase-1","phase-2"]' || {
      printf '%s\n' "$status_response" >&2
      echo "completed run did not record ordered phase completion" >&2
      exit 1
    }
    ;;
  stopped_with_backup)
    printf '%s' "$status_response" | jq -e '.stopped_phase_id and .ceiling' >/dev/null || {
      printf '%s\n' "$status_response" >&2
      echo "backup stop did not record the stopped phase and ceiling" >&2
      exit 1
    }
    ;;
  failed)
    # This mode deliberately uses a non-publishing credential. It must reach
    # the Git publish boundary; an earlier workflow or infrastructure error is
    # not valid failure-path evidence.
    printf '%s' "$status_response" | jq -e '.failure_detail | strings | contains("could not publish feature branch")' >/dev/null || {
      printf '%s\n' "$status_response" >&2
      echo "failed run did not fail closed at the expected Git publish boundary" >&2
      exit 1
    }
    ;;
esac

run_hash="$(printf %s "$run_id" | shasum -a 256 | cut -c1-20)"
if kubectl -n "$execution_namespace" get jobs,pods,secrets -l "cogito.dev/run-hash=${run_hash}" -o name | grep -q .; then
  echo "execution resources leaked for run ${run_id}" >&2
  exit 1
fi

printf 'Phase 8 Kind E2E passed: run_id=%s status=%s marker=%s\n' "$run_id" "$status" "$marker"
