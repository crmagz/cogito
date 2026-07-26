"""Native pytest port of the Phase 8 Kind execution acceptance test."""

from __future__ import annotations

import os
import secrets

import pytest

from .kind_helpers import KindHarness


pytestmark = pytest.mark.kind_e2e


def test_phase8_execution_e2e() -> None:
    harness = KindHarness.from_environment(default_context="kind-cogito")
    target_repo = os.environ.get("COGITO_E2E_TARGET_REPO")
    spec_ref = os.environ.get("COGITO_E2E_SPEC_REF")
    if not target_repo or not spec_ref:
        pytest.fail("COGITO_E2E_TARGET_REPO and COGITO_E2E_SPEC_REF are required")
    expected = os.environ.get("COGITO_E2E_EXPECTED_STATUS", "completed")
    assert expected in {"completed", "stopped_with_backup", "failed"}
    harness.assert_context()
    harness.kubectl("-n", harness.namespace, "rollout", "status", f"deployment/{harness.release}-api", "--timeout=120s")
    harness.kubectl("-n", harness.namespace, "rollout", "status", f"deployment/{harness.release}-worker", "--timeout=120s")
    for verb, resource in (("get", "pods"), ("create", "secrets")):
        assert harness.kubectl("auth", "can-i", verb, resource, "--as", f"system:serviceaccount:{harness.namespace}:{harness.release}-worker", "-n", harness.execution_namespace).strip() == "yes"
    source_secret = harness.kubectl("-n", harness.namespace, "get", "configmap", f"{harness.release}-worker-config", "-o", "jsonpath={.data.COGITO_EXECUTION_GIT_CREDENTIALS_SECRET}").strip()
    assert source_secret
    execution_secret = harness.kubectl("-n", harness.execution_namespace, "get", "secret", source_secret, "-o", "name", check=False)
    assert not execution_secret.strip(), "long-lived Git credential leaked into execution namespace"
    marker = f".cogito-kind-e2e-{secrets.token_hex(6)}"
    plan = {
        "title": "Kind Phase 8 E2E", "summary": "Validate ordered execution and cleanup.",
        "target_repos": [target_repo], "spec_set": spec_ref,
        "phases": [
            {"id": "phase-1", "name": "Create marker", "description": "Create the marker.", "tasks": [f"Create {marker} containing phase-1, then commit it."], "acceptance_criteria": [f"Feature branch contains {marker}."], "verification": [f"test -f {marker}"], "depends_on": []},
            {"id": "phase-2", "name": "Update marker", "description": "Append the dependent marker.", "tasks": [f"Append phase-2 to {marker}, then commit it."], "acceptance_criteria": [f"{marker} contains phase-1 and phase-2."], "verification": [f"grep -qx phase-1 {marker}", f"grep -qx phase-2 {marker}"], "depends_on": ["phase-1"]},
        ],
        "constraints": {"max_wall_clock_minutes": 5, "max_cost_usd": 1.0, "max_review_rounds": 1, "max_turns_per_phase": 50, "backup_reserve_turns": 20}, "review_profile": "minimal",
    }
    status, accepted = harness.api("POST", "/api/v1/runs", {"plan": plan})
    assert status == 202, accepted
    run_id = str(accepted["run_id"])
    terminal = harness.wait_for(f"/api/v1/runs/{run_id}/status", expected)
    if expected == "completed": assert terminal["completed_phase_ids"] == ["phase-1", "phase-2"]
    if expected == "stopped_with_backup": assert terminal.get("stopped_phase_id") and terminal.get("ceiling")
    if expected == "failed": assert "could not publish feature branch" in str(terminal.get("failure_detail", ""))
    harness.assert_no_execution_resources(run_id)
