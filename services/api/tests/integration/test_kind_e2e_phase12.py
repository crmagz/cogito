"""Native pytest port of the Phase 12 approval-gated registry E2E test."""

from __future__ import annotations

import os
import secrets

import pytest

from .kind_helpers import KindHarness


pytestmark = pytest.mark.kind_e2e


def test_phase12_approval_gated_registry_e2e() -> None:
    harness = KindHarness.from_environment(default_context="kind-cogito-observability")
    spec_ref = os.environ.get("COGITO_E2E_SPEC_REF")
    if not spec_ref:
        pytest.fail("COGITO_E2E_SPEC_REF is required")
    target_repo = os.environ.get("COGITO_E2E_TARGET_REPO", "https://github.com/crmagz/cogito-kind-e2e-fixture.git#7d1ddc14c1cbaf666641c7235c89fa937bb1bd50")
    harness.assert_context()
    for deployment in ("api", "worker", "litellm"):
        harness.kubectl("-n", harness.namespace, "rollout", "status", f"deployment/{harness.release}-{deployment}", "--timeout=240s")
    marker = f".cogito-phase12-{secrets.token_hex(6)}"
    payload = {"initial_specification": f"Create exactly one implementation phase. Create {marker} containing phase-12, commit it, and verify only with test -f {marker}.", "target_repos": [target_repo], "spec_set": spec_ref, "constraints": {"max_wall_clock_minutes": 8, "max_cost_usd": 3.0, "max_review_rounds": 1, "max_turns_per_phase": 50, "backup_reserve_turns": 20}, "priority": "normal"}
    status, planning = harness.api("POST", "/api/v1/planning-runs", payload)
    assert status == 202, planning
    run_id = str(planning["run_id"])
    status, generated = harness.api("POST", f"/api/v1/planning-runs/{run_id}/generate-plan", {})
    assert status == 202, generated
    plan_sha = str(dict(generated["plan_artifact"])["sha256"])
    plan = harness.wait_for(f"/api/v1/planning-runs/{run_id}", "awaiting_plan_approval")
    assert str(dict(plan["plan_artifact"])["sha256"]) == plan_sha
    snapshot = harness.snapshot(str(dict(plan["plan_artifact"])["ref"]))
    assert len(snapshot["phases"]) == 1 and marker in " ".join(snapshot["phases"][0]["tasks"])
    roles = harness.registry_roles(run_id)
    assert len(roles) == 6
    assert all(len(role) == 4 and len(role[3]) == 64 for role in roles)
    status, _ = harness.api("POST", f"/api/v1/runs/{run_id}/approvals/plan", {"decision": "approve", "artifact_sha256": plan_sha}, authenticated=True)
    assert status == 202
    implementation = harness.wait_for(f"/api/v1/planning-runs/{run_id}", "awaiting_implementation_approval")
    implementation_artifact = dict(implementation["implementation_artifact"])
    evidence = harness.snapshot(str(implementation_artifact["ref"]))
    assert evidence["validation"]["status"] == "passed"
    approval = {"decision": "approve", "artifact_sha256": str(implementation_artifact["sha256"])}
    first_status, first = harness.api("POST", f"/api/v1/runs/{run_id}/approvals/implementation", approval, authenticated=True)
    second_status, second = harness.api("POST", f"/api/v1/runs/{run_id}/approvals/implementation", approval, authenticated=True)
    assert first_status == second_status == 202 and first["decision_id"] == second["decision_id"]
    completed = harness.wait_for(f"/api/v1/planning-runs/{run_id}", "completed")
    assert completed["status"] == "completed"
    status, execution = harness.api("GET", f"/api/v1/runs/{run_id}/status")
    assert status == 200 and execution["execution_status"] == "completed"
    assert isinstance(dict(execution["pull_request"])["number"], int)
    assert str(dict(execution["pull_request"])["url"]).startswith("https://github.com/")
    harness.assert_no_execution_resources(run_id)
