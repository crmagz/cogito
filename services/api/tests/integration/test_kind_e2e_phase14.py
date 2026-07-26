"""Native Kind coverage for the scoped Workbench read boundary."""

from __future__ import annotations

import secrets

import pytest

from .kind_helpers import KindHarness


pytestmark = pytest.mark.kind_e2e


def test_phase14_workbench_e2e() -> None:
    """Deploy a source artifact then prove the Workbench can read only through its API."""

    harness = KindHarness.from_environment(default_context="kind-cogito-observability")
    harness.assert_context()
    harness.kubectl("-n", harness.namespace, "rollout", "status", f"deployment/{harness.release}-api", "--timeout=240s")
    marker = f"workbench-kind-{secrets.token_hex(6)}"
    submission = {
        "initial_specification": f"Record immutable source evidence for {marker}.",
        "target_repos": ["https://github.com/crmagz/cogito-kind-e2e-fixture.git#7d1ddc14c1cbaf666641c7235c89fa937bb1bd50"],
        "spec_set": "kind-workbench@v1#sha256=" + "a" * 64,
        "constraints": {
            "max_wall_clock_minutes": 8,
            "max_cost_usd": 3.0,
            "max_review_rounds": 1,
            "max_turns_per_phase": 50,
            "backup_reserve_turns": 20,
        },
        "priority": "normal",
    }

    status, created = harness.api("POST", "/api/v1/planning-runs", submission)
    assert status == 202, created
    run_id = str(created["run_id"])
    source_sha256 = str(dict(created["source_artifact"])["sha256"])
    unauthenticated, _ = harness.api("GET", "/api/v1/workbench/runs", authenticated=False)
    assert unauthenticated == 401
    listed, queue = harness.api("GET", "/api/v1/workbench/runs", authenticated=True)
    assert listed == 200, queue
    item = next(entry for entry in list(queue["items"]) if entry["run_id"] == run_id)
    assert item["project_id"] == "default"
    assert item["artifacts"] == [{"kind": "source", "sha256": source_sha256}]
    evidence_status, evidence = harness.api(
        "GET",
        f"/api/v1/workbench/runs/{run_id}/evidence/source?artifact_sha256={source_sha256}",
        authenticated=True,
    )
    assert evidence_status == 200, evidence
    assert marker in str(evidence["content"])
