from __future__ import annotations

from cogito_api.models import ArtifactReference, PlanningRunStatus

from .fakes import FakeRunStarter, InMemorySupervisorStore
from .test_approvals import _awaiting_plan, _headers


async def _awaiting_implementation(
    client, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> tuple[str, str]:
    run_id, plan_digest = _awaiting_plan(client, valid_plan)
    approved = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": plan_digest},
        headers=_headers(),
    )
    assert approved.status_code == 202
    artifact = ArtifactReference(ref=f"s3://plan-snapshots/runs/{run_id}/implementation/artifact.json", sha256="b" * 64)
    await supervisor_store.record_implementation_artifact(run_id, artifact)
    assert supervisor_store.planning_runs[run_id].status is PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL
    return run_id, artifact.sha256


async def test_matching_implementation_approval_is_delivered_once(
    client, valid_plan: dict, starter: FakeRunStarter, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id, digest = await _awaiting_implementation(client, valid_plan, supervisor_store)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/implementation",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "implementation-1"},
    )

    assert response.status_code == 202
    assert response.json()["delivered"] is True
    assert len(starter.implementation_approvals) == 1
    assert supervisor_store.planning_runs[run_id].status is PlanningRunStatus.FINALIZING


async def test_stale_implementation_approval_does_not_reach_temporal(
    client, valid_plan: dict, starter: FakeRunStarter, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id, _ = await _awaiting_implementation(client, valid_plan, supervisor_store)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/implementation",
        json={"decision": "approve", "artifact_sha256": "0" * 64},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "implementation-stale"},
    )

    assert response.status_code == 409
    assert starter.implementation_approvals == []


async def test_rejected_implementation_never_enters_finalizing(
    client, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id, digest = await _awaiting_implementation(client, valid_plan, supervisor_store)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/implementation",
        json={"decision": "reject", "artifact_sha256": digest, "comment": "Do not publish this change."},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "implementation-reject"},
    )

    assert response.status_code == 202
    assert supervisor_store.planning_runs[run_id].status is PlanningRunStatus.REJECTED
