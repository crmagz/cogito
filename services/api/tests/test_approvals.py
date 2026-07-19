from __future__ import annotations

from fastapi.testclient import TestClient

from cogito_api.outbox import PlanApprovalOutboxDispatcher

from .fakes import FakeRunStarter, InMemorySupervisorStore
from .test_planning_runs import _planning_request


def _awaiting_plan(client: TestClient, valid_plan: dict) -> tuple[str, str]:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    planned = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert planned.status_code == 200
    return run_id, planned.json()["plan_artifact"]["sha256"]


def _headers() -> dict[str, str]:
    return {"Authorization": "Bearer operator-test-token", "Idempotency-Key": "approval-1"}


def test_approval_requires_authenticated_operator(client: TestClient, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Idempotency-Key": "approval-1"},
    )

    assert response.status_code == 401


def test_matching_approval_is_audited_and_delivered_to_temporal(
    client: TestClient,
    valid_plan: dict,
    starter: FakeRunStarter,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=_headers(),
    )

    assert response.status_code == 202
    body = response.json()
    assert body["actor_id"] == "test-operator"
    assert body["delivered"] is True
    assert starter.plan_approvals == [
        (
            run_id,
            {"decision_id": body["decision_id"], "artifact_sha256": digest, "decision": "approve"},
        )
    ]
    assert supervisor_store.planning_runs[run_id].status.value == "implementing"


def test_stale_plan_digest_is_rejected_without_temporal_delivery(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": "0" * 64},
        headers=_headers(),
    )

    assert response.status_code == 409
    assert starter.plan_approvals == []


def test_rejection_requires_comment(client: TestClient, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "reject", "artifact_sha256": digest},
        headers=_headers(),
    )

    assert response.status_code == 422


def test_replayed_approval_is_idempotent(client: TestClient, valid_plan: dict, starter: FakeRunStarter) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    request = {"decision": "approve", "artifact_sha256": digest}

    first = client.post(f"/api/v1/runs/{run_id}/approvals/plan", json=request, headers=_headers())
    second = client.post(f"/api/v1/runs/{run_id}/approvals/plan", json=request, headers=_headers())

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["decision_id"] == first.json()["decision_id"]
    assert len(starter.plan_approvals) == 1


def test_idempotency_key_cannot_authorize_a_different_decision(client: TestClient, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    first = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=_headers(),
    )
    conflicting = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "reject", "artifact_sha256": digest, "comment": "different decision"},
        headers=_headers(),
    )

    assert first.status_code == 202
    assert conflicting.status_code == 409


async def test_persisted_approval_is_retried_after_temporal_delivery_failure(
    client: TestClient,
    valid_plan: dict,
    starter: FakeRunStarter,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    starter.approval_error = ConnectionError("Temporal temporarily unavailable")

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=_headers(),
    )

    assert response.status_code == 202
    assert response.json()["delivered"] is False
    assert len(supervisor_store.outbox) == 1
    starter.approval_error = None
    dispatcher = PlanApprovalOutboxDispatcher(supervisor_store, starter)

    delivered = await dispatcher.deliver_once()

    assert delivered == {response.json()["decision_id"]}
    assert supervisor_store.planning_runs[run_id].status.value == "implementing"
    assert supervisor_store.outbox == {}
