from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from cogito_api.main import create_app
from cogito_api.models import AiPlan, McpToolGrant, McpToolSelection
from cogito_api.supervisor import ApprovalConflictError, _canonical_mcp_selection
from cogito_api.outbox import PlanApprovalOutboxDispatcher

from .conftest import make_settings
from .fakes import FakePlanner, FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore
from .test_planning_runs import _planning_request


def _awaiting_plan(client: TestClient, valid_plan: dict) -> tuple[str, str]:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    planned = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert planned.status_code == 200
    return run_id, planned.json()["plan_artifact"]["sha256"]


def _headers() -> dict[str, str]:
    return {"Authorization": "Bearer operator-test-token", "Idempotency-Key": "approval-1"}


def _mcp_client(valid_plan: dict) -> tuple[TestClient, FakeRunStarter, InMemorySupervisorStore]:
    """Build an approval client with the governed MCP policy enabled."""

    starter = FakeRunStarter()
    supervisor_store = InMemorySupervisorStore()
    client = TestClient(
        create_app(
            store=InMemoryPlanStore(),
            settings=make_settings(mcp_enabled=True),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    return client, starter, supervisor_store


def _developer_mcp_selection(starter: FakeRunStarter) -> dict[str, str]:
    developer = next(item for item in starter.started_runs[0].registry_resolutions if item.role == "developer")
    grant = developer.mcp_grants[0]
    return {
        "role": "developer",
        "server_id": grant.server_id,
        "server_version": grant.server_version,
        "server_manifest_sha256": grant.server_manifest_sha256,
        "tool_name": grant.tool_name,
        "input_schema_sha256": grant.input_schema_sha256,
    }


def test_approval_requires_authenticated_operator(client: TestClient, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Authorization": "Bearer invalid-token", "Idempotency-Key": "approval-1"},
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
                starter.started_runs[0].workflow_id,
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


def test_approved_mcp_selection_is_persisted_and_delivered_as_an_exact_subset(valid_plan: dict) -> None:
    client, starter, supervisor_store = _mcp_client(valid_plan)
    run_id, digest = _awaiting_plan(client, valid_plan)
    selection = _developer_mcp_selection(starter)

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [selection]},
        headers=_headers(),
    )

    assert response.status_code == 202
    assert response.json()["mcp_selection"] == [selection]
    assert starter.plan_approvals[-1][1]["mcp_selection"] == [selection]
    persisted = next(item for item in supervisor_store.approvals.values() if item.run_id == run_id)
    assert [item.model_dump(mode="json") for item in persisted.mcp_selection or []] == [selection]


def test_plan_approval_rejects_mcp_selection_outside_the_pinned_policy_grants(valid_plan: dict) -> None:
    client, starter, supervisor_store = _mcp_client(valid_plan)
    run_id, digest = _awaiting_plan(client, valid_plan)
    expansion = _developer_mcp_selection(starter) | {"tool_name": "catalog_delete"}

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [expansion]},
        headers=_headers(),
    )

    assert response.status_code == 409
    assert "not a subset" in response.json()["detail"]
    assert not supervisor_store.approvals
    assert not supervisor_store.outbox


async def test_mcp_selection_retry_reuses_the_persisted_canonical_payload(valid_plan: dict) -> None:
    client, starter, supervisor_store = _mcp_client(valid_plan)
    run_id, digest = _awaiting_plan(client, valid_plan)
    selection = _developer_mcp_selection(starter)
    starter.approval_error = ConnectionError("Temporal temporarily unavailable")

    first = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [selection]},
        headers=_headers(),
    )
    starter.approval_error = None
    delivered = await PlanApprovalOutboxDispatcher(supervisor_store, starter).deliver_once()

    assert first.status_code == 202
    assert delivered == {first.json()["decision_id"]}
    assert starter.plan_approvals[-1][1]["mcp_selection"] == [selection]


def test_plan_approval_rejects_a_second_selection_before_the_first_delivery(valid_plan: dict) -> None:
    client, starter, supervisor_store = _mcp_client(valid_plan)
    run_id, digest = _awaiting_plan(client, valid_plan)
    starter.approval_error = ConnectionError("Temporal temporarily unavailable")
    selection = _developer_mcp_selection(starter)

    first = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": []},
        headers=_headers(),
    )
    second = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [selection]},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "approval-2"},
    )

    assert first.status_code == 202
    assert first.json()["delivered"] is False
    assert second.status_code == 409
    assert "already recorded" in second.json()["detail"]
    assert len(supervisor_store.approvals) == 1


def test_plan_approval_rejects_a_pinned_non_developer_mcp_selection(valid_plan: dict) -> None:
    client, starter, supervisor_store = _mcp_client(valid_plan)
    run_id, digest = _awaiting_plan(client, valid_plan)
    developer = next(item for item in starter.started_runs[0].registry_resolutions if item.role == "developer")
    grant = developer.mcp_grants[0]
    supervisor_store.run_mcp_tool_resolutions[(run_id, "reviewer")] = [
        McpToolGrant(
            server_id=grant.server_id,
            server_version=grant.server_version,
            server_manifest_sha256=grant.server_manifest_sha256,
            tool_name=grant.tool_name,
            input_schema_sha256=grant.input_schema_sha256,
        )
    ]
    reviewer_selection = _developer_mcp_selection(starter) | {"role": "reviewer"}

    response = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [reviewer_selection]},
        headers=_headers(),
    )

    assert response.status_code == 409
    assert "not a subset" in response.json()["detail"]


def test_store_selection_canonicalization_rejects_duplicates_and_sorts() -> None:
    first = McpToolSelection(
        role="developer",
        server_id="cogito_readonly_mcp",
        server_version="1.0.1",
        server_manifest_sha256="a" * 64,
        tool_name="catalog_read",
        input_schema_sha256="b" * 64,
    )
    second = first.model_copy(update={"tool_name": "catalog_list"})

    assert _canonical_mcp_selection([first, second]) == [second, first]
    with pytest.raises(ApprovalConflictError, match="must be unique"):
        _canonical_mcp_selection([first, first])
