"""Provider-neutral coordination API and authority-bound action coverage."""

from __future__ import annotations

from dataclasses import replace

from cogito_api.models import ArtifactReference, PlanningRunStatus

from .test_approvals import _awaiting_plan


def _headers(key: str = "coordination-action") -> dict[str, str]:
    return {"Authorization": "Bearer operator-test-token", "Idempotency-Key": key}


def test_coordination_detail_requires_existing_operator_auth(client, valid_plan: dict) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)

    response = client.get(
        f"/api/v1/planning-runs/{run_id}/coordination",
        headers={"Authorization": "Bearer invalid-token"},
    )

    assert response.status_code == 401


def test_coordination_detail_exposes_safe_plan_gate_event(client, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.get(f"/api/v1/planning-runs/{run_id}/coordination", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["active_gate"] == "plan"
    event = next(item for item in body["events"] if item["event_type"] == "plan_approval_requested")
    assert event["artifact"]["sha256"] == digest
    assert "comment" not in event
    assert "payload" not in event


def test_coordination_detail_exposes_only_the_stage_invocation_envelope(
    client, valid_plan: dict, supervisor_store
) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store._append_coordination_event(
        run_id,
        "stage_invocation_started",
        invocation={
            "invocation_id": "a" * 64,
            "source": "worker_phase",
            "stage_id": "implement-api",
            "role": "developer",
            "attempt": 1,
            "trace_context_available": True,
            "unexpected": "must not be exposed",
        },
    )

    response = client.get(f"/api/v1/planning-runs/{run_id}/coordination", headers=_headers())

    assert response.status_code == 200
    event = next(item for item in response.json()["events"] if item["event_type"] == "stage_invocation_started")
    assert event["invocation"] == {
        "invocation_id": "a" * 64,
        "source": "worker_phase",
        "stage_id": "implement-api",
        "role": "developer",
        "attempt": 1,
        "trace_context_available": True,
    }


def test_coordination_detail_exposes_only_the_aggregate_mcp_envelope(
    client, valid_plan: dict, supervisor_store
) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store._append_coordination_event(
        run_id,
        "mcp_invocation_observed",
        mcp_invocation={
            "invocation_id": "a" * 64,
            "server_id": "readonly",
            "server_version": "1.0.0",
            "server_manifest_sha256": "b" * 64,
            "tool_name": "catalog_read",
            "input_schema_sha256": "c" * 64,
            "outcome": "success",
            "invocation_count": 2,
            "request_body": "must not be exposed",
        },
    )

    response = client.get(f"/api/v1/planning-runs/{run_id}/coordination", headers=_headers())

    assert response.status_code == 200
    event = next(item for item in response.json()["events"] if item["event_type"] == "mcp_invocation_observed")
    assert event["mcp_invocation"] == {
        "invocation_id": "a" * 64,
        "server_id": "readonly",
        "server_version": "1.0.0",
        "server_manifest_sha256": "b" * 64,
        "tool_name": "catalog_read",
        "input_schema_sha256": "c" * 64,
        "outcome": "success",
        "invocation_count": 2,
    }


def test_coordination_detail_exposes_an_opaque_execution_workspace_lifecycle(
    client, valid_plan: dict, supervisor_store
) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store._append_coordination_event(
        run_id,
        "execution_workspace_lifecycle",
        execution_workspace={
            "workspace_id": "a" * 64,
            "source": "execution_job",
            "lifecycle": "provisioned",
            "job_name": "must not be exposed",
        },
    )

    response = client.get(f"/api/v1/planning-runs/{run_id}/coordination", headers=_headers())

    assert response.status_code == 200
    event = next(item for item in response.json()["events"] if item["event_type"] == "execution_workspace_lifecycle")
    assert event["execution_workspace"] == {
        "workspace_id": "a" * 64,
        "source": "execution_job",
        "lifecycle": "provisioned",
    }


def test_normalized_plan_action_reuses_existing_digest_bound_authority(client, valid_plan: dict, starter) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    payload = {"decision": "approve", "artifact_sha256": digest}

    first = client.post(f"/api/v1/coordination/runs/{run_id}/actions/plan", json=payload, headers=_headers())
    second = client.post(f"/api/v1/coordination/runs/{run_id}/actions/plan", json=payload, headers=_headers())

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["decision_id"] == second.json()["decision_id"]
    assert len(starter.plan_approvals) == 1
    coordination = client.get(
        f"/api/v1/planning-runs/{run_id}/coordination",
        headers={"Authorization": "Bearer operator-test-token"},
    )
    recorded = next(
        item for item in coordination.json()["events"] if item["event_type"] == "plan_approval_recorded"
    )
    assert recorded["artifact"]["sha256"] == digest
    assert recorded["artifact"]["ref"].endswith("/plan.json")


def test_normalized_action_rejects_stale_digest_without_temporal_delivery(client, valid_plan: dict, starter) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": "0" * 64},
        headers=_headers(),
    )

    assert response.status_code == 409
    assert starter.plan_approvals == []


async def test_coordination_list_is_authenticated_and_bounded(client, valid_plan: dict, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], status=PlanningRunStatus.IMPLEMENTING
    )
    await supervisor_store.record_implementation_artifact(
        run_id,
        ArtifactReference(ref=f"s3://plans/runs/{run_id}/implementation.json", sha256="b" * 64),
    )

    unauthenticated = client.get("/api/v1/coordination/runs", headers={"Authorization": "Bearer invalid-token"})
    response = client.get("/api/v1/coordination/runs?limit=1", headers=_headers())

    assert unauthenticated.status_code == 401
    assert response.status_code == 200
    assert [item["run_id"] for item in response.json()["items"]] == [run_id]
    assert response.json()["items"][0]["active_gate"] == "implementation"


def test_normalized_action_requires_idempotency_key(client, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Authorization": "Bearer operator-test-token"},
    )

    assert response.status_code == 422


def test_unauthenticated_normalized_action_fails_auth_before_request_validation(client, valid_plan: dict) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Authorization": "Bearer invalid-token"},
    )

    assert response.status_code == 401
