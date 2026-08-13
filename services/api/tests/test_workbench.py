"""Scoped Operator Workbench contract and evidence-reader coverage."""

from __future__ import annotations

from dataclasses import replace
import json

from fastapi.testclient import TestClient

from cogito_api.main import create_app
from cogito_api.models import AiPlan, PlanningRunStatus

from .conftest import make_settings
from .fakes import FakePlanner, FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore
from .test_approvals import _awaiting_plan
from .test_planning_runs import _planning_request


def _headers(key: str = "workbench-action") -> dict[str, str]:
    return {"Authorization": "Bearer operator-test-token", "Idempotency-Key": key}


def _selection_from_started_run(starter: FakeRunStarter) -> dict[str, str]:
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


def _mcp_workbench(valid_plan: dict) -> tuple[TestClient, FakeRunStarter, InMemorySupervisorStore, InMemoryPlanStore]:
    store = InMemoryPlanStore()
    starter = FakeRunStarter()
    supervisor_store = InMemorySupervisorStore()
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=True),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    return client, starter, supervisor_store, store


def test_workbench_queue_filters_to_authorized_project(client, valid_plan, supervisor_store) -> None:
    allowed_run, _ = _awaiting_plan(client, valid_plan)
    foreign_run, _ = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[allowed_run] = replace(
        supervisor_store.planning_runs[allowed_run], project_id="default"
    )
    supervisor_store.planning_runs[foreign_run] = replace(
        supervisor_store.planning_runs[foreign_run], project_id="other-project"
    )

    response = client.get("/api/v1/workbench/runs", headers=_headers())

    assert response.status_code == 200
    assert [item["run_id"] for item in response.json()["items"]] == [allowed_run]
    assert response.json()["items"][0]["active_gate"] == "plan"
    assert "ref" not in str(response.json())


def test_workbench_project_inventory_and_selected_project_are_scope_filtered(client, valid_plan, supervisor_store) -> None:
    allowed_run, _ = _awaiting_plan(client, valid_plan)
    foreign_run, _ = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[allowed_run] = replace(
        supervisor_store.planning_runs[allowed_run], project_id="default"
    )
    supervisor_store.planning_runs[foreign_run] = replace(
        supervisor_store.planning_runs[foreign_run], project_id="other-project"
    )

    projects = client.get("/api/v1/workbench/projects", headers=_headers())
    selected = client.get("/api/v1/workbench/runs?project_id=default", headers=_headers())
    forged = client.get("/api/v1/workbench/runs?project_id=other-project", headers=_headers())

    assert projects.status_code == 200
    assert projects.json() == {"items": [{"project_id": "default"}]}
    assert selected.status_code == 200
    assert [item["run_id"] for item in selected.json()["items"]] == [allowed_run]
    assert forged.status_code == 404


def test_workbench_multiple_authorized_projects_return_only_the_selected_scope(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    writer = TestClient(
        create_app(
            store=store,
            settings=make_settings(),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    alpha_run, _ = _awaiting_plan(writer, valid_plan)
    beta_run, _ = _awaiting_plan(writer, valid_plan)
    supervisor_store.planning_runs[alpha_run] = replace(supervisor_store.planning_runs[alpha_run], project_id="alpha")
    supervisor_store.planning_runs[beta_run] = replace(supervisor_store.planning_runs[beta_run], project_id="beta")
    app = create_app(
        store=store,
        settings=make_settings(auth_static_projects=("alpha", "beta")),
        starter=FakeRunStarter(),
        supervisor_store=supervisor_store,
        planner=FakePlanner(AiPlan.model_validate(valid_plan)),
    )
    reader = TestClient(app, headers={"Authorization": "Bearer operator-test-token"})

    projects = reader.get("/api/v1/workbench/projects", headers=_headers())
    alpha = reader.get("/api/v1/workbench/runs?project_id=alpha", headers=_headers())
    beta = reader.get("/api/v1/workbench/runs?project_id=beta", headers=_headers())

    assert projects.json() == {"items": [{"project_id": "alpha"}, {"project_id": "beta"}]}
    assert [item["run_id"] for item in alpha.json()["items"]] == [alpha_run]
    assert [item["run_id"] for item in beta.json()["items"]] == [beta_run]


def test_workbench_detail_and_evidence_are_scope_and_digest_bound(client, valid_plan, supervisor_store) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(supervisor_store.planning_runs[run_id], project_id="default")

    detail = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    evidence = client.get(
        f"/api/v1/workbench/runs/{run_id}/evidence/plan",
        params={"artifact_sha256": digest},
        headers=_headers(),
    )
    forged = client.get(
        f"/api/v1/workbench/runs/{run_id}/evidence/plan",
        params={"artifact_sha256": "0" * 64},
        headers=_headers(),
    )

    assert detail.status_code == 200
    assert detail.json()["artifacts"] == [
        {"kind": "source", "sha256": supervisor_store.planning_runs[run_id].source_artifact.sha256},
        {
            "kind": "product_specification",
            "sha256": supervisor_store.planning_runs[run_id].product_specification_artifact.sha256,
        },
        {"kind": "plan", "sha256": digest},
    ]
    assert evidence.status_code == 200
    assert evidence.json()["sha256"] == digest
    assert '"title"' in evidence.json()["content"]
    assert forged.status_code == 404


def test_workbench_approver_can_view_pinned_mcp_capabilities_and_submit_selection(valid_plan: dict) -> None:
    client, starter, supervisor_store, _ = _mcp_workbench(valid_plan)
    run_id, digest = _awaiting_plan(client, valid_plan)
    selection = _selection_from_started_run(starter)

    before = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    approved = client.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [selection]},
        headers=_headers("mcp-workbench"),
    )
    after = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert before.status_code == 200
    assert before.json()["mcp_capabilities"] == {
        "state": "awaiting_plan_approval",
        "pinned_grants": [selection],
        "selected_grants": None,
        "invocation_evidence_available": False,
    }
    assert approved.status_code == 202
    assert approved.json()["mcp_selection"] == [selection]
    assert after.status_code == 200
    assert after.json()["mcp_capabilities"] == {
        "state": "approved",
        "pinned_grants": [selection],
        "selected_grants": [selection],
        "invocation_evidence_available": False,
    }
    assert after.headers["etag"] != before.headers["etag"]
    assert supervisor_store.planning_runs[run_id].status is PlanningRunStatus.IMPLEMENTING


def test_workbench_exposes_the_repository_scope_of_a_github_mcp_grant(valid_plan: dict) -> None:
    store = InMemoryPlanStore()
    starter = FakeRunStarter()
    supervisor_store = InMemorySupervisorStore()
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(
                mcp_enabled=True,
                mcp_github_enabled=True,
                mcp_target_repository_scopes={"github_readonly_mcp@1.0.0": "Acme/API-Gateway"},
            ),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    run_id, _ = _awaiting_plan(client, valid_plan)

    response = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    github_pins = [
        grant
        for grant in response.json()["mcp_capabilities"]["pinned_grants"]
        if grant["server_id"] == "github_readonly_mcp"
    ]
    assert len(github_pins) == 4
    assert {grant["repository_scope"] for grant in github_pins} == {"acme/api-gateway"}


def test_workbench_viewer_cannot_see_or_configure_mcp_capabilities(valid_plan) -> None:
    writer, starter, supervisor_store, store = _mcp_workbench(valid_plan)
    run_id, digest = _awaiting_plan(writer, valid_plan)
    selection = _selection_from_started_run(starter)
    viewer = TestClient(
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=True, auth_static_roles=("cogito-viewer",)),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    detail = viewer.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    action = viewer.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": digest, "mcp_selection": [selection]},
        headers=_headers("viewer-mcp"),
    )

    assert detail.status_code == 200
    assert detail.json()["mcp_capabilities"] is None
    assert "catalog_read" not in detail.text
    assert action.status_code == 403


def test_workbench_feedback_is_digest_bound_idempotent_and_non_executable(client, valid_plan, supervisor_store) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(supervisor_store.planning_runs[run_id], project_id="default")
    payload = {
        "intent": "note",
        "artifact_sha256": digest,
        "stage_id": "plan_approval",
        "comment": "Please make the rollout impact explicit.",
    }

    first = client.post(f"/api/v1/workbench/runs/{run_id}/feedback", json=payload, headers=_headers("feedback-key"))
    replay = client.post(f"/api/v1/workbench/runs/{run_id}/feedback", json=payload, headers=_headers("feedback-key"))
    stale = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={**payload, "artifact_sha256": "0" * 64},
        headers=_headers("feedback-stale"),
    )

    assert first.status_code == 202
    assert replay.status_code == 202
    assert replay.json() == first.json()
    assert first.json()["actor_id"] == "test-operator"
    assert first.json()["intent"] == "note"
    assert supervisor_store.planning_runs[run_id].status is PlanningRunStatus.AWAITING_PLAN_APPROVAL
    assert stale.status_code == 409

    whitespace_replay = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={**payload, "comment": f"  {payload['comment']}  "},
        headers=_headers("feedback-key"),
    )
    assert whitespace_replay.status_code == 202
    assert whitespace_replay.json() == first.json()

    collision = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={**payload, "comment": "A different note."},
        headers=_headers("feedback-key"),
    )
    assert collision.status_code == 409

    mismatched_stage = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={**payload, "stage_id": "specification"},
        headers=_headers("feedback-mismatched-stage"),
    )
    assert mismatched_stage.status_code == 409

    invalid_stage = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={**payload, "stage_id": "arbitrary-agent-command"},
        headers=_headers("feedback-invalid-stage"),
    )
    assert invalid_stage.status_code == 422


def test_workbench_feedback_accepts_product_specification_review_context(client, valid_plan, supervisor_store) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    supervisor_store.planning_runs[run_id] = replace(supervisor_store.planning_runs[run_id], project_id="default")

    response = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={
            "intent": "note",
            "artifact_sha256": draft["product_specification_artifact"]["sha256"],
            "stage_id": "product_specification",
            "comment": "Clarify the acceptance criteria before selection.",
        },
        headers=_headers("product-specification-feedback"),
    )

    assert response.status_code == 202
    assert response.json()["stage_id"] == "product_specification"


def test_workbench_feedback_accepts_a_512_character_oidc_subject(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    subject = "o" * 512
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(auth_static_subject=subject),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    run_id, digest = _awaiting_plan(client, valid_plan)

    response = client.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={
            "intent": "note",
            "artifact_sha256": digest,
            "stage_id": "plan_approval",
            "comment": "Keep the full OIDC subject.",
        },
        headers=_headers("long-actor"),
    )

    assert response.status_code == 202
    assert response.json()["actor_id"] == subject


def test_workbench_feedback_hides_foreign_run(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    writer = TestClient(create_app(store=store, settings=make_settings(), starter=FakeRunStarter(), supervisor_store=supervisor_store, planner=FakePlanner(AiPlan.model_validate(valid_plan))), headers={"Authorization": "Bearer operator-test-token"})
    run_id, digest = _awaiting_plan(writer, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(supervisor_store.planning_runs[run_id], project_id="foreign")
    reader = TestClient(create_app(store=store, settings=make_settings(auth_static_projects=("default",)), starter=FakeRunStarter(), supervisor_store=supervisor_store, planner=FakePlanner(AiPlan.model_validate(valid_plan))), headers={"Authorization": "Bearer operator-test-token"})

    response = reader.post(
        f"/api/v1/workbench/runs/{run_id}/feedback",
        json={"intent": "note", "artifact_sha256": digest, "stage_id": "planning", "comment": "Need context."},
        headers=_headers("foreign-feedback"),
    )

    assert response.status_code == 404


def test_workbench_projects_authoritative_workflow_identity_and_scoped_timeline(client, valid_plan, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], project_id="default", workflow_id="planning-run-42-revision-1"
    )

    detail = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    timeline = client.get(f"/api/v1/workbench/runs/{run_id}/timeline", headers=_headers())
    unchanged = client.get(
        f"/api/v1/workbench/runs/{run_id}/timeline",
        headers={"Authorization": "Bearer operator-test-token", "If-None-Match": timeline.headers["etag"]},
    )

    assert detail.status_code == 200
    assert detail.json()["workflow_id"] == "planning-run-42-revision-1"
    assert timeline.status_code == 200
    assert timeline.json()["items"]
    stages_by_type = {item["event_type"]: item for item in timeline.json()["items"]}
    assert stages_by_type["specification_recorded"]["stage_ids"] == ["specification"]
    assert stages_by_type["planning_started"]["stage_ids"] == ["planning"]
    assert stages_by_type["plan_approval_requested"]["stage_ids"] == ["planning", "plan_approval"]
    assert stages_by_type["plan_approval_requested"]["stage_id"] == "plan_approval"
    assert "artifact_ref" not in timeline.text
    assert "last_error" not in timeline.text
    assert unchanged.status_code == 304
    assert unchanged.content == b""


def test_workbench_timeline_does_not_guess_a_stage_for_unknown_events(client, valid_plan, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store._append_coordination_event(run_id, "future_lifecycle_event")

    response = client.get(f"/api/v1/workbench/runs/{run_id}/timeline", headers=_headers())

    assert response.status_code == 200
    unknown = next(item for item in response.json()["items"] if item["event_type"] == "future_lifecycle_event")
    assert unknown["stage_id"] is None
    assert unknown["stage_ids"] == []


def test_workbench_timeline_tolerates_unknown_persisted_enum_values(client, valid_plan, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    event_id, event = next(iter(supervisor_store.coordination_events.items()))
    supervisor_store.coordination_events[event_id] = replace(
        event, gate="future-gate", decision="future-decision", lifecycle_status="future-status"
    )

    response = client.get(f"/api/v1/workbench/runs/{run_id}/timeline", headers=_headers())

    assert response.status_code == 200
    item = next(item for item in response.json()["items"] if item["event_id"] == event_id)
    assert item["gate"] is None
    assert item["decision"] is None
    assert item["lifecycle_status"] is None


def test_workbench_timeline_accepts_quoted_and_weak_etags(client, valid_plan) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    first = client.get(f"/api/v1/workbench/runs/{run_id}/timeline", headers=_headers())
    etag = first.headers["etag"]

    quoted = client.get(
        f"/api/v1/workbench/runs/{run_id}/timeline",
        headers={"Authorization": "Bearer operator-test-token", "If-None-Match": f'"{etag}"'},
    )
    weak = client.get(
        f"/api/v1/workbench/runs/{run_id}/timeline",
        headers={"Authorization": "Bearer operator-test-token", "If-None-Match": f'W/"{etag}"'},
    )

    assert first.status_code == 200
    assert quoted.status_code == 304
    assert weak.status_code == 304


def test_workbench_stage_projection_is_typed_and_never_copies_terminal_run_state(client, valid_plan, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], status=PlanningRunStatus.COMPLETED, plan_artifact=None,
        implementation_artifact=None
    )

    response = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    stages = {item["stage_id"]: item for item in response.json()["stages"]}
    assert stages["specification"] == {
        "stage_id": "specification",
        "label": "Specification",
        "state": "completed",
        "availability": "authoritative",
        "reason": "An immutable submitted specification is recorded.",
        "artifact_kind": "source",
    }
    assert stages["planning"]["state"] == "unavailable"
    assert stages["plan_approval"]["state"] == "unavailable"
    assert stages["implementation"]["state"] == "unavailable"
    assert stages["implementation_approval"]["state"] == "unavailable"
    graph = response.json()["workflow_graph"]
    assert [(node["stage_id"], node["node_type"]) for node in graph["nodes"]] == [
        ("specification", "queue"),
        ("product_specification", "queue"),
        ("planning", "agent"),
        ("plan_approval", "gate"),
        ("implementation", "agent"),
        ("implementation_approval", "gate"),
    ]
    assert [(edge["source_node_id"], edge["target_node_id"]) for edge in graph["edges"]] == [
        ("specification", "product_specification"),
        ("product_specification", "planning"),
        ("planning", "plan_approval"),
        ("plan_approval", "implementation"),
        ("implementation", "implementation_approval"),
    ]


def test_workbench_stage_projection_attributes_rejections_only_when_artifacts_prove_the_gate(
    client, valid_plan, supervisor_store
) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    original = supervisor_store.planning_runs[run_id]

    supervisor_store.planning_runs[run_id] = replace(
        original, status=PlanningRunStatus.REJECTED, implementation_artifact=None
    )
    plan_rejected = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    assert plan_rejected.status_code == 200
    plan_stages = {item["stage_id"]: item for item in plan_rejected.json()["stages"]}
    assert plan_stages["plan_approval"]["state"] == "failed"
    assert plan_stages["implementation_approval"]["state"] == "unavailable"

    supervisor_store.planning_runs[run_id] = replace(
        original,
        status=PlanningRunStatus.REJECTED,
        implementation_artifact=original.plan_artifact,
    )
    implementation_rejected = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    assert implementation_rejected.status_code == 200
    implementation_stages = {item["stage_id"]: item for item in implementation_rejected.json()["stages"]}
    assert implementation_stages["plan_approval"]["state"] == "completed"
    assert implementation_stages["implementation"]["state"] == "completed"
    assert implementation_stages["implementation_approval"]["state"] == "failed"

    supervisor_store.planning_runs[run_id] = replace(
        original, status=PlanningRunStatus.REVISION_REQUESTED, implementation_artifact=None
    )
    plan_revision = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    assert plan_revision.status_code == 200
    plan_revision_stages = {item["stage_id"]: item for item in plan_revision.json()["stages"]}
    assert plan_revision_stages["plan_approval"]["state"] == "needs_revision"
    assert plan_revision_stages["plan_approval"]["availability"] == "authoritative"
    assert plan_revision_stages["implementation_approval"]["state"] == "unavailable"

    supervisor_store.planning_runs[run_id] = replace(
        original,
        status=PlanningRunStatus.REVISION_REQUESTED,
        implementation_artifact=original.plan_artifact,
    )
    implementation_revision = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    assert implementation_revision.status_code == 200
    implementation_revision_stages = {item["stage_id"]: item for item in implementation_revision.json()["stages"]}
    assert implementation_revision_stages["plan_approval"]["state"] == "completed"
    assert implementation_revision_stages["implementation_approval"]["state"] == "needs_revision"


def test_workbench_timeline_hides_foreign_run_without_disclosing_events(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    writer = TestClient(
        create_app(
            store=store,
            settings=make_settings(),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    run_id, _ = _awaiting_plan(writer, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(supervisor_store.planning_runs[run_id], project_id="foreign")
    reader = TestClient(
        create_app(
            store=store,
            settings=make_settings(auth_static_projects=("default",)),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = reader.get(f"/api/v1/workbench/runs/{run_id}/timeline", headers=_headers())

    assert response.status_code == 404
    assert response.json() == {"detail": "planning run not found"}


def test_workbench_returns_not_modified_for_matching_revision(client, valid_plan) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)

    first = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    second = client.get(
        f"/api/v1/workbench/runs/{run_id}",
        headers={"Authorization": "Bearer operator-test-token", "If-None-Match": first.headers["etag"]},
    )

    assert first.status_code == 200
    assert second.status_code == 304
    assert second.headers["etag"] == first.headers["etag"]
    assert second.content == b""


def test_workbench_detail_projects_bounded_execution_budget_and_approval_history(
    client, valid_plan, store, supervisor_store
) -> None:
    run_id, digest = _awaiting_plan(client, valid_plan)
    approved = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=_headers("workbench-history"),
    )
    assert approved.status_code == 202
    evidence = {
        "version": 1,
        "run_id": run_id,
        "phase_results": [
            {"phase_id": "one", "succeeded": True, "verification": [{"passed": True}, {"passed": False}]},
            {"phase_id": "two", "succeeded": False, "verification": []},
        ],
        "review": {"status": "converged"},
        "validation": {"status": "passed"},
        "cost_usd": 1.25,
        "turns_used": 42,
    }
    artifact = store.put_artifact(
        f"s3://plans/runs/{run_id}/implementation.json", json.dumps(evidence).encode()
    )
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id],
        implementation_artifact=artifact,
        status=PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL,
    )

    response = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["budget"] == {
        "max_cost_usd": 3.0,
        "max_wall_clock_minutes": 45,
        "max_review_rounds": 2,
        "actual_cost_usd": 1.25,
        "turns_used": 42,
    }
    assert body["execution"] == {
        "phase_count": 2,
        "succeeded_phase_count": 1,
        "failed_phase_count": 1,
        "verification_passed": 1,
        "verification_failed": 1,
        "review_status": "converged",
        "validation_status": "passed",
    }
    assert body["approval_history"] == [
        {
            "decision_id": approved.json()["decision_id"],
            "gate": "plan",
            "decision": "approve",
            "artifact_sha256": digest,
            "actor_id": "test-operator",
            "created_at": approved.json()["created_at"],
            "delivered": True,
        }
    ]
    assert body["external_links"] == [{"kind": "repository", "label": "Repository", "url": "https://github.com/acme/api-gateway"}]


def test_workbench_omits_nonstandard_port_repository_link(client, valid_plan, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id],
        target_repos=["https://github.com:444/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"],
    )

    response = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    assert response.json()["external_links"] == []


def test_workbench_detail_treats_malformed_execution_evidence_as_unavailable(client, valid_plan, store, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    artifact = store.put_artifact(f"s3://plans/runs/{run_id}/implementation.json", b"not-json")
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], implementation_artifact=artifact
    )

    response = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    assert response.json()["execution"] is None
    assert response.json()["budget"]["actual_cost_usd"] is None


def test_workbench_detail_omits_non_finite_actual_cost(client, valid_plan, store, supervisor_store) -> None:
    run_id, _ = _awaiting_plan(client, valid_plan)
    evidence = {"run_id": run_id, "phase_results": [], "cost_usd": float("inf"), "turns_used": 0}
    artifact = store.put_artifact(f"s3://plans/runs/{run_id}/implementation.json", json.dumps(evidence).encode())
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], implementation_artifact=artifact
    )

    response = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    assert response.json()["budget"]["actual_cost_usd"] is None


def test_workbench_detail_does_not_leak_approval_history_to_viewers(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    writer = TestClient(
        create_app(
            store=store,
            settings=make_settings(),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    run_id, digest = _awaiting_plan(writer, valid_plan)
    decision = writer.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=_headers("viewer-history"),
    )
    assert decision.status_code == 202
    viewer = TestClient(
        create_app(
            store=store,
            settings=make_settings(auth_static_roles=("cogito-viewer",)),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = viewer.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())

    assert response.status_code == 200
    assert response.json()["approval_history_available"] is False
    assert response.json()["approval_history"] == []
    assert "test-operator" not in response.text


def test_workbench_viewer_cannot_read_implementation_mcp_selection_evidence(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    writer = TestClient(
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=True),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    run_id, _ = _awaiting_plan(writer, valid_plan)
    artifact = store.put_artifact(
        f"s3://plans/runs/{run_id}/implementation.json",
        json.dumps({"mcp_invocations": {"selected_grants": [{"tool_name": "catalog_read"}]}}).encode(),
    )
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], implementation_artifact=artifact
    )
    viewer = TestClient(
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=True, auth_static_roles=("cogito-viewer",)),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = viewer.get(
        f"/api/v1/workbench/runs/{run_id}/evidence/implementation",
        params={"artifact_sha256": artifact.sha256},
        headers=_headers(),
    )

    assert response.status_code == 403
    assert "catalog_read" not in response.text


def test_workbench_queue_returns_bodyless_not_modified_response(client, valid_plan) -> None:
    _awaiting_plan(client, valid_plan)

    first = client.get("/api/v1/workbench/runs", headers=_headers())
    second = client.get(
        "/api/v1/workbench/runs",
        headers={"Authorization": "Bearer operator-test-token", "If-None-Match": first.headers["etag"]},
    )

    assert first.status_code == 200
    assert second.status_code == 304
    assert second.headers["etag"] == first.headers["etag"]
    assert second.content == b""


def test_workbench_hides_foreign_detail_and_rejects_viewer_actions(valid_plan) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    writer = TestClient(
        create_app(
            store=store,
            settings=make_settings(),
            starter=FakeRunStarter(),
            supervisor_store=supervisor_store,
            planner=FakePlanner(AiPlan.model_validate(valid_plan)),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    run_id, digest = _awaiting_plan(writer, valid_plan)
    app = create_app(
        store=store,
        settings=make_settings(auth_static_projects=("alpha",), auth_static_roles=("cogito-viewer",)),
        starter=FakeRunStarter(),
        supervisor_store=supervisor_store,
        planner=FakePlanner(AiPlan.model_validate(valid_plan)),
    )
    client = TestClient(app, headers={"Authorization": "Bearer operator-test-token"})
    legacy_detail = client.get(f"/api/v1/planning-runs/{run_id}", headers=_headers())
    coordination = client.get(f"/api/v1/planning-runs/{run_id}/coordination", headers=_headers())
    supervisor_store.planning_runs[run_id] = replace(
        supervisor_store.planning_runs[run_id], project_id="beta", status=PlanningRunStatus.AWAITING_PLAN_APPROVAL
    )

    detail = client.get(f"/api/v1/workbench/runs/{run_id}", headers=_headers())
    action = client.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=_headers(),
    )

    assert detail.status_code == 404
    assert legacy_detail.status_code == 403
    assert coordination.status_code == 403
    assert action.status_code == 403
