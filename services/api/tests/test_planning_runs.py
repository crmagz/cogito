from __future__ import annotations

import copy

from fastapi.testclient import TestClient

from .fakes import FakePlanner, FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore
from .conftest import make_settings
from cogito_api.main import create_app
from cogito_api.models import AiPlan


def _planning_request(valid_plan: dict) -> dict:
    return {
        "initial_specification": "Add a rate limiter with bounded, observable behavior.",
        "target_repos": valid_plan["target_repos"],
        "spec_set": valid_plan["spec_set"],
        "constraints": valid_plan["constraints"],
        "priority": "normal",
    }


def test_submit_planning_run_persists_immutable_source_artifact_and_run(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    response = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "planning"
    assert body["source_artifact"]["ref"].endswith(f"runs/{body['run_id']}/source-spec.json")
    assert len(body["source_artifact"]["sha256"]) == 64
    assert store.source_specifications[body["run_id"]] == "Add a rate limiter with bounded, observable behavior."
    record = supervisor_store.planning_runs[body["run_id"]]
    assert record.source_artifact.sha256 == body["source_artifact"]["sha256"]
    assert record.target_repos == valid_plan["target_repos"]


def test_submit_planning_run_rejects_unpinned_repository_without_writing(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    payload = _planning_request(valid_plan)
    payload["target_repos"] = ["https://github.com/acme/api-gateway.git#main"]

    response = client.post("/api/v1/planning-runs", json=payload)

    assert response.status_code == 422
    assert store.source_specifications == {}
    assert supervisor_store.planning_runs == {}


def test_dry_run_planning_validates_without_writing(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    payload = _planning_request(valid_plan)
    payload["dry_run"] = True

    response = client.post("/api/v1/planning-runs", json=payload)

    assert response.status_code == 200
    assert response.json()["status"] == "validated"
    assert store.source_specifications == {}
    assert supervisor_store.planning_runs == {}


def test_get_planning_run_returns_authoritative_supervisor_record(
    client: TestClient, valid_plan: dict
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    response = client.get(f"/api/v1/planning-runs/{submitted.json()['run_id']}")

    assert response.status_code == 200
    assert response.json() == submitted.json()


def test_generate_plan_persists_validated_artifact_and_enters_approval_state(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]

    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "awaiting_plan_approval"
    assert body["plan_artifact"]["ref"].endswith(
        f"/plans/{run_id}/revisions/1/{body['plan_artifact']['sha256']}/plan.json"
    )
    assert len(body["plan_artifact"]["sha256"]) == 64
    assert store.plans[run_id].title == valid_plan["title"]
    assert supervisor_store.planning_runs[run_id].plan_artifact is not None


def test_generate_plan_retries_workflow_start_without_regenerating_artifact(
    client: TestClient, valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert first.status_code == 200
    assert response.status_code == 200
    assert response.json()["plan_artifact"] == first.json()["plan_artifact"]
    assert len(planner.contexts) == 1
    assert len(starter.started_runs) == 1


def test_concurrent_generation_converges_on_the_persisted_plan(
    valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    class ConcurrentPlanStore(InMemorySupervisorStore):
        async def attach_generated_plan(self, *args, **kwargs):
            await super().attach_generated_plan(*args, **kwargs)
            raise ValueError("another caller persisted the active plan")

    store = InMemoryPlanStore()
    supervisor_store = ConcurrentPlanStore()
    racing_client = TestClient(
        create_app(
            store=store,
            settings=make_settings(),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=planner,
        )
    )
    submitted = racing_client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    response = racing_client.post(f"/api/v1/planning-runs/{submitted.json()['run_id']}/generate-plan")

    assert response.status_code == 200
    assert response.json()["plan_artifact"] == supervisor_store.planning_runs[submitted.json()["run_id"]].plan_artifact.model_dump()
    assert len(starter.started_runs) == 1


def test_generate_plan_reports_retryable_temporal_start_failure(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    starter.start_error = ConnectionError("Temporal unavailable")

    failed = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert failed.status_code == 503
    starter.start_error = None
    retried = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert retried.status_code == 200


def test_revision_reopens_planning_with_a_new_artifact_and_workflow(
    client: TestClient, valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    first_digest = first.json()["plan_artifact"]["sha256"]
    revision = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "request_revision", "artifact_sha256": first_digest, "comment": "Narrow the scope."},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "revision-1"},
    )

    assert revision.status_code == 202
    reopened = client.get(f"/api/v1/planning-runs/{run_id}").json()
    assert reopened["status"] == "planning"
    assert reopened["plan_artifact"] is None
    revised_plan = copy.deepcopy(valid_plan)
    revised_plan["title"] = "Add a narrower rate limiter"
    planner.plan = AiPlan.model_validate(revised_plan)
    second = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    second_digest = second.json()["plan_artifact"]["sha256"]

    assert second.status_code == 200
    assert second_digest != first_digest
    assert len(starter.started_runs) == 2
    assert starter.started_runs[0].workflow_id != starter.started_runs[1].workflow_id
    stale = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": first_digest},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "stale-after-revision"},
    )
    assert stale.status_code == 409


def test_revision_scopes_workflow_and_idempotency_when_plan_content_is_identical(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    digest = first.json()["plan_artifact"]["sha256"]
    headers = {"Authorization": "Bearer operator-test-token", "Idempotency-Key": "same-key"}
    revision = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "request_revision", "artifact_sha256": digest, "comment": "Regenerate."},
        headers=headers,
    )

    assert revision.status_code == 202
    assert client.get(f"/api/v1/planning-runs/{run_id}").json()["plan_artifact"] is None
    second = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert second.status_code == 200
    assert second.json()["plan_artifact"]["sha256"] == digest
    assert second.json()["plan_artifact"]["ref"] != first.json()["plan_artifact"]["ref"]
    assert starter.started_runs[0].workflow_id != starter.started_runs[1].workflow_id
    approved = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=headers,
    )
    assert approved.status_code == 202
    assert len(starter.plan_approvals) == 2


def test_existing_direct_plan_submission_contract_remains_compatible(
    client: TestClient, valid_plan: dict
) -> None:
    plan = copy.deepcopy(valid_plan)

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    assert "plan_ref" in response.json()
