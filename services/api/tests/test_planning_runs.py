from __future__ import annotations

import copy

from fastapi.testclient import TestClient

from .fakes import InMemoryPlanStore, InMemorySupervisorStore


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


def test_existing_direct_plan_submission_contract_remains_compatible(
    client: TestClient, valid_plan: dict
) -> None:
    plan = copy.deepcopy(valid_plan)

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    assert "plan_ref" in response.json()
