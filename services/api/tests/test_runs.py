from __future__ import annotations

import copy

from fastapi.testclient import TestClient

from cogito_api.storage import PlanStoreUnavailableError

from .fakes import FakeRunStarter, InMemoryPlanStore


def test_submit_valid_plan_returns_202_with_run_id_and_plan_ref(client: TestClient, valid_plan: dict):
    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert "run_id" in body
    assert body["plan_ref"].endswith(f"plans/{body['run_id']}/plan.json")


def test_submit_valid_plan_persists_plan_in_store(client: TestClient, valid_plan: dict, store: InMemoryPlanStore):
    response = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = response.json()["run_id"]

    assert run_id in store.plans
    assert store.plans[run_id].title == valid_plan["title"]


def test_submit_plan_returns_retryable_error_when_snapshot_storage_is_unavailable(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    starter: FakeRunStarter,
    monkeypatch,
):
    def fail_put_plan(*args, **kwargs):
        raise PlanStoreUnavailableError("plan snapshot storage is unavailable")

    monkeypatch.setattr(store, "put_plan", fail_put_plan)

    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 503
    assert response.json()["detail"] == "run storage is temporarily unavailable"
    assert starter.started_runs == []


def test_submit_missing_required_field_returns_422(client: TestClient, valid_plan: dict):
    plan = copy.deepcopy(valid_plan)
    del plan["title"]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "validation_failed"
    assert any("title" in v["field"] for v in body["violations"])


def test_submit_dag_cycle_returns_422(client: TestClient, valid_plan: dict):
    plan = copy.deepcopy(valid_plan)
    plan["phases"][0]["depends_on"] = ["phase-2"]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "validation_failed"
    assert any("cycle" in v["message"] for v in body["violations"])


def test_submit_unknown_phase_dependency_returns_422(client: TestClient, valid_plan: dict):
    plan = copy.deepcopy(valid_plan)
    plan["phases"][1]["depends_on"] = ["phase-3"]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert any("phase-3" in v["message"] for v in body["violations"])


def test_submit_constraints_exceeding_system_maximum_returns_422(client: TestClient, valid_plan: dict):
    plan = copy.deepcopy(valid_plan)
    plan["constraints"]["max_cost_usd"] = 10_000.0

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert any(v["field"] == "constraints.max_cost_usd" for v in body["violations"])


def test_submit_rejects_non_https_or_credentialed_repository_urls(client: TestClient, valid_plan: dict):
    plan = copy.deepcopy(valid_plan)
    plan["target_repos"] = ["ssh://git@github.com/acme/api-gateway.git", "https://token@github.com/acme/private.git"]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert {violation["field"] for violation in body["violations"]} == {
        "target_repos[0]",
        "target_repos[1]",
    }


def test_submit_rejects_malformed_or_unpinned_repository_urls(client: TestClient, valid_plan: dict):
    plan = copy.deepcopy(valid_plan)
    plan["target_repos"] = ["https://[bad", "https://git.example.test/repository.git#main"]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    assert {violation["field"] for violation in response.json()["violations"]} == {"target_repos[0]", "target_repos[1]"}


def test_dry_run_validates_without_persisting(client: TestClient, valid_plan: dict, store: InMemoryPlanStore):
    response = client.post("/api/v1/runs", json={"plan": valid_plan, "dry_run": True})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "validated"
    assert body["dry_run"] is True
    assert store.plans == {}


def test_submit_valid_plan_starts_workflow(client: TestClient, valid_plan: dict, starter: FakeRunStarter):
    response = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = response.json()["run_id"]

    assert len(starter.started_runs) == 1
    envelope = starter.started_runs[0]
    assert envelope.run_id == run_id
    assert envelope.plan_ref == response.json()["plan_ref"]
    assert len(envelope.plan_sha256) == 64
    assert envelope.spec_ref == valid_plan["spec_set"]


def test_dry_run_does_not_start_workflow(client: TestClient, valid_plan: dict, starter: FakeRunStarter):
    client.post("/api/v1/runs", json={"plan": valid_plan, "dry_run": True})

    assert starter.started_runs == []


def test_get_status_for_existing_run_returns_queued(client: TestClient, valid_plan: dict):
    submit = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = submit.json()["run_id"]

    response = client.get(f"/api/v1/runs/{run_id}/status")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["lifecycle_status"] == "QUEUED"
    assert len(response.json()["trace_id"]) == 32


def test_get_status_for_unknown_run_returns_404(client: TestClient):
    response = client.get("/api/v1/runs/does-not-exist/status")

    assert response.status_code == 404
