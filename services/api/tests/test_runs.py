from __future__ import annotations

import copy
from dataclasses import replace

import pytest
from cogito_api.models import AgentRunStatus
from cogito_api.main import create_app
from cogito_api.storage import PlanStoreUnavailableError
from fastapi.testclient import TestClient

from .fakes import FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore
from .conftest import make_settings


def test_submit_valid_plan_returns_202_with_run_id_and_plan_ref(
    client: TestClient, valid_plan: dict
):
    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert "run_id" in body
    assert body["plan_ref"].endswith(f"plans/{body['run_id']}/plan.json")


def test_submit_valid_plan_persists_plan_in_store(
    client: TestClient, valid_plan: dict, store: InMemoryPlanStore
):
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


def test_submit_missing_required_field_returns_422(
    client: TestClient, valid_plan: dict
):
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


def test_submit_unknown_phase_dependency_returns_422(
    client: TestClient, valid_plan: dict
):
    plan = copy.deepcopy(valid_plan)
    plan["phases"][1]["depends_on"] = ["phase-3"]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert any("phase-3" in v["message"] for v in body["violations"])


def test_submit_constraints_exceeding_system_maximum_returns_422(
    client: TestClient, valid_plan: dict
):
    plan = copy.deepcopy(valid_plan)
    plan["constraints"]["max_cost_usd"] = 10_000.0

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert any(v["field"] == "constraints.max_cost_usd" for v in body["violations"])


def test_submit_rejects_non_https_or_credentialed_repository_urls(
    client: TestClient, valid_plan: dict
):
    plan = copy.deepcopy(valid_plan)
    plan["target_repos"] = [
        "ssh://git@github.com/acme/api-gateway.git",
        "https://token@github.com/acme/private.git",
    ]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    body = response.json()
    assert {violation["field"] for violation in body["violations"]} == {
        "target_repos[0]",
        "target_repos[1]",
    }


def test_submit_rejects_malformed_or_unpinned_repository_urls(
    client: TestClient, valid_plan: dict
):
    plan = copy.deepcopy(valid_plan)
    plan["target_repos"] = [
        "https://[bad",
        "https://git.example.test/repository.git#main",
    ]

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 422
    assert {violation["field"] for violation in response.json()["violations"]} == {
        "target_repos[0]",
        "target_repos[1]",
    }


def test_dry_run_validates_without_persisting(
    client: TestClient, valid_plan: dict, store: InMemoryPlanStore
):
    response = client.post("/api/v1/runs", json={"plan": valid_plan, "dry_run": True})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "validated"
    assert body["dry_run"] is True
    assert store.plans == {}


def test_submit_valid_plan_starts_workflow(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
):
    response = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = response.json()["run_id"]

    assert len(starter.started_runs) == 1
    envelope = starter.started_runs[0]
    assert envelope.run_id == run_id
    assert envelope.plan_ref == response.json()["plan_ref"]
    assert len(envelope.plan_sha256) == 64
    assert envelope.spec_ref == valid_plan["spec_set"]


def test_default_configuration_does_not_issue_mcp_grants(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter, supervisor_store: InMemorySupervisorStore
) -> None:
    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    assert all(not resolution.mcp_grants for resolution in starter.started_runs[0].registry_resolutions)
    assert set(supervisor_store.registry_policies) == {"phase12_initial"}


def test_submit_resolves_a_pinned_agent_gateway_route(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter, supervisor_store: InMemorySupervisorStore
) -> None:
    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    developer = next(
        resolution for resolution in starter.started_runs[0].registry_resolutions if resolution.role == "developer"
    )
    assert developer.gateway is not None
    assert developer.gateway.model_alias == "complex"
    assert developer.gateway.max_budget_usd == 25
    assert developer.gateway.toolset == "development-restricted"
    assert set(supervisor_store.registry_agent_gateway_policies) == {"agent_gateway_initial"}


def test_enabled_configuration_uses_independent_mcp_policy(
    valid_plan: dict, store: InMemoryPlanStore, starter: FakeRunStarter
) -> None:
    supervisor_store = InMemorySupervisorStore()
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=True),
            starter=starter,
            supervisor_store=supervisor_store,
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    developer = next(
        resolution for resolution in starter.started_runs[0].registry_resolutions if resolution.role == "developer"
    )
    assert [(grant.server_id, grant.server_version, grant.tool_name) for grant in developer.mcp_grants] == [
        ("cogito_readonly_mcp", "1.0.1", "catalog_read")
    ]
    assert set(supervisor_store.registry_policies) == {"phase12_initial", "governed_mcp_initial"}


def test_github_connector_uses_a_separate_policy_and_exact_pinned_tools(
    valid_plan: dict, store: InMemoryPlanStore, starter: FakeRunStarter
) -> None:
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
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    developer = next(
        resolution for resolution in starter.started_runs[0].registry_resolutions if resolution.role == "developer"
    )
    assert {(grant.server_id, grant.tool_name) for grant in developer.mcp_grants} == {
        ("cogito_readonly_mcp", "catalog_read"),
        ("github_readonly_mcp", "repository_get"),
        ("github_readonly_mcp", "file_get"),
        ("github_readonly_mcp", "issue_get"),
        ("github_readonly_mcp", "pull_request_get"),
    }
    assert {
        grant.repository_scope for grant in developer.mcp_grants if grant.server_id == "github_readonly_mcp"
    } == {"acme/api-gateway"}
    assert set(supervisor_store.registry_policies) == {"phase12_initial", "governed_mcp_github_initial"}


def test_github_connector_grants_are_absent_when_the_run_does_not_target_its_repository(
    valid_plan: dict, store: InMemoryPlanStore, starter: FakeRunStarter
) -> None:
    valid_plan["target_repos"] = ["https://github.com/acme/unrelated.git#" + "a" * 40]
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(
                mcp_enabled=True,
                mcp_github_enabled=True,
                mcp_target_repository_scopes={"github_readonly_mcp@1.0.0": "acme/api-gateway"},
            ),
            starter=starter,
            supervisor_store=InMemorySupervisorStore(),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    developer = next(
        resolution for resolution in starter.started_runs[0].registry_resolutions if resolution.role == "developer"
    )
    assert [(grant.server_id, grant.tool_name) for grant in developer.mcp_grants] == [
        ("cogito_readonly_mcp", "catalog_read")
    ]


def test_github_connector_accepts_a_pinned_target_url_without_a_dot_git_suffix(
    valid_plan: dict, store: InMemoryPlanStore, starter: FakeRunStarter
) -> None:
    valid_plan["target_repos"] = ["https://github.com/acme/api-gateway#" + "a" * 40]
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(
                mcp_enabled=True,
                mcp_github_enabled=True,
                mcp_target_repository_scopes={"github_readonly_mcp@1.0.0": "acme/api-gateway"},
            ),
            starter=starter,
            supervisor_store=InMemorySupervisorStore(),
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    developer = next(
        resolution for resolution in starter.started_runs[0].registry_resolutions if resolution.role == "developer"
    )
    github_grants = [grant for grant in developer.mcp_grants if grant.server_id == "github_readonly_mcp"]
    assert len(github_grants) == 4
    assert {grant.repository_scope for grant in github_grants} == {"acme/api-gateway"}


def test_github_connector_requires_governed_mcp(
    store: InMemoryPlanStore, starter: FakeRunStarter, supervisor_store: InMemorySupervisorStore
) -> None:
    with pytest.raises(ValueError, match="requires COGITO_MCP_ENABLED"):
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=False, mcp_github_enabled=True),
            starter=starter,
            supervisor_store=supervisor_store,
        )


def test_github_connector_requires_a_target_repository_scope(
    store: InMemoryPlanStore, starter: FakeRunStarter, supervisor_store: InMemorySupervisorStore
) -> None:
    with pytest.raises(ValueError, match="target repository scope"):
        create_app(
            store=store,
            settings=make_settings(mcp_enabled=True, mcp_github_enabled=True),
            starter=starter,
            supervisor_store=supervisor_store,
        )


def test_dry_run_does_not_start_workflow(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
):
    client.post("/api/v1/runs", json={"plan": valid_plan, "dry_run": True})

    assert starter.started_runs == []


def test_get_status_for_existing_run_returns_authoritative_lifecycle_and_execution_evidence(
    client: TestClient, valid_plan: dict, store: InMemoryPlanStore
):
    submit = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = submit.json()["run_id"]
    store.statuses[run_id] = {
        "run_id": run_id,
        "status": "completed",
        "completed_phase_ids": ["phase-1"],
        "phase_results": [{"phase_id": "phase-1", "turns_used": 4}],
    }

    response = client.get(f"/api/v1/runs/{run_id}/status")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["lifecycle_status"] == "QUEUED"
    assert response.json()["execution_status"] == "completed"
    assert response.json()["completed_phase_ids"] == ["phase-1"]
    assert response.json()["phase_results"] == [
        {"phase_id": "phase-1", "turns_used": 4}
    ]
    assert len(response.json()["trace_id"]) == 32


def test_get_status_keeps_backup_execution_evidence_beside_terminal_lifecycle(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
):
    submit = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = submit.json()["run_id"]
    supervisor_store.agent_runs[run_id] = replace(
        supervisor_store.agent_runs[run_id], status=AgentRunStatus.TIMED_OUT
    )
    store.statuses[run_id] = {
        "run_id": run_id,
        "status": "stopped_with_backup",
        "ceiling": "cost",
        "completed_phase_ids": ["phase-1"],
        "stopped_phase_id": "phase-2",
        "unfinished_phase_ids": ["phase-2"],
        "branch_name": f"adp/{run_id}",
    }

    response = client.get(f"/api/v1/runs/{run_id}/status")

    assert response.status_code == 200
    assert response.json()["status"] == "timed_out"
    assert response.json()["lifecycle_status"] == "TIMED_OUT"
    assert response.json()["execution_status"] == "stopped_with_backup"
    assert response.json()["ceiling"] == "cost"
    assert response.json()["completed_phase_ids"] == ["phase-1"]
    assert response.json()["unfinished_phase_ids"] == ["phase-2"]


def test_get_status_exposes_escalated_review_evidence_without_overwriting_lifecycle(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
):
    submit = client.post("/api/v1/runs", json={"plan": valid_plan})
    run_id = submit.json()["run_id"]
    supervisor_store.agent_runs[run_id] = replace(
        supervisor_store.agent_runs[run_id], status=AgentRunStatus.SUCCEEDED
    )
    store.statuses[run_id] = {
        "run_id": run_id,
        "status": "escalated",
        "review": {
            "status": "escalated",
            "reason": "max_review_rounds",
            "rounds": [{"round": 1, "findings": [{"severity": "blocking"}]}],
        },
    }

    response = client.get(f"/api/v1/runs/{run_id}/status")

    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert response.json()["lifecycle_status"] == "SUCCEEDED"
    assert response.json()["execution_status"] == "escalated"
    assert response.json()["review"]["reason"] == "max_review_rounds"


def test_get_status_for_unknown_run_returns_404(client: TestClient):
    response = client.get("/api/v1/runs/does-not-exist/status")

    assert response.status_code == 404
