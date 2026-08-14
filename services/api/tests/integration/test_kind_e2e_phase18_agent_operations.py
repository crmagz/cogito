"""Native Kind coverage for the project-scoped Agent Operations read API."""

from __future__ import annotations

import json
import os

import pytest

from .kind_helpers import KindHarness


pytestmark = pytest.mark.kind_e2e


def test_phase18_agent_operations_read_api_e2e() -> None:
    """Prove deployed Postgres-backed agent inventory and immutable route reads stay read-only."""

    harness = KindHarness.from_environment(default_context="kind-cogito-observability")
    harness.assert_context()
    harness.kubectl("-n", harness.namespace, "rollout", "status", f"deployment/{harness.release}-api", "--timeout=240s")
    probe = json.loads(harness.exec_python(f"deployment/{harness.release}-api", _CREATE_AGENT_OPERATIONS_PROBE))
    run_id = str(probe["run_id"])
    registration_id = str(probe["registration_id"])
    registration_version = str(probe["registration_version"])
    policy_revision = str(probe["policy_revision"])
    try:
        unauthenticated, _ = harness.api("GET", "/api/v1/workbench/agents?project_id=default", authenticated=False)
        inventory_status, inventory = harness.api("GET", "/api/v1/workbench/agents?project_id=default")
        detail_status, detail = harness.api(
            "GET", f"/api/v1/workbench/agents/{registration_id}/{registration_version}?project_id=default"
        )
        history_status, history = harness.api(
            "GET", f"/api/v1/workbench/agents/{registration_id}/{registration_version}/invocations?project_id=default"
        )
        invocation_status, invocation = harness.api(
            "GET", f"/api/v1/workbench/agent-invocations/{run_id}/developer?project_id=default"
        )

        assert unauthenticated == 401
        assert inventory_status == detail_status == history_status == invocation_status == 200
        agent = next(
            item
            for item in list(inventory["items"])
            if item["registration_id"] == registration_id and item["registration_version"] == registration_version
        )
        assert agent["gateway_routes"] == [
            {
                "policy_revision": policy_revision,
                "role": "developer",
                "model_alias": "complex",
                "max_budget_usd": 25.0,
                "toolset": "development-restricted",
            }
        ]
        assert detail["manifest_sha256"] == agent["manifest_sha256"]
        binding = next(item for item in list(history["items"]) if item["run_id"] == run_id)
        assert binding["registration_id"] == registration_id
        assert binding["registration_version"] == registration_version
        assert binding["gateway_route"]["policy_revision"] == policy_revision
        assert invocation["run_id"] == run_id
        assert invocation["run_lifecycle_status"] == "QUEUED"
        assert invocation["workflow_available"] is False
        assert invocation["lifecycle_transitions"][-1]["to_status"] == "QUEUED"
        assert invocation["evidence"]["actual_cost"] == "unavailable"
        assert invocation["mcp_grants"] == []
        assert not {"worker_id", "trace_id", "result_artifact_uri", "error_summary"}.intersection(invocation)
        assert "kind-e2e-private-worker" not in json.dumps(invocation)
        assert "s3://kind-e2e/private-artifact" not in json.dumps(invocation)
        assert "kind-e2e-private-error" not in json.dumps(invocation)
    finally:
        if os.environ.get("COGITO_E2E_KEEP_RUNS") == "1":
            print(f"Retained Phase 18 Agent Operations run ID: {run_id}")
        else:
            harness.exec_python(
                f"deployment/{harness.release}-api", _DELETE_AGENT_OPERATIONS_PROBE.replace("__RUN_ID__", run_id)
            )


_CREATE_AGENT_OPERATIONS_PROBE = r'''
import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from cogito_api.config import load_settings
from cogito_api.models import AgentRunStatus
from cogito_api.registry import load_agent_gateway_policy, load_component_catalog
from cogito_api.supervisor import AgentRunRecord, PostgresSupervisorStore


async def main() -> None:
    settings = load_settings()
    catalog = load_component_catalog(Path(settings.registry_catalog_path))
    gateway_policy = load_agent_gateway_policy(Path(settings.registry_catalog_path), catalog)
    assignments = {
        item.registration_id: f"{item.registration_id}@{item.version}"
        for item in catalog.components
        if item.kind.value == "agent"
    }
    store = PostgresSupervisorStore(settings.supervisor_database_url)
    run_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        await store.bootstrap_registry(catalog.components, "phase12_planner_v1_1_0", assignments)
        await store.bootstrap_agent_gateway_policy(gateway_policy)
        await store.create_agent_run(
            AgentRunRecord(
                run_id=run_id,
                root_run_id=run_id,
                parent_run_id=None,
                agent_name="phase18-kind-e2e",
                status=AgentRunStatus.QUEUED,
                trace_id="a" * 32,
                created_at=timestamp,
                updated_at=timestamp,
                worker_id="kind-e2e-private-worker",
                result_artifact_uri="s3://kind-e2e/private-artifact",
                error_summary="kind-e2e-private-error",
            )
        )
        async with store._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE agent_runs
                    SET worker_id = :worker_id,
                        result_artifact_uri = :result_artifact_uri,
                        error_summary = :error_summary
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "worker_id": "kind-e2e-private-worker",
                    "result_artifact_uri": "s3://kind-e2e/private-artifact",
                    "error_summary": "kind-e2e-private-error",
                },
            )
        developer = next(item for item in catalog.components if item.registration_id == "developer")
        registration = await store.resolve_run_registration(
            run_id, "developer", "phase12_planner_v1_1_0", developer
        )
        await store.resolve_run_agent_gateway(run_id, "developer", "default", registration, gateway_policy)
        print(json.dumps({
            "run_id": run_id,
            "registration_id": developer.registration_id,
            "registration_version": developer.version,
            "policy_revision": gateway_policy.policy_revision,
        }))
    except BaseException:
        async with store._engine.begin() as connection:
            for statement in (
                "DELETE FROM run_mcp_tool_resolutions WHERE run_id = :run_id",
                "DELETE FROM run_agent_gateway_resolutions WHERE run_id = :run_id",
                "DELETE FROM run_registration_resolutions WHERE run_id = :run_id",
                "DELETE FROM agent_run_events WHERE run_id = :run_id",
                "DELETE FROM agent_runs WHERE run_id = :run_id",
            ):
                await connection.execute(text(statement), {"run_id": run_id})
        raise
    finally:
        await store._engine.dispose()


asyncio.run(main())
'''


_DELETE_AGENT_OPERATIONS_PROBE = r'''
import asyncio

from sqlalchemy import text

from cogito_api.config import load_settings
from cogito_api.supervisor import PostgresSupervisorStore


async def main() -> None:
    store = PostgresSupervisorStore(load_settings().supervisor_database_url)
    try:
        async with store._engine.begin() as connection:
            for statement in (
                "DELETE FROM run_mcp_tool_resolutions WHERE run_id = :run_id",
                "DELETE FROM run_agent_gateway_resolutions WHERE run_id = :run_id",
                "DELETE FROM run_registration_resolutions WHERE run_id = :run_id",
                "DELETE FROM agent_run_events WHERE run_id = :run_id",
                "DELETE FROM agent_runs WHERE run_id = :run_id",
            ):
                await connection.execute(text(statement), {"run_id": "__RUN_ID__"})
    finally:
        await store._engine.dispose()


asyncio.run(main())
'''
