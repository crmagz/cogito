"""Repeatable Kind coverage for governed MCP registration and gateway authorization."""

from __future__ import annotations

import json

import pytest

from .kind_helpers import KindHarness


pytestmark = pytest.mark.kind_e2e


def test_governed_mcp_registration_and_gateway_authorization_e2e() -> None:
    """Resolve policy, then prove a real run key permits only its MCP tool."""

    harness = KindHarness.from_environment(default_context="kind-cogito-observability")
    harness.assert_context()
    for deployment in ("api", "worker", "litellm", "readonly-mcp"):
        harness.kubectl(
            "-n",
            harness.namespace,
            "rollout",
            "status",
            f"deployment/{harness.release}-{deployment}",
            "--timeout=240s",
        )

    resolution = json.loads(
        harness.exec_python(f"deployment/{harness.release}-api", _POLICY_RESOLUTION_PROBE)
    )
    assert resolution == {
        "grant_count": 1,
        "grant_is_catalog_read": True,
    }

    gateway = json.loads(
        harness.exec_python(f"deployment/{harness.release}-worker", _GATEWAY_AUTHORIZATION_PROBE)
    )
    assert gateway == {
        "allowed_call_is_readonly": True,
        "allowed_initialize_status": 200,
        "allowed_list_status": 200,
        "allowed_tools": ["cogito_readonly-catalog_read"],
        "denied_initialize_status": 200,
        "denied_list_status": 200,
        "denied_tool_count": 0,
        "run_key_secrets_cleaned": True,
    }


_POLICY_RESOLUTION_PROBE = r'''
import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from cogito_api.config import load_settings
from cogito_api.models import AgentRunStatus
from cogito_api.registry import load_component_catalog, load_mcp_binding_policy
from cogito_api.supervisor import AgentRunRecord, PostgresSupervisorStore


async def main() -> None:
    settings = load_settings()
    catalog = load_component_catalog(Path(settings.registry_catalog_path))
    policy = load_mcp_binding_policy(Path(settings.registry_catalog_path), catalog)
    assignments = {
        item.registration_id: f"{item.registration_id}@{item.version}"
        for item in catalog.components
        if item.kind.value == "agent"
    }
    store = PostgresSupervisorStore(settings.supervisor_database_url)
    run_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        await store.bootstrap_registry(catalog.components, policy.policy_revision, assignments, policy)
        await store.create_agent_run(
            AgentRunRecord(
                run_id=run_id,
                root_run_id=run_id,
                parent_run_id=None,
                agent_name="governed-mcp-kind-e2e",
                status=AgentRunStatus.QUEUED,
                trace_id="0" * 32,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        developer = next(item for item in catalog.components if item.registration_id == "developer")
        await store.resolve_run_registration(run_id, "developer", policy.policy_revision, developer)
        grants = await store.resolve_run_mcp_tools(run_id, "developer", "default", policy.policy_revision)
        print(json.dumps({
            "grant_count": len(grants),
            "grant_is_catalog_read": [(grant.server_id, grant.tool_name) for grant in grants]
            == [("cogito_readonly_mcp", "catalog_read")],
        }, sort_keys=True))
    finally:
        async with store._engine.begin() as connection:
            for statement in (
                "DELETE FROM run_mcp_tool_resolutions WHERE run_id = :run_id",
                "DELETE FROM run_registration_resolutions WHERE run_id = :run_id",
                "DELETE FROM agent_run_events WHERE run_id = :run_id",
                "DELETE FROM agent_runs WHERE run_id = :run_id",
            ):
                await connection.execute(text(statement), {"run_id": run_id})
        await store._engine.dispose()


asyncio.run(main())
'''


_GATEWAY_AUTHORIZATION_PROBE = r'''
import asyncio
import json
import uuid
from urllib.request import Request, urlopen

from cogito_worker.budgets import KubernetesLiteLLMRunKeyManager, RunBudget, _secret_token
from cogito_worker.config import load_settings


def decode_response(payload: str) -> dict:
    for line in payload.splitlines():
        if line.startswith("data:"):
            return json.loads(line.removeprefix("data:").strip())
    return json.loads(payload)


def mcp_request(endpoint: str, token: str, body: dict, session_id: str | None = None) -> tuple[int, str | None, dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-06-18",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    request = Request(endpoint, data=json.dumps(body).encode(), headers=headers, method="POST")
    with urlopen(request, timeout=20) as response:  # nosec B310: trusted in-cluster gateway setting
        return response.status, response.headers.get("Mcp-Session-Id"), decode_response(response.read().decode())


async def tools_for_permission(manager, settings, server_id: str, tool_name: str) -> tuple[int, int, list[str], bool, bool]:
    run_id = f"governed-mcp-kind-e2e-{uuid.uuid4()}"
    secret_name = ""
    result: tuple[int, int, list[str], bool] | None = None
    cleaned = False
    try:
        secret_name = await manager.provision(
            RunBudget(run_id, 0.01, settings.execution_litellm_model, 300, {server_id: (tool_name,)})
        )
        token = _secret_token(await manager._read_secret(secret_name))
        assert token is not None
        endpoint = settings.execution_litellm_endpoint.rstrip("/") + "/cogito_readonly/mcp"
        initialize_status, session_id, _ = mcp_request(
            endpoint,
            token,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "governed-mcp-kind-e2e", "version": "1"},
                },
            },
        )
        list_status, _, listed = mcp_request(
            endpoint, token, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, session_id
        )
        tools = [item["name"] for item in listed.get("result", {}).get("tools", [])]
        readonly = False
        if tool_name == "catalog_read":
            call_status, _, called = mcp_request(
                endpoint,
                token,
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "catalog_read", "arguments": {}},
                },
                session_id,
            )
            assert call_status == 200
            readonly = called["result"]["structuredContent"]["read_only"] is True
        result = (initialize_status, list_status, tools, readonly)
    finally:
        if secret_name:
            await manager.cleanup(run_id, secret_name)
            cleaned = await manager._read_secret(secret_name) is None
        if secret_name and not cleaned:
            raise RuntimeError("temporary MCP run-key Secret was not removed")
    assert result is not None
    return (*result, cleaned)


async def main() -> None:
    settings = load_settings()
    server_id = settings.execution_mcp_gateway_server_ids["cogito_readonly_mcp"]
    manager = KubernetesLiteLLMRunKeyManager(
        settings.execution_namespace,
        settings.execution_litellm_endpoint,
        settings.execution_litellm_management_key,
    )
    allowed_initialize, allowed_list, allowed_tools, allowed_readonly, allowed_cleaned = await tools_for_permission(
        manager, settings, server_id, "catalog_read"
    )
    denied_initialize, denied_list, denied_tools, _, denied_cleaned = await tools_for_permission(
        manager, settings, server_id, "not_allowed"
    )
    print(json.dumps({
        "allowed_call_is_readonly": allowed_readonly,
        "allowed_initialize_status": allowed_initialize,
        "allowed_list_status": allowed_list,
        "allowed_tools": allowed_tools,
        "denied_initialize_status": denied_initialize,
        "denied_list_status": denied_list,
        "denied_tool_count": len(denied_tools),
        "run_key_secrets_cleaned": allowed_cleaned and denied_cleaned,
    }, sort_keys=True))


asyncio.run(main())
'''
