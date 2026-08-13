"""Opt-in Kind proof for the GitHub read-only MCP's live trust boundary."""

from __future__ import annotations

import json
import os

import pytest

from .kind_helpers import KindHarness


pytestmark = pytest.mark.kind_e2e

_FIXTURE_REPOSITORY = "crmagz/cogito-kind-e2e-fixture"
_GITHUB_SERVER_RELEASE = "github_readonly_mcp@1.0.0"
_GITHUB_MANIFEST_SHA256 = "33aab86822040fa5880ce0f9d89eebd000d24fc3c9986e020bd7b89ac0e2a298"
_REPOSITORY_GET_SCHEMA_SHA256 = "c1c5fc9c248210272b3821c6482812ee03cd4e43f682f5f18151e2c4f7c6d2ac"


def test_github_readonly_mcp_e2e() -> None:
    """Prove a run key reaches one fixture repository and nothing broader."""

    if os.environ.get("COGITO_E2E_GITHUB_MCP") != "1":
        pytest.skip("set COGITO_E2E_GITHUB_MCP=1 to exercise the live GitHub App connector")

    repository = os.environ.get("COGITO_E2E_GITHUB_MCP_REPOSITORY", _FIXTURE_REPOSITORY).strip().casefold()
    if repository != _FIXTURE_REPOSITORY:
        pytest.fail("the GitHub MCP Kind E2E is pinned to the disposable fixture repository")

    harness = KindHarness.from_environment(default_context="kind-cogito-observability")
    harness.assert_context()
    for deployment in ("worker", "litellm", "github-readonly-mcp"):
        harness.kubectl(
            "-n",
            harness.namespace,
            "rollout",
            "status",
            f"deployment/{harness.release}-{deployment}",
            "--timeout=240s",
        )

    result = json.loads(
        harness.exec_python(
            f"deployment/{harness.release}-worker",
            _GITHUB_GATEWAY_PROBE.replace("__FIXTURE_REPOSITORY__", json.dumps(repository)),
        )
    )
    assert result == {
        "allowed_call_succeeds": True,
        "allowed_initialize_status": 200,
        "allowed_tools": ["github_readonly_90bc59fea7dc-repository_get"],
        "cross_repository_call_is_denied": True,
        "invocation_evidence": {
            "version": 1,
            "status": "observed",
            "events": [
                {
                    "server_id": "github_readonly_mcp",
                    "server_version": "1.0.0",
                    "server_manifest_sha256": _GITHUB_MANIFEST_SHA256,
                    "tool_name": "repository_get",
                    "input_schema_sha256": _REPOSITORY_GET_SCHEMA_SHA256,
                    # LiteLLM observes the permitted tool route before the
                    # connector rejects the cross-repository input.
                    "outcome": "success",
                    "invocation_count": 2,
                }
            ],
        },
        "run_key_secret_cleaned": True,
    }


_GITHUB_GATEWAY_PROBE = r'''
import asyncio
import json
import uuid
from urllib.request import Request, urlopen

from cogito_worker.budgets import KubernetesLiteLLMRunKeyManager, RunBudget, _secret_token
from cogito_worker.config import load_settings
from cogito_worker.models import McpToolGrant


def _decode_response(payload: str) -> dict:
    for line in payload.splitlines():
        if line.startswith("data:"):
            return json.loads(line.removeprefix("data:").strip())
    return json.loads(payload)


def _mcp_request(endpoint: str, token: str, body: dict, session_id: str | None = None) -> tuple[int, str | None, dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-06-18",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    request = Request(endpoint, data=json.dumps(body).encode(), headers=headers, method="POST")
    with urlopen(request, timeout=30) as response:  # nosec B310: trusted in-cluster gateway setting
        return response.status, response.headers.get("Mcp-Session-Id"), _decode_response(response.read().decode())


async def main() -> None:
    settings = load_settings()
    server = settings.execution_mcp_gateway_servers["github_readonly_mcp@1.0.0"]
    fixture_repository = __FIXTURE_REPOSITORY__
    if server.repository_scope != fixture_repository:
        raise RuntimeError("GitHub MCP gateway scope does not match the disposable fixture repository")

    manager = KubernetesLiteLLMRunKeyManager(
        settings.execution_namespace,
        settings.execution_litellm_endpoint,
        settings.execution_litellm_management_key,
    )
    run_id = f"github-readonly-mcp-kind-e2e-{uuid.uuid4()}"
    secret_name = ""
    cleaned = False
    result = None
    try:
        secret_name = await manager.provision(
            RunBudget(run_id, 0.01, settings.execution_litellm_model, 300, {server.gateway_server_id: ("repository_get",)})
        )
        token = _secret_token(await manager._read_secret(secret_name))
        if token is None:
            raise RuntimeError("run key Secret has no API key")
        endpoint = f"{settings.execution_litellm_endpoint.rstrip('/')}/{server.route}/mcp"
        initialize_status, session_id, _ = _mcp_request(
            endpoint,
            token,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "github-readonly-mcp-kind-e2e", "version": "1"},
                },
            },
        )
        if session_id is None:
            raise RuntimeError("LiteLLM MCP initialization returned no session")
        _, _, listed = _mcp_request(endpoint, token, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, session_id)
        _, _, allowed = _mcp_request(
            endpoint,
            token,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "repository_get", "arguments": {"repository": fixture_repository}},
            },
            session_id,
        )
        _, _, denied = _mcp_request(
            endpoint,
            token,
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "repository_get", "arguments": {"repository": "crmagz/cogito"}},
            },
            session_id,
        )
        grant = McpToolGrant(
            server_id="github_readonly_mcp",
            server_version="1.0.0",
            server_manifest_sha256="33aab86822040fa5880ce0f9d89eebd000d24fc3c9986e020bd7b89ac0e2a298",
            tool_name="repository_get",
            input_schema_sha256="c1c5fc9c248210272b3821c6482812ee03cd4e43f682f5f18151e2c4f7c6d2ac",
            repository_scope=server.repository_scope,
        )
        evidence = await manager.collect_mcp_invocations(
            run_id,
            secret_name,
            [grant],
            {("github_readonly_mcp", "1.0.0"): server.route},
        )
        result = {
            "allowed_call_succeeds": not allowed.get("result", {}).get("isError", True),
            "allowed_initialize_status": initialize_status,
            "allowed_tools": [tool["name"] for tool in listed.get("result", {}).get("tools", [])],
            "cross_repository_call_is_denied": denied.get("result", {}).get("isError") is True,
            "invocation_evidence": evidence,
        }
    finally:
        if secret_name:
            await manager.cleanup(run_id, secret_name)
            cleaned = await manager._read_secret(secret_name) is None
        if secret_name and not cleaned:
            raise RuntimeError("temporary MCP run-key Secret was not removed")
    if result is None:
        raise RuntimeError("GitHub MCP proof did not produce a result")
    result["run_key_secret_cleaned"] = cleaned
    print(json.dumps(result, sort_keys=True))


asyncio.run(main())
'''
