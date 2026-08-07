from __future__ import annotations

import json

import pytest

from cogito_worker.config import load_settings


def test_load_settings_parses_an_exact_mcp_release_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "COGITO_EXECUTION_MCP_GATEWAY_SERVERS",
        json.dumps(
            {
                "cogito_readonly_mcp@1.0.1": {
                    "gateway_server_id": "a" * 32,
                    "route": "cogito_readonly",
                    "server_manifest_sha256": "b" * 64,
                    "tool_names": ["catalog_read"],
                }
            }
        ),
    )

    settings = load_settings()

    server = settings.execution_mcp_gateway_servers["cogito_readonly_mcp@1.0.1"]
    assert server.gateway_server_id == "a" * 32
    assert server.server_manifest_sha256 == "b" * 64
    assert server.tool_names == ("catalog_read",)


def test_load_settings_normalizes_a_repository_scoped_mcp_release(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "COGITO_EXECUTION_MCP_GATEWAY_SERVERS",
        json.dumps(
            {
                "github_readonly_mcp@1.0.0": {
                    "gateway_server_id": "a" * 32,
                    "route": "github_readonly_abcd1234",
                    "server_manifest_sha256": "b" * 64,
                    "tool_names": ["repository_get"],
                    "repository_scope": "Acme/Widget",
                }
            }
        ),
    )

    settings = load_settings()

    assert settings.execution_mcp_gateway_servers["github_readonly_mcp@1.0.0"].repository_scope == "acme/widget"


def test_load_settings_rejects_incomplete_mcp_release_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "COGITO_EXECUTION_MCP_GATEWAY_SERVERS",
        '{"cogito_readonly_mcp@1.0.1":{"route":"cogito_readonly"}}',
    )

    with pytest.raises(ValueError, match="complete immutable gateway mapping"):
        load_settings()
