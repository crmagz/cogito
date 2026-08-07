from __future__ import annotations

import base64
import json
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from cogito_worker import github_readonly_mcp
from cogito_worker.github_readonly_mcp import (
    GitHubAppClient,
    GitHubConnectorError,
    GitHubConnectorSettings,
    _MAX_FILE_BYTES,
)


def _settings(tmp_path: Path) -> GitHubConnectorSettings:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key_file = tmp_path / "github-app.pem"
    key_file.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return GitHubConnectorSettings(
        app_id="123",
        installation_id="456",
        private_key_file=key_file,
        api_url="https://api.github.test",
        api_version="2026-03-10",
        allowed_repositories=("acme/widget",),
        timeout_seconds=10,
    )


def _client(tmp_path: Path, responses: list[dict[str, object]]) -> tuple[GitHubAppClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = responses.pop(0)
        return httpx.Response(int(payload.pop("status", 200)), json=payload, request=request)

    return GitHubAppClient(_settings(tmp_path), httpx.Client(transport=httpx.MockTransport(handler))), requests


def test_repository_reads_use_a_repository_scoped_installation_token(tmp_path: Path) -> None:
    client, requests = _client(
        tmp_path,
        [
            {"status": 201, "token": "installation-token"},
            {
                "full_name": "acme/widget",
                "description": "Useful component",
                "default_branch": "main",
                "visibility": "private",
                "archived": False,
                "updated_at": "2026-08-06T00:00:00Z",
                "html_url": "https://github.com/acme/widget",
            },
        ],
    )

    result = client.get_repository("acme/widget")

    assert result["repository"] == "acme/widget"
    assert [request.url.path for request in requests] == [
        "/app/installations/456/access_tokens",
        "/repos/acme/widget",
    ]
    assert json.loads(requests[0].content) == {
        "repositories": ["widget"],
        "permissions": {"contents": "read", "issues": "read", "pull_requests": "read"},
    }
    assert requests[1].headers["authorization"] == "Bearer installation-token"
    assert requests[0].headers["authorization"].startswith("Bearer ey")
    assert requests[1].headers["x-github-api-version"] == "2026-03-10"


def test_file_read_accepts_github_base64_line_wrapping_and_returns_text(tmp_path: Path) -> None:
    content = base64.b64encode(b"line one\nline two\n").decode()
    client, _ = _client(
        tmp_path,
        [
            {"status": 201, "token": "token"},
            {"type": "file", "content": content[:8] + "\n" + content[8:], "sha": "a" * 40, "size": 18},
        ],
    )

    assert client.get_file("acme/widget", "docs/readme.txt", "main") == {
        "repository": "acme/widget",
        "path": "docs/readme.txt",
        "ref": "main",
        "sha": "a" * 40,
        "size": 18,
        "content": "line one\nline two\n",
    }


def test_connector_rejects_unallowlisted_repositories_without_contacting_github(tmp_path: Path) -> None:
    client, requests = _client(tmp_path, [])

    with pytest.raises(GitHubConnectorError, match="not allow-listed"):
        client.get_repository("acme/other")

    assert requests == []


def test_connector_compares_github_repository_names_case_insensitively(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = GitHubAppClient(
        GitHubConnectorSettings(
            app_id=settings.app_id,
            installation_id=settings.installation_id,
            private_key_file=settings.private_key_file,
            api_url=settings.api_url,
            api_version=settings.api_version,
            allowed_repositories=("Acme/Widget",),
            timeout_seconds=settings.timeout_seconds,
        )
    )

    client._authorize_repository("acme/widget")


def test_connector_bounds_file_responses_and_masks_api_failure_bodies(tmp_path: Path) -> None:
    oversized = base64.b64encode(b"x" * (_MAX_FILE_BYTES + 1)).decode()
    client, _ = _client(
        tmp_path,
        [{"status": 201, "token": "token"}, {"type": "file", "content": oversized}],
    )
    with pytest.raises(GitHubConnectorError, match="response limit"):
        client.get_file("acme/widget", "large.txt")

    client, _ = _client(
        tmp_path,
        [{"status": 201, "token": "token"}, {"status": 403, "message": "token=secret"}],
    )
    with pytest.raises(GitHubConnectorError, match=r"status 403") as error:
        client.get_issue("acme/widget", 7)
    assert "secret" not in str(error.value)


def test_environment_configuration_requires_one_repository_and_an_absolute_key_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_GITHUB_MCP_APP_ID", "1")
    monkeypatch.setenv("COGITO_GITHUB_MCP_INSTALLATION_ID", "2")
    monkeypatch.setenv("COGITO_GITHUB_MCP_PRIVATE_KEY_FILE", "relative.pem")
    monkeypatch.setenv("COGITO_GITHUB_MCP_ALLOWED_REPOSITORIES", '["acme/one", "other/two"]')

    with pytest.raises(ValueError, match="absolute path"):
        GitHubConnectorSettings.from_environment()

    monkeypatch.setenv("COGITO_GITHUB_MCP_PRIVATE_KEY_FILE", "/run/secrets/key")
    with pytest.raises(ValueError, match="exactly one repository"):
        GitHubConnectorSettings.from_environment()


def test_main_starts_the_streamable_http_server(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[str] = []

    monkeypatch.setattr(github_readonly_mcp, "_connector", lambda: object())
    monkeypatch.setattr(github_readonly_mcp._SERVER, "run", lambda *, transport: started.append(transport))

    github_readonly_mcp.main()

    assert started == ["streamable-http"]
