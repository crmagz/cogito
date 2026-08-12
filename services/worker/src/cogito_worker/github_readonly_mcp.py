"""A bounded GitHub App-backed read-only MCP connector."""

from __future__ import annotations

import base64
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from mcp.server.fastmcp import FastMCP

_MAX_TEXT_BYTES = 16 * 1024
_MAX_FILE_BYTES = 64 * 1024
_MAX_FILE_BASE64_BYTES = 4 * ((_MAX_FILE_BYTES + 2) // 3)
_REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@+-]{0,255}$")


class GitHubConnectorError(RuntimeError):
    """Raised when a GitHub operation cannot safely return a result."""


@dataclass(frozen=True)
class GitHubConnectorSettings:
    """Non-secret runtime configuration for one GitHub App installation."""

    app_id: str
    installation_id: str
    private_key_file: Path
    api_url: str
    api_version: str
    allowed_repositories: tuple[str, ...]
    timeout_seconds: float

    @classmethod
    def from_environment(cls) -> "GitHubConnectorSettings":
        """Load a fail-closed GitHub App configuration from connector-only inputs."""

        app_id = os.environ.get("COGITO_GITHUB_MCP_APP_ID", "").strip()
        installation_id = os.environ.get("COGITO_GITHUB_MCP_INSTALLATION_ID", "").strip()
        private_key_file = Path(os.environ.get("COGITO_GITHUB_MCP_PRIVATE_KEY_FILE", "").strip())
        api_url = os.environ.get("COGITO_GITHUB_MCP_API_URL", "https://api.github.com").rstrip("/")
        api_version = os.environ.get("COGITO_GITHUB_MCP_API_VERSION", "2026-03-10").strip()
        timeout_value = os.environ.get("COGITO_GITHUB_MCP_TIMEOUT_SECONDS", "10")
        repositories = _string_array("COGITO_GITHUB_MCP_ALLOWED_REPOSITORIES")

        if not app_id.isdecimal() or not installation_id.isdecimal():
            raise ValueError("GitHub App and installation IDs must be positive decimal values")
        if int(app_id) < 1 or int(installation_id) < 1:
            raise ValueError("GitHub App and installation IDs must be positive decimal values")
        if not str(private_key_file) or not private_key_file.is_absolute():
            raise ValueError("COGITO_GITHUB_MCP_PRIVATE_KEY_FILE must be an absolute path")
        parsed = urlparse(api_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query:
            raise ValueError("COGITO_GITHUB_MCP_API_URL must be an HTTPS URL without credentials or query")
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", api_version):
            raise ValueError("COGITO_GITHUB_MCP_API_VERSION must be a GitHub API date")
        if len(repositories) != 1:
            raise ValueError("COGITO_GITHUB_MCP_ALLOWED_REPOSITORIES must identify exactly one repository")
        if not all(_REPOSITORY_PATTERN.fullmatch(repository) for repository in repositories):
            raise ValueError("COGITO_GITHUB_MCP_ALLOWED_REPOSITORIES contains an invalid repository")
        try:
            timeout_seconds = float(timeout_value)
        except ValueError as error:
            raise ValueError("COGITO_GITHUB_MCP_TIMEOUT_SECONDS must be a positive number") from error
        if not 0 < timeout_seconds <= 60:
            raise ValueError("COGITO_GITHUB_MCP_TIMEOUT_SECONDS must be between 0 and 60")
        return cls(
            app_id=app_id,
            installation_id=installation_id,
            private_key_file=private_key_file,
            api_url=api_url,
            api_version=api_version,
            allowed_repositories=(repositories[0].casefold(),),
            timeout_seconds=timeout_seconds,
        )


class GitHubAppClient:
    """Read-only GitHub REST client with a repository-scoped installation token."""

    def __init__(self, settings: GitHubConnectorSettings, client: httpx.Client | None = None) -> None:
        self._settings = settings
        self._http = client or httpx.Client(timeout=settings.timeout_seconds)
        self._installation_token: str | None = None
        self._token_refresh_after = 0.0

    def get_repository(self, repository: str) -> dict[str, object]:
        """Return bounded repository metadata for an allow-listed repository."""

        value = self._get(repository, "")
        return {
            "repository": repository,
            "description": _bounded_text(value.get("description")),
            "default_branch": _string(value.get("default_branch")),
            "visibility": _string(value.get("visibility")),
            "archived": value.get("archived") is True,
            "updated_at": _string(value.get("updated_at")),
            "html_url": _string(value.get("html_url")),
        }

    def get_file(self, repository: str, path: str, ref: str | None = None) -> dict[str, object]:
        """Return UTF-8 file content bounded to a safe MCP response size."""

        _validate_file_path(path)
        if ref is not None and (not _REF_PATTERN.fullmatch(ref) or ".." in ref):
            raise GitHubConnectorError("GitHub ref is invalid")
        query = {"ref": ref} if ref else None
        value = self._get(repository, f"contents/{quote(path, safe='/')}", params=query)
        if value.get("type") != "file" or not isinstance(value.get("content"), str):
            raise GitHubConnectorError("GitHub path does not identify a file")
        encoded = value["content"]
        declared_size = value.get("size")
        if isinstance(declared_size, int) and declared_size > _MAX_FILE_BYTES:
            raise GitHubConnectorError("GitHub file exceeds the connector response limit")
        encoded_length = len(encoded) - encoded.count("\n")
        if encoded_length > _MAX_FILE_BASE64_BYTES:
            raise GitHubConnectorError("GitHub file exceeds the connector response limit")
        normalized = encoded.replace("\n", "")
        padding_bytes = 2 if normalized.endswith("==") else 1 if normalized.endswith("=") else 0
        if (len(normalized) // 4) * 3 - padding_bytes > _MAX_FILE_BYTES:
            raise GitHubConnectorError("GitHub file exceeds the connector response limit")
        try:
            decoded = base64.b64decode(normalized, validate=True)
            content = decoded.decode("utf-8")
        except (UnicodeDecodeError, ValueError) as error:
            raise GitHubConnectorError("GitHub file is not valid UTF-8 text") from error
        if len(decoded) > _MAX_FILE_BYTES:
            raise GitHubConnectorError("GitHub file exceeds the connector response limit")
        return {
            "repository": repository,
            "path": path,
            "ref": ref,
            "sha": _string(value.get("sha")),
            "size": value.get("size") if isinstance(value.get("size"), int) else len(decoded),
            "content": content,
        }

    def get_issue(self, repository: str, number: int) -> dict[str, object]:
        """Return a bounded issue representation without comments or mutations."""

        value = self._get(repository, f"issues/{_issue_number(number)}")
        return _issue_result(repository, number, value)

    def get_pull_request(self, repository: str, number: int) -> dict[str, object]:
        """Return bounded pull-request metadata and description without file diffs."""

        value = self._get(repository, f"pulls/{_issue_number(number)}")
        head = value.get("head") if isinstance(value.get("head"), dict) else {}
        base = value.get("base") if isinstance(value.get("base"), dict) else {}
        return {
            **_issue_result(repository, number, value),
            "draft": value.get("draft") is True,
            "merged": value.get("merged") is True,
            "head": _string(head.get("ref")),
            "base": _string(base.get("ref")),
        }

    def _get(self, repository: str, suffix: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        self._authorize_repository(repository)
        path = f"/repos/{quote(repository.split('/', 1)[0], safe='')}/{quote(repository.split('/', 1)[1], safe='')}"
        if suffix:
            path += f"/{suffix}"
        response = self._request("GET", path, params=params)
        value = _json_object(response)
        return value

    def _request(
        self, method: str, path: str, *, params: dict[str, str] | None = None, json_body: dict[str, object] | None = None
    ) -> httpx.Response:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._access_token()}",
            "X-GitHub-Api-Version": self._settings.api_version,
        }
        try:
            response = self._http.request(method, f"{self._settings.api_url}{path}", headers=headers, params=params, json=json_body)
        except httpx.HTTPError as error:
            raise GitHubConnectorError("GitHub API request failed") from error
        if response.status_code < 200 or response.status_code >= 300:
            raise GitHubConnectorError(f"GitHub API request failed with status {response.status_code}")
        return response

    def _access_token(self) -> str:
        if self._installation_token is not None and time.monotonic() < self._token_refresh_after:
            return self._installation_token
        app_jwt = _app_jwt(self._settings.app_id, self._settings.private_key_file)
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {app_jwt}",
            "X-GitHub-Api-Version": self._settings.api_version,
        }
        body: dict[str, object] = {
            "repositories": [repository.split("/", 1)[1] for repository in self._settings.allowed_repositories],
            "permissions": {"contents": "read", "issues": "read", "pull_requests": "read"},
        }
        try:
            response = self._http.post(
                f"{self._settings.api_url}/app/installations/{self._settings.installation_id}/access_tokens",
                headers=headers,
                json=body,
            )
        except httpx.HTTPError as error:
            raise GitHubConnectorError("GitHub installation-token request failed") from error
        if response.status_code != 201:
            raise GitHubConnectorError(f"GitHub installation-token request failed with status {response.status_code}")
        value = _json_object(response)
        token = value.get("token")
        if not isinstance(token, str) or not token:
            raise GitHubConnectorError("GitHub installation-token response is invalid")
        self._installation_token = token
        # GitHub installation tokens are currently valid for one hour. Refresh
        # early without parsing an untrusted timestamp from the response.
        self._token_refresh_after = time.monotonic() + 45 * 60
        return token

    def _authorize_repository(self, repository: str) -> None:
        if repository.casefold() not in {allowed.casefold() for allowed in self._settings.allowed_repositories}:
            raise GitHubConnectorError("GitHub repository is not allow-listed for this connector")


def mcp_port() -> int:
    """Read the chart-configured GitHub MCP listener port with a safe bound."""

    try:
        port = int(os.environ.get("COGITO_GITHUB_MCP_PORT", "8000"))
    except ValueError as error:
        raise ValueError("COGITO_GITHUB_MCP_PORT must be an integer") from error
    if not 1 <= port <= 65535:
        raise ValueError("COGITO_GITHUB_MCP_PORT must be between 1 and 65535")
    return port


_SERVER = FastMCP("Cogito GitHub Readonly", host="0.0.0.0", port=mcp_port())
_client: GitHubAppClient | None = None


def _connector() -> GitHubAppClient:
    global _client
    if _client is None:
        _client = GitHubAppClient(GitHubConnectorSettings.from_environment())
    return _client


@_SERVER.tool(name="repository_get", description="Read bounded metadata for an allow-listed GitHub repository.")
def repository_get(repository: str) -> dict[str, object]:
    """Read repository metadata; this tool cannot modify GitHub."""

    return _connector().get_repository(repository)


@_SERVER.tool(name="file_get", description="Read one bounded UTF-8 file from an allow-listed GitHub repository.")
def file_get(repository: str, path: str, ref: str | None = None) -> dict[str, object]:
    """Read one file at an optional ref; this tool cannot modify GitHub."""

    return _connector().get_file(repository, path, ref)


@_SERVER.tool(name="issue_get", description="Read one bounded issue from an allow-listed GitHub repository.")
def issue_get(repository: str, number: int) -> dict[str, object]:
    """Read one issue; this tool cannot modify GitHub."""

    return _connector().get_issue(repository, number)


@_SERVER.tool(name="pull_request_get", description="Read one bounded pull request from an allow-listed GitHub repository.")
def pull_request_get(repository: str, number: int) -> dict[str, object]:
    """Read one pull request; this tool cannot modify GitHub."""

    return _connector().get_pull_request(repository, number)


def main() -> None:
    """Validate connector-only configuration before serving streamable HTTP."""

    _connector()
    _SERVER.run(transport="streamable-http")


def _string_array(name: str) -> list[str]:
    try:
        value = json.loads(os.environ.get(name, "[]"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{name} must be a JSON string array") from error
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{name} must be a JSON string array")
    return value


def _app_jwt(app_id: str, private_key_file: Path) -> str:
    try:
        private_key = serialization.load_pem_private_key(private_key_file.read_bytes(), password=None)
    except (OSError, ValueError, TypeError) as error:
        raise GitHubConnectorError("GitHub App private key is unavailable or invalid") from error
    now = int(time.time())
    header = _base64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = _base64url(
        json.dumps({"iat": now - 60, "exp": now + 540, "iss": app_id}, separators=(",", ":")).encode()
    )
    signing_input = f"{header}.{claims}".encode()
    try:
        signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    except (AttributeError, TypeError, ValueError) as error:
        raise GitHubConnectorError("GitHub App private key cannot sign RS256 tokens") from error
    return f"{header}.{claims}.{_base64url(signature)}"


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _json_object(response: httpx.Response) -> dict[str, Any]:
    try:
        value = response.json()
    except ValueError as error:
        raise GitHubConnectorError("GitHub API response is invalid") from error
    if not isinstance(value, dict):
        raise GitHubConnectorError("GitHub API response is invalid")
    return value


def _validate_file_path(path: str) -> None:
    if not path or len(path) > 1024 or path.startswith("/") or "\\" in path or any(part in {"", ".", ".."} for part in path.split("/")):
        raise GitHubConnectorError("GitHub file path is invalid")


def _issue_number(number: int) -> str:
    if isinstance(number, bool) or not isinstance(number, int) or number < 1:
        raise GitHubConnectorError("GitHub issue or pull-request number must be positive")
    return str(number)


def _issue_result(repository: str, number: int, value: dict[str, Any]) -> dict[str, object]:
    return {
        "repository": repository,
        "number": number,
        "title": _bounded_text(value.get("title")),
        "body": _bounded_text(value.get("body")),
        "state": _string(value.get("state")),
        "author": _string(value.get("user", {}).get("login")) if isinstance(value.get("user"), dict) else None,
        "html_url": _string(value.get("html_url")),
        "updated_at": _string(value.get("updated_at")),
    }


def _bounded_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    encoded = value.encode("utf-8")
    if len(encoded) <= _MAX_TEXT_BYTES:
        return value
    return encoded[:_MAX_TEXT_BYTES].decode("utf-8", errors="ignore") + "\n[truncated]"


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


if __name__ == "__main__":
    main()
