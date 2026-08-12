"""Bounded, read-only GitHub App REST client."""

from __future__ import annotations

import base64
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .config import GitHubConnectorSettings
from .errors import GitHubConnectorError

_MAX_TEXT_BYTES = 16 * 1024
_MAX_FILE_BYTES = 64 * 1024
_MAX_FILE_BASE64_BYTES = 4 * ((_MAX_FILE_BYTES + 2) // 3)
_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@+-]{0,255}$")


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
        value = self._get(repository, f"contents/{quote(path, safe='/')}", params={"ref": ref} if ref else None)
        if value.get("type") != "file" or not isinstance(value.get("content"), str):
            raise GitHubConnectorError("GitHub path does not identify a file")
        encoded = value["content"]
        declared_size = value.get("size")
        if isinstance(declared_size, int) and declared_size > _MAX_FILE_BYTES:
            raise GitHubConnectorError("GitHub file exceeds the connector response limit")
        normalized = encoded.replace("\n", "")
        padding_bytes = 2 if normalized.endswith("==") else 1 if normalized.endswith("=") else 0
        if (
            len(encoded) - encoded.count("\n") > _MAX_FILE_BASE64_BYTES
            or (len(normalized) // 4) * 3 - padding_bytes > _MAX_FILE_BYTES
        ):
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
        return _issue_result(repository, number, self._get(repository, f"issues/{_issue_number(number)}"))

    def get_pull_request(self, repository: str, number: int) -> dict[str, object]:
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
        owner, name = repository.split("/", 1)
        response = self._request(
            "GET",
            f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}" + (f"/{suffix}" if suffix else ""),
            params=params,
        )
        return _json_object(response)

    def _request(self, method: str, path: str, *, params: dict[str, str] | None = None) -> httpx.Response:
        try:
            response = self._http.request(
                method,
                f"{self._settings.api_url}{path}",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {self._access_token()}",
                    "X-GitHub-Api-Version": self._settings.api_version,
                },
                params=params,
            )
        except httpx.HTTPError as error:
            raise GitHubConnectorError("GitHub API request failed") from error
        if not 200 <= response.status_code < 300:
            raise GitHubConnectorError(f"GitHub API request failed with status {response.status_code}")
        return response

    def _access_token(self) -> str:
        if self._installation_token is not None and time.monotonic() < self._token_refresh_after:
            return self._installation_token
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {_app_jwt(self._settings.app_id, self._settings.private_key_file)}",
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
        token = _json_object(response).get("token")
        if not isinstance(token, str) or not token:
            raise GitHubConnectorError("GitHub installation-token response is invalid")
        self._installation_token, self._token_refresh_after = token, time.monotonic() + 45 * 60
        return token

    def _authorize_repository(self, repository: str) -> None:
        if repository.casefold() not in {allowed.casefold() for allowed in self._settings.allowed_repositories}:
            raise GitHubConnectorError("GitHub repository is not allow-listed for this connector")


def _app_jwt(app_id: str, private_key_file: Path) -> str:
    try:
        private_key = serialization.load_pem_private_key(private_key_file.read_bytes(), password=None)
    except (OSError, ValueError, TypeError) as error:
        raise GitHubConnectorError("GitHub App private key is unavailable or invalid") from error
    now = int(time.time())
    header = _base64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = _base64url(json.dumps({"iat": now - 60, "exp": now + 540, "iss": app_id}, separators=(",", ":")).encode())
    try:
        signature = private_key.sign(f"{header}.{claims}".encode(), padding.PKCS1v15(), hashes.SHA256())
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
    if (
        not path
        or len(path) > 1024
        or path.startswith("/")
        or "\\" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
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
