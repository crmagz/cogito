"""Validated, non-secret runtime configuration for the GitHub connector."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

_REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")


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

        if not app_id.isdecimal() or not installation_id.isdecimal() or int(app_id) < 1 or int(installation_id) < 1:
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
        return cls(app_id, installation_id, private_key_file, api_url, api_version, (repositories[0].casefold(),), timeout_seconds)


def mcp_port() -> int:
    """Read the chart-configured GitHub MCP listener port with a safe bound."""

    try:
        port = int(os.environ.get("COGITO_GITHUB_MCP_PORT", "8000"))
    except ValueError as error:
        raise ValueError("COGITO_GITHUB_MCP_PORT must be an integer") from error
    if not 1 <= port <= 65535:
        raise ValueError("COGITO_GITHUB_MCP_PORT must be between 1 and 65535")
    return port


def _string_array(name: str) -> list[str]:
    try:
        value = json.loads(os.environ.get(name, "[]"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{name} must be a JSON string array") from error
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{name} must be a JSON string array")
    return value
