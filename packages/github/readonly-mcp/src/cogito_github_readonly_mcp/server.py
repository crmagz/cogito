"""MCP transport entry point for the GitHub read-only connector."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from .client import GitHubAppClient
from .config import GitHubConnectorSettings, mcp_port

_SERVER = FastMCP("Cogito GitHub Readonly", host="0.0.0.0", port=mcp_port())
_client: GitHubAppClient | None = None


def _connector() -> GitHubAppClient:
    """Create the connector lazily after its runtime configuration is validated."""

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
