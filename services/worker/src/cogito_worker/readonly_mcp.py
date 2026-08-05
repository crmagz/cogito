"""A deterministic, credentialless MCP server used for governed-path validation."""

from __future__ import annotations

import os

from mcp.server.fastmcp import FastMCP


def mcp_port() -> int:
    """Read the chart-configured listener port with a fail-closed bound."""

    try:
        port = int(os.environ.get("COGITO_MCP_PORT", "8000"))
    except ValueError as error:
        raise ValueError("COGITO_MCP_PORT must be an integer") from error
    if not 1 <= port <= 65535:
        raise ValueError("COGITO_MCP_PORT must be between 1 and 65535")
    return port


_SERVER = FastMCP("Cogito Readonly", host="0.0.0.0", port=mcp_port())


@_SERVER.tool(name="catalog_read", description="Read the fixed Cogito capability catalog.")
def catalog_read() -> dict[str, object]:
    """Return an immutable fixture without accessing credentials or external systems."""

    return {
        "catalog_version": "1.0.0",
        "capabilities": ["governed_mcp_validation"],
        "read_only": True,
    }


def main() -> None:
    """Serve the internal validation tool over streamable HTTP."""

    _SERVER.run(transport="streamable-http")


if __name__ == "__main__":
    main()
