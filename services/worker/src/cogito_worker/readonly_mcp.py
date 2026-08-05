"""A deterministic, credentialless MCP server used for governed-path validation."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

_SERVER = FastMCP("Cogito Readonly", host="0.0.0.0", port=8000)


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
