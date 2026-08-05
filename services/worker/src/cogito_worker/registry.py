"""Worker-side validation for Supervisor-pinned registry releases."""

from __future__ import annotations

from .models import RegistrationReference, RunEnvelope


class RegistryAuthorizationError(RuntimeError):
    """Raised before a role reaches a tool or provider without a pinned grant."""


def require_role(envelope: RunEnvelope, role: str) -> RegistrationReference | None:
    """Return the pinned role release or preserve legacy envelopes during migration."""

    if not envelope.registry_resolutions:
        return None
    for resolution in envelope.registry_resolutions:
        if resolution.role == role:
            return resolution
    raise RegistryAuthorizationError(f"run does not include a pinned '{role}' registration")


def require_tool(resolution: RegistrationReference | None, tool_id: str, scope: str) -> None:
    """Reject a tool call unless the pinned role release explicitly grants its scope."""

    if resolution is None:
        return
    for grant in resolution.grants:
        if grant.tool_id == tool_id and grant.scope == scope:
            return
    raise RegistryAuthorizationError(f"role '{resolution.role}' is not granted tool '{tool_id}' scope '{scope}'")


def require_mcp_tool(resolution: RegistrationReference | None, server_id: str, tool_name: str) -> None:
    """Reject an MCP invocation unless this run pins the exact server tool."""

    if resolution is None:
        return
    if any(grant.server_id == server_id and grant.tool_name == tool_name for grant in resolution.mcp_grants):
        return
    raise RegistryAuthorizationError(
        f"role '{resolution.role}' is not granted MCP server '{server_id}' tool '{tool_name}'"
    )
