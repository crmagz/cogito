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
        if (grant.tool_id, grant.tool_version, grant.scope) == (tool_id, "1.0.0", scope):
            return
    raise RegistryAuthorizationError(f"role '{resolution.role}' is not granted tool '{tool_id}' scope '{scope}'")
