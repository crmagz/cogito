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
