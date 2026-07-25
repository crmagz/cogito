from __future__ import annotations

import pytest

from cogito_worker.models import RegistrationReference, RunEnvelope
from cogito_worker.registry import RegistryAuthorizationError, require_role, require_tool


def _reference(role: str) -> RegistrationReference:
    return RegistrationReference(
        role=role,
        registration_id=role,
        version="1.0.0",
        manifest_sha256="a" * 64,
        component_id=role,
        component_version="1.0.0",
    )


def test_resolved_envelope_requires_the_named_role() -> None:
    envelope = RunEnvelope(run_id="run-1", plan_ref="plan", spec_ref="spec", registry_resolutions=[_reference("planner")])

    assert require_role(envelope, "planner") == _reference("planner")
    with pytest.raises(RegistryAuthorizationError, match="developer"):
        require_role(envelope, "developer")


def test_legacy_envelope_remains_compatible_during_migration() -> None:
    envelope = RunEnvelope(run_id="run-1", plan_ref="plan", spec_ref="spec")

    assert require_role(envelope, "developer") is None


def test_resolved_role_rejects_an_ungranted_tool() -> None:
    resolution = _reference("planner")

    with pytest.raises(RegistryAuthorizationError, match="planning_model"):
        require_tool(resolution, "planning_model", "plan_generation")
