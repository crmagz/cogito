from __future__ import annotations

import pytest
from temporalio.converter import DataConverter

from cogito_worker.models import RegistrationReference, RunEnvelope, ToolGrant
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


def test_resolved_role_accepts_a_typed_pinned_tool_grant_at_any_catalog_version() -> None:
    reference = _reference("planner")
    resolution = RegistrationReference(
        role=reference.role,
        registration_id=reference.registration_id,
        version=reference.version,
        manifest_sha256=reference.manifest_sha256,
        component_id=reference.component_id,
        component_version=reference.component_version,
        grants=[ToolGrant(tool_id="planning_model", tool_version="1.1.0", scope="plan_generation")],
    )

    require_tool(resolution, "planning_model", "plan_generation")


async def test_temporal_deserializes_pinned_grants_into_typed_worker_contracts() -> None:
    payloads = await DataConverter.default.encode(
        [
            {
                "run_id": "run-1",
                "plan_ref": "plan",
                "spec_ref": "spec",
                "registry_resolutions": [
                    {
                        "role": "planner",
                        "registration_id": "planner",
                        "version": "1.0.0",
                        "manifest_sha256": "a" * 64,
                        "component_id": "planner",
                        "component_version": "1.0.0",
                        "grants": [
                            {"tool_id": "planning_model", "tool_version": "1.0.0", "scope": "plan_generation"}
                        ],
                    }
                ],
            }
        ]
    )

    envelope = (await DataConverter.default.decode(payloads, [RunEnvelope]))[0]

    grant = envelope.registry_resolutions[0].grants[0]
    assert grant == ToolGrant(tool_id="planning_model", tool_version="1.0.0", scope="plan_generation")
    require_tool(envelope.registry_resolutions[0], "planning_model", "plan_generation")
