from __future__ import annotations

import json
from pathlib import Path

import pytest

from cogito_api.models import RegistrationManifest
from cogito_api.registry import (
    RegistryAuthorizationError,
    canonical_manifest_bytes,
    load_component_catalog,
    manifest_sha256,
    registration_reference,
    require_tool,
)
from cogito_api.supervisor import RegistryConflictError

from .fakes import InMemorySupervisorStore


def _catalog_root() -> Path:
    return Path(__file__).parents[3] / "components"


def test_component_catalog_is_complete_and_versioned() -> None:
    catalog = load_component_catalog(_catalog_root())

    registrations = {item.registration_id: item for item in catalog.components}
    assert set(registrations) == {
        "planner",
        "developer",
        "reviewer",
        "validator",
        "ephemeral_environment_tester",
        "pull_request_publisher",
        "planning_model",
        "execution_workspace",
        "developer_harness",
        "review_model",
        "validation_runner",
        "ephemeral_environment",
        "github_publisher",
    }
    assert all(item.version == "1.0.0" for item in catalog.components)
    assert all(item.execution_class.value == "adapter" for item in catalog.components)
    assert all(item.owner == "cogito-platform" for item in catalog.components)


def test_manifest_identity_is_canonical_and_role_reference_is_audit_safe() -> None:
    manifest = RegistrationManifest.model_validate(
        json.loads((_catalog_root() / "agents" / "planner" / "component.json").read_text(encoding="utf-8"))
    )

    assert canonical_manifest_bytes(manifest) == canonical_manifest_bytes(manifest)
    reference = registration_reference("planner", manifest)

    assert reference.registration_id == "planner"
    assert reference.version == "1.0.0"
    assert reference.component_id == "planner"
    assert reference.manifest_sha256 == manifest_sha256(manifest)
    require_tool(reference, "planning_model", "plan_generation")


def test_api_tool_guard_rejects_an_ungranted_pinned_tool() -> None:
    manifest = RegistrationManifest.model_validate(
        json.loads((_catalog_root() / "agents" / "planner" / "component.json").read_text(encoding="utf-8"))
    )
    reference = registration_reference("planner", manifest.model_copy(update={"grants": []}))

    with pytest.raises(RegistryAuthorizationError, match="planning_model"):
        require_tool(reference, "planning_model", "plan_generation")


def test_component_catalog_rejects_agent_grant_to_unknown_tool(tmp_path: Path) -> None:
    component = json.loads((_catalog_root() / "agents" / "planner" / "component.json").read_text(encoding="utf-8"))
    component["grants"][0]["tool_id"] = "missing_tool"
    definition = tmp_path / "agents" / "planner" / "component.json"
    definition.parent.mkdir(parents=True)
    definition.write_text(json.dumps(component), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown tool"):
        load_component_catalog(tmp_path)


async def test_run_resolution_pins_the_policy_selected_release() -> None:
    catalog = load_component_catalog(_catalog_root())
    manifests = list(catalog.components)
    planner = next(item for item in manifests if item.registration_id == "planner")
    store = InMemorySupervisorStore()

    await store.bootstrap_registry(manifests, "phase12_initial", {"planner": "planner@1.0.0"})
    first = await store.resolve_run_registration("run-1", "planner", "phase12_initial", planner)
    repeated = await store.resolve_run_registration("run-1", "planner", "phase12_initial", planner)

    assert repeated == first
    assert store.run_registration_resolutions[("run-1", "planner")] == first


async def test_run_resolution_rejects_unselected_or_changed_release() -> None:
    catalog = load_component_catalog(_catalog_root())
    manifests = list(catalog.components)
    planner = next(item for item in manifests if item.registration_id == "planner")
    store = InMemorySupervisorStore()

    await store.bootstrap_registry(manifests, "phase12_initial", {"reviewer": "reviewer@1.0.0"})

    with pytest.raises(RegistryConflictError, match="does not select"):
        await store.resolve_run_registration("run-1", "planner", "phase12_initial", planner)
