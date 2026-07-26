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
from cogito_api.supervisor import PostgresSupervisorStore, RegistryConflictError

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


async def test_postgres_resolution_converges_when_a_concurrent_insert_wins() -> None:
    catalog = load_component_catalog(_catalog_root())
    planner = next(item for item in catalog.components if item.registration_id == "planner")
    expected = registration_reference("planner", planner)

    class Result:
        def __init__(self, row):
            self.row = row

        def mappings(self):
            return self

        def one_or_none(self):
            return self.row

    class Connection:
        def __init__(self) -> None:
            self.run_resolution_reads = 0
            self.queries: list[str] = []

        async def execute(self, statement, _parameters):
            query = str(statement)
            self.queries.append(query)
            if "FROM run_registration_resolutions" in query:
                self.run_resolution_reads += 1
                if self.run_resolution_reads == 1:
                    return Result(None)
                return Result(
                    {
                        "registration_id": expected.registration_id,
                        "registration_version": expected.version,
                        "manifest_sha256": expected.manifest_sha256,
                        "component_id": expected.component_id,
                        "component_version": expected.component_version,
                        "policy_revision": "phase12_initial",
                    }
                )
            if "FROM registry_policy_revisions" in query:
                return Result({"assignments": {"planner": "planner@1.0.0"}})
            if "FROM registry_registrations" in query:
                return Result(
                    {
                        "lifecycle": "active",
                        "manifest_sha256": expected.manifest_sha256,
                        "component_id": expected.component_id,
                        "component_version": expected.component_version,
                    }
                )
            if "INSERT INTO run_registration_resolutions" in query:
                return Result(None)
            raise AssertionError(f"unexpected query: {query}")

    class Transaction:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, *_):
            return False

    class Engine:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        def begin(self):
            return Transaction(self.connection)

    connection = Connection()
    store = object.__new__(PostgresSupervisorStore)
    store._engine = Engine(connection)

    resolved = await store.resolve_run_registration("run-1", "planner", "phase12_initial", planner)

    assert resolved == expected
    assert connection.run_resolution_reads == 2
    assert any("ON CONFLICT (run_id, role) DO NOTHING" in query for query in connection.queries)
