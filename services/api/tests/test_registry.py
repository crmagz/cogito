from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from cogito_api.models import RegistrationManifest
from cogito_api.registry import (
    RegistryAuthorizationError,
    canonical_manifest_bytes,
    load_component_catalog,
    load_mcp_binding_policy,
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
        "cogito_readonly_mcp",
    }
    assert all(item.version == "1.0.0" for item in catalog.components)
    assert all(
        item.execution_class.value == ("worker_service" if item.registration_id == "cogito_readonly_mcp" else "adapter")
        for item in catalog.components
    )
    assert all(item.owner == "cogito-platform" for item in catalog.components)


def test_mcp_policy_is_explicit_and_references_only_catalog_tools() -> None:
    catalog = load_component_catalog(_catalog_root())

    policy = load_mcp_binding_policy(_catalog_root(), catalog)

    assert policy.policy_revision == "phase18_initial"
    assert policy.bindings[0].role == "developer"
    assert policy.bindings[0].tools == ["catalog_read"]


def test_mcp_policy_rejects_unknown_tool(tmp_path: Path) -> None:
    shutil.copytree(_catalog_root(), tmp_path, dirs_exist_ok=True)
    policy = json.loads((tmp_path / "mcp_policy.json").read_text(encoding="utf-8"))
    policy["bindings"][0]["tools"] = ["unknown_tool"]
    (tmp_path / "mcp_policy.json").write_text(json.dumps(policy), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown tool"):
        load_mcp_binding_policy(tmp_path, load_component_catalog(tmp_path))


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


async def test_mcp_run_resolution_is_project_scoped_and_immutable() -> None:
    catalog = load_component_catalog(_catalog_root())
    policy = load_mcp_binding_policy(_catalog_root(), catalog)
    manifests = list(catalog.components)
    assignments = {
        item.registration_id: f"{item.registration_id}@{item.version}"
        for item in manifests
        if item.kind.value == "agent"
    }
    store = InMemorySupervisorStore()

    await store.bootstrap_registry(manifests, policy.policy_revision, assignments, policy)
    granted = await store.resolve_run_mcp_tools("run-1", "developer", "default", policy.policy_revision)
    repeated = await store.resolve_run_mcp_tools("run-1", "developer", "default", policy.policy_revision)
    denied = await store.resolve_run_mcp_tools("run-2", "developer", "different-project", policy.policy_revision)

    assert repeated == granted
    assert [(grant.server_id, grant.tool_name) for grant in granted] == [("cogito_readonly_mcp", "catalog_read")]
    assert denied == []
    assert store.run_mcp_tool_resolutions[("run-1", "developer")] == granted


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


async def test_workbench_list_materializes_authorized_runs_with_one_database_query() -> None:
    """The Workbench queue must not issue one planning-run query per listed row."""

    row = {
        "run_id": "run-1",
        "status": "planning",
        "source_artifact_ref": "s3://plans/run-1/source.json",
        "source_artifact_sha256": "a" * 64,
        "target_repos": ["https://github.com/acme/service.git#0123456789abcdef0123456789abcdef01234567"],
        "spec_set": "python@1#sha256=" + "b" * 64,
        "constraints": {},
        "priority": "normal",
        "submitted_at": datetime(2026, 7, 26, tzinfo=timezone.utc),
        "submitted_by": "operator-1",
        "plan_artifact_ref": None,
        "plan_artifact_sha256": None,
        "planner_model": None,
        "active_workflow_id": None,
        "plan_revision": 0,
        "implementation_artifact_ref": None,
        "implementation_artifact_sha256": None,
        "implementation_revision": 0,
        "project_id": "default",
    }

    class Result:
        def mappings(self):
            return self

        def all(self):
            return [row]

    class Connection:
        def __init__(self) -> None:
            self.queries: list[str] = []

        async def execute(self, statement, _parameters):
            self.queries.append(str(statement))
            return Result()

    class Session:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, *_):
            return False

    class Engine:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        def connect(self):
            return Session(self.connection)

    connection = Connection()
    store = object.__new__(PostgresSupervisorStore)
    store._engine = Engine(connection)

    records = await store.list_workbench_runs(project_ids=frozenset({"default"}), limit=50)

    assert [record.run_id for record in records] == ["run-1"]
    assert len(connection.queries) == 1
    assert "source_artifact_ref" in connection.queries[0]
