from __future__ import annotations

import pytest
from temporalio.testing import ActivityEnvironment

from cogito_worker.activities import WorkerActivities
from cogito_worker.execution import ExecutionJobSettings, ExecutionWorkspaceService
from cogito_worker.models import ExecutionRequest, ExecutionWorkspace, McpToolGrant, ValidationRequest
from cogito_worker.run_state import RunStateReporter

from .fakes import (
    InMemoryExecutionJobClient,
    InMemoryHarness,
    InMemoryExecutionWorkspaces,
    InMemoryRunStore,
)


@pytest.fixture
def store() -> InMemoryRunStore:
    return InMemoryRunStore()


@pytest.fixture
def activities(store: InMemoryRunStore) -> WorkerActivities:
    return WorkerActivities(store, InMemoryExecutionWorkspaces(), InMemoryHarness())


@pytest.fixture
def env() -> ActivityEnvironment:
    return ActivityEnvironment()


async def test_load_plan_returns_plan_from_store(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    store.plans["s3://plans/plans/run-1/plan.json"] = {"title": "Test plan"}

    result = await env.run(activities.load_plan, "s3://plans/plans/run-1/plan.json")

    assert result == {"title": "Test plan"}


async def test_load_resolved_workflow_returns_immutable_artifact_from_store(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    workflow_ref = "s3://plan-snapshots/runs/run-1/resolved-workflow.json"
    resolution = {"run_id": "run-1", "gates": [], "phases": []}
    store.plans[workflow_ref] = resolution

    result = await env.run(activities.load_resolved_workflow, workflow_ref)

    assert result == resolution


async def test_report_status_creates_status_when_none_exists(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    await env.run(activities.report_status, "run-1", "claimed")

    assert store.statuses["run-1"]["status"] == "claimed"
    assert store.statuses["run-1"]["run_id"] == "run-1"
    assert "updated_at" in store.statuses["run-1"]


async def test_report_status_preserves_existing_fields(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    store.statuses["run-1"] = {"run_id": "run-1", "status": "queued", "plan_ref": "s3://plans/plans/run-1/plan.json"}

    await env.run(activities.report_status, "run-1", "completed")

    assert store.statuses["run-1"]["status"] == "completed"
    assert store.statuses["run-1"]["plan_ref"] == "s3://plans/plans/run-1/plan.json"


async def test_report_status_forwards_only_bounded_transition_metadata(env: ActivityEnvironment, store: InMemoryRunStore):
    class RecordingRunStateReporter:
        calls: list[tuple[str, str, str | None, dict | None]] = []

        async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict | None) -> None:
            self.calls.append((run_id, status, failure_detail, metadata))

    reporter: RunStateReporter = RecordingRunStateReporter()
    activities = WorkerActivities(store, InMemoryExecutionWorkspaces(), InMemoryHarness(), run_state=reporter)

    await env.run(activities.report_status, "run-1", "completed", None, {"phase_result": {"summary": "secret"}})

    assert reporter.calls == [("run-1", "completed", None, {"phase_result": {"summary": "secret"}})]


async def test_validator_accepts_converged_evidence_with_passing_verification(
    env: ActivityEnvironment, activities: WorkerActivities
) -> None:
    result = await env.run(
        activities.validate_implementation,
        ValidationRequest(
            run_id="run-1",
            phase_results=[{"succeeded": True, "verification": [{"passed": True}]}],
            review={"status": "converged"},
        ),
    )

    assert result.status == "passed"
    assert result.checked_phases == 1


@pytest.mark.parametrize("verification", [[], [{"passed": False}]])
async def test_validator_rejects_missing_or_failed_verification(
    verification: list[dict],
    env: ActivityEnvironment, activities: WorkerActivities
) -> None:
    result = await env.run(
        activities.validate_implementation,
        ValidationRequest(
            run_id="run-1",
            phase_results=[{"succeeded": True, "verification": verification}],
            review={"status": "converged"},
        ),
    )

    assert result.status == "failed"
    assert result.reason == "verification_not_passed"


async def test_freeze_implementation_artifact_adds_only_bounded_mcp_evidence(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
) -> None:
    workspace = ExecutionWorkspace(
        run_id="run-1",
        job_name="cogito-execution-example",
        workspace_root="/workspace",
        mcp_grants=[
            McpToolGrant(
                server_id="cogito_readonly_mcp",
                server_version="1.0.1",
                server_manifest_sha256="b" * 64,
                tool_name="catalog_read",
                input_schema_sha256="c" * 64,
            )
        ],
    )

    artifact = await env.run(activities.freeze_implementation_artifact, "run-1", {"review": {}}, workspace)

    assert store.implementation_artifacts[artifact.sha256] == {
        "review": {},
        "mcp_invocations": {
            "version": 1,
            "status": "observed",
            "events": [],
            "selected_grants": [
                {
                    "role": "developer",
                    "server_id": "cogito_readonly_mcp",
                    "server_version": "1.0.1",
                    "server_manifest_sha256": "b" * 64,
                    "tool_name": "catalog_read",
                    "input_schema_sha256": "c" * 64,
                }
            ],
        },
    }


async def test_freeze_implementation_artifact_records_an_explicit_empty_mcp_selection(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
) -> None:
    workspace = ExecutionWorkspace(
        run_id="run-1",
        job_name="cogito-execution-example",
        workspace_root="/workspace",
        mcp_selection_explicit=True,
    )

    artifact = await env.run(activities.freeze_implementation_artifact, "run-1", {"review": {}}, workspace)

    assert store.implementation_artifacts[artifact.sha256]["mcp_invocations"] == {
        "version": 1,
        "status": "not_applicable",
        "events": [],
        "selected_grants": [],
    }


async def test_execution_workspace_activities_manage_only_the_current_run(
    env: ActivityEnvironment, store: InMemoryRunStore
):
    jobs = InMemoryExecutionJobClient()
    activities = WorkerActivities(
        store,
        ExecutionWorkspaceService(
            ExecutionJobSettings(
                namespace="cogito",
                image="cogito-worker:local",
                image_pull_policy="IfNotPresent",
                workspace_root="/workspace",
                idle_seconds=3600,
                startup_timeout_seconds=30,
                cleanup_timeout_seconds=90,
                active_deadline_seconds=3900,
                ttl_seconds_after_finished=300,
                termination_grace_period_seconds=10,
                workspace_size_limit="2Gi",
                resources={"limits": {"memory": "1Gi"}},
                allowed_git_hosts=("github.com",),
                minio_endpoint="cogito-minio:9000",
                minio_secure=False,
                specs_bucket="specs",
                specs_prefix="specs",
                specs_max_archive_bytes=1024 * 1024,
                specs_max_extracted_bytes=2 * 1024 * 1024,
                object_store_secret="cogito-minio",
                object_store_access_key_secret_key="rootUser",
                object_store_secret_key_secret_key="rootPassword",
                litellm_endpoint="http://cogito-litellm:4000",
                litellm_model="complex",
                litellm_key_secret="cogito-developer-key",
                litellm_key_secret_key="api-key",
                git_author_name="Cogito Agent",
                git_author_email="cogito@local.invalid",
                command_output_limit_bytes=262144,
            ),
            jobs,
        ),
        InMemoryHarness(),
    )

    workspace = await env.run(
        activities.provision_execution_workspace,
        ExecutionRequest(run_id="run-1", spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64, target_repos=[]),
    )
    await env.run(activities.cleanup_execution_workspace, workspace)

    assert [job_name for job_name, _ in jobs.created] == [workspace.job_name]
    assert jobs.awaited == [(workspace.job_name, 30)]
    assert jobs.deleted == [workspace.job_name]
