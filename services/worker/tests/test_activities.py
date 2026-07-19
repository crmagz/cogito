from __future__ import annotations

import pytest
from temporalio.testing import ActivityEnvironment

from cogito_worker.activities import WorkerActivities
from cogito_worker.execution import ExecutionJobSettings, ExecutionWorkspaceService
from cogito_worker.models import ExecutionRequest

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
                git_credentials_secret="cogito-developer-git",
                git_credentials_secret_key="token",
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
