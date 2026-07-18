from __future__ import annotations

import uuid
import hashlib
import json

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cogito_worker.activities import WorkerActivities
from cogito_worker.models import RunEnvelope, RunResult
from cogito_worker.workflows import DeveloperRunWorkflow, _failure_detail, _validate_plan_snapshot

from .fakes import InMemoryExecutionWorkspaces, InMemoryRunStore


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


async def test_workflow_runs_activities_and_reports_completion(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    store.plans["s3://plans/plans/run-1/plan.json"] = {
        "title": "Test plan",
        "spec_set": "typescript-backend@v2.1#sha256=" + "a" * 64,
        "target_repos": [],
    }
    plan_sha256 = hashlib.sha256(
        json.dumps(store.plans["s3://plans/plans/run-1/plan.json"], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-1",
                plan_ref="s3://plans/plans/run-1/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-1", status="completed")
    assert store.statuses["run-1"]["status"] == "completed"
    assert workspaces.provisioned == ["run-1"]
    assert [workspace.run_id for workspace in workspaces.cleaned] == ["run-1"]


def test_plan_snapshot_validation_rejects_a_mutated_plan() -> None:
    plan = {
        "title": "Test plan",
        "spec_set": "typescript-backend@v2.1#sha256=" + "a" * 64,
        "target_repos": [],
    }
    envelope = RunEnvelope(
        run_id="run-1",
        plan_ref="s3://plans/plans/run-1/plan.json",
        plan_sha256="0" * 64,
        spec_ref=plan["spec_set"],
        target_repos=[],
    )

    with pytest.raises(ValueError, match="digest"):
        _validate_plan_snapshot(plan, envelope)


def test_failure_detail_includes_nested_activity_cause() -> None:
    nested = RuntimeError("workspace preparation failed")
    error = RuntimeError("Activity task failed")
    error.__cause__ = nested
    error.cause = nested  # type: ignore[attr-defined]

    assert _failure_detail(error) == "Activity task failed | workspace preparation failed"
