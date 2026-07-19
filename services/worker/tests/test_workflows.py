from __future__ import annotations

import asyncio
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


async def _wait_for_status(store: InMemoryRunStore, run_id: str, expected: str) -> None:
    for _ in range(50):
        if store.statuses.get(run_id, {}).get("status") == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"run {run_id} did not reach {expected}")


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


async def test_workflow_waits_for_matching_plan_approval_before_provisioning(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    store.plans["s3://plans/plans/run-approval/plan.json"] = {
        "title": "Test plan",
        "spec_set": "typescript-backend@v2.1#sha256=" + "a" * 64,
        "target_repos": [],
    }
    plan_sha256 = hashlib.sha256(
        json.dumps(
            store.plans["s3://plans/plans/run-approval/plan.json"], sort_keys=True, separators=(",", ":")
        ).encode()
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
        handle = await env.client.start_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-approval",
                plan_ref="s3://plans/plans/run-approval/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
                requires_plan_approval=True,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await _wait_for_status(store, "run-approval", "awaiting_plan_approval")
        assert workspaces.provisioned == []

        accepted = await handle.execute_update(
            "submit_plan_approval",
            {"decision_id": "decision-1", "artifact_sha256": plan_sha256, "decision": "approve"},
        )
        result = await handle.result()

    assert accepted is True
    assert result == RunResult(run_id="run-approval", status="completed")
    assert workspaces.provisioned == ["run-approval"]


async def test_workflow_rejects_stale_plan_approval(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    store.plans["s3://plans/plans/run-stale/plan.json"] = {
        "title": "Test plan",
        "spec_set": "typescript-backend@v2.1#sha256=" + "a" * 64,
        "target_repos": [],
    }
    plan_sha256 = hashlib.sha256(
        json.dumps(store.plans["s3://plans/plans/run-stale/plan.json"], sort_keys=True, separators=(",", ":")).encode()
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
        handle = await env.client.start_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-stale",
                plan_ref="s3://plans/plans/run-stale/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
                requires_plan_approval=True,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await _wait_for_status(store, "run-stale", "awaiting_plan_approval")
        accepted = await handle.execute_update(
            "submit_plan_approval",
            {"decision_id": "decision-stale", "artifact_sha256": "0" * 64, "decision": "approve"},
        )
        assert accepted is False
        assert workspaces.provisioned == []


async def test_duplicate_plan_approval_is_an_idempotent_acknowledgement() -> None:
    workflow_instance = DeveloperRunWorkflow()
    workflow_instance._awaiting_plan_approval = True
    workflow_instance._plan_sha256 = "a" * 64
    decision = {"decision_id": "decision-1", "artifact_sha256": "a" * 64, "decision": "approve"}

    assert await workflow_instance.submit_plan_approval(decision) is True
    assert await workflow_instance.submit_plan_approval(decision) is True
    assert workflow_instance._plan_decision == decision


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
