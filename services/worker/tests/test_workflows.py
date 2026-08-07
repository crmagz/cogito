from __future__ import annotations

import asyncio
import hashlib
import json
import uuid

import pytest
from cogito_worker.activities import WorkerActivities
from cogito_worker.models import (
    ExecutionWorkspace,
    McpToolGrant,
    PhaseResult,
    RegistrationReference,
    ReviewFinding,
    ReviewResult,
    RunEnvelope,
    RunResult,
    ToolGrant,
    VerificationResult,
)
from cogito_worker.workflows import (
    DeveloperRunWorkflow,
    _implementation_evidence,
    _execution_plan,
    _failure_detail,
    _is_timeout_error,
    _redact_failure_message,
    _validate_plan_snapshot,
)
from temporalio.exceptions import TimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from .fakes import (
    InMemoryExecutionWorkspaces,
    InMemoryHarness,
    InMemoryPullRequestPublisher,
    InMemoryReviewer,
    InMemoryRunStore,
)


async def _wait_for_status(store: InMemoryRunStore, run_id: str, expected: str) -> None:
    for _ in range(50):
        if store.statuses.get(run_id, {}).get("status") == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"run {run_id} did not reach {expected}")


def _single_phase_plan(spec_ref: str, target_repos: list[str]) -> dict:
    return {
        "title": "Test plan",
        "spec_set": spec_ref,
        "target_repos": target_repos,
        "phases": [
            {
                "id": "phase-1",
                "name": "Implement test change",
                "description": "Exercise the harness workflow path.",
                "tasks": ["Update the implementation."],
                "acceptance_criteria": ["The change is committed."],
                "verification": ["true"],
            }
        ],
        "constraints": {
            "max_turns_per_phase": 50,
            "max_wall_clock_minutes": 1,
            "max_cost_usd": 1.0,
        },
    }


def _mcp_grant(tool_name: str, marker: str) -> McpToolGrant:
    return McpToolGrant(
        server_id="catalog_mcp",
        server_version="1.0.0",
        server_manifest_sha256=marker * 64,
        tool_name=tool_name,
        input_schema_sha256=("c" if marker == "b" else "d") * 64,
    )


def _mcp_registry_resolutions(grants: list[McpToolGrant]) -> list[RegistrationReference]:
    def reference(role: str, tool_grants: list[ToolGrant], mcp_grants: list[McpToolGrant] | None = None) -> RegistrationReference:
        return RegistrationReference(
            role=role,
            registration_id=role,
            version="1.0.0",
            manifest_sha256="a" * 64,
            component_id=role,
            component_version="1.0.0",
            grants=tool_grants,
            mcp_grants=mcp_grants or [],
        )

    return [
        reference("planner", [ToolGrant("planning_model", "1.0.0", "plan_generation")]),
        reference(
            "developer",
            [
                ToolGrant("execution_workspace", "1.0.0", "run_scoped_workspace"),
                ToolGrant("developer_harness", "1.0.0", "approved_phase"),
            ],
            grants,
        ),
        reference(
            "reviewer",
            [
                ToolGrant("execution_workspace", "1.0.0", "read_only_workspace"),
                ToolGrant("review_model", "1.0.0", "read_only_review"),
            ],
        ),
        reference("validator", [ToolGrant("validation_runner", "1.0.0", "approved_verification")]),
    ]


def _selection(role: str, grant: McpToolGrant) -> dict[str, str]:
    return {
        "role": role,
        "server_id": grant.server_id,
        "server_version": grant.server_version,
        "server_manifest_sha256": grant.server_manifest_sha256,
        "tool_name": grant.tool_name,
        "input_schema_sha256": grant.input_schema_sha256,
    }


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


async def test_workflow_runs_activities_and_reports_completion(
    env: WorkflowEnvironment,
):
    store = InMemoryRunStore()
    store.plans["s3://plans/plans/run-1/plan.json"] = _single_phase_plan(
        "typescript-backend@v2.1#sha256=" + "a" * 64, []
    )
    plan_sha256 = hashlib.sha256(
        json.dumps(
            store.plans["s3://plans/plans/run-1/plan.json"],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    harness = InMemoryHarness()
    activities = WorkerActivities(store, workspaces, harness)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
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
    assert harness.requests[0].max_turns == 25
    assert store.statuses["run-1"]["phase_results"][0]["turns_used"] == 3


async def test_resolved_run_rejects_missing_developer_before_workspace_provisioning(
    env: WorkflowEnvironment,
):
    store = InMemoryRunStore()
    plan_ref = "s3://plans/plans/run-missing-developer/plan.json"
    spec_ref = "typescript-backend@v2.1#sha256=" + "a" * 64
    store.plans[plan_ref] = _single_phase_plan(spec_ref, [])
    plan_sha256 = hashlib.sha256(
        json.dumps(store.plans[plan_ref], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces, InMemoryHarness())
    task_queue = f"test-queue-{uuid.uuid4()}"
    planner = RegistrationReference(
        role="planner",
        registration_id="planner",
        version="1.0.0",
        manifest_sha256="a" * 64,
        component_id="planner",
        component_version="1.0.0",
        grants=[ToolGrant(tool_id="planning_model", tool_version="1.0.0", scope="plan_generation")],
    )

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.provision_execution_workspace,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-missing-developer",
                plan_ref=plan_ref,
                plan_sha256=plan_sha256,
                spec_ref=spec_ref,
                registry_resolutions=[planner],
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-missing-developer", status="failed")
    assert workspaces.provisioned == []


async def test_workflow_runs_dependency_ordered_phases_in_one_workspace(
    env: WorkflowEnvironment,
):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    plan["phases"] = [
        {
            **plan["phases"][0],
            "id": "phase-2",
            "name": "Second",
            "depends_on": ["phase-1"],
        },
        {
            **plan["phases"][0],
            "id": "phase-3",
            "name": "Third",
            "depends_on": ["phase-1"],
        },
        {**plan["phases"][0], "id": "phase-1", "name": "First", "depends_on": []},
    ]
    store.plans["s3://plans/plans/run-multi/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    harness = InMemoryHarness()
    activities = WorkerActivities(store, workspaces, harness)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-multi",
                plan_ref="s3://plans/plans/run-multi/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-multi", status="completed")
    assert [request.phase.id for request in harness.requests] == [
        "phase-1",
        "phase-2",
        "phase-3",
    ]
    assert store.statuses["run-multi"]["completed_phase_ids"] == [
        "phase-1",
        "phase-2",
        "phase-3",
    ]
    assert len(workspaces.provisioned) == 1
    assert len(workspaces.cleaned) == 1


async def test_workflow_revises_verified_blocker_then_converges(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    plan["constraints"]["max_review_rounds"] = 2
    store.plans["s3://plans/plans/run-review-converges/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    blocker = ReviewFinding(
        severity="blocking",
        lens="correctness",
        model="balanced",
        file="src/main.py",
        line=1,
        description="intentional issue",
        evidence="reproduced",
        verified=True,
    )
    reviewer = InMemoryReviewer(
        results=[ReviewResult(findings=[blocker]), ReviewResult(findings=[])],
        verified=[blocker],
    )
    workspaces = InMemoryExecutionWorkspaces()
    harness = InMemoryHarness()
    activities = WorkerActivities(store, workspaces, harness, reviewer=reviewer)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-review-converges",
                plan_ref="s3://plans/plans/run-review-converges/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-review-converges", status="completed")
    assert len(reviewer.requests) == 2
    assert len(harness.review_revision_requests) == 1
    assert store.statuses["run-review-converges"]["review"]["status"] == "converged"
    assert len(workspaces.cleaned) == 1


async def test_workflow_downgrades_unverified_blocker_without_revision(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    store.plans["s3://plans/plans/run-review-unverified/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    blocker = ReviewFinding(
        severity="blocking",
        lens="correctness",
        model="balanced",
        file="src/main.py",
        line=1,
        description="unverified issue",
    )
    reviewer = InMemoryReviewer(
        results=[ReviewResult(findings=[blocker])],
        verified=[
            ReviewFinding(
                severity="advisory",
                lens=blocker.lens,
                model=blocker.model,
                file=blocker.file,
                line=blocker.line,
                description=blocker.description,
                verified=False,
            )
        ],
    )
    workspaces = InMemoryExecutionWorkspaces()
    harness = InMemoryHarness()
    activities = WorkerActivities(store, workspaces, harness, reviewer=reviewer)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-review-unverified",
                plan_ref="s3://plans/plans/run-review-unverified/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-review-unverified", status="completed")
    assert harness.review_revision_requests == []
    finding = store.statuses["run-review-unverified"]["review"]["rounds"][0]["findings"][0]
    assert finding["severity"] == "advisory"
    assert finding["verified"] is False


async def test_workflow_escalates_when_verified_blocker_reaches_review_cap(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    plan["constraints"]["max_review_rounds"] = 1
    store.plans["s3://plans/plans/run-review-escalates/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    blocker = ReviewFinding(
        severity="blocking",
        lens="blast_radius",
        model="complex",
        file="src/main.py",
        line=1,
        description="persistent issue",
        verified=True,
    )
    reviewer = InMemoryReviewer(results=[ReviewResult(findings=[blocker])], verified=[blocker])
    workspaces = InMemoryExecutionWorkspaces()
    harness = InMemoryHarness()
    activities = WorkerActivities(store, workspaces, harness, reviewer=reviewer)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-review-escalates",
                plan_ref="s3://plans/plans/run-review-escalates/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-review-escalates", status="escalated")
    assert harness.review_revision_requests == []
    assert store.statuses["run-review-escalates"]["status"] == "escalated"
    assert store.statuses["run-review-escalates"]["review"]["reason"] == "max_review_rounds"


async def test_workflow_escalates_when_reviewer_is_unavailable(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    store.plans["s3://plans/plans/run-review-unavailable/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    harness = InMemoryHarness()
    activities = WorkerActivities(
        store,
        workspaces,
        harness,
        reviewer=InMemoryReviewer(review_error=RuntimeError("LiteLLM unavailable")),
    )
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-review-unavailable",
                plan_ref="s3://plans/plans/run-review-unavailable/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-review-unavailable", status="escalated")
    review = store.statuses["run-review-unavailable"]["review"]
    assert review["reason"] == "review_unavailable"
    assert review["rounds"] == [{"round": 1, "error": "review activity did not complete"}]
    assert len(workspaces.cleaned) == 1


async def test_workflow_backs_up_and_stops_on_a_known_ceiling(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    store.plans["s3://plans/plans/run-backup/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    harness = InMemoryHarness(
        result=PhaseResult(
            phase_id="phase-1",
            branch_name="adp/run-backup",
            succeeded=False,
            turns_used=25,
            cost_usd=0.01,
            changed_files=[],
            commits={},
            verification=[],
            summary="turn ceiling reached",
            outcome="ceiling_reached",
            ceiling="turns",
        )
    )
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces, harness)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-backup",
                plan_ref="s3://plans/plans/run-backup/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-backup", status="stopped_with_backup")
    assert [request.ceiling for request in harness.backup_requests] == ["turns"]
    assert store.statuses["run-backup"]["ceiling"] == "turns"
    assert store.statuses["run-backup"]["unfinished_phase_ids"] == ["phase-1"]
    assert len(workspaces.cleaned) == 1


async def test_workflow_records_failure_when_cleanup_fails_after_backup(
    env: WorkflowEnvironment,
):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    store.plans["s3://plans/plans/run-backup-cleanup-failure/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    harness = InMemoryHarness(
        result=PhaseResult(
            phase_id="phase-1",
            branch_name="adp/run-backup-cleanup-failure",
            succeeded=False,
            turns_used=25,
            cost_usd=0.01,
            changed_files=[],
            commits={},
            verification=[],
            summary="turn ceiling reached",
            outcome="ceiling_reached",
            ceiling="turns",
        )
    )
    workspaces = InMemoryExecutionWorkspaces(
        cleanup_error=RuntimeError("workspace cleanup failed")
    )
    activities = WorkerActivities(store, workspaces, harness)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-backup-cleanup-failure",
                plan_ref="s3://plans/plans/run-backup-cleanup-failure/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-backup-cleanup-failure", status="failed")
    assert store.statuses["run-backup-cleanup-failure"]["status"] == "failed"
    assert len(workspaces.cleaned) == 3


@pytest.mark.parametrize("ceiling", ["turns", "cost", "wall_clock"])
async def test_workflow_backs_up_a_dependent_phase_for_each_trusted_ceiling(
    env: WorkflowEnvironment, ceiling: str
):
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    plan["phases"] = [
        {**plan["phases"][0], "id": "phase-1", "name": "First", "depends_on": []},
        {
            **plan["phases"][0],
            "id": "phase-2",
            "name": "Second",
            "depends_on": ["phase-1"],
        },
    ]
    store.plans["s3://plans/plans/run-dependent-backup/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    harness = InMemoryHarness(
        results=[
            PhaseResult(
                phase_id="phase-1",
                branch_name="adp/run-dependent-backup",
                succeeded=True,
                turns_used=3,
                cost_usd=0.01,
                changed_files=["/workspace/repos/example:phase-1.txt"],
                commits={"/workspace/repos/example": "a" * 40},
                verification=[],
                summary="phase one complete",
            ),
            PhaseResult(
                phase_id="phase-2",
                branch_name="adp/run-dependent-backup",
                succeeded=False,
                turns_used=25,
                cost_usd=0.02,
                changed_files=["/workspace/repos/example:phase-2.txt"],
                commits={"/workspace/repos/example": "b" * 40},
                verification=[],
                summary=f"{ceiling} ceiling reached",
                outcome="ceiling_reached",
                ceiling=ceiling,
            ),
        ]
    )
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces, harness)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-dependent-backup",
                plan_ref="s3://plans/plans/run-dependent-backup/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(
        run_id="run-dependent-backup", status="stopped_with_backup"
    )
    assert [request.phase.id for request in harness.requests] == ["phase-1", "phase-2"]
    assert [request.phase.id for request in harness.backup_requests] == ["phase-2"]
    assert harness.backup_requests[0].ceiling == ceiling
    assert store.statuses["run-dependent-backup"]["completed_phase_ids"] == ["phase-1"]
    assert store.statuses["run-dependent-backup"]["stopped_phase_id"] == "phase-2"
    assert store.statuses["run-dependent-backup"]["unfinished_phase_ids"] == ["phase-2"]
    assert len(workspaces.cleaned) == 1


async def test_workflow_records_an_ordinary_phase_failure_as_a_terminal_result(
    env: WorkflowEnvironment,
):
    """A durable failed run must not keep retrying its workflow task."""

    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    store.plans["s3://plans/plans/run-failed/plan.json"] = plan
    plan_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    harness = InMemoryHarness(
        result=PhaseResult(
            phase_id="phase-1",
            branch_name="adp/run-failed",
            succeeded=False,
            turns_used=1,
            cost_usd=0.01,
            changed_files=[],
            commits={},
            verification=[],
            summary="verification failed",
            outcome="failed",
        )
    )
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces, harness)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        result = await env.client.execute_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-failed",
                plan_ref="s3://plans/plans/run-failed/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    assert result == RunResult(run_id="run-failed", status="failed")
    assert store.statuses["run-failed"]["status"] == "failed"
    assert len(workspaces.cleaned) == 1


async def test_workflow_waits_for_matching_plan_approval_before_provisioning(
    env: WorkflowEnvironment,
):
    store = InMemoryRunStore()
    store.plans["s3://plans/plans/run-approval/plan.json"] = _single_phase_plan(
        "typescript-backend@v2.1#sha256=" + "a" * 64, []
    )
    plan_sha256 = hashlib.sha256(
        json.dumps(
            store.plans["s3://plans/plans/run-approval/plan.json"],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces, InMemoryHarness())
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
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
            {
                "decision_id": "decision-1",
                "artifact_sha256": plan_sha256,
                "decision": "approve",
            },
        )
        result = await handle.result()

    assert accepted is True
    assert result == RunResult(run_id="run-approval", status="completed")
    assert workspaces.provisioned == ["run-approval"]


async def test_workflow_provisions_only_the_approved_mcp_subset(env: WorkflowEnvironment) -> None:
    store = InMemoryRunStore()
    plan_ref = "s3://plans/plans/run-mcp-selection/plan.json"
    spec_ref = "typescript-backend@v2.1#sha256=" + "a" * 64
    store.plans[plan_ref] = _single_phase_plan(spec_ref, [])
    plan_sha256 = hashlib.sha256(json.dumps(store.plans[plan_ref], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(
        store,
        workspaces,
        InMemoryHarness(
            result=PhaseResult(
                phase_id="phase-1",
                branch_name="adp/run-mcp-selection",
                succeeded=True,
                turns_used=1,
                cost_usd=0.01,
                changed_files=[],
                commits={},
                verification=[VerificationResult(command="true", passed=True, output="")],
                summary="completed",
            )
        ),
    )
    task_queue = f"test-queue-{uuid.uuid4()}"
    granted = [_mcp_grant("catalog_read", "b"), _mcp_grant("catalog_list", "e")]

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.validate_implementation,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        handle = await env.client.start_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-mcp-selection",
                plan_ref=plan_ref,
                plan_sha256=plan_sha256,
                spec_ref=spec_ref,
                requires_plan_approval=True,
                registry_resolutions=_mcp_registry_resolutions(granted),
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await _wait_for_status(store, "run-mcp-selection", "awaiting_plan_approval")
        accepted = await handle.execute_update(
            "submit_plan_approval_with_mcp_selection",
            {
                "decision_id": "decision-mcp-selection",
                "artifact_sha256": plan_sha256,
                "decision": "approve",
                "mcp_selection": [_selection("developer", granted[0])],
            },
        )
        result = await handle.result()

    assert accepted is True
    assert result == RunResult(run_id="run-mcp-selection", status="completed")
    assert workspaces.requests[0].mcp_grants == [granted[0]]
    assert workspaces.requests[0].mcp_selection_explicit is True


async def test_workflow_rejects_an_expanding_mcp_selection_update() -> None:
    workflow_instance = DeveloperRunWorkflow()
    granted = _mcp_grant("catalog_read", "b")
    workflow_instance._awaiting_plan_approval = True
    workflow_instance._plan_sha256 = "a" * 64
    workflow_instance._pinned_mcp_selection_keys = {
        (
            "developer",
            granted.server_id,
            granted.server_version,
            granted.server_manifest_sha256,
            granted.tool_name,
            granted.input_schema_sha256,
        )
    }

    accepted = await workflow_instance.submit_plan_approval_with_mcp_selection(
        {
            "decision_id": "decision-expansion",
            "artifact_sha256": "a" * 64,
            "decision": "approve",
            "mcp_selection": [_selection("developer", _mcp_grant("catalog_delete", "f"))],
        }
    )

    assert accepted is False
    assert workflow_instance._plan_decision is None


async def test_workflow_rejects_stale_plan_approval(env: WorkflowEnvironment):
    store = InMemoryRunStore()
    store.plans["s3://plans/plans/run-stale/plan.json"] = _single_phase_plan(
        "typescript-backend@v2.1#sha256=" + "a" * 64, []
    )
    plan_sha256 = hashlib.sha256(
        json.dumps(
            store.plans["s3://plans/plans/run-stale/plan.json"],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    activities = WorkerActivities(store, workspaces, InMemoryHarness())
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
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
            {
                "decision_id": "decision-stale",
                "artifact_sha256": "0" * 64,
                "decision": "approve",
            },
        )
        assert accepted is False
        assert workspaces.provisioned == []


async def test_duplicate_plan_approval_is_an_idempotent_acknowledgement() -> None:
    workflow_instance = DeveloperRunWorkflow()
    workflow_instance._awaiting_plan_approval = True
    workflow_instance._plan_sha256 = "a" * 64
    decision = {
        "decision_id": "decision-1",
        "artifact_sha256": "a" * 64,
        "decision": "approve",
    }

    assert await workflow_instance.submit_plan_approval(decision) is True
    assert await workflow_instance.submit_plan_approval(decision) is True
    assert workflow_instance._plan_decision == decision


async def test_workflow_waits_for_implementation_approval_then_opens_one_pr(env: WorkflowEnvironment) -> None:
    store = InMemoryRunStore()
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, ["https://github.com/acme/example.git#" + "1" * 40])
    store.plans["s3://plans/plans/run-implementation/plan.json"] = plan
    plan_sha256 = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    workspaces = InMemoryExecutionWorkspaces()
    publisher = InMemoryPullRequestPublisher()
    activities = WorkerActivities(store, workspaces, InMemoryHarness(), pull_request_publisher=publisher)
    task_queue = f"test-queue-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.freeze_implementation_artifact,
            activities.open_pull_request,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
            activities.backup_phase,
            activities.review,
            activities.verify_review_findings,
            activities.address_review_findings,
        ],
    ):
        handle = await env.client.start_workflow(
            DeveloperRunWorkflow.run,
            RunEnvelope(
                run_id="run-implementation",
                plan_ref="s3://plans/plans/run-implementation/plan.json",
                plan_sha256=plan_sha256,
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
                target_repos=plan["target_repos"],
                requires_implementation_approval=True,
            ),
            id=f"test-workflow-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await _wait_for_status(store, "run-implementation", "awaiting_implementation_approval")
        assert len(workspaces.cleaned) == 1
        digest = store.statuses["run-implementation"]["implementation_artifact"]["sha256"]
        accepted = await handle.execute_update(
            "submit_implementation_approval",
            {"decision_id": "implementation-decision-1", "artifact_sha256": digest, "decision": "approve"},
        )
        result = await handle.result()

    assert accepted is True
    assert result == RunResult(run_id="run-implementation", status="completed")
    assert len(publisher.requests) == 1
    assert store.statuses["run-implementation"]["pull_request"]["number"] == 42


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


def test_execution_plan_orders_multi_phase_dependencies_stably() -> None:
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    plan["phases"] = [
        {
            **plan["phases"][0],
            "id": "phase-2",
            "name": "Second",
            "depends_on": ["phase-1"],
        },
        {**plan["phases"][0], "id": "phase-3", "name": "Independent"},
        {**plan["phases"][0], "id": "phase-1", "name": "First"},
    ]

    phases, max_turns, timeout, reserve, max_cost_usd, max_review_rounds, review_profile = _execution_plan(plan)

    assert [phase.id for phase in phases] == ["phase-3", "phase-1", "phase-2"]
    assert max_turns == 25
    assert timeout.total_seconds() == 60
    assert reserve == 25
    assert max_cost_usd == 1.0
    assert max_review_rounds == 3
    assert review_profile == "standard"


def test_execution_plan_requires_an_approved_verification_command() -> None:
    plan = _single_phase_plan("typescript-backend@v2.1#sha256=" + "a" * 64, [])
    plan["phases"][0]["verification"] = []

    with pytest.raises(ValueError, match="non-empty tasks"):
        _execution_plan(plan)


def test_implementation_evidence_excludes_raw_command_and_reviewer_output() -> None:
    evidence = _implementation_evidence(
        RunEnvelope(run_id="run-safe", plan_ref="s3://plans/plan.json", plan_sha256="a" * 64, spec_ref="spec@v1#sha256=" + "a" * 64),
        ExecutionWorkspace(
            run_id="run-safe", job_name="job", workspace_root="/workspace", repository_origins={"/repo": "https://github.com/acme/example.git"}
        ),
        [{"phase_id": "phase-1", "verification": [{"command": "pytest", "passed": True, "output": "secret output"}]}],
        {"status": "converged", "rounds": [{"round": 1, "findings": [{"severity": "advisory", "description": "safe summary", "evidence": "raw diff"}]}]},
    )

    assert "secret output" not in str(evidence)
    assert "raw diff" not in str(evidence)


def test_failure_detail_includes_nested_activity_cause() -> None:
    nested = RuntimeError("workspace preparation failed")
    error = RuntimeError("Activity task failed")
    error.__cause__ = nested
    error.cause = nested  # type: ignore[attr-defined]

    assert (
        _failure_detail(error) == "Activity task failed | workspace preparation failed"
    )


def test_failure_detail_stops_at_safe_github_category_and_redacts_tokens() -> None:
    provider_error = RuntimeError("Illegal header value Bearer gho_secretToken123")
    safe_error = RuntimeError("GitHub pull-request publication failed")
    safe_error.cause = provider_error  # type: ignore[attr-defined]
    error = RuntimeError("Activity task failed")
    error.cause = safe_error  # type: ignore[attr-defined]

    detail = _failure_detail(error)

    assert detail == "Activity task failed | GitHub pull-request publication failed"
    assert "gho_secretToken123" not in detail


def test_failure_redaction_removes_bearer_and_github_tokens() -> None:
    detail = _redact_failure_message("request failed with Bearer gho_secretToken123 and github_pat_secretToken456")

    assert "gho_secretToken123" not in detail
    assert "github_pat_secretToken456" not in detail
    assert "[REDACTED]" in detail


def test_timeout_detection_requires_a_temporal_timeout_in_the_cause_chain() -> None:
    timeout = TimeoutError("activity timed out", type=None, last_heartbeat_details=[])
    outer = RuntimeError("activity failed")
    outer.__cause__ = timeout

    assert _is_timeout_error(outer) is True
    assert _is_timeout_error(RuntimeError("ordinary activity failure")) is False
