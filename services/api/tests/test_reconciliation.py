from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from cogito_api.main import ApplicationReadiness, create_app
from cogito_api.models import AgentRunStatus, ArtifactReference, PlanConstraints, PlanningRunStatus
from cogito_api.reconciliation import ReconciliationHealth, WorkflowProjectionReconciler, stop_reconciler
from cogito_api.supervisor import AgentRunRecord, PlanningRunRecord

from .conftest import make_settings
from .fakes import InMemorySupervisorStore
from .fakes import FakePlanner, FakeRunStarter, InMemoryPlanStore


class _Inspector:
    def __init__(self, outcomes: dict[str, str | None]) -> None:
        self.outcomes = outcomes
        self.workflow_ids: list[str] = []

    async def get_terminal_outcome(self, workflow_id: str) -> str | None:
        self.workflow_ids.append(workflow_id)
        return self.outcomes[workflow_id]


class _Telemetry:
    def __init__(self) -> None:
        self.passes: list[tuple[int, int, int]] = []

    def reconciliation_pass(self, *, inspected: int, repaired: int, failures: int) -> None:
        self.passes.append((inspected, repaired, failures))


def _run(status: PlanningRunStatus = PlanningRunStatus.IMPLEMENTING) -> PlanningRunRecord:
    return PlanningRunRecord(
        run_id="run-1",
        status=status,
        source_artifact=ArtifactReference(ref="s3://plans/run-1/source", sha256="a" * 64),
        target_repos=["https://github.com/acme/project.git#0123456789abcdef0123456789abcdef01234567"],
        spec_set="typescript@v1#sha256=" + "b" * 64,
        constraints=PlanConstraints(),
        priority="normal",
        submitted_at=datetime.now(timezone.utc).isoformat(),
        submitted_by="api",
        workflow_id="run-1:plan:1",
        project_id="default",
    )


def _agent(status: AgentRunStatus = AgentRunStatus.RUNNING) -> AgentRunRecord:
    now = datetime.now(timezone.utc).isoformat()
    return AgentRunRecord(
        run_id="run-1",
        root_run_id="run-1",
        parent_run_id=None,
        agent_name="planner",
        status=status,
        trace_id="a" * 32,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_reconciler_repairs_a_completed_temporal_workflow_after_status_interruption() -> None:
    store = InMemorySupervisorStore()
    store.planning_runs["run-1"] = _run()
    store.agent_runs["run-1"] = _agent()
    inspector = _Inspector({"run-1:plan:1": "completed"})

    repaired = await WorkflowProjectionReconciler(store, inspector).reconcile_once()

    assert repaired == 1
    assert store.planning_runs["run-1"].status is PlanningRunStatus.COMPLETED
    assert store.agent_runs["run-1"].status is AgentRunStatus.SUCCEEDED
    assert inspector.workflow_ids == ["run-1:plan:1"]


@pytest.mark.asyncio
async def test_reconciler_repairs_a_workflow_that_ended_before_implementation_status() -> None:
    """A schedule-to-start timeout cannot strand the plan-approval gate."""

    store = InMemorySupervisorStore()
    store.planning_runs["run-1"] = _run(PlanningRunStatus.AWAITING_PLAN_APPROVAL)
    store.agent_runs["run-1"] = _agent(AgentRunStatus.WAITING_FOR_APPROVAL)

    repaired = await WorkflowProjectionReconciler(store, _Inspector({"run-1:plan:1": "failed"})).reconcile_once()

    assert repaired == 1
    assert store.planning_runs["run-1"].status is PlanningRunStatus.PLANNING_FAILED
    assert store.agent_runs["run-1"].status is AgentRunStatus.FAILED


@pytest.mark.asyncio
async def test_reconciler_does_not_guess_from_a_live_or_unrecognized_workflow() -> None:
    store = InMemorySupervisorStore()
    store.planning_runs["run-1"] = _run()
    store.agent_runs["run-1"] = _agent()

    repaired = await WorkflowProjectionReconciler(store, _Inspector({"run-1:plan:1": None})).reconcile_once()

    assert repaired == 0
    assert store.planning_runs["run-1"].status is PlanningRunStatus.IMPLEMENTING
    assert store.agent_runs["run-1"].status is AgentRunStatus.RUNNING


@pytest.mark.asyncio
async def test_reconciler_refuses_to_overwrite_a_newer_terminal_agent_projection() -> None:
    store = InMemorySupervisorStore()
    store.planning_runs["run-1"] = _run(PlanningRunStatus.FINALIZING)
    store.agent_runs["run-1"] = _agent(AgentRunStatus.FAILED)

    repaired = await WorkflowProjectionReconciler(store, _Inspector({"run-1:plan:1": "completed"})).reconcile_once()

    assert repaired == 0
    assert store.planning_runs["run-1"].status is PlanningRunStatus.FINALIZING
    assert store.agent_runs["run-1"].status is AgentRunStatus.FAILED


@pytest.mark.asyncio
async def test_reconciler_reports_aggregate_inspection_failure_without_mutating_projection() -> None:
    store = InMemorySupervisorStore()
    store.planning_runs["run-1"] = _run()
    store.agent_runs["run-1"] = _agent()
    telemetry = _Telemetry()

    repaired = await WorkflowProjectionReconciler(
        store,
        _Inspector({}),
        telemetry=telemetry,
    ).reconcile_once()

    assert repaired == 0
    assert telemetry.passes == [(1, 0, 1)]
    assert store.planning_runs["run-1"].status is PlanningRunStatus.IMPLEMENTING


def test_readiness_requires_startup_and_bounded_reconciliation_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 100.0
    monkeypatch.setattr("cogito_api.reconciliation.time.monotonic", lambda: now)
    health = ReconciliationHealth(stall_seconds=10)
    readiness = ApplicationReadiness(health)

    assert readiness.is_ready() is False
    health.started()
    readiness.started()
    assert readiness.is_ready() is True

    now = 111.0
    assert readiness.is_ready() is False


@pytest.mark.asyncio
async def test_reconciler_run_owns_its_health_lifecycle() -> None:
    reconciler = WorkflowProjectionReconciler(InMemorySupervisorStore(), _Inspector({}))

    assert reconciler.health.is_healthy() is False
    task = asyncio.create_task(reconciler.run())
    await asyncio.sleep(0)
    assert reconciler.health.is_healthy() is True

    await stop_reconciler(task)

    assert reconciler.health.is_healthy() is False


def test_api_readiness_is_separate_from_process_liveness(valid_plan: dict) -> None:
    app = create_app(
        store=InMemoryPlanStore(),
        settings=make_settings(),
        starter=FakeRunStarter(),
        supervisor_store=InMemorySupervisorStore(),
        planner=FakePlanner(valid_plan),
    )

    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/readyz").json() == {"status": "ready"}
