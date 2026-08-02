from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from cogito_api.models import AgentRunStatus, ArtifactReference, PlanConstraints, PlanningRunStatus
from cogito_api.reconciliation import WorkflowProjectionReconciler
from cogito_api.supervisor import AgentRunRecord, PlanningRunRecord

from .fakes import InMemorySupervisorStore


class _Inspector:
    def __init__(self, outcomes: dict[str, str | None]) -> None:
        self.outcomes = outcomes
        self.workflow_ids: list[str] = []

    async def get_terminal_outcome(self, workflow_id: str) -> str | None:
        self.workflow_ids.append(workflow_id)
        return self.outcomes[workflow_id]


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
