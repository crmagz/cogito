from __future__ import annotations

import hashlib
import json
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import TimeoutError

with workflow.unsafe.imports_passed_through():
    from .activities import WorkerActivities
    from .models import (
        BackupExecutionRequest,
        ExecutionRequest,
        PhaseExecutionRequest,
        PlanPhase,
        RunEnvelope,
        RunResult,
    )

_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_CLEANUP_ACTIVITY_TIMEOUT = timedelta(seconds=120)
_PROVISION_RETRY_POLICY = RetryPolicy(maximum_attempts=3)
_RUN_PHASE_RETRY_POLICY = RetryPolicy(maximum_attempts=1)
_BACKUP_PHASE_RETRY_POLICY = RetryPolicy(maximum_attempts=3)
_BACKUP_ACTIVITY_TIMEOUT = timedelta(seconds=120)


@workflow.defn
class DeveloperRunWorkflow:
    def __init__(self) -> None:
        self._awaiting_plan_approval = False
        self._plan_sha256 = ""
        self._plan_decision: dict[str, str] | None = None
        self._processed_decision_ids: set[str] = set()

    @workflow.update
    async def submit_plan_approval(self, decision: dict[str, str]) -> bool:
        """Accept one idempotent decision only while the workflow waits for this plan."""

        decision_id = decision.get("decision_id", "")
        if not decision_id:
            return False
        if decision_id in self._processed_decision_ids:
            # The control-plane outbox can retry after Temporal has accepted
            # the update but before it records delivery. A repeated durable
            # decision ID is therefore an acknowledgement, never a new vote.
            return True
        if not self._awaiting_plan_approval:
            return False
        if decision.get("artifact_sha256") != self._plan_sha256:
            return False
        if decision.get("decision") not in {"approve", "reject", "request_revision"}:
            return False
        self._processed_decision_ids.add(decision_id)
        self._plan_decision = decision
        return True

    @workflow.run
    async def run(self, envelope: RunEnvelope) -> RunResult:
        try:
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "claimed"],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            plan = await workflow.execute_activity(
                WorkerActivities.load_plan,
                args=[envelope.plan_ref],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            _validate_plan_snapshot(plan, envelope)
            phases, productive_turns, run_timeout, backup_reserve_turns, max_cost_usd = _execution_plan(plan)
            if envelope.requires_plan_approval:
                self._plan_sha256 = envelope.plan_sha256
                self._awaiting_plan_approval = True
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "awaiting_plan_approval"],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                await workflow.wait_condition(lambda: self._plan_decision is not None)
                self._awaiting_plan_approval = False
                assert self._plan_decision is not None
                decision = self._plan_decision["decision"]
                if decision == "reject":
                    await workflow.execute_activity(
                        WorkerActivities.report_status,
                        args=[envelope.run_id, "rejected"],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    )
                    return RunResult(run_id=envelope.run_id, status="rejected")
                if decision == "request_revision":
                    await workflow.execute_activity(
                        WorkerActivities.report_status,
                        args=[envelope.run_id, "revision_requested"],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    )
                    return RunResult(run_id=envelope.run_id, status="revision_requested")
            workspace = await workflow.execute_activity(
                WorkerActivities.provision_execution_workspace,
                args=[
                    ExecutionRequest(
                        run_id=envelope.run_id,
                        spec_ref=envelope.spec_ref,
                        target_repos=envelope.target_repos,
                        execution_timeout_seconds=(
                            int(run_timeout.total_seconds()) + int(_BACKUP_ACTIVITY_TIMEOUT.total_seconds())
                        ),
                        max_cost_usd=max_cost_usd,
                    )
                ],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_PROVISION_RETRY_POLICY,
            )
            try:
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "implementing"],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                deadline = workflow.now() + run_timeout
                completed_phase_ids: list[str] = []
                phase_results: list[dict] = []
                for phase in phases:
                    remaining = deadline - workflow.now()
                    if remaining <= timedelta():
                        phase_result = await _backup_phase(phase, workspace, "wall_clock")
                    else:
                        try:
                            phase_result = await workflow.execute_activity(
                                WorkerActivities.run_phase,
                                args=[
                                    PhaseExecutionRequest(
                                        phase=phase,
                                        workspace=workspace,
                                        max_turns=productive_turns,
                                        timeout_seconds=max(1, int(remaining.total_seconds()) - 1),
                                        backup_reserve_turns=backup_reserve_turns,
                                    )
                                ],
                                start_to_close_timeout=remaining,
                                retry_policy=_RUN_PHASE_RETRY_POLICY,
                            )
                        except Exception as error:
                            if not _is_timeout_error(error):
                                raise
                            phase_result = await _backup_phase(phase, workspace, "wall_clock")
                        if phase_result.outcome == "ceiling_reached":
                            phase_result = await _backup_phase(phase, workspace, phase_result.ceiling or "unknown")
                    phase_results.append(phase_result.metadata())
                    if phase_result.outcome == "stopped_with_backup":
                        await workflow.execute_activity(
                            WorkerActivities.report_status,
                            args=[
                                envelope.run_id,
                                "stopped_with_backup",
                                None,
                                {
                                    "phase_results": phase_results,
                                    "completed_phase_ids": completed_phase_ids,
                                    "stopped_phase_id": phase.id,
                                    "unfinished_phase_ids": [
                                        candidate.id for candidate in phases if candidate.id not in completed_phase_ids
                                    ],
                                    "branch_name": phase_result.branch_name,
                                    "ceiling": phase_result.ceiling,
                                },
                            ],
                            start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        )
                        return RunResult(run_id=envelope.run_id, status="stopped_with_backup")
                    await workflow.execute_activity(
                        WorkerActivities.report_status,
                        args=[
                            envelope.run_id,
                            "phase_complete" if phase_result.succeeded else "phase_failed",
                            None,
                            {"phase_results": phase_results, "completed_phase_ids": completed_phase_ids},
                        ],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    )
                    if not phase_result.succeeded:
                        raise RuntimeError(f"phase {phase.id} failed: {phase_result.summary}")
                    completed_phase_ids.append(phase.id)
            finally:
                await workflow.execute_activity(
                    WorkerActivities.cleanup_execution_workspace,
                    args=[workspace],
                    start_to_close_timeout=_CLEANUP_ACTIVITY_TIMEOUT,
                )
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "completed"],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            return RunResult(run_id=envelope.run_id, status="completed")
        except Exception as error:
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "failed", _failure_detail(error)],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            # A failed run is a durable terminal business outcome. Returning
            # prevents Temporal from replaying this workflow task indefinitely
            # after the status has already been recorded as failed.
            return RunResult(run_id=envelope.run_id, status="failed")


def _validate_plan_snapshot(plan: dict, envelope: RunEnvelope) -> None:
    """Reject a workflow envelope that does not match its immutable plan snapshot."""

    actual_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    if actual_sha256 != envelope.plan_sha256:
        raise ValueError("run plan snapshot digest does not match the submitted envelope")
    if plan.get("spec_set") != envelope.spec_ref or plan.get("target_repos") != envelope.target_repos:
        raise ValueError("run envelope does not match its immutable plan snapshot")


async def _backup_phase(phase: PlanPhase, workspace, ceiling: str):
    """Run the deterministic recovery activity with bounded, safe retries."""

    return await workflow.execute_activity(
        WorkerActivities.backup_phase,
        args=[
            BackupExecutionRequest(
                phase=phase,
                workspace=workspace,
                ceiling=ceiling,
                timeout_seconds=120,
            )
        ],
        start_to_close_timeout=_BACKUP_ACTIVITY_TIMEOUT,
        retry_policy=_BACKUP_PHASE_RETRY_POLICY,
    )


def _execution_plan(plan: dict) -> tuple[list[PlanPhase], int, timedelta, int, float]:
    """Parse limits and return a source-order-stable topological phase order."""

    phases = plan.get("phases")
    if not isinstance(phases, list) or not phases:
        raise ValueError("execution requires at least one approved plan phase")
    constraints = plan.get("constraints")
    if not isinstance(constraints, dict):
        raise ValueError("plan constraints are missing")
    max_turns = constraints.get("max_turns_per_phase")
    max_wall_clock_minutes = constraints.get("max_wall_clock_minutes")
    max_cost_usd = constraints.get("max_cost_usd")
    backup_reserve_turns = constraints.get("backup_reserve_turns", 25)
    if not isinstance(max_turns, int) or isinstance(max_turns, bool) or max_turns < 1:
        raise ValueError("plan max_turns_per_phase must be a positive integer")
    if (
        not isinstance(max_wall_clock_minutes, int)
        or isinstance(max_wall_clock_minutes, bool)
        or max_wall_clock_minutes < 1
    ):
        raise ValueError("plan max_wall_clock_minutes must be a positive integer")
    if (
        not isinstance(backup_reserve_turns, int)
        or isinstance(backup_reserve_turns, bool)
        or not 20 <= backup_reserve_turns <= 30
    ):
        raise ValueError("plan backup_reserve_turns must be an integer between 20 and 30")
    if max_turns <= backup_reserve_turns:
        raise ValueError("plan max_turns_per_phase must exceed backup_reserve_turns")
    if (
        not isinstance(max_cost_usd, int | float)
        or isinstance(max_cost_usd, bool)
        or max_cost_usd <= 0
        or max_cost_usd == float("inf")
        or max_cost_usd != max_cost_usd
    ):
        raise ValueError("plan max_cost_usd must be a positive finite number")
    parsed_phases = [PlanPhase.from_dict(phase) for phase in phases]
    phase_ids = [phase.id for phase in parsed_phases]
    if len(set(phase_ids)) != len(phase_ids):
        raise ValueError("plan phase IDs must be unique")
    known_ids = set(phase_ids)
    if any(dependency not in known_ids for phase in parsed_phases for dependency in phase.depends_on):
        raise ValueError("plan phase dependencies must reference approved phases")
    remaining_dependencies = {phase.id: set(phase.depends_on) for phase in parsed_phases}
    ordered: list[PlanPhase] = []
    while remaining_dependencies:
        ready = next(
            (
                phase
                for phase in parsed_phases
                if phase.id in remaining_dependencies and not remaining_dependencies[phase.id]
            ),
            None,
        )
        if ready is None:
            raise ValueError("plan phase dependencies must not contain a cycle")
        ordered.append(ready)
        del remaining_dependencies[ready.id]
        for dependencies in remaining_dependencies.values():
            dependencies.discard(ready.id)
    return (
        ordered,
        max_turns - backup_reserve_turns,
        timedelta(minutes=max_wall_clock_minutes),
        backup_reserve_turns,
        float(max_cost_usd),
    )


def _failure_detail(error: Exception) -> str:
    """Return a bounded failure summary suitable for durable workflow status."""

    messages: list[str] = []
    current: BaseException | None = error
    while current is not None and len(messages) < 5:
        message = " ".join(str(current).split())
        if message and message not in messages:
            messages.append(message)
        next_error = getattr(current, "cause", None)
        current = next_error if isinstance(next_error, BaseException) else None
    return " | ".join(messages)[:4096] or error.__class__.__name__


def _is_timeout_error(error: BaseException) -> bool:
    """Recognize Temporal's nested activity-timeout failure without broad recovery."""

    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TimeoutError):
            return True
        cause = getattr(current, "__cause__", None) or getattr(current, "cause", None)
        current = cause if isinstance(cause, BaseException) else None
    return False
