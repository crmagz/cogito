from __future__ import annotations

import hashlib
import json
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import WorkerActivities
    from .models import ExecutionRequest, PhaseExecutionRequest, PlanPhase, RunEnvelope, RunResult

_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_CLEANUP_ACTIVITY_TIMEOUT = timedelta(seconds=120)
_PROVISION_RETRY_POLICY = RetryPolicy(maximum_attempts=3)
_RUN_PHASE_RETRY_POLICY = RetryPolicy(maximum_attempts=1)


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
            phase, max_turns, phase_timeout = _single_phase_execution_limits(plan)
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
                phase_result = await workflow.execute_activity(
                    WorkerActivities.run_phase,
                    args=[
                        PhaseExecutionRequest(
                            phase=phase,
                            workspace=workspace,
                            max_turns=max_turns,
                            timeout_seconds=int(phase_timeout.total_seconds()),
                        )
                    ],
                    start_to_close_timeout=phase_timeout,
                    retry_policy=_RUN_PHASE_RETRY_POLICY,
                )
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[
                        envelope.run_id,
                        "phase_complete" if phase_result.succeeded else "phase_failed",
                        None,
                        {"phase_result": phase_result.metadata()},
                    ],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                if not phase_result.succeeded:
                    raise RuntimeError(f"phase {phase.id} failed: {phase_result.summary}")
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
            raise


def _validate_plan_snapshot(plan: dict, envelope: RunEnvelope) -> None:
    """Reject a workflow envelope that does not match its immutable plan snapshot."""

    actual_sha256 = hashlib.sha256(
        json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    if actual_sha256 != envelope.plan_sha256:
        raise ValueError("run plan snapshot digest does not match the submitted envelope")
    if plan.get("spec_set") != envelope.spec_ref or plan.get("target_repos") != envelope.target_repos:
        raise ValueError("run envelope does not match its immutable plan snapshot")


def _single_phase_execution_limits(plan: dict) -> tuple[PlanPhase, int, timedelta]:
    """Extract one executable phase and reject multi-phase plans until Phase 8 sequencing exists."""

    phases = plan.get("phases")
    if not isinstance(phases, list) or len(phases) != 1:
        raise ValueError("single-phase execution requires exactly one approved plan phase")
    constraints = plan.get("constraints")
    if not isinstance(constraints, dict):
        raise ValueError("plan constraints are missing")
    max_turns = constraints.get("max_turns_per_phase")
    max_wall_clock_minutes = constraints.get("max_wall_clock_minutes")
    if not isinstance(max_turns, int) or isinstance(max_turns, bool) or max_turns < 1:
        raise ValueError("plan max_turns_per_phase must be a positive integer")
    if (
        not isinstance(max_wall_clock_minutes, int)
        or isinstance(max_wall_clock_minutes, bool)
        or max_wall_clock_minutes < 1
    ):
        raise ValueError("plan max_wall_clock_minutes must be a positive integer")
    return PlanPhase.from_dict(phases[0]), max_turns, timedelta(minutes=max_wall_clock_minutes)


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
