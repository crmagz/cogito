from __future__ import annotations

import hashlib
import json
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import WorkerActivities
    from .models import ExecutionRequest, RunEnvelope, RunResult

_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_CLEANUP_ACTIVITY_TIMEOUT = timedelta(seconds=120)
_PROVISION_RETRY_POLICY = RetryPolicy(maximum_attempts=3)


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
            return False
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
                # The future harness runs inside the prepared execution pod.
                # Keep cleanup in finally so a later harness failure cannot leak its workspace.
                pass
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
