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
