from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from .activities import WorkerActivities
    from .models import RunEnvelope, RunResult

_ACTIVITY_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class DeveloperRunWorkflow:
    @workflow.run
    async def run(self, envelope: RunEnvelope) -> RunResult:
        await workflow.execute_activity(
            WorkerActivities.report_status,
            args=[envelope.run_id, "claimed"],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        await workflow.execute_activity(
            WorkerActivities.load_plan,
            args=[envelope.plan_ref],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        await workflow.execute_activity(
            WorkerActivities.report_status,
            args=[envelope.run_id, "completed"],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        return RunResult(run_id=envelope.run_id, status="completed")
