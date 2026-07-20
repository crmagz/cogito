from __future__ import annotations

from typing import Any

from temporalio import activity

from .execution import ExecutionWorkspaceService
from .harness import ClaudeCodeHarness
from .models import (
    BackupExecutionRequest,
    ExecutionRequest,
    ExecutionWorkspace,
    PhaseExecutionRequest,
    PhaseResult,
)
from .observability import WorkerTelemetry
from .run_state import NullRunStateReporter, RunStateReporter
from .storage import RunStore, now_iso


class WorkerActivities:
    def __init__(
        self,
        store: RunStore,
        execution_workspaces: ExecutionWorkspaceService,
        harness: ClaudeCodeHarness,
        telemetry: WorkerTelemetry | None = None,
        run_state: RunStateReporter | None = None,
    ):
        self._store = store
        self._execution_workspaces = execution_workspaces
        self._harness = harness
        self._telemetry = telemetry or WorkerTelemetry()
        self._run_state = run_state or NullRunStateReporter()

    @activity.defn
    async def load_plan(self, plan_ref: str) -> dict:
        activity.logger.info("loading plan", extra={"plan_ref": plan_ref})
        return self._store.get_plan(plan_ref)

    @activity.defn
    async def report_status(
        self,
        run_id: str,
        status: str,
        failure_detail: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        activity.logger.info("reporting status", extra={"run_id": run_id, "status": status})
        record = self._store.get_status(run_id) or {"run_id": run_id}
        record["status"] = status
        record["updated_at"] = now_iso()
        if failure_detail is not None:
            record["failure_detail"] = failure_detail
        if metadata is not None:
            record.update(metadata)
        self._store.put_status(run_id, record)
        await self._run_state.report(run_id, status, failure_detail, metadata)

    @activity.defn
    async def provision_execution_workspace(self, request: ExecutionRequest) -> ExecutionWorkspace:
        """Create the isolated execution Job for this run."""

        activity.logger.info("provisioning execution workspace", extra={"run_id": request.run_id})
        return await self._execution_workspaces.provision(request)

    @activity.defn
    async def cleanup_execution_workspace(self, workspace: ExecutionWorkspace) -> None:
        """Delete the execution Job and its pod-local `emptyDir` workspace."""

        activity.logger.info(
            "cleaning execution workspace",
            extra={"run_id": workspace.run_id, "job_name": workspace.job_name},
        )
        await self._execution_workspaces.cleanup(workspace)

    @activity.defn
    async def run_phase(self, request: PhaseExecutionRequest) -> PhaseResult:
        """Run a single approved phase and return durable execution evidence."""

        activity.logger.info(
            "running approved plan phase",
            extra={"run_id": request.workspace.run_id, "phase_id": request.phase.id},
        )
        with self._telemetry.span("cogito.worker.phase", request.traceparent, request.tracestate):
            return await self._harness.execute_phase(request)

    @activity.defn
    async def backup_phase(self, request: BackupExecutionRequest) -> PhaseResult:
        """Commit and push recoverable progress after a known execution ceiling."""

        activity.logger.info(
            "backing up stopped plan phase",
            extra={
                "run_id": request.workspace.run_id,
                "phase_id": request.phase.id,
                "ceiling": request.ceiling,
            },
        )
        return await self._harness.backup_phase(request)
