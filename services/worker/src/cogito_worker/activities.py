from __future__ import annotations

from temporalio import activity

from .execution import ExecutionWorkspaceService
from .models import ExecutionRequest, ExecutionWorkspace
from .storage import RunStore, now_iso


class WorkerActivities:
    def __init__(
        self,
        store: RunStore,
        execution_workspaces: ExecutionWorkspaceService,
    ):
        self._store = store
        self._execution_workspaces = execution_workspaces

    @activity.defn
    async def load_plan(self, plan_ref: str) -> dict:
        activity.logger.info("loading plan", extra={"plan_ref": plan_ref})
        return self._store.get_plan(plan_ref)

    @activity.defn
    async def report_status(self, run_id: str, status: str, failure_detail: str | None = None) -> None:
        activity.logger.info("reporting status", extra={"run_id": run_id, "status": status})
        record = self._store.get_status(run_id) or {"run_id": run_id}
        record["status"] = status
        record["updated_at"] = now_iso()
        if failure_detail is not None:
            record["failure_detail"] = failure_detail
        self._store.put_status(run_id, record)

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
