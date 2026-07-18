from __future__ import annotations

from cogito_worker.models import ExecutionRequest, ExecutionWorkspace


class InMemoryRunStore:
    def __init__(self) -> None:
        self.plans: dict[str, dict] = {}
        self.statuses: dict[str, dict] = {}

    def get_plan(self, plan_ref: str) -> dict:
        return self.plans[plan_ref]

    def get_status(self, run_id: str) -> dict | None:
        return self.statuses.get(run_id)

    def put_status(self, run_id: str, status: dict) -> None:
        self.statuses[run_id] = status


class InMemoryExecutionWorkspaces:
    def __init__(self) -> None:
        self.provisioned: list[str] = []
        self.cleaned: list[ExecutionWorkspace] = []

    async def provision(self, request: ExecutionRequest) -> ExecutionWorkspace:
        self.provisioned.append(request.run_id)
        return ExecutionWorkspace(
            run_id=request.run_id,
            job_name=f"cogito-execution-{request.run_id}",
            workspace_root="/workspace",
        )

    async def cleanup(self, workspace: ExecutionWorkspace) -> None:
        self.cleaned.append(workspace)


class InMemoryExecutionJobClient:
    def __init__(self) -> None:
        self.created: list[tuple[str, dict[str, object]]] = []
        self.deleted: list[str] = []
        self.awaited: list[tuple[str, int]] = []

    async def create_job(self, job_name: str, body: dict[str, object]) -> None:
        self.created.append((job_name, body))

    async def delete_job(self, job_name: str) -> None:
        self.deleted.append(job_name)

    async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
        self.awaited.append((job_name, timeout_seconds))
