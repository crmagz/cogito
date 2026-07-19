from __future__ import annotations

from cogito_worker.execution import CommandResult
from cogito_worker.models import ExecutionRequest, ExecutionWorkspace, PhaseExecutionRequest, PhaseResult


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
            repositories=["/workspace/repos/example"] if request.target_repos else [],
        )

    async def cleanup(self, workspace: ExecutionWorkspace) -> None:
        self.cleaned.append(workspace)


class InMemoryExecutionJobClient:
    def __init__(self) -> None:
        self.created: list[tuple[str, dict[str, object]]] = []
        self.deleted: list[str] = []
        self.awaited: list[tuple[str, int]] = []
        self.executed: list[tuple[str, list[str], str]] = []

    async def create_job(self, job_name: str, body: dict[str, object]) -> None:
        self.created.append((job_name, body))

    async def delete_job(self, job_name: str) -> None:
        self.deleted.append(job_name)

    async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
        self.awaited.append((job_name, timeout_seconds))

    async def execute(
        self,
        job_name: str,
        command: list[str],
        stdin: str,
        timeout_seconds: int,
        output_limit_bytes: int,
    ) -> CommandResult:
        self.executed.append((job_name, command, stdin))
        return CommandResult(exit_code=0, stdout="", stderr="")


class InMemoryHarness:
    """Returns preconfigured phase results while recording workflow activity inputs."""

    def __init__(self, result: PhaseResult | None = None) -> None:
        self.requests: list[PhaseExecutionRequest] = []
        self.result = result

    async def execute_phase(self, request: PhaseExecutionRequest) -> PhaseResult:
        self.requests.append(request)
        if self.result is not None:
            return self.result
        return PhaseResult(
            phase_id=request.phase.id,
            branch_name=f"adp/{request.workspace.run_id}",
            succeeded=True,
            turns_used=3,
            cost_usd=0.01,
            changed_files=["/workspace/repos/example:src/main.py"],
            commits={"/workspace/repos/example": "a" * 40},
            verification=[],
            summary="completed",
        )
