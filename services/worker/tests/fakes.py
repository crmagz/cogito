from __future__ import annotations

from cogito_worker.execution import CommandResult
from cogito_worker.github import PullRequestResult
from cogito_worker.models import (
    BackupExecutionRequest,
    ExecutionRequest,
    ExecutionWorkspace,
    ImplementationArtifact,
    PhaseExecutionRequest,
    PhaseResult,
    ReviewFinding,
    ReviewRequest,
    ReviewResult,
    ReviewRevisionRequest,
    ReviewRevisionResult,
)


class InMemoryRunStore:
    def __init__(self) -> None:
        self.plans: dict[str, dict] = {}
        self.statuses: dict[str, dict] = {}
        self.implementation_artifacts: dict[str, dict] = {}

    def get_plan(self, plan_ref: str) -> dict:
        return self.plans[plan_ref]

    def get_status(self, run_id: str) -> dict | None:
        return self.statuses.get(run_id)

    def put_status(self, run_id: str, status: dict) -> None:
        self.statuses[run_id] = status

    def put_implementation_artifact(self, run_id: str, artifact: dict) -> ImplementationArtifact:
        from hashlib import sha256
        import json

        digest = sha256(json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.implementation_artifacts[digest] = artifact
        return ImplementationArtifact(ref=f"s3://plan-snapshots/runs/{run_id}/implementation/{digest}/artifact.json", sha256=digest)


class InMemoryExecutionWorkspaces:
    def __init__(self, cleanup_error: Exception | None = None) -> None:
        self.provisioned: list[str] = []
        self.cleaned: list[ExecutionWorkspace] = []
        self.cleanup_error = cleanup_error

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
        if self.cleanup_error is not None:
            raise self.cleanup_error


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

    def __init__(
        self,
        result: PhaseResult | None = None,
        backup_result: PhaseResult | None = None,
        results: list[PhaseResult] | None = None,
    ) -> None:
        self.requests: list[PhaseExecutionRequest] = []
        self.backup_requests: list[BackupExecutionRequest] = []
        self.review_revision_requests: list[ReviewRevisionRequest] = []
        self.result = result
        self.backup_result = backup_result
        self.results = list(results or [])

    async def execute_phase(self, request: PhaseExecutionRequest) -> PhaseResult:
        self.requests.append(request)
        if self.results:
            return self.results.pop(0)
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

    async def backup_phase(self, request: BackupExecutionRequest) -> PhaseResult:
        self.backup_requests.append(request)
        if self.backup_result is not None:
            return self.backup_result
        return PhaseResult(
            phase_id=request.phase.id,
            branch_name=f"adp/{request.workspace.run_id}",
            succeeded=True,
            turns_used=None,
            cost_usd=None,
            changed_files=[],
            commits={},
            verification=[],
            summary="recoverable progress backed up",
            outcome="stopped_with_backup",
            ceiling=request.ceiling,
        )

    async def address_review_findings(
        self, request: ReviewRevisionRequest
    ) -> ReviewRevisionResult:
        self.review_revision_requests.append(request)
        return ReviewRevisionResult(
            succeeded=True,
            summary="verified blockers addressed",
            commits={"/workspace/repos/example": "c" * 40},
            changed_files=["/workspace/repos/example:src/main.py"],
            verification=[],
        )


class InMemoryReviewer:
    """Returns configured review outcomes while recording each read-only round."""

    def __init__(
        self,
        results: list[ReviewResult] | None = None,
        verified: list[ReviewFinding] | None = None,
        review_error: Exception | None = None,
    ) -> None:
        self.requests: list[ReviewRequest] = []
        self.verification_requests: list[tuple[ReviewRequest, list[ReviewFinding]]] = []
        self.results = list(results or [])
        self.verified = verified
        self.review_error = review_error

    async def review(self, request: ReviewRequest) -> ReviewResult:
        self.requests.append(request)
        if self.review_error is not None:
            raise self.review_error
        if self.results:
            return self.results.pop(0)
        return ReviewResult(findings=[])

    async def verify_blocking(
        self, request: ReviewRequest, findings: list[ReviewFinding]
    ) -> list[ReviewFinding]:
        self.verification_requests.append((request, findings))
        if not findings:
            return []
        return self.verified if self.verified is not None else findings


class InMemoryPullRequestPublisher:
    """Captures approved publication attempts without contacting GitHub."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict]] = []

    async def open_or_reuse(self, artifact_sha256: str, evidence: dict) -> PullRequestResult:
        self.requests.append((artifact_sha256, evidence))
        return PullRequestResult(number=42, url="https://github.com/acme/example/pull/42", reused=False)
