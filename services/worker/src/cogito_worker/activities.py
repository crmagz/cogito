from __future__ import annotations

from typing import Any

from temporalio import activity

from .execution import ExecutionWorkspaceService
from .github import PullRequestPublisher, PullRequestResult
from .harness import ClaudeCodeHarness
from .models import (
    BackupExecutionRequest,
    ExecutionRequest,
    ExecutionWorkspace,
    ImplementationArtifact,
    McpToolGrant,
    PhaseExecutionRequest,
    PhaseResult,
    ReviewFinding,
    ReviewRequest,
    ReviewResult,
    ReviewRevisionRequest,
    ReviewRevisionResult,
    ValidationRequest,
    ValidationResult,
)
from .observability import WorkerTelemetry
from .review import LiteLLMReviewHarness
from .run_state import NullRunStateReporter, RunStateReporter
from .storage import RunStore, now_iso


def _mcp_grant_evidence(grant: McpToolGrant, role: str = "developer") -> dict[str, str]:
    """Serialize only non-secret pinned grant identity for immutable audit evidence."""

    evidence = {
        "role": role,
        "server_id": grant.server_id,
        "server_version": grant.server_version,
        "server_manifest_sha256": grant.server_manifest_sha256,
        "tool_name": grant.tool_name,
        "input_schema_sha256": grant.input_schema_sha256,
    }
    if grant.repository_scope is not None:
        evidence["repository_scope"] = grant.repository_scope
    return evidence


class WorkerActivities:
    def __init__(
        self,
        store: RunStore,
        execution_workspaces: ExecutionWorkspaceService,
        harness: ClaudeCodeHarness,
        telemetry: WorkerTelemetry | None = None,
        run_state: RunStateReporter | None = None,
        reviewer: LiteLLMReviewHarness | None = None,
        pull_request_publisher: PullRequestPublisher | None = None,
    ):
        self._store = store
        self._execution_workspaces = execution_workspaces
        self._harness = harness
        self._reviewer = reviewer or _NoopReviewHarness()
        self._pull_request_publisher = pull_request_publisher or _NoopPullRequestPublisher()
        self._telemetry = telemetry or WorkerTelemetry()
        self._run_state = run_state or NullRunStateReporter()

    @activity.defn
    async def load_plan(self, plan_ref: str) -> dict:
        activity.logger.info("loading plan", extra={"plan_ref": plan_ref})
        return self._store.get_plan(plan_ref)

    @activity.defn
    async def load_resolved_workflow(self, workflow_ref: str) -> dict:
        """Load the API-compiled immutable workflow before execution begins."""

        activity.logger.info("loading resolved workflow", extra={"workflow_ref": workflow_ref})
        return self._store.get_artifact(workflow_ref)

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
    async def freeze_implementation_artifact(
        self, run_id: str, evidence: dict[str, Any], workspace: ExecutionWorkspace | None = None
    ) -> ImplementationArtifact:
        """Persist the exact converged implementation evidence before human approval."""

        activity.logger.info("freezing implementation artifact", extra={"run_id": run_id})
        frozen_evidence = dict(evidence)
        if workspace is not None and (workspace.mcp_grants or workspace.mcp_selection_explicit):
            invocation_evidence = await self._execution_workspaces.collect_mcp_invocations(workspace)
            frozen_evidence["mcp_invocations"] = {
                **(
                    invocation_evidence
                    if invocation_evidence is not None
                    else {"version": 1, "status": "not_applicable", "events": []}
                ),
                "selected_grants": [_mcp_grant_evidence(grant) for grant in workspace.mcp_grants],
            }
        return self._store.put_implementation_artifact(run_id, frozen_evidence)

    @activity.defn
    async def validate_implementation(self, request: ValidationRequest) -> ValidationResult:
        """Evaluate only durable implementation evidence without mutating a repository."""

        activity.logger.info("validating implementation evidence", extra={"run_id": request.run_id})
        if request.review.get("status") != "converged":
            return ValidationResult(status="failed", checked_phases=0, reason="review_not_converged")
        if not request.phase_results:
            return ValidationResult(status="failed", checked_phases=0, reason="no_phase_results")
        for phase in request.phase_results:
            if phase.get("succeeded") is not True:
                return ValidationResult(status="failed", checked_phases=0, reason="phase_not_succeeded")
            verification = phase.get("verification")
            if not verification or not isinstance(verification, list) or not all(
                isinstance(item, dict) and item.get("passed") is True for item in verification
            ):
                return ValidationResult(status="failed", checked_phases=0, reason="verification_not_passed")
        return ValidationResult(status="passed", checked_phases=len(request.phase_results))

    @activity.defn
    async def open_pull_request(self, artifact_sha256: str, evidence: dict[str, Any]) -> PullRequestResult:
        """Create or recover exactly one GitHub pull request for an approved artifact."""

        safe_evidence = dict(evidence)
        safe_evidence["artifact_sha256"] = artifact_sha256
        try:
            return await self._pull_request_publisher.open_or_reuse(artifact_sha256, safe_evidence)
        except Exception as error:
            # HTTP/provider exceptions can retain request details. Preserve a
            # safe category for the workflow status instead of propagating it.
            raise RuntimeError("GitHub pull-request publication failed") from error

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

    @activity.defn
    async def review(self, request: ReviewRequest) -> ReviewResult:
        """Run independent read-only reviewer lenses against the exact branch diff."""

        activity.logger.info(
            "reviewing implementation diff",
            extra={"run_id": request.workspace.run_id, "round": request.round_number},
        )
        return await self._reviewer.review(request)

    @activity.defn
    async def verify_review_findings(
        self, request: ReviewRequest, findings: list[ReviewFinding]
    ) -> list[ReviewFinding]:
        """Confirm only blocking review findings before developer revision."""

        return await self._reviewer.verify_blocking(request, findings)

    @activity.defn
    async def address_review_findings(
        self, request: ReviewRevisionRequest
    ) -> ReviewRevisionResult:
        """Ask the developer harness to address verified blockers only."""

        activity.logger.info(
            "addressing verified review findings",
            extra={"run_id": request.workspace.run_id, "findings": len(request.findings)},
        )
        return await self._harness.address_review_findings(request)


class _NoopReviewHarness:
    """Test-only default preserving existing activity construction seams."""

    async def review(self, request: ReviewRequest) -> ReviewResult:
        del request
        return ReviewResult(findings=[])

    async def verify_blocking(
        self, request: ReviewRequest, findings: list[ReviewFinding]
    ) -> list[ReviewFinding]:
        del request
        return findings


class _NoopPullRequestPublisher:
    """Fails closed when the worker lacks an explicitly configured GitHub publisher."""

    async def open_or_reuse(self, artifact_sha256: str, evidence: dict[str, Any]) -> PullRequestResult:
        del artifact_sha256, evidence
        raise RuntimeError("GitHub pull-request publisher is not configured")
