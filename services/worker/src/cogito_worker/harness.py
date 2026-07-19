"""Claude Code harness implementation for one approved plan phase."""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass

from .execution import CommandResult, ExecutionWorkspaceService, _sanitize_diagnostics
from .execution_prepare import feature_branch_name
from .models import BackupExecutionRequest, PhaseExecutionRequest, PhaseResult, VerificationResult


@dataclass(frozen=True)
class _AgentResult:
    turns_used: int | None
    cost_usd: float | None
    summary: str
    succeeded: bool
    ceiling: str | None = None


class ClaudeCodeHarness:
    """Runs a pinned Claude Code CLI only inside an approved execution workspace."""

    def __init__(self, workspaces: ExecutionWorkspaceService):
        self._workspaces = workspaces

    async def execute_phase(self, request: PhaseExecutionRequest) -> PhaseResult:
        """Execute, verify, and publish exactly one previously approved phase."""

        if not request.workspace.repositories:
            raise ValueError("single-phase execution requires at least one target repository")
        branch_name = feature_branch_name(request.workspace.run_id)
        await self._assert_expected_repositories(request, branch_name)
        before_commits = await self._head_commits(request)
        agent = await self._run_agent(request)
        if not agent.succeeded:
            return PhaseResult(
                phase_id=request.phase.id,
                branch_name=branch_name,
                succeeded=False,
                turns_used=agent.turns_used,
                cost_usd=agent.cost_usd,
                changed_files=[],
                commits={},
                verification=[],
                summary=agent.summary,
                outcome="ceiling_reached" if agent.ceiling else "failed",
                ceiling=agent.ceiling,
            )

        commits = await self._head_commits(request)
        await self._assert_expected_repositories(request, branch_name)
        changed_files = await self._changed_files(request, before_commits, commits)
        if not changed_files:
            return PhaseResult(
                phase_id=request.phase.id,
                branch_name=branch_name,
                succeeded=False,
                turns_used=agent.turns_used,
                cost_usd=agent.cost_usd,
                changed_files=[],
                commits=commits,
                verification=[],
                summary="agent completed without a committed change on the feature branch",
                outcome="failed",
            )

        dirty_repositories = await self._dirty_repositories(request)
        if dirty_repositories:
            return PhaseResult(
                phase_id=request.phase.id,
                branch_name=branch_name,
                succeeded=False,
                turns_used=agent.turns_used,
                cost_usd=agent.cost_usd,
                changed_files=changed_files,
                commits=commits,
                verification=[],
                summary=f"agent left uncommitted changes in {', '.join(dirty_repositories)}",
                outcome="failed",
            )

        verification = await self._verify(request)
        if not all(result.passed for result in verification):
            return PhaseResult(
                phase_id=request.phase.id,
                branch_name=branch_name,
                succeeded=False,
                turns_used=agent.turns_used,
                cost_usd=agent.cost_usd,
                changed_files=changed_files,
                commits=commits,
                verification=verification,
                summary="one or more approved verification commands failed",
                outcome="failed",
            )

        dirty_repositories = await self._dirty_repositories(request)
        if dirty_repositories:
            return PhaseResult(
                phase_id=request.phase.id,
                branch_name=branch_name,
                succeeded=False,
                turns_used=agent.turns_used,
                cost_usd=agent.cost_usd,
                changed_files=changed_files,
                commits=commits,
                verification=verification,
                summary=f"verification left uncommitted changes in {', '.join(dirty_repositories)}",
                outcome="failed",
            )

        push_failure = await self._push_feature_branch(request, branch_name)
        if push_failure is not None:
            return PhaseResult(
                phase_id=request.phase.id,
                branch_name=branch_name,
                succeeded=False,
                turns_used=agent.turns_used,
                cost_usd=agent.cost_usd,
                changed_files=changed_files,
                commits=commits,
                verification=verification,
                summary=push_failure,
                outcome="failed",
            )

        return PhaseResult(
            phase_id=request.phase.id,
            branch_name=branch_name,
            succeeded=True,
            turns_used=agent.turns_used,
            cost_usd=agent.cost_usd,
            changed_files=changed_files,
            commits=commits,
            verification=verification,
            summary=agent.summary,
        )

    async def backup_phase(self, request: BackupExecutionRequest) -> PhaseResult:
        """Commit and push existing work without invoking a productive model command."""

        if not request.workspace.repositories:
            raise ValueError("backup requires at least one target repository")
        branch_name = feature_branch_name(request.workspace.run_id)
        execution_request = PhaseExecutionRequest(
            phase=request.phase,
            workspace=request.workspace,
            max_turns=1,
            timeout_seconds=request.timeout_seconds,
        )
        await self._assert_expected_repositories(execution_request, branch_name)
        before_commits = await self._head_commits(execution_request)
        for repository in request.workspace.repositories:
            staged = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "add", "-A"],
                timeout_seconds=request.timeout_seconds,
            )
            if staged.exit_code != 0:
                return _backup_failure(
                    request,
                    branch_name,
                    f"could not stage recovery changes: {_command_error(staged)}",
                )
            staged_changes = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "diff", "--cached", "--quiet"],
                timeout_seconds=request.timeout_seconds,
            )
            if staged_changes.exit_code not in {0, 1}:
                return _backup_failure(
                    request,
                    branch_name,
                    f"could not inspect staged recovery changes: {_command_error(staged_changes)}",
                )
            if staged_changes.exit_code == 1:
                committed = await self._workspaces.execute(
                    request.workspace,
                    ["git", "-C", repository, "commit", "-m", f"cogito backup: {request.phase.id} ({request.ceiling})"],
                    timeout_seconds=request.timeout_seconds,
                )
                if committed.exit_code != 0:
                    return _backup_failure(
                        request,
                        branch_name,
                        f"could not commit recovery changes: {_command_error(committed)}",
                    )
        commits = await self._head_commits(execution_request)
        await self._assert_expected_repositories(execution_request, branch_name)
        changed_files = await self._changed_files(execution_request, before_commits, commits)
        push_failure = await self._push_feature_branch(execution_request, branch_name)
        if push_failure is not None:
            return _backup_failure(request, branch_name, push_failure, commits=commits, changed_files=changed_files)
        return PhaseResult(
            phase_id=request.phase.id,
            branch_name=branch_name,
            succeeded=True,
            turns_used=None,
            cost_usd=None,
            changed_files=changed_files,
            commits=commits,
            verification=[],
            summary=f"progress preserved after {request.ceiling} ceiling",
            outcome="stopped_with_backup",
            ceiling=request.ceiling,
        )

    async def _run_agent(self, request: PhaseExecutionRequest) -> _AgentResult:
        result = await self._workspaces.execute(
            request.workspace,
            [
                "claude",
                "--print",
                "--output-format",
                "json",
                "--max-turns",
                str(request.max_turns),
                "--dangerously-skip-permissions",
            ],
            stdin=_assemble_prompt(request),
            timeout_seconds=request.timeout_seconds,
        )
        return _parse_agent_result(result, request.max_turns)

    async def _head_commits(self, request: PhaseExecutionRequest) -> dict[str, str]:
        commits: dict[str, str] = {}
        for repository in request.workspace.repositories:
            result = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "rev-parse", "HEAD"],
                timeout_seconds=30,
            )
            if result.exit_code != 0:
                raise RuntimeError(f"could not resolve feature-branch head: {_command_error(result)}")
            commits[repository] = result.stdout.strip()
        return commits

    async def _assert_expected_repositories(self, request: PhaseExecutionRequest, branch_name: str) -> None:
        """Reject an agent-altered branch or origin before reading or publishing changes."""

        for repository in request.workspace.repositories:
            expected_origin = request.workspace.repository_origins.get(repository)
            if expected_origin is None:
                raise RuntimeError(f"workspace is missing the expected origin for {repository}")
            branch = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "branch", "--show-current"],
                timeout_seconds=30,
            )
            if branch.exit_code != 0 or branch.stdout.strip() != branch_name:
                raise RuntimeError(f"repository is not on expected feature branch {branch_name}")
            origin = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "remote", "get-url", "origin"],
                timeout_seconds=30,
            )
            if origin.exit_code != 0 or origin.stdout.strip() != expected_origin:
                raise RuntimeError("repository origin no longer matches the approved repository")

    async def _changed_files(
        self,
        request: PhaseExecutionRequest,
        before_commits: dict[str, str],
        after_commits: dict[str, str],
    ) -> list[str]:
        changed_files: list[str] = []
        for repository in request.workspace.repositories:
            before = before_commits[repository]
            after = after_commits[repository]
            result = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "diff", "--name-only", f"{before}..{after}"],
                timeout_seconds=30,
            )
            if result.exit_code != 0:
                raise RuntimeError(f"could not collect committed changes: {_command_error(result)}")
            changed_files.extend(f"{repository}:{path}" for path in result.stdout.splitlines() if path)
        return changed_files

    async def _dirty_repositories(self, request: PhaseExecutionRequest) -> list[str]:
        dirty: list[str] = []
        for repository in request.workspace.repositories:
            result = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "status", "--porcelain=v1"],
                timeout_seconds=30,
            )
            if result.exit_code != 0:
                raise RuntimeError(f"could not inspect feature-branch state: {_command_error(result)}")
            if result.stdout.strip():
                dirty.append(repository)
        return dirty

    async def _verify(self, request: PhaseExecutionRequest) -> list[VerificationResult]:
        results: list[VerificationResult] = []
        for command in request.phase.verification:
            for repository in request.workspace.repositories:
                shell_command = f"cd -- {shlex.quote(repository)} && {command}"
                result = await self._workspaces.execute(
                    request.workspace,
                    ["/bin/sh", "-lc", shell_command],
                    timeout_seconds=request.timeout_seconds,
                )
                output = _command_output(result)
                results.append(
                    VerificationResult(
                        command=f"{repository}: {command}",
                        passed=result.exit_code == 0,
                        output=output,
                    )
                )
        return results

    async def _push_feature_branch(self, request: PhaseExecutionRequest, branch_name: str) -> str | None:
        for repository in request.workspace.repositories:
            result = await self._workspaces.execute(
                request.workspace,
                ["git", "-C", repository, "push", "--set-upstream", "origin", branch_name],
                timeout_seconds=request.timeout_seconds,
            )
            if result.exit_code != 0:
                return f"could not publish feature branch: {_command_error(result)}"
        return None


def _assemble_prompt(request: PhaseExecutionRequest) -> str:
    """Build a constrained prompt containing the approved phase and workspace context."""

    phase = request.phase
    specifications_root = f"{request.workspace.workspace_root}/specs"
    repositories = "\n".join(f"- {repository}" for repository in request.workspace.repositories)
    tasks = "\n".join(f"- {task}" for task in phase.tasks)
    acceptance = "\n".join(f"- {criterion}" for criterion in phase.acceptance_criteria)
    return f"""You are executing one human-approved software-delivery phase.

Phase ID: {phase.id}
Phase name: {phase.name}
Objective: {phase.description}

Approved tasks:
{tasks}

Acceptance criteria:
{acceptance}

Workspace context:
- Repositories (already checked out on feature branch `adp/{request.workspace.run_id}`):
{repositories}
- Resolved immutable specifications: {specifications_root}

Read all relevant specification files before editing. Work only inside the listed repositories.
Do not create or modify credentials, deployment control-plane resources, or files outside the workspace.
Do not push: the harness publishes a clean, verified feature branch after you finish.
Make the implementation, run any useful focused checks, commit all intended changes on the existing feature branch,
and leave every repository clean. In your final response, summarize the implementation and checks performed.
"""


def _parse_agent_result(result: CommandResult, max_turns: int) -> _AgentResult:
    """Parse Claude Code's structured result without trusting an unbounded subprocess response."""

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return _AgentResult(
            turns_used=None,
            cost_usd=None,
            summary=f"Claude Code did not produce a structured result: {_command_error(result)}",
            succeeded=False,
            ceiling=_ceiling_from_command_result(result),
        )
    if not isinstance(payload, dict):
        return _AgentResult(
            None,
            None,
            "Claude Code returned an invalid result payload",
            False,
            _ceiling_from_command_result(result),
        )
    turns = payload.get("num_turns")
    cost = payload.get("total_cost_usd")
    summary = payload.get("result")
    summary_text = (
        _sanitize_diagnostics(summary)
        if isinstance(summary, str) and summary.strip()
        else "Claude Code completed"
    )
    succeeded = result.exit_code == 0 and payload.get("is_error") is False
    ceiling = None if succeeded else _ceiling_from_command_result(result)
    if ceiling is None and not succeeded and isinstance(turns, int) and not isinstance(turns, bool) and turns >= max_turns:
        ceiling = "turns"
    return _AgentResult(
        turns_used=turns if isinstance(turns, int) and not isinstance(turns, bool) and turns >= 0 else None,
        cost_usd=float(cost)
        if isinstance(cost, int | float) and not isinstance(cost, bool) and cost >= 0
        else None,
        summary=summary_text,
        succeeded=succeeded,
        ceiling=ceiling,
    )


def _command_error(result: CommandResult) -> str:
    """Return a bounded command-error summary without choosing a trusted output stream."""

    return _command_output(result) or f"command exited with status {result.exit_code}"


def _command_output(result: CommandResult) -> str:
    """Return bounded output suitable for durable verification evidence."""

    return _sanitize_diagnostics("\n".join(value for value in (result.stdout.strip(), result.stderr.strip()) if value))


def _ceiling_from_command_result(result: CommandResult) -> str | None:
    """Classify only known local timeout and pinned gateway budget signals."""

    if result.exit_code == 124:
        return "wall_clock"
    output = "\n".join((result.stdout, result.stderr)).lower()
    if result.exit_code != 0 and "429" in output and (
        "max budget limit reached" in output or "budget has been exceeded" in output
    ):
        return "cost"
    return None


def _backup_failure(
    request: BackupExecutionRequest,
    branch_name: str,
    summary: str,
    *,
    commits: dict[str, str] | None = None,
    changed_files: list[str] | None = None,
) -> PhaseResult:
    """Return a terminal failed result when recoverable progress cannot be published."""

    return PhaseResult(
        phase_id=request.phase.id,
        branch_name=branch_name,
        succeeded=False,
        turns_used=None,
        cost_usd=None,
        changed_files=changed_files or [],
        commits=commits or {},
        verification=[],
        summary=summary,
        outcome="failed",
        ceiling=request.ceiling,
    )
