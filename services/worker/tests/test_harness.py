from __future__ import annotations

import json

from cogito_worker.execution import CommandResult
from cogito_worker.harness import ClaudeCodeHarness
from cogito_worker.models import BackupExecutionRequest, ExecutionWorkspace, PhaseExecutionRequest, PlanPhase


class ScriptedWorkspaces:
    """Records harness commands and returns a deterministic one-repository execution."""

    def __init__(
        self,
        *,
        verification_exit_code: int = 0,
        agent_stdout: str | None = None,
        agent_exit_code: int = 0,
        agent_stderr: str = "",
        origin: str = "https://github.com/acme/example.git",
        dirty_after_verification: bool = False,
        staged_changes: bool = False,
    ) -> None:
        self.calls: list[tuple[list[str], str]] = []
        self._head_calls = 0
        self._verification_exit_code = verification_exit_code
        self._origin = origin
        self._dirty_after_verification = dirty_after_verification
        self._status_calls = 0
        self._agent_stdout = agent_stdout or json.dumps(
            {"is_error": False, "num_turns": 4, "total_cost_usd": 0.12, "result": "implemented feature"}
        )
        self._agent_exit_code = agent_exit_code
        self._agent_stderr = agent_stderr
        self._staged_changes = staged_changes

    async def execute(
        self,
        workspace: ExecutionWorkspace,
        command: list[str],
        stdin: str = "",
        timeout_seconds: int = 60,
    ) -> CommandResult:
        self.calls.append((command, stdin))
        if command[0] == "claude":
            return CommandResult(self._agent_exit_code, self._agent_stdout, self._agent_stderr)
        if command[-2:] == ["branch", "--show-current"]:
            return CommandResult(0, "adp/run-1\n", "")
        if command[-3:] == ["remote", "get-url", "origin"]:
            return CommandResult(0, f"{self._origin}\n", "")
        if command[-2:] == ["rev-parse", "HEAD"]:
            self._head_calls += 1
            return CommandResult(0, "before\n" if self._head_calls == 1 else "after\n", "")
        if command[-3:] == ["diff", "--cached", "--quiet"]:
            return CommandResult(1 if self._staged_changes else 0, "", "")
        if "diff" in command:
            return CommandResult(0, "src/feature.py\n", "")
        if command[-2:] == ["add", "-A"] or "commit" in command:
            return CommandResult(0, "", "")
        if command[-2:] == ["status", "--porcelain=v1"]:
            self._status_calls += 1
            if self._dirty_after_verification and self._status_calls == 2:
                return CommandResult(0, " M generated.lock\n", "")
            return CommandResult(0, "", "")
        if command[:2] == ["/bin/sh", "-lc"]:
            return CommandResult(self._verification_exit_code, "verification output", "")
        if "push" in command:
            return CommandResult(0, "published", "")
        raise AssertionError(f"unexpected command: {command}")


def _request() -> PhaseExecutionRequest:
    return PhaseExecutionRequest(
        phase=PlanPhase(
            id="phase-1",
            name="Implement feature",
            description="Add the requested feature.",
            tasks=["Implement the feature."],
            acceptance_criteria=["Feature passes tests."],
            verification=["npm test"],
        ),
        workspace=ExecutionWorkspace(
            run_id="run-1",
            job_name="cogito-execution-aabbcc",
            workspace_root="/workspace",
            repositories=["/workspace/repos/example"],
            repository_origins={"/workspace/repos/example": "https://github.com/acme/example.git"},
        ),
        max_turns=7,
        timeout_seconds=60,
    )


async def test_harness_records_turns_cost_changes_verification_and_published_commit() -> None:
    workspaces = ScriptedWorkspaces()

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.succeeded is True
    assert result.turns_used == 4
    assert result.cost_usd == 0.12
    assert result.changed_files == ["/workspace/repos/example:src/feature.py"]
    assert result.commits == {"/workspace/repos/example": "after"}
    assert result.verification[0].passed is True
    agent_command, prompt = next((command, stdin) for command, stdin in workspaces.calls if command[0] == "claude")
    assert agent_command == [
        "claude",
        "--print",
        "--output-format",
        "json",
        "--max-turns",
        "7",
        "--dangerously-skip-permissions",
    ]
    assert "Resolved immutable specifications: /workspace/specs" in prompt
    assert "adp/run-1" in prompt
    assert workspaces.calls[-1][0][-4:] == ["push", "--set-upstream", "origin", "adp/run-1"]


async def test_harness_does_not_publish_when_verification_fails() -> None:
    workspaces = ScriptedWorkspaces(verification_exit_code=1)

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.succeeded is False
    assert result.verification[0].passed is False
    assert all("push" not in command for command, _ in workspaces.calls)


async def test_harness_rejects_unstructured_agent_output_without_publishing() -> None:
    workspaces = ScriptedWorkspaces(agent_stdout="not-json")

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.succeeded is False
    assert "structured result" in result.summary
    assert all("push" not in command for command, _ in workspaces.calls)


async def test_harness_redacts_a_secret_from_agent_summary() -> None:
    workspaces = ScriptedWorkspaces(
        agent_stdout=json.dumps(
            {"is_error": False, "num_turns": 1, "total_cost_usd": 0.01, "result": "token=super-secret"}
        )
    )

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert "super-secret" not in result.summary
    assert "[REDACTED]" in result.summary


async def test_harness_does_not_coerce_boolean_telemetry() -> None:
    workspaces = ScriptedWorkspaces(
        agent_stdout=json.dumps({"is_error": False, "num_turns": True, "total_cost_usd": True, "result": "done"})
    )

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.succeeded is True
    assert result.turns_used is None
    assert result.cost_usd is None


async def test_harness_rejects_an_agent_altered_repository_origin() -> None:
    workspaces = ScriptedWorkspaces(origin="https://github.com/attacker/example.git")

    try:
        await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]
    except RuntimeError as error:
        assert "origin" in str(error)
    else:
        raise AssertionError("expected altered origin to be rejected")
    assert all("push" not in command for command, _ in workspaces.calls)


async def test_harness_does_not_publish_when_verification_dirties_the_repository() -> None:
    workspaces = ScriptedWorkspaces(dirty_after_verification=True)

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.succeeded is False
    assert "verification left uncommitted changes" in result.summary
    assert all("push" not in command for command, _ in workspaces.calls)


async def test_harness_classifies_only_known_gateway_budget_429_as_cost_ceiling() -> None:
    workspaces = ScriptedWorkspaces(
        agent_stdout=json.dumps({"is_error": True, "num_turns": 1, "result": "gateway rejected request"}),
        agent_exit_code=1,
        agent_stderr="HTTP 429: Max budget limit reached.",
    )

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.outcome == "ceiling_reached"
    assert result.ceiling == "cost"


async def test_harness_fails_closed_for_an_unrecognized_429() -> None:
    workspaces = ScriptedWorkspaces(
        agent_stdout=json.dumps({"is_error": True, "num_turns": 1, "result": "gateway rejected request"}),
        agent_exit_code=1,
        agent_stderr="HTTP 429: rate limit exceeded",
    )

    result = await ClaudeCodeHarness(workspaces).execute_phase(_request())  # type: ignore[arg-type]

    assert result.outcome == "failed"
    assert result.ceiling is None


async def test_harness_commits_and_publishes_without_calling_the_agent_during_backup() -> None:
    workspaces = ScriptedWorkspaces(staged_changes=True)
    phase_request = _request()
    backup = BackupExecutionRequest(
        phase=phase_request.phase,
        workspace=phase_request.workspace,
        ceiling="turns",
        timeout_seconds=60,
    )

    result = await ClaudeCodeHarness(workspaces).backup_phase(backup)  # type: ignore[arg-type]

    assert result.outcome == "stopped_with_backup"
    assert result.succeeded is True
    assert any("commit" in command for command, _ in workspaces.calls)
    assert any("push" in command for command, _ in workspaces.calls)
    assert all(command[0] != "claude" for command, _ in workspaces.calls)
