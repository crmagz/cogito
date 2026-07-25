from __future__ import annotations

import json

import httpx
import pytest
from cogito_worker.execution import CommandResult
from cogito_worker.models import ExecutionWorkspace, ReviewFinding, ReviewRequest
from cogito_worker.review import LiteLLMReviewHarness, ReviewError


class _Workspaces:
    def __init__(self, diff: str) -> None:
        self.diff = diff
        self.commands: list[list[str]] = []

    async def execute(
        self,
        workspace: ExecutionWorkspace,
        command: list[str],
        stdin: str = "",
        timeout_seconds: int = 60,
    ) -> CommandResult:
        del workspace, stdin, timeout_seconds
        self.commands.append(command)
        if command[-3:] == ["merge-base", "HEAD", "origin/HEAD"]:
            return CommandResult(exit_code=0, stdout="b" * 40 + "\n", stderr="")
        return CommandResult(exit_code=0, stdout=self.diff, stderr="")


def _request() -> ReviewRequest:
    repository = "/workspace/repos/example"
    return ReviewRequest(
        workspace=ExecutionWorkspace(
            run_id="run-1",
            job_name="cogito-execution-run-1",
            workspace_root="/workspace",
            repositories=[repository],
            base_commits={repository: "a" * 40},
        ),
        phase_results=[{"phase_id": "phase-1", "succeeded": True}],
        round_number=1,
        review_profile="standard",
    )


async def test_review_harness_parses_classified_findings_from_distinct_models() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        model = json.loads(request.content)["model"]
        body = {"findings": []}
        if model == "balanced":
            body = {
                "findings": [
                    {
                        "severity": "blocking",
                        "file": "src/main.py",
                        "line": 7,
                        "description": "missing validation",
                        "evidence": "diff removes the guard",
                        "suggested_fix": "restore the guard",
                    }
                ]
            }
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(body)}}]})

    workspaces = _Workspaces("diff --git a/src/main.py b/src/main.py\n")
    harness = LiteLLMReviewHarness(
        workspaces,  # type: ignore[arg-type]
        "http://litellm.test",
        "reviewer-key",
        "reviewer-secondary-key",
        "balanced",
        "complex",
        transport=httpx.MockTransport(handler),
    )

    result = await harness.review(_request())

    assert len(result.findings) == 2
    assert result.findings[0].severity == "blocking"
    assert result.findings[0].lens == "correctness"
    assert {json.loads(request.content)["model"] for request in requests} == {"balanced", "complex"}
    assert workspaces.commands == [
        ["git", "-C", "/workspace/repos/example", "diff", "--no-ext-diff", f"{'a' * 40}..HEAD"]
    ]


async def test_review_harness_retries_one_malformed_completion_and_excludes_developer_narrative() -> None:
    calls_by_lens: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        lens = json.loads(payload["messages"][1]["content"])["lens"]
        calls_by_lens[lens] = calls_by_lens.get(lens, 0) + 1
        assert payload["max_tokens"] == 1_200
        assert payload["temperature"] == 0
        assert "untrusted developer narrative" not in payload["messages"][1]["content"]
        content = "" if calls_by_lens[lens] == 1 else '{"findings":[]}'
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    request = _request()
    request.phase_results[0]["summary"] = "untrusted developer narrative"
    harness = LiteLLMReviewHarness(
        _Workspaces("diff"),  # type: ignore[arg-type]
        "http://litellm.test",
        "reviewer-key",
        "reviewer-secondary-key",
        "balanced",
        "complex",
        transport=httpx.MockTransport(handler),
    )

    result = await harness.review(request)

    assert result.findings == []
    assert calls_by_lens == {"correctness": 2, "standards": 2, "blast_radius": 2}


async def test_review_harness_rejects_unsafe_finding_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        body = {
            "findings": [
                {
                    "severity": "blocking",
                    "file": "../secret",
                    "line": 1,
                    "description": "unsafe",
                }
            ]
        }
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(body)}}]})

    harness = LiteLLMReviewHarness(
        _Workspaces("diff"),  # type: ignore[arg-type]
        "http://litellm.test",
        "reviewer-key",
        "reviewer-secondary-key",
        "balanced",
        "complex",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(ReviewError, match="unsafe file path"):
        await harness.review(_request())


async def test_review_harness_surfaces_failed_phase_verification_without_a_model_judgment() -> None:
    request = _request()
    request.phase_results[0]["verification"] = [
        {"command": "pytest", "output": "1 failed", "passed": False}
    ]
    harness = LiteLLMReviewHarness(
        _Workspaces("diff"),  # type: ignore[arg-type]
        "http://litellm.test",
        "reviewer-key",
        "reviewer-secondary-key",
        "balanced",
        "complex",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"choices": [{"message": {"content": '{"findings":[]}'}}]},
            )
        ),
    )

    result = await harness.review(request)

    assert result.findings == [
        ReviewFinding(
            severity="blocking",
            lens="verification",
            model="deterministic",
            file="phase-phase-1-verification",
            line=None,
            description="approved phase verification did not pass",
            evidence="1 failed",
            suggested_fix="pytest",
        )
    ]


async def test_review_harness_reads_merge_base_when_activity_state_has_no_baseline() -> None:
    request = _request()
    request.workspace.base_commits.clear()
    workspaces = _Workspaces("diff --git a/src/main.py b/src/main.py\n")
    harness = LiteLLMReviewHarness(
        workspaces,  # type: ignore[arg-type]
        "http://litellm.test",
        "reviewer-key",
        "reviewer-secondary-key",
        "balanced",
        "complex",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"choices": [{"message": {"content": '{"findings":[]}'}}]},
            )
        ),
    )

    result = await harness.review(request)

    assert result.findings == []
    repository = request.workspace.repositories[0]
    assert workspaces.commands == [
        ["git", "-C", repository, "merge-base", "HEAD", "origin/HEAD"],
        ["git", "-C", repository, "diff", "--no-ext-diff", f"{'b' * 40}..HEAD"],
    ]


async def test_review_harness_requires_distinct_aliases() -> None:
    harness = LiteLLMReviewHarness(
        _Workspaces("diff"),  # type: ignore[arg-type]
        "http://litellm.test",
        "reviewer-key",
        "reviewer-secondary-key",
        "balanced",
        "balanced",
    )

    with pytest.raises(ReviewError, match="must be distinct"):
        await harness.review(_request())
