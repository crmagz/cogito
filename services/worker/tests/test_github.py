from __future__ import annotations

import httpx
import pytest

import cogito_worker.github as github
from cogito_worker.github import GitHubPullRequestPublisher, _repository_from_evidence, _safe_inline_location


def test_publisher_strips_secret_file_trailing_newline() -> None:
    publisher = GitHubPullRequestPublisher("token-from-secret\n", "https://api.github.com", "main")

    assert publisher._token == "token-from-secret"


def test_repository_requires_one_https_github_origin() -> None:
    evidence = {
        "commits": {"/workspace/repositories/example": "a" * 40},
        "repository_origin": "https://github.com/acme/example.git",
    }

    assert _repository_from_evidence(evidence) == "acme/example"

    evidence["repository_origin"] = "git@github.com:acme/example.git"
    with pytest.raises(ValueError, match="HTTPS"):
        _repository_from_evidence(evidence)


async def test_publisher_reuses_an_artifact_marker_found_on_a_later_page(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[int] = []
    marker = "<!-- cogito-implementation-artifact:" + "a" * 64 + " -->"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        page = int(request.url.params["page"])
        requests.append(page)
        if page == 1:
            return httpx.Response(200, json=[{"body": ""} for _ in range(100)])
        return httpx.Response(200, json=[{"number": 42, "html_url": "https://github.com/acme/example/pull/42", "body": marker}])

    transport = httpx.MockTransport(handler)
    async_client = httpx.AsyncClient
    monkeypatch.setattr(
        github.httpx,
        "AsyncClient",
        lambda *args, **kwargs: async_client(*args, transport=transport, **kwargs),
    )
    publisher = GitHubPullRequestPublisher("token", "https://api.github.com", "main")

    result = await publisher.open_or_reuse(
        "a" * 64,
        {
            "branch_name": "adp/run-1",
            "commits": {"/workspace/repo": "b" * 40},
            "repository_origin": "https://github.com/acme/example.git",
        },
    )

    assert result.number == 42
    assert result.reused is True
    assert requests == [1, 2]


@pytest.mark.parametrize(
    ("path", "line", "expected"),
    [
        ("src/main.py", 4, True),
        ("../secret", 1, False),
        ("/etc/passwd", 1, False),
        ("src/main.py", 0, False),
    ],
)
def test_inline_comments_only_allow_safe_changed_file_locations(path: str, line: int, expected: bool) -> None:
    assert _safe_inline_location(path, line, "a" * 40) is expected
