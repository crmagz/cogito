from __future__ import annotations

import pytest

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
