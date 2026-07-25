"""Narrow GitHub pull-request publisher for approved implementation artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse

import httpx


@dataclass(frozen=True)
class PullRequestResult:
    """Safe external identity returned after creating or reusing one pull request."""

    number: int
    url: str
    reused: bool


class PullRequestPublisher(Protocol):
    """Publishes only the reviewed feature branch represented by immutable evidence."""

    async def open_or_reuse(self, artifact_sha256: str, evidence: dict) -> PullRequestResult: ...


class GitHubPullRequestPublisher:
    """GitHub REST client with an artifact marker to make ambiguous retries safe."""

    def __init__(self, token: str, api_url: str, base_branch: str):
        # Kubernetes Secrets created from a file commonly retain its trailing
        # newline. HTTP header values cannot contain it, while GitHub tokens
        # themselves never rely on leading or trailing whitespace.
        self._token = token.strip()
        self._api_url = api_url.rstrip("/")
        self._base_branch = base_branch

    async def open_or_reuse(self, artifact_sha256: str, evidence: dict) -> PullRequestResult:
        """Find the existing marker first, then create exactly one approved PR."""

        if not self._token:
            raise RuntimeError("GitHub pull-request credential is not configured")
        repository = _repository_from_evidence(evidence)
        branch_name = evidence.get("branch_name")
        if not isinstance(branch_name, str) or not branch_name.startswith("adp/"):
            raise ValueError("implementation evidence has an invalid feature branch")
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        async with httpx.AsyncClient(base_url=self._api_url, headers=headers, timeout=20.0) as client:
            owner = repository.split("/", maxsplit=1)[0]
            response = await client.get(
                f"/repos/{repository}/pulls",
                params={"state": "all", "head": f"{owner}:{branch_name}", "base": self._base_branch},
            )
            response.raise_for_status()
            marker = f"<!-- cogito-implementation-artifact:{artifact_sha256} -->"
            for existing in response.json():
                if isinstance(existing, dict) and marker in str(existing.get("body", "")):
                    return PullRequestResult(number=int(existing["number"]), url=str(existing["html_url"]), reused=True)
            created = await client.post(
                f"/repos/{repository}/pulls",
                json={
                    "title": f"Cogito implementation {evidence.get('run_id', '')}",
                    "head": branch_name,
                    "base": self._base_branch,
                    "body": _pull_request_body(marker, evidence),
                },
            )
            created.raise_for_status()
            body = created.json()
            number = int(body["number"])
            await _post_advisory_comments(client, repository, number, evidence, body.get("head", {}).get("sha"))
            return PullRequestResult(number=number, url=str(body["html_url"]), reused=False)


def _repository_from_evidence(evidence: dict) -> str:
    commits = evidence.get("commits")
    if not isinstance(commits, dict) or len(commits) != 1:
        raise ValueError("pull-request publication requires exactly one approved repository")
    origin = next(iter(commits))
    # The evidence keys are workspace paths. The approved remote is therefore
    # intentionally carried in `repository_origin`, not inferred from a path.
    repository_origin = evidence.get("repository_origin")
    if not isinstance(repository_origin, str):
        raise ValueError("implementation evidence lacks an approved repository origin")
    parsed = urlparse(repository_origin.removesuffix(".git"))
    if parsed.scheme != "https" or parsed.hostname != "github.com":
        raise ValueError("implementation origin is not an approved GitHub HTTPS repository")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2 or any(part in {".", ".."} for part in parts):
        raise ValueError("implementation origin does not identify one GitHub repository")
    del origin
    return "/".join(parts)


def _pull_request_body(marker: str, evidence: dict) -> str:
    phases = evidence.get("phase_results") if isinstance(evidence.get("phase_results"), list) else []
    models = evidence.get("models") if isinstance(evidence.get("models"), list) else []
    return "\n".join(
        [
            marker,
            "## Cogito implementation evidence",
            f"- Run: `{evidence.get('run_id', '')}`",
            f"- Artifact SHA-256: `{evidence.get('artifact_sha256', '')}`",
            f"- Phases completed: {len(phases)}",
            f"- Reviewer models: {', '.join(str(model) for model in models[:8]) or 'none'}",
            f"- Turns used: {evidence.get('turns_used', 0)}",
            f"- Cost (USD): {evidence.get('cost_usd', 0.0)}",
            "- Review: converged before human implementation approval.",
        ]
    )


async def _post_advisory_comments(
    client: httpx.AsyncClient, repository: str, number: int, evidence: dict, commit_sha: object
) -> None:
    """Post only bounded PR-level advisory summaries; never expose raw reviewer diagnostics."""

    review = evidence.get("review")
    if not isinstance(review, dict):
        return
    summaries: list[str] = []
    for round_evidence in review.get("rounds", []):
        if not isinstance(round_evidence, dict):
            continue
        for finding in round_evidence.get("findings", []):
            if not isinstance(finding, dict) or finding.get("severity") not in {"advisory", "nit"}:
                continue
            description = finding.get("description")
            if isinstance(description, str) and description.strip():
                inline_posted = False
                path = finding.get("file")
                line = finding.get("line")
                if _safe_inline_location(path, line, commit_sha):
                    inline = await client.post(
                        f"/repos/{repository}/pulls/{number}/comments",
                        json={
                            "body": description.strip()[:500],
                            "commit_id": commit_sha,
                            "path": path,
                            "line": line,
                            "side": "RIGHT",
                        },
                    )
                    inline_posted = inline.is_success
                if not inline_posted:
                    summaries.append(f"- {description.strip()[:500]}")
    if summaries:
        await client.post(
            f"/repos/{repository}/issues/{number}/comments",
            json={"body": "## Advisory review findings\n" + "\n".join(summaries[:20])},
        )


def _safe_inline_location(path: object, line: object, commit_sha: object) -> bool:
    """Allow only a repository-relative source location accepted by GitHub's diff API."""

    return (
        isinstance(path, str)
        and path.strip() == path
        and path
        and not path.startswith(("/", "\\"))
        and ".." not in path.split("/")
        and isinstance(line, int)
        and line > 0
        and isinstance(commit_sha, str)
        and len(commit_sha) == 40
        and all(character in "0123456789abcdef" for character in commit_sha.lower())
    )
