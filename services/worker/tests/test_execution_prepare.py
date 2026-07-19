from __future__ import annotations

import subprocess
from hashlib import sha256
from pathlib import Path

import pytest

from cogito_worker.execution_prepare import (
    WorkspacePreparationError,
    clone_repositories,
    create_git_askpass_helper,
    feature_branch_name,
    load_feature_branch,
    materialize_generic_specs,
    repository_directory_name,
    validate_https_repository_url,
)
from cogito_worker.models import ResolvedSpecFile, ResolvedSpecSet


class FakeResolver:
    def resolve_generic(self, ref: str) -> ResolvedSpecSet:
        assert ref == IMMUTABLE_SPEC_REF
        return ResolvedSpecSet(
            ref=ref,
            files=[ResolvedSpecFile(path="rules/naming.md", content="Use clear names.\n", priority="high")],
        )


IMMUTABLE_SPEC_REF = "typescript-backend@v2.1#sha256=" + "a" * 64
COMMIT = "0123456789abcdef0123456789abcdef01234567"
REPOSITORY = f"https://github.com/acme/repository.git#{COMMIT}"


@pytest.mark.parametrize(
    "repository",
    [
        "http://github.com/acme/repository.git",
        "ssh://git@github.com/acme/repository.git",
        "https://token@github.com/acme/repository.git",
        "https://github.com/acme/repository.git?ref=main",
    ],
)
def test_https_repository_validation_rejects_unsupported_url_forms(repository: str) -> None:
    with pytest.raises(WorkspacePreparationError, match="HTTPS URL"):
        validate_https_repository_url(repository, ("github.com",))


def test_clone_repositories_uses_git_argument_list_and_private_destination(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(
        arguments: list[str], *, check: bool, env: dict[str, str], **_: object
    ) -> subprocess.CompletedProcess[str]:
        assert check is True
        calls.append((arguments, env))
        stdout = f"{COMMIT}\n" if arguments[-2:] == ["rev-parse", "HEAD"] else ""
        return subprocess.CompletedProcess(arguments, 0, stdout=stdout)

    monkeypatch.setattr("cogito_worker.execution_prepare.subprocess.run", fake_run)

    destinations = clone_repositories([REPOSITORY], tmp_path, ("github.com",), "adp/run-1")

    assert destinations == [tmp_path / "repos" / repository_directory_name(REPOSITORY, ("github.com",))]
    assert calls[0][0][:6] == ["git", "clone", "--no-checkout", "--depth", "1", "--no-tags"]
    assert calls[0][1]["GIT_TERMINAL_PROMPT"] == "0"
    assert calls[0][1]["GIT_CONFIG_KEY_0"] == "http.followRedirects"
    assert any(call[0][-2:] == ["rev-parse", "HEAD"] for call in calls)
    assert any(call[0][-3:] == ["checkout", "-b", "adp/run-1"] for call in calls)
    assert calls[-2][0][-3:] == ["config", "user.name", "Cogito Agent"]
    assert calls[-1][0][-3:] == ["config", "user.email", "cogito@local.invalid"]


def test_clone_repositories_rejects_a_checkout_that_does_not_match_the_pin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(arguments: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        stdout = "f" * 40 if arguments[-2:] == ["rev-parse", "HEAD"] else ""
        return subprocess.CompletedProcess(arguments, 0, stdout=stdout)

    monkeypatch.setattr("cogito_worker.execution_prepare.subprocess.run", fake_run)

    with pytest.raises(WorkspacePreparationError, match="does not match"):
        clone_repositories([REPOSITORY], tmp_path, ("github.com",), "adp/run-1")


def test_clone_repositories_rejects_duplicate_urls_before_git_runs(tmp_path: Path) -> None:
    with pytest.raises(WorkspacePreparationError, match="duplicate"):
        clone_repositories([REPOSITORY, REPOSITORY], tmp_path, ("github.com",), "adp/run-1")


def test_feature_branch_name_rejects_ref_injection() -> None:
    with pytest.raises(WorkspacePreparationError, match="feature branch"):
        feature_branch_name("run-1..main")


def test_workspace_entrypoint_rejects_a_branch_without_the_expected_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COGITO_FEATURE_BRANCH", "run-1")

    with pytest.raises(WorkspacePreparationError, match="adp/ prefix"):
        load_feature_branch()


def test_git_askpass_helper_contains_no_credential(tmp_path: Path) -> None:
    helper = create_git_askpass_helper(tmp_path)

    assert helper.stat().st_mode & 0o777 == 0o500
    assert "COGITO_GIT_HTTPS_TOKEN" in helper.read_text(encoding="utf-8")
    assert "super-secret" not in helper.read_text(encoding="utf-8")


def test_materialize_generic_specs_writes_only_validated_specs_read_only(tmp_path: Path) -> None:
    destination = materialize_generic_specs(IMMUTABLE_SPEC_REF, FakeResolver(), tmp_path)
    spec_file = destination / "rules" / "naming.md"

    assert spec_file.read_text(encoding="utf-8") == "Use clear names.\n"
    assert spec_file.stat().st_mode & 0o222 == 0


def test_materialize_generic_specs_rejects_path_like_references(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="name@version"):
        materialize_generic_specs("../../plans@v1", FakeResolver(), tmp_path)
