"""Prepare a run-private workspace before the execution harness starts."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

from minio import Minio

from .specs import SpecSetResolver
from .storage import MinioSpecStore, validate_spec_ref

_LOGGER = logging.getLogger(__name__)
_REPOSITORY_DIRECTORY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class WorkspacePreparationError(ValueError):
    """Raised when non-secret execution inputs cannot safely initialize a workspace."""


_COMMIT_PATTERN = re.compile(r"[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?")
_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,62})")


def validate_https_repository_url(repository: str, allowed_hosts: tuple[str, ...]) -> tuple[SplitResult, str]:
    """Validate an approved HTTPS Git URL and its immutable commit SHA fragment."""

    try:
        parsed = urlsplit(repository)
    except ValueError as error:
        raise WorkspacePreparationError("repository URL is malformed") from error
    host = parsed.hostname
    if (
        parsed.scheme != "https"
        or not host
        or _is_ip_address(host)
        or not _host_is_allowed(host, allowed_hosts)
        or parsed.username is not None
        or parsed.password is not None
        or not parsed.path.strip("/")
        or parsed.query
        or not _COMMIT_PATTERN.fullmatch(parsed.fragment)
    ):
        raise WorkspacePreparationError(
            "repository must be an approved HTTPS URL without credentials, pinned with a commit SHA fragment"
        )
    return parsed, parsed.fragment.lower()


def repository_directory_name(repository: str, allowed_hosts: tuple[str, ...]) -> str:
    """Return a collision-resistant, filesystem-safe clone directory name."""

    parsed, _ = validate_https_repository_url(repository, allowed_hosts)
    name = Path(parsed.path).name.removesuffix(".git")
    if not _REPOSITORY_DIRECTORY_PATTERN.fullmatch(name):
        raise WorkspacePreparationError("repository URL has an unsupported final path segment")
    digest = hashlib.sha256(repository.encode()).hexdigest()[:12]
    return f"{name}-{digest}"


def repository_clone_url(repository: str, allowed_hosts: tuple[str, ...]) -> str:
    """Return the credential-free origin URL for a validated repository reference."""

    parsed, _ = validate_https_repository_url(repository, allowed_hosts)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def feature_branch_name(run_id: str) -> str:
    """Return a safe, stable feature-branch name for one run."""

    if not _RUN_ID_PATTERN.fullmatch(run_id):
        raise WorkspacePreparationError("run ID is not safe for a feature branch")
    return f"adp/{run_id}"


def clone_repositories(
    repositories: list[str],
    workspace_root: Path,
    allowed_hosts: tuple[str, ...],
    feature_branch: str,
    author_name: str = "Cogito Agent",
    author_email: str = "cogito@local.invalid",
) -> list[Path]:
    """Clone validated HTTPS repositories under the workspace without shell invocation."""

    destinations: list[tuple[str, str, Path]] = []
    seen_destinations: set[Path] = set()
    repositories_root = workspace_root / "repos"
    for repository in repositories:
        parsed, commit = validate_https_repository_url(repository, allowed_hosts)
        destination = repositories_root / repository_directory_name(repository, allowed_hosts)
        if destination in seen_destinations:
            raise WorkspacePreparationError("repository list contains a duplicate URL")
        seen_destinations.add(destination)
        clone_url = repository_clone_url(repository, allowed_hosts)
        destinations.append((clone_url, commit, destination))

    repositories_root.mkdir(parents=True, exist_ok=True)
    command_environment = {
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": "http.followRedirects",
        "GIT_CONFIG_VALUE_0": "false",
        "GIT_CONFIG_KEY_1": "protocol.file.allow",
        "GIT_CONFIG_VALUE_1": "never",
        "HOME": str(workspace_root),
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
    }
    git_token = os.environ.get("COGITO_GIT_HTTPS_TOKEN")
    if git_token:
        command_environment.update(
            {
                "COGITO_GIT_HTTPS_TOKEN": git_token,
                "GIT_ASKPASS": str(workspace_root / ".cogito" / "git-askpass"),
            }
        )
    for repository, commit, destination in destinations:
        subprocess.run(
            ["git", "clone", "--no-checkout", "--depth", "1", "--no-tags", "--", repository, str(destination)],
            check=True,
            env=command_environment,
        )
        subprocess.run(
            ["git", "-C", str(destination), "fetch", "--depth", "1", "origin", commit],
            check=True,
            env=command_environment,
        )
        subprocess.run(
            ["git", "-C", str(destination), "checkout", "--detach", "FETCH_HEAD"],
            check=True,
            env=command_environment,
        )
        checked_out_commit = subprocess.run(
            ["git", "-C", str(destination), "rev-parse", "HEAD"],
            check=True,
            env=command_environment,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
        if checked_out_commit != commit:
            raise WorkspacePreparationError("repository checkout does not match the requested commit SHA")
        subprocess.run(
            ["git", "-C", str(destination), "checkout", "-b", feature_branch],
            check=True,
            env=command_environment,
        )
        subprocess.run(
            ["git", "-C", str(destination), "config", "user.name", author_name],
            check=True,
            env=command_environment,
        )
        subprocess.run(
            ["git", "-C", str(destination), "config", "user.email", author_email],
            check=True,
            env=command_environment,
        )
    return [destination for _, _, destination in destinations]


def materialize_generic_specs(spec_ref: str, resolver: SpecSetResolver, workspace_root: Path) -> Path:
    """Write only the validated always-on spec files into the shared workspace."""

    ref = validate_spec_ref(spec_ref)
    resolved = resolver.resolve_generic(ref.value)
    destination_root = workspace_root / "specs" / ref.name_version
    destination_root.mkdir(parents=True, exist_ok=True)
    resolved_root = destination_root.resolve()
    for spec_file in resolved.files:
        destination = destination_root / spec_file.path
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination.resolve().relative_to(resolved_root)
        except ValueError as error:
            raise WorkspacePreparationError("resolved spec path escapes its workspace directory") from error
        destination.write_text(spec_file.content, encoding="utf-8")
        destination.chmod(0o444)
    return destination_root


def create_git_askpass_helper(workspace_root: Path) -> Path:
    """Create a token-free askpass helper for the repository-scoped Git credential."""

    helper = workspace_root / ".cogito" / "git-askpass"
    helper.parent.mkdir(parents=True, exist_ok=True)
    helper.write_text(
        "#!/bin/sh\n"
        "case \"$1\" in\n"
        "  *Username*) printf '%s\\n' x-access-token ;;\n"
        "  *Password*) printf '%s\\n' \"$COGITO_GIT_HTTPS_TOKEN\" ;;\n"
        "  *) exit 1 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    helper.chmod(0o500)
    return helper


def load_feature_branch() -> str:
    """Load and validate the run feature branch supplied by the trusted Job manifest."""

    feature_branch_value = os.environ["COGITO_FEATURE_BRANCH"]
    if not feature_branch_value.startswith("adp/"):
        raise WorkspacePreparationError("execution feature branch must use the adp/ prefix")
    return feature_branch_name(feature_branch_value.removeprefix("adp/"))


def _load_repositories() -> list[str]:
    try:
        repositories = json.loads(os.environ["COGITO_TARGET_REPOS"])
    except (KeyError, json.JSONDecodeError) as error:
        raise WorkspacePreparationError("COGITO_TARGET_REPOS must be a JSON array") from error
    if not isinstance(repositories, list) or not all(isinstance(repository, str) for repository in repositories):
        raise WorkspacePreparationError("COGITO_TARGET_REPOS must contain only repository URL strings")
    return repositories


def _load_allowed_hosts() -> tuple[str, ...]:
    try:
        hosts = json.loads(os.environ["COGITO_ALLOWED_GIT_HOSTS"])
    except (KeyError, json.JSONDecodeError) as error:
        raise WorkspacePreparationError("COGITO_ALLOWED_GIT_HOSTS must be a JSON array") from error
    if not isinstance(hosts, list) or not hosts or not all(isinstance(host, str) for host in hosts):
        raise WorkspacePreparationError("COGITO_ALLOWED_GIT_HOSTS must be a non-empty string array")
    return tuple(hosts)


def _host_is_allowed(host: str, allowed_hosts: tuple[str, ...]) -> bool:
    normalized_host = host.lower().rstrip(".")
    for allowed_host in allowed_hosts:
        normalized_allowed = allowed_host.lower().rstrip(".")
        if normalized_allowed.startswith("*."):
            suffix = normalized_allowed[1:]
            if normalized_host.endswith(suffix) and normalized_host != suffix[1:]:
                return True
        elif normalized_host == normalized_allowed:
            return True
    return False


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


def main() -> None:
    """Create the workspace's generic specs and HTTPS repository clones."""

    logging.basicConfig(level=logging.INFO)
    workspace_root = Path(os.environ["COGITO_EXECUTION_WORKSPACE_ROOT"])
    spec_ref = os.environ["COGITO_SPEC_REF"]
    workspace_root.mkdir(parents=True, exist_ok=True)
    client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )
    resolver = SpecSetResolver(
        MinioSpecStore(
            client,
            os.environ["MINIO_SPECS_BUCKET"],
            os.environ["MINIO_SPECS_PREFIX"],
            int(os.environ["MINIO_SPECS_MAX_ARCHIVE_BYTES"]),
        ),
        int(os.environ["MINIO_SPECS_MAX_EXTRACTED_BYTES"]),
    )
    repositories = _load_repositories()
    allowed_hosts = _load_allowed_hosts()
    materialize_generic_specs(spec_ref, resolver, workspace_root)
    create_git_askpass_helper(workspace_root)
    feature_branch = load_feature_branch()
    clone_repositories(
        repositories,
        workspace_root,
        allowed_hosts,
        feature_branch,
        os.environ.get("COGITO_GIT_AUTHOR_NAME", "Cogito Agent"),
        os.environ.get("COGITO_GIT_AUTHOR_EMAIL", "cogito@local.invalid"),
    )
    _LOGGER.info("execution workspace prepared", extra={"repository_count": len(repositories), "spec_ref": spec_ref})


if __name__ == "__main__":
    main()
