from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
import shlex
import time
from dataclasses import dataclass, replace
from typing import Any, Protocol

from .execution_prepare import feature_branch_name, repository_clone_url, repository_directory_name
from .models import ExecutionRequest, ExecutionWorkspace
from .budgets import RunBudget, RunGitCredentialManager, RunKeyManager

_EXECUTION_JOB_PREFIX = "cogito-execution-"
_RUN_HASH_LABEL = "cogito.dev/run-hash"
_SENSITIVE_DIAGNOSTIC_PATTERN = re.compile(
    r"(?i)(access[_ -]?key|secret(?:[_ -]?key)?|token|password|api[_ -]?key)[\"']?\s*[:=]\s*[\"']?[^\s,}\]\"']+"
)
_BEARER_TOKEN_PATTERN = re.compile(r"(?i)bearer\s+\S+")


@dataclass(frozen=True)
class ExecutionJobSettings:
    """Settings used to create a run-specific execution Job."""

    namespace: str
    image: str
    image_pull_policy: str
    workspace_root: str
    idle_seconds: int
    startup_timeout_seconds: int
    cleanup_timeout_seconds: int
    active_deadline_seconds: int
    ttl_seconds_after_finished: int
    termination_grace_period_seconds: int
    workspace_size_limit: str
    resources: dict[str, object]
    allowed_git_hosts: tuple[str, ...]
    minio_endpoint: str
    minio_secure: bool
    specs_bucket: str
    specs_prefix: str
    specs_max_archive_bytes: int
    specs_max_extracted_bytes: int
    object_store_secret: str
    object_store_access_key_secret_key: str
    object_store_secret_key_secret_key: str
    litellm_endpoint: str
    litellm_model: str
    litellm_key_secret: str
    litellm_key_secret_key: str
    git_credentials_secret: str
    git_credentials_secret_key: str
    git_author_name: str
    git_author_email: str
    command_output_limit_bytes: int


@dataclass(frozen=True)
class CommandResult:
    """Bounded stdout and stderr from a command in an execution workspace."""

    exit_code: int
    stdout: str
    stderr: str


class ExecutionJobClient(Protocol):
    """Creates and removes run-scoped Kubernetes Jobs."""

    async def create_job(self, job_name: str, body: dict[str, object]) -> None:
        """Create a Job, accepting a prior idempotent creation."""

    async def delete_job(self, job_name: str) -> None:
        """Delete a Job, accepting a prior deletion."""

    async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
        """Wait until the execution container starts after workspace preparation."""

    async def execute(
        self,
        job_name: str,
        command: list[str],
        stdin: str,
        timeout_seconds: int,
        output_limit_bytes: int,
    ) -> CommandResult:
        """Run one command inside the ready execution container."""


def execution_job_name(run_id: str) -> str:
    """Return a stable DNS-safe Job name without exposing the raw run ID."""

    run_hash = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    return f"{_EXECUTION_JOB_PREFIX}{run_hash}"


def build_execution_job(
    *, request: ExecutionRequest, job_name: str, settings: ExecutionJobSettings
) -> dict[str, object]:
    """Build the namespaced Job manifest for one isolated execution workspace."""

    run_hash = hashlib.sha256(request.run_id.encode()).hexdigest()[:20]
    labels = {
        "app.kubernetes.io/name": "execution",
        "app.kubernetes.io/component": "run-workspace",
        _RUN_HASH_LABEL: run_hash,
    }
    active_deadline_seconds = request.execution_timeout_seconds or settings.active_deadline_seconds
    if active_deadline_seconds > settings.active_deadline_seconds:
        raise ValueError("approved execution timeout exceeds the operator-configured Job deadline")
    if active_deadline_seconds < 1:
        raise ValueError("approved execution timeout must be positive")
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "labels": labels},
        "spec": {
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "activeDeadlineSeconds": active_deadline_seconds,
                    "ttlSecondsAfterFinished": settings.ttl_seconds_after_finished,
                    "terminationGracePeriodSeconds": settings.termination_grace_period_seconds,
                    "initContainers": [
                        {
                            "name": "prepare-workspace",
                            "image": settings.image,
                            "imagePullPolicy": settings.image_pull_policy,
                            "command": ["python", "-m", "cogito_worker.execution_prepare"],
                            "env": [
                                {"name": "COGITO_SPEC_REF", "value": request.spec_ref},
                                {"name": "COGITO_TARGET_REPOS", "value": json.dumps(request.target_repos)},
                                {"name": "COGITO_ALLOWED_GIT_HOSTS", "value": json.dumps(settings.allowed_git_hosts)},
                                {
                                    "name": "COGITO_EXECUTION_WORKSPACE_ROOT",
                                    "value": settings.workspace_root,
                                },
                                {"name": "COGITO_FEATURE_BRANCH", "value": feature_branch_name(request.run_id)},
                                {"name": "COGITO_GIT_AUTHOR_NAME", "value": settings.git_author_name},
                                {"name": "COGITO_GIT_AUTHOR_EMAIL", "value": settings.git_author_email},
                                {"name": "MINIO_ENDPOINT", "value": settings.minio_endpoint},
                                {"name": "MINIO_SECURE", "value": str(settings.minio_secure).lower()},
                                {"name": "MINIO_SPECS_BUCKET", "value": settings.specs_bucket},
                                {"name": "MINIO_SPECS_PREFIX", "value": settings.specs_prefix},
                                {
                                    "name": "MINIO_SPECS_MAX_ARCHIVE_BYTES",
                                    "value": str(settings.specs_max_archive_bytes),
                                },
                                {
                                    "name": "MINIO_SPECS_MAX_EXTRACTED_BYTES",
                                    "value": str(settings.specs_max_extracted_bytes),
                                },
                                {
                                    "name": "MINIO_ACCESS_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": settings.object_store_secret,
                                            "key": settings.object_store_access_key_secret_key,
                                        }
                                    },
                                },
                                {
                                    "name": "MINIO_SECRET_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": settings.object_store_secret,
                                            "key": settings.object_store_secret_key_secret_key,
                                        }
                                    },
                                },
                                {
                                    "name": "COGITO_GIT_HTTPS_TOKEN",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": request.run_git_secret or settings.git_credentials_secret,
                                            "key": settings.git_credentials_secret_key,
                                        }
                                    },
                                },
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": settings.resources,
                            "volumeMounts": [
                                {"name": "workspace", "mountPath": settings.workspace_root}
                            ],
                        }
                    ],
                    "containers": [
                        {
                            "name": "execution",
                            "image": settings.image,
                            "imagePullPolicy": settings.image_pull_policy,
                            "command": ["python", "-m", "cogito_worker.execution_pod"],
                            "env": [
                                {"name": "COGITO_RUN_ID", "value": request.run_id},
                                {
                                    "name": "COGITO_EXECUTION_WORKSPACE_ROOT",
                                    "value": settings.workspace_root,
                                },
                                {
                                    "name": "COGITO_EXECUTION_IDLE_SECONDS",
                                    "value": str(min(settings.idle_seconds, active_deadline_seconds)),
                                },
                                {"name": "ANTHROPIC_BASE_URL", "value": settings.litellm_endpoint},
                                {"name": "ANTHROPIC_MODEL", "value": settings.litellm_model},
                                # Claude Code otherwise remaps an arbitrary gateway
                                # alias to its legacy model selection. Keep the
                                # immutable LiteLLM alias chosen by the run key.
                                {"name": "CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP", "value": "1"},
                                {"name": "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "value": "1"},
                                {"name": "DISABLE_AUTOUPDATER", "value": "1"},
                                {"name": "GIT_TERMINAL_PROMPT", "value": "0"},
                                {
                                    "name": "GIT_ASKPASS",
                                    "value": f"{settings.workspace_root}/.cogito/git-askpass",
                                },
                                {
                                    "name": "ANTHROPIC_AUTH_TOKEN",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                    "name": request.run_key_secret or settings.litellm_key_secret,
                                    "key": settings.litellm_key_secret_key,
                                        }
                                    },
                                },
                                {
                                    "name": "COGITO_GIT_HTTPS_TOKEN",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": request.run_git_secret or settings.git_credentials_secret,
                                            "key": settings.git_credentials_secret_key,
                                        }
                                    },
                                },
                            ],
                            "volumeMounts": [
                                {"name": "workspace", "mountPath": settings.workspace_root}
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": settings.resources,
                        }
                    ],
                    "volumes": [{"name": "workspace", "emptyDir": {"sizeLimit": settings.workspace_size_limit}}],
                },
            },
        },
    }


class KubernetesExecutionJobClient:
    """Kubernetes API adapter that manages Jobs using in-cluster credentials."""

    def __init__(self, namespace: str, cleanup_timeout_seconds: int):
        try:
            from kubernetes import client, config
            from kubernetes.client.exceptions import ApiException
            from kubernetes.stream import stream
        except ImportError as error:
            message = "Kubernetes execution requires the worker's kubernetes dependency"
            raise RuntimeError(message) from error

        self._namespace = namespace
        self._cleanup_timeout_seconds = cleanup_timeout_seconds
        config.load_incluster_config()
        self._batch_api = client.BatchV1Api()
        self._core_api = client.CoreV1Api()
        self._api_exception: type[Exception] = ApiException
        self._stream = stream
        self._foreground_delete_options = client.V1DeleteOptions(propagation_policy="Foreground")

    async def create_job(self, job_name: str, body: dict[str, object]) -> None:
        """Create the Job unless an activity retry already created it."""

        try:
            await asyncio.to_thread(self._batch_api.create_namespaced_job, self._namespace, body)
        except self._api_exception as error:
            if error.status != 409:
                raise

    async def delete_job(self, job_name: str) -> None:
        """Delete a Job and its labelled execution Pods before returning."""

        run_hash = job_name.removeprefix(_EXECUTION_JOB_PREFIX)
        try:
            await asyncio.to_thread(
                self._batch_api.delete_namespaced_job,
                job_name,
                self._namespace,
                body=self._foreground_delete_options,
            )
        except self._api_exception as error:
            if error.status != 404:
                raise

        deadline = time.monotonic() + self._cleanup_timeout_seconds
        while time.monotonic() < deadline:
            try:
                await asyncio.to_thread(self._batch_api.read_namespaced_job, job_name, self._namespace)
            except self._api_exception as error:
                if error.status == 404:
                    break
                raise
            await asyncio.sleep(0.25)
        else:
            raise TimeoutError(f"execution Job {job_name} was not deleted within {self._cleanup_timeout_seconds} seconds")

        await self._delete_labeled_pods(run_hash, deadline)

    async def _delete_labeled_pods(self, run_hash: str, deadline: float) -> None:
        """Explicitly remove Pods so cleanup does not rely on owner-reference garbage collection."""

        label_selector = f"{_RUN_HASH_LABEL}={run_hash}"
        while time.monotonic() < deadline:
            pods = await asyncio.to_thread(
                self._core_api.list_namespaced_pod,
                self._namespace,
                label_selector=label_selector,
            )
            if not pods.items:
                return
            for pod in pods.items:
                name = getattr(getattr(pod, "metadata", None), "name", None)
                if not name:
                    raise RuntimeError("execution Pod is missing its Kubernetes name")
                try:
                    await asyncio.to_thread(
                        self._core_api.delete_namespaced_pod,
                        name,
                        self._namespace,
                        body=self._foreground_delete_options,
                    )
                except self._api_exception as error:
                    if error.status != 404:
                        raise
            await asyncio.sleep(0.25)
        raise TimeoutError(f"execution Pods for {run_hash} were not deleted within {self._cleanup_timeout_seconds} seconds")

    async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
        """Wait for the main container only after its init container has succeeded."""

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            job = await asyncio.to_thread(self._batch_api.read_namespaced_job, job_name, self._namespace)
            status = job.status
            job_failed = status is not None and status.failed
            labels = getattr(job.metadata, "labels", None) or {}
            run_hash = labels.get(_RUN_HASH_LABEL)
            if run_hash:
                pods = await asyncio.to_thread(
                    self._core_api.list_namespaced_pod,
                    self._namespace,
                    label_selector=f"{_RUN_HASH_LABEL}={run_hash}",
                )
                for pod in pods.items:
                    pod_status = getattr(pod, "status", None)
                    if getattr(pod_status, "phase", None) == "Failed":
                        diagnostics = await self._prepare_failure_diagnostics(pod)
                        raise RuntimeError(
                            f"execution Job {job_name} pod failed during workspace preparation: {diagnostics}"
                        )
                    statuses = getattr(pod_status, "container_statuses", None) or []
                    for container in statuses:
                        state = getattr(container, "state", None)
                        if (
                            getattr(container, "name", None) == "execution"
                            and getattr(state, "running", None) is not None
                            and getattr(container, "ready", False)
                        ):
                            return
            if job_failed:
                raise RuntimeError(f"execution Job {job_name} failed to start without a retained execution pod")
            await asyncio.sleep(0.25)
        raise TimeoutError(f"execution Job {job_name} did not become ready within {timeout_seconds} seconds")

    async def execute(
        self,
        job_name: str,
        command: list[str],
        stdin: str,
        timeout_seconds: int,
        output_limit_bytes: int,
    ) -> CommandResult:
        """Run a command through the Kubernetes exec subresource for this run only."""

        pod_name = await self._running_pod_name(job_name)
        if stdin:
            # The pinned Kubernetes WebSocket client cannot half-close stdin.
            # Feed trusted command input through the remote shell instead, so
            # the child process receives EOF without closing stdout/stderr.
            command = ["/bin/sh", "-lc", f"printf '%s' {shlex.quote(stdin)} | exec {shlex.join(command)}"]
            stdin = ""
        return await asyncio.to_thread(
            self._execute_in_pod,
            pod_name,
            command,
            stdin,
            timeout_seconds,
            output_limit_bytes,
        )

    async def _running_pod_name(self, job_name: str) -> str:
        run_hash = job_name.removeprefix(_EXECUTION_JOB_PREFIX)
        pods = await asyncio.to_thread(
            self._core_api.list_namespaced_pod,
            self._namespace,
            label_selector=f"{_RUN_HASH_LABEL}={run_hash}",
        )
        for pod in pods.items:
            status = getattr(pod, "status", None)
            metadata = getattr(pod, "metadata", None)
            name = getattr(metadata, "name", None)
            if getattr(status, "phase", None) == "Running" and name:
                return name
        raise RuntimeError(f"execution Job {job_name} has no running execution pod")

    def _execute_in_pod(
        self,
        pod_name: str,
        command: list[str],
        stdin: str,
        timeout_seconds: int,
        output_limit_bytes: int,
    ) -> CommandResult:
        response = self._stream(
            self._core_api.connect_get_namespaced_pod_exec,
            pod_name,
            self._namespace,
            command=command,
            container="execution",
            stderr=True,
            stdin=True,
            stdout=True,
            tty=False,
            _preload_content=False,
        )
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        stdout_bytes = 0
        stderr_bytes = 0
        stdout_truncated = False
        stderr_truncated = False
        deadline = time.monotonic() + timeout_seconds
        try:
            if stdin:
                response.write_stdin(stdin)
            while response.is_open():
                if time.monotonic() >= deadline:
                    response.close()
                    return CommandResult(
                        exit_code=124,
                        stdout=_bounded_output(stdout_parts, output_limit_bytes),
                        stderr="command timed out",
                    )
                response.update(timeout=1)
                stdout_bytes, stdout_truncated = _append_bounded_output(
                    stdout_parts, response.read_stdout(), stdout_bytes, output_limit_bytes, stdout_truncated
                )
                stderr_bytes, stderr_truncated = _append_bounded_output(
                    stderr_parts, response.read_stderr(), stderr_bytes, output_limit_bytes, stderr_truncated
                )
            return CommandResult(
                exit_code=response.returncode or 0,
                stdout=_bounded_output(stdout_parts, output_limit_bytes, stdout_truncated),
                stderr=_bounded_output(stderr_parts, output_limit_bytes, stderr_truncated),
            )
        finally:
            response.close()

    async def _prepare_failure_diagnostics(self, pod: object) -> str:
        """Return a bounded, sanitized init-container failure summary for durable run status."""

        metadata = getattr(pod, "metadata", None)
        pod_name = getattr(metadata, "name", None)
        if not pod_name:
            return "execution pod name was unavailable"
        try:
            log_output = await asyncio.to_thread(
                self._core_api.read_namespaced_pod_log,
                pod_name,
                self._namespace,
                container="prepare-workspace",
                tail_lines=50,
                limit_bytes=4096,
            )
        except self._api_exception as error:
            return f"prepare-workspace logs unavailable (Kubernetes API status {error.status})"
        return _sanitize_diagnostics(log_output)


class ExecutionWorkspaceService:
    """Owns the lifecycle of a run's pod-local execution workspace."""

    def __init__(
        self,
        settings: ExecutionJobSettings,
        jobs: ExecutionJobClient,
        run_keys: RunKeyManager | None = None,
        run_git_credentials: RunGitCredentialManager | None = None,
    ):
        self._settings = settings
        self._jobs = jobs
        self._run_keys = run_keys
        self._run_git_credentials = run_git_credentials

    async def provision(self, request: ExecutionRequest) -> ExecutionWorkspace:
        """Create the execution Job and return its non-secret descriptor."""

        job_name = execution_job_name(request.run_id)
        run_key_secret = request.run_key_secret
        run_git_secret = request.run_git_secret
        try:
            if self._run_git_credentials is not None:
                run_git_secret = await self._run_git_credentials.provision(request.run_id)
            if self._run_keys is not None:
                run_key_secret = await self._run_keys.provision(
                    RunBudget(
                        run_id=request.run_id,
                        max_cost_usd=request.max_cost_usd,
                        model=self._settings.litellm_model,
                        expires_in_seconds=request.execution_timeout_seconds,
                    )
                )
        except Exception:
            if self._run_git_credentials is not None and run_git_secret:
                await self._run_git_credentials.cleanup(request.run_id, run_git_secret)
            raise
        job_request = replace(request, run_key_secret=run_key_secret, run_git_secret=run_git_secret)
        body = build_execution_job(request=job_request, job_name=job_name, settings=self._settings)
        try:
            await self._jobs.create_job(job_name, body)
        except Exception:
            if self._run_keys is not None:
                await self._run_keys.cleanup(request.run_id, run_key_secret)
            if self._run_git_credentials is not None:
                await self._run_git_credentials.cleanup(request.run_id, run_git_secret)
            raise
        try:
            await self._jobs.wait_until_ready(job_name, self._settings.startup_timeout_seconds)
        except Exception:
            await self._jobs.delete_job(job_name)
            if self._run_keys is not None:
                await self._run_keys.cleanup(request.run_id, run_key_secret)
            if self._run_git_credentials is not None:
                await self._run_git_credentials.cleanup(request.run_id, run_git_secret)
            raise
        return ExecutionWorkspace(
            run_id=request.run_id,
            job_name=job_name,
            workspace_root=self._settings.workspace_root,
            repositories=[
                f"{self._settings.workspace_root}/repos/"
                f"{repository_directory_name(repository, self._settings.allowed_git_hosts)}"
                for repository in request.target_repos
            ],
            repository_origins={
                f"{self._settings.workspace_root}/repos/"
                f"{repository_directory_name(repository, self._settings.allowed_git_hosts)}": repository_clone_url(
                    repository, self._settings.allowed_git_hosts
                )
                for repository in request.target_repos
            },
            run_key_secret=run_key_secret,
            run_git_secret=run_git_secret,
        )

    async def cleanup(self, workspace: ExecutionWorkspace) -> None:
        """Remove the execution Job and its `emptyDir` workspace."""

        # A transient Kubernetes deletion error must not leave an execution
        # credential valid after the workflow has reached a terminal outcome.
        try:
            await self._jobs.delete_job(workspace.job_name)
        finally:
            try:
                if self._run_keys is not None:
                    await self._run_keys.cleanup(workspace.run_id, workspace.run_key_secret)
            finally:
                if self._run_git_credentials is not None:
                    await self._run_git_credentials.cleanup(workspace.run_id, workspace.run_git_secret)

    async def execute(
        self,
        workspace: ExecutionWorkspace,
        command: list[str],
        stdin: str = "",
        timeout_seconds: int = 60,
    ) -> CommandResult:
        """Execute a command only in the run-specific workspace pod."""

        if workspace.run_id == "" or not workspace.job_name.startswith(_EXECUTION_JOB_PREFIX):
            raise ValueError("execution workspace descriptor is invalid")
        return await self._jobs.execute(
            workspace.job_name,
            command,
            stdin,
            timeout_seconds,
            self._settings.command_output_limit_bytes,
        )


def _sanitize_diagnostics(value: str | bytes) -> str:
    """Limit and redact diagnostic output before it reaches logs or durable status."""

    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    if text.startswith(("b'", 'b"')):
        try:
            encoded_value = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            encoded_value = None
        if isinstance(encoded_value, bytes):
            text = encoded_value.decode("utf-8", errors="replace")
    normalized = " ".join(line.strip() for line in text.splitlines()[-10:] if line.strip())[:4096]
    redacted = _SENSITIVE_DIAGNOSTIC_PATTERN.sub("[REDACTED]", normalized)
    redacted = _BEARER_TOKEN_PATTERN.sub("Bearer [REDACTED]", redacted)
    return redacted or "prepare-workspace exited without diagnostic output"


def _append_bounded_output(
    parts: list[str], value: str, current_bytes: int, limit_bytes: int, truncated: bool
) -> tuple[int, bool]:
    """Append only the remaining output budget so a command cannot exhaust worker memory."""

    if truncated:
        return current_bytes, True
    encoded = value.encode("utf-8")
    remaining = limit_bytes - current_bytes
    if remaining <= 0:
        return current_bytes, True
    if len(encoded) <= remaining:
        parts.append(value)
        return current_bytes + len(encoded), False
    parts.append(encoded[:remaining].decode("utf-8", errors="ignore"))
    return limit_bytes, True


def _bounded_output(parts: list[str], limit_bytes: int, truncated: bool = False) -> str:
    """Return bounded, redacted command output without splitting UTF-8 code points."""

    output = "".join(parts)
    if len(output.encode("utf-8")) > limit_bytes:
        output = output.encode("utf-8")[:limit_bytes].decode("utf-8", errors="ignore") + "\n[output truncated]"
    elif truncated:
        output += "\n[output truncated]"
    return output
