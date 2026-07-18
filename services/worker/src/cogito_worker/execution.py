from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Protocol

from .models import ExecutionRequest, ExecutionWorkspace

_EXECUTION_JOB_PREFIX = "cogito-execution-"
_RUN_HASH_LABEL = "cogito.dev/run-hash"
_SENSITIVE_DIAGNOSTIC_PATTERN = re.compile(
    r"(?i)(access[_ -]?key|secret(?:[_ -]?key)?|token|password)=\S+"
)


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


class ExecutionJobClient(Protocol):
    """Creates and removes run-scoped Kubernetes Jobs."""

    async def create_job(self, job_name: str, body: dict[str, object]) -> None:
        """Create a Job, accepting a prior idempotent creation."""

    async def delete_job(self, job_name: str) -> None:
        """Delete a Job, accepting a prior deletion."""

    async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
        """Wait until the execution container starts after workspace preparation."""


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
                    "activeDeadlineSeconds": settings.active_deadline_seconds,
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
                                    "value": str(settings.idle_seconds),
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
        except ImportError as error:
            message = "Kubernetes execution requires the worker's kubernetes dependency"
            raise RuntimeError(message) from error

        self._namespace = namespace
        self._cleanup_timeout_seconds = cleanup_timeout_seconds
        config.load_incluster_config()
        self._batch_api = client.BatchV1Api()
        self._core_api = client.CoreV1Api()
        self._api_exception: type[Exception] = ApiException
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

    def __init__(self, settings: ExecutionJobSettings, jobs: ExecutionJobClient):
        self._settings = settings
        self._jobs = jobs

    async def provision(self, request: ExecutionRequest) -> ExecutionWorkspace:
        """Create the execution Job and return its non-secret descriptor."""

        job_name = execution_job_name(request.run_id)
        body = build_execution_job(request=request, job_name=job_name, settings=self._settings)
        await self._jobs.create_job(job_name, body)
        try:
            await self._jobs.wait_until_ready(job_name, self._settings.startup_timeout_seconds)
        except Exception:
            await self._jobs.delete_job(job_name)
            raise
        return ExecutionWorkspace(
            run_id=request.run_id,
            job_name=job_name,
            workspace_root=self._settings.workspace_root,
        )

    async def cleanup(self, workspace: ExecutionWorkspace) -> None:
        """Remove the execution Job and its `emptyDir` workspace."""

        await self._jobs.delete_job(workspace.job_name)


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
    redacted = _SENSITIVE_DIAGNOSTIC_PATTERN.sub(r"\1=[REDACTED]", normalized)
    return redacted or "prepare-workspace exited without diagnostic output"
