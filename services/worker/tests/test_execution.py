from __future__ import annotations

import json

import pytest

from cogito_worker.execution import (
    ExecutionJobSettings,
    ExecutionWorkspaceService,
    build_execution_job,
    execution_job_name,
    _sanitize_diagnostics,
)
from cogito_worker.models import ExecutionRequest

from .fakes import InMemoryExecutionJobClient


def execution_settings() -> ExecutionJobSettings:
    return ExecutionJobSettings(
        namespace="cogito",
        image="cogito-worker:local",
        image_pull_policy="IfNotPresent",
        workspace_root="/workspace",
        idle_seconds=3600,
        startup_timeout_seconds=30,
        cleanup_timeout_seconds=90,
        active_deadline_seconds=3900,
        ttl_seconds_after_finished=300,
        termination_grace_period_seconds=10,
        workspace_size_limit="2Gi",
        resources={"limits": {"memory": "1Gi"}},
        allowed_git_hosts=("github.com",),
        minio_endpoint="cogito-minio:9000",
        minio_secure=False,
        specs_bucket="specs",
        specs_prefix="specs",
        specs_max_archive_bytes=1024 * 1024,
        specs_max_extracted_bytes=2 * 1024 * 1024,
        object_store_secret="cogito-minio",
        object_store_access_key_secret_key="rootUser",
        object_store_secret_key_secret_key="rootPassword",
    )


def test_execution_job_template_uses_an_isolated_emptydir_workspace() -> None:
    """Execution jobs use a private workspace and cannot access Kubernetes APIs."""

    job_name = execution_job_name("run-1")
    job = build_execution_job(
        request=ExecutionRequest(
            run_id="run-1",
            spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            target_repos=["https://github.com/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"],
        ),
        job_name=job_name,
        settings=execution_settings(),
    )

    pod_spec = job["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    init_container = pod_spec["initContainers"][0]

    assert job["metadata"]["name"] == job_name
    assert job["metadata"]["labels"]["cogito.dev/run-hash"]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["securityContext"]["runAsNonRoot"] is True
    assert pod_spec["volumes"] == [{"name": "workspace", "emptyDir": {"sizeLimit": "2Gi"}}]
    assert pod_spec["activeDeadlineSeconds"] == 3900
    assert pod_spec["ttlSecondsAfterFinished"] == 300
    assert container["volumeMounts"] == [{"name": "workspace", "mountPath": "/workspace"}]
    assert container["command"] == ["python", "-m", "cogito_worker.execution_pod"]
    assert init_container["command"] == ["python", "-m", "cogito_worker.execution_prepare"]
    assert all(env["name"] not in {"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"} for env in container["env"])
    secret_env = {env["name"]: env["valueFrom"] for env in init_container["env"] if "valueFrom" in env}
    assert secret_env["MINIO_ACCESS_KEY"]["secretKeyRef"]["name"] == "cogito-minio"
    assert json.loads(next(env["value"] for env in init_container["env"] if env["name"] == "COGITO_TARGET_REPOS")) == [
        "https://github.com/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"
    ]


async def test_provisioning_removes_a_job_when_its_pod_never_becomes_active() -> None:
    """A failed startup does not leak a Job or its future workspace."""

    class FailingExecutionJobClient(InMemoryExecutionJobClient):
        async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
            await super().wait_until_ready(job_name, timeout_seconds)
            raise TimeoutError("execution pod did not start")

    jobs = FailingExecutionJobClient()
    service = ExecutionWorkspaceService(
        execution_settings(),
        jobs,
    )

    with pytest.raises(TimeoutError, match="did not start"):
        await service.provision(ExecutionRequest(run_id="run-1", spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64, target_repos=[]))

    assert jobs.deleted == [execution_job_name("run-1")]


def test_execution_failure_diagnostics_redact_sensitive_values() -> None:
    output = _sanitize_diagnostics("b'MINIO_SECRET_KEY=super-secret token=abc123\\nworkspace failed'")

    assert "super-secret" not in output
    assert "abc123" not in output
    assert "[REDACTED]" in output
