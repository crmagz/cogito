from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    temporal_host: str
    temporal_namespace: str
    task_queue: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    plans_bucket: str
    plan_snapshots_bucket: str
    specs_bucket: str
    specs_prefix: str
    specs_max_archive_bytes: int
    specs_max_extracted_bytes: int
    execution_namespace: str
    allowed_git_hosts: tuple[str, ...]
    execution_image: str
    execution_image_pull_policy: str
    execution_workspace_root: str
    execution_idle_seconds: int
    execution_startup_timeout_seconds: int
    execution_cleanup_timeout_seconds: int
    execution_active_deadline_seconds: int
    execution_ttl_seconds_after_finished: int
    execution_termination_grace_period_seconds: int
    execution_workspace_size_limit: str
    execution_resources: dict[str, object]
    execution_minio_endpoint: str
    execution_minio_secure: bool
    execution_object_store_secret: str
    execution_object_store_access_key_secret_key: str
    execution_object_store_secret_key_secret_key: str
    execution_litellm_endpoint: str
    execution_litellm_model: str
    execution_litellm_key_secret: str
    execution_litellm_key_secret_key: str
    execution_git_credentials_secret: str
    execution_git_credentials_secret_key: str
    execution_git_author_name: str
    execution_git_author_email: str
    execution_command_output_limit_bytes: int


def load_settings() -> Settings:
    allowed_git_hosts = json.loads(os.environ.get("COGITO_ALLOWED_GIT_HOSTS", '["github.com"]'))
    execution_resources = json.loads(
        os.environ.get(
            "COGITO_EXECUTION_RESOURCES",
            '{"requests":{"cpu":"100m","memory":"256Mi","ephemeral-storage":"1Gi"},"limits":{"cpu":"1","memory":"1Gi","ephemeral-storage":"2Gi"}}',
        )
    )
    if (
        not isinstance(allowed_git_hosts, list)
        or not allowed_git_hosts
        or not all(isinstance(host, str) and host.strip() for host in allowed_git_hosts)
    ):
        raise ValueError("COGITO_ALLOWED_GIT_HOSTS must be a non-empty JSON string array")
    if not isinstance(execution_resources, dict):
        raise ValueError("COGITO_EXECUTION_RESOURCES must be a JSON object")
    return Settings(
        temporal_host=os.environ.get("COGITO_TEMPORAL_HOST", "localhost:7233"),
        temporal_namespace=os.environ.get("COGITO_TEMPORAL_NAMESPACE", "default"),
        task_queue=os.environ.get("COGITO_TEMPORAL_TASK_QUEUE", "developer-tasks"),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        minio_secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        plans_bucket=os.environ.get("MINIO_PLANS_BUCKET", "plans"),
        plan_snapshots_bucket=os.environ.get("MINIO_PLAN_SNAPSHOTS_BUCKET", "plan-snapshots"),
        specs_bucket=os.environ.get("MINIO_SPECS_BUCKET", "specs"),
        specs_prefix=os.environ.get("MINIO_SPECS_PREFIX", "specs"),
        specs_max_archive_bytes=int(os.environ.get("MINIO_SPECS_MAX_ARCHIVE_BYTES", str(10 * 1024 * 1024))),
        specs_max_extracted_bytes=int(
            os.environ.get("MINIO_SPECS_MAX_EXTRACTED_BYTES", str(25 * 1024 * 1024))
        ),
        execution_namespace=os.environ.get("COGITO_EXECUTION_NAMESPACE", "cogito-executions"),
        allowed_git_hosts=tuple(allowed_git_hosts),
        execution_image=os.environ.get("COGITO_EXECUTION_IMAGE", "cogito-worker:local"),
        execution_image_pull_policy=os.environ.get("COGITO_EXECUTION_IMAGE_PULL_POLICY", "IfNotPresent"),
        execution_workspace_root=os.environ.get("COGITO_EXECUTION_WORKSPACE_ROOT", "/workspace"),
        execution_idle_seconds=int(os.environ.get("COGITO_EXECUTION_IDLE_SECONDS", "3600")),
        execution_startup_timeout_seconds=int(os.environ.get("COGITO_EXECUTION_STARTUP_TIMEOUT_SECONDS", "30")),
        execution_cleanup_timeout_seconds=int(os.environ.get("COGITO_EXECUTION_CLEANUP_TIMEOUT_SECONDS", "90")),
        execution_active_deadline_seconds=int(os.environ.get("COGITO_EXECUTION_ACTIVE_DEADLINE_SECONDS", "3900")),
        execution_ttl_seconds_after_finished=int(
            os.environ.get("COGITO_EXECUTION_TTL_SECONDS_AFTER_FINISHED", "300")
        ),
        execution_termination_grace_period_seconds=int(
            os.environ.get("COGITO_EXECUTION_TERMINATION_GRACE_PERIOD_SECONDS", "10")
        ),
        execution_workspace_size_limit=os.environ.get("COGITO_EXECUTION_WORKSPACE_SIZE_LIMIT", "2Gi"),
        execution_resources=execution_resources,
        execution_minio_endpoint=os.environ.get(
            "COGITO_EXECUTION_MINIO_ENDPOINT", os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        ),
        execution_minio_secure=os.environ.get(
            "COGITO_EXECUTION_MINIO_SECURE", os.environ.get("MINIO_SECURE", "false")
        ).lower()
        == "true",
        execution_object_store_secret=os.environ.get("COGITO_EXECUTION_OBJECT_STORE_SECRET", "cogito-minio"),
        execution_object_store_access_key_secret_key=os.environ.get(
            "COGITO_EXECUTION_OBJECT_STORE_ACCESS_KEY_SECRET_KEY", "rootUser"
        ),
        execution_object_store_secret_key_secret_key=os.environ.get(
            "COGITO_EXECUTION_OBJECT_STORE_SECRET_KEY_SECRET_KEY", "rootPassword"
        ),
        execution_litellm_endpoint=os.environ.get("COGITO_EXECUTION_LITELLM_ENDPOINT", "http://cogito-litellm:4000"),
        execution_litellm_model=os.environ.get("COGITO_EXECUTION_LITELLM_MODEL", "complex"),
        execution_litellm_key_secret=os.environ.get("COGITO_EXECUTION_LITELLM_KEY_SECRET", "cogito-developer-key"),
        execution_litellm_key_secret_key=os.environ.get(
            "COGITO_EXECUTION_LITELLM_KEY_SECRET_KEY", "api-key"
        ),
        execution_git_credentials_secret=os.environ.get(
            "COGITO_EXECUTION_GIT_CREDENTIALS_SECRET", "cogito-developer-git"
        ),
        execution_git_credentials_secret_key=os.environ.get(
            "COGITO_EXECUTION_GIT_CREDENTIALS_SECRET_KEY", "token"
        ),
        execution_git_author_name=os.environ.get("COGITO_EXECUTION_GIT_AUTHOR_NAME", "Cogito Agent"),
        execution_git_author_email=os.environ.get("COGITO_EXECUTION_GIT_AUTHOR_EMAIL", "cogito@local.invalid"),
        execution_command_output_limit_bytes=int(
            os.environ.get("COGITO_EXECUTION_COMMAND_OUTPUT_LIMIT_BYTES", str(256 * 1024))
        ),
    )
