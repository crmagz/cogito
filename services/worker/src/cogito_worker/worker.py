from __future__ import annotations

import asyncio
import logging

from minio import Minio
from temporalio.client import Client
from temporalio.worker import Worker

from .activities import WorkerActivities
from .config import load_settings
from .execution import ExecutionJobSettings, ExecutionWorkspaceService, KubernetesExecutionJobClient
from .harness import ClaudeCodeHarness
from .storage import MinioRunStore
from .workflows import DeveloperRunWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()

    client = await Client.connect(settings.temporal_host, namespace=settings.temporal_namespace)
    minio_client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    store = MinioRunStore(minio_client, settings.plans_bucket, settings.plan_snapshots_bucket)
    execution_settings = ExecutionJobSettings(
        namespace=settings.execution_namespace,
        image=settings.execution_image,
        image_pull_policy=settings.execution_image_pull_policy,
        workspace_root=settings.execution_workspace_root,
        idle_seconds=settings.execution_idle_seconds,
        startup_timeout_seconds=settings.execution_startup_timeout_seconds,
        cleanup_timeout_seconds=settings.execution_cleanup_timeout_seconds,
        active_deadline_seconds=settings.execution_active_deadline_seconds,
        ttl_seconds_after_finished=settings.execution_ttl_seconds_after_finished,
        termination_grace_period_seconds=settings.execution_termination_grace_period_seconds,
        workspace_size_limit=settings.execution_workspace_size_limit,
        resources=settings.execution_resources,
        allowed_git_hosts=settings.allowed_git_hosts,
        minio_endpoint=settings.execution_minio_endpoint,
        minio_secure=settings.execution_minio_secure,
        specs_bucket=settings.specs_bucket,
        specs_prefix=settings.specs_prefix,
        specs_max_archive_bytes=settings.specs_max_archive_bytes,
        specs_max_extracted_bytes=settings.specs_max_extracted_bytes,
        object_store_secret=settings.execution_object_store_secret,
        object_store_access_key_secret_key=settings.execution_object_store_access_key_secret_key,
        object_store_secret_key_secret_key=settings.execution_object_store_secret_key_secret_key,
        litellm_endpoint=settings.execution_litellm_endpoint,
        litellm_model=settings.execution_litellm_model,
        litellm_key_secret=settings.execution_litellm_key_secret,
        litellm_key_secret_key=settings.execution_litellm_key_secret_key,
        git_credentials_secret=settings.execution_git_credentials_secret,
        git_credentials_secret_key=settings.execution_git_credentials_secret_key,
        git_author_name=settings.execution_git_author_name,
        git_author_email=settings.execution_git_author_email,
        command_output_limit_bytes=settings.execution_command_output_limit_bytes,
    )
    execution_workspaces = ExecutionWorkspaceService(
        execution_settings,
        KubernetesExecutionJobClient(settings.execution_namespace, settings.execution_cleanup_timeout_seconds),
    )
    activities = WorkerActivities(
        store,
        execution_workspaces,
        ClaudeCodeHarness(execution_workspaces),
    )

    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[
            activities.load_plan,
            activities.report_status,
            activities.provision_execution_workspace,
            activities.cleanup_execution_workspace,
            activities.run_phase,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
