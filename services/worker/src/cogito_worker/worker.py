from __future__ import annotations

import asyncio
import logging

from minio import Minio
from temporalio.client import Client
from temporalio.worker import Worker

from .activities import WorkerActivities
from .config import load_settings
from .storage import MinioRunStore
from .workflows import DeveloperRunWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()

    client = await Client.connect(settings.temporal_host, namespace=settings.temporal_namespace)
    store = MinioRunStore(
        Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        ),
        settings.plans_bucket,
    )
    activities = WorkerActivities(store)

    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[DeveloperRunWorkflow],
        activities=[activities.load_plan, activities.report_status],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
