from __future__ import annotations

import asyncio
from typing import Protocol

from temporalio.client import Client

from .models import RunEnvelope


class RunStarter(Protocol):
    async def start_run(self, envelope: RunEnvelope) -> None: ...


class TemporalRunStarter:
    def __init__(self, host: str, namespace: str, task_queue: str):
        self._host = host
        self._namespace = namespace
        self._task_queue = task_queue
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    async def start_run(self, envelope: RunEnvelope) -> None:
        client = await self._get_client()
        await client.start_workflow(
            "DeveloperRunWorkflow",
            args=[envelope.model_dump()],
            id=envelope.run_id,
            task_queue=self._task_queue,
        )

    async def _get_client(self) -> Client:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = await Client.connect(self._host, namespace=self._namespace)
        return self._client
