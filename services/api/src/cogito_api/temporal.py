from __future__ import annotations

import asyncio
from typing import Protocol

from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from .models import RunEnvelope


class RunStarter(Protocol):
    async def start_run(self, envelope: RunEnvelope) -> None: ...

    async def submit_plan_approval(self, workflow_id: str, decision: dict[str, str]) -> bool: ...


class TemporalRunStarter:
    def __init__(self, host: str, namespace: str, task_queue: str):
        self._host = host
        self._namespace = namespace
        self._task_queue = task_queue
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    async def start_run(self, envelope: RunEnvelope) -> None:
        client = await self._get_client()
        try:
            await client.start_workflow(
                "DeveloperRunWorkflow",
                args=[envelope.model_dump()],
                id=envelope.workflow_id or envelope.run_id,
                task_queue=self._task_queue,
            )
        except WorkflowAlreadyStartedError:
            # A caller can lose its response after Temporal accepted a start.
            # The immutable workflow ID makes that retry safe and idempotent.
            return

    async def submit_plan_approval(self, workflow_id: str, decision: dict[str, str]) -> bool:
        """Deliver an idempotent, digest-bound decision through a Temporal Update."""

        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        return await handle.execute_update("submit_plan_approval", decision)

    async def _get_client(self) -> Client:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = await Client.connect(self._host, namespace=self._namespace)
        return self._client
