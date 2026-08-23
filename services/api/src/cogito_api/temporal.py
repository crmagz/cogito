from __future__ import annotations

import asyncio
from typing import Any, Protocol

from temporalio.client import Client
from temporalio.api.enums.v1 import WorkflowExecutionStatus
from temporalio.exceptions import WorkflowAlreadyStartedError

from .models import RunEnvelope


class RunStarter(Protocol):
    async def start_run(self, envelope: RunEnvelope) -> None: ...

    async def submit_plan_approval(self, workflow_id: str, decision: dict[str, Any]) -> bool: ...

    async def submit_implementation_approval(self, workflow_id: str, decision: dict[str, str]) -> bool: ...

    async def submit_workflow_gate(self, workflow_id: str, gate_id: str, decision: dict[str, Any]) -> bool: ...


class WorkflowOutcomeInspector(Protocol):
    """Read a closed workflow's typed business outcome without mutating it."""

    async def get_terminal_outcome(self, workflow_id: str) -> str | None: ...


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

    async def submit_plan_approval(self, workflow_id: str, decision: dict[str, Any]) -> bool:
        """Deliver an idempotent, digest-bound decision through a Temporal Update."""

        decision_id = decision.get("decision_id")
        if not decision_id:
            return False
        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        # Temporal persists an Update ID. Reusing the durable approval ID lets
        # an outbox retry recover the original accepted result even if its
        # database acknowledgement failed after Temporal accepted the update.
        update_name = (
            "submit_plan_approval_with_mcp_selection"
            if decision.get("mcp_selection") is not None
            else "submit_plan_approval"
        )
        return await handle.execute_update(update_name, decision, id=decision_id)

    async def submit_implementation_approval(self, workflow_id: str, decision: dict[str, str]) -> bool:
        """Deliver an idempotent decision for the frozen implementation artifact."""

        decision_id = decision.get("decision_id")
        if not decision_id:
            return False
        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        return await handle.execute_update("submit_implementation_approval", decision, id=decision_id)

    async def submit_workflow_gate(self, workflow_id: str, gate_id: str, decision: dict[str, Any]) -> bool:
        """Deliver a resolution-bound gate decision, retaining legacy fallbacks in dispatchers."""

        decision_id = decision.get("decision_id")
        if not decision_id:
            return False
        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        return await handle.execute_update(
            "submit_workflow_gate", args=[gate_id, decision], id=decision_id
        )

    async def get_terminal_outcome(self, workflow_id: str) -> str | None:
        """Return a recognized terminal workflow result, or ``None`` while it is live.

        The Supervisor uses this only to repair its own projection after a
        worker-side status activity was interrupted.  It deliberately trusts
        neither a missing heartbeat nor a generic Temporal close status: the
        workflow result must carry one of Cogito's explicit business outcomes.
        """

        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe()
        status = description.raw_description.workflow_execution_info.status
        if status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED:
            return None
        return _terminal_outcome(await handle.result(follow_runs=False))

    async def _get_client(self) -> Client:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = await Client.connect(self._host, namespace=self._namespace)
        return self._client


def _terminal_outcome(result: Any) -> str | None:
    """Extract only a supported outcome from a Temporal workflow result."""

    status = result.get("status") if isinstance(result, dict) else getattr(result, "status", None)
    if status in {"completed", "failed", "stopped_with_backup"}:
        return status
    return None
