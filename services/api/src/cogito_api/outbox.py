"""Durable, leased delivery of human decisions to Temporal."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

from .supervisor import SupervisorStore
from .temporal import RunStarter


@dataclass(frozen=True)
class PendingPlanApproval:
    """A claimed, immutable decision awaiting Temporal delivery."""

    decision_id: str
    run_id: str
    payload: dict[str, str]
    attempt_count: int


class PlanApprovalOutboxDispatcher:
    """Delivers persisted approvals without losing them during transient failures."""

    def __init__(self, store: SupervisorStore, starter: RunStarter, poll_seconds: float = 1.0):
        self._store = store
        self._starter = starter
        self._poll_seconds = poll_seconds

    async def deliver_once(self, decision_id: str | None = None, limit: int = 10) -> set[str]:
        """Claim and attempt a bounded batch; return only accepted decision IDs."""

        delivered: set[str] = set()
        pending = await self._store.claim_plan_approval_deliveries(
            limit=limit,
            lease_seconds=30,
            decision_id=decision_id,
        )
        for item in pending:
            try:
                accepted = await self._starter.submit_plan_approval(item.run_id, item.payload)
            except Exception as error:
                await self._store.release_plan_approval_delivery(
                    item.decision_id,
                    retry_seconds=_retry_delay(item.attempt_count),
                    error=_error_detail(error),
                )
                continue
            if accepted:
                await self._store.mark_plan_approval_delivered(item.decision_id)
                delivered.add(item.decision_id)
            else:
                await self._store.release_plan_approval_delivery(
                    item.decision_id,
                    retry_seconds=_retry_delay(item.attempt_count),
                    error="Temporal workflow did not accept the approval update",
                )
        return delivered

    async def run(self) -> None:
        """Poll until cancelled; leasing makes this safe with multiple API replicas."""

        while True:
            await self.deliver_once()
            await asyncio.sleep(self._poll_seconds)


async def stop_dispatcher(task: asyncio.Task[None]) -> None:
    """Cancel and await a background dispatcher without leaking cancellation."""

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _retry_delay(attempt_count: int) -> int:
    """Use a bounded exponential retry interval for transient Temporal failures."""

    return min(60, 2 ** min(attempt_count, 6))


def _error_detail(error: Exception) -> str:
    """Persist a bounded, non-secret diagnostic string for operators."""

    return " ".join(str(error).split())[:1024] or error.__class__.__name__
