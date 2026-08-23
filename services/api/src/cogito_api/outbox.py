"""Durable, leased delivery of human decisions to Temporal."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

from .supervisor import PlanningGenerationDelivery, SupervisorStore
from .temporal import RunStarter

_LOGGER = logging.getLogger(__name__)


class PlanningGenerationDispatcher:
    """Lease accepted specifications until planning reaches a durable outcome."""

    def __init__(
        self,
        store: SupervisorStore,
        deliver: Callable[[PlanningGenerationDelivery], Awaitable[bool]],
        poll_seconds: float = 1.0,
        lease_seconds: int = 90,
        lease_renewal_seconds: float = 20.0,
    ) -> None:
        self._store = store
        self._deliver = deliver
        self._poll_seconds = poll_seconds
        self._lease_seconds = lease_seconds
        self._lease_renewal_seconds = lease_renewal_seconds
        self._active: dict[str, asyncio.Task[bool]] = {}
        self._cancelled: set[str] = set()

    async def deliver_once(self, *, limit: int = 10) -> int:
        """Deliver a bounded planning batch and release transient failures."""

        delivered = 0
        for item in await self._store.claim_planning_generation_deliveries(
            limit=limit, lease_seconds=self._lease_seconds
        ):
            task = asyncio.create_task(self._deliver(item), name=f"planning-generation:{item.run_id}")
            lease_renewer = asyncio.create_task(
                self._renew_lease(item), name=f"planning-generation-lease:{item.run_id}"
            )
            self._active[item.run_id] = task
            try:
                complete = await task
            except asyncio.CancelledError:
                if item.run_id not in self._cancelled:
                    raise
                self._cancelled.remove(item.run_id)
                complete = True
            except Exception:
                complete = False
                _LOGGER.warning("planning generation delivery failed", extra={"run_id": item.run_id}, exc_info=False)
            finally:
                self._active.pop(item.run_id, None)
                lease_renewer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await lease_renewer
            if complete:
                delivered += 1
            else:
                await self._store.release_planning_generation_delivery(
                    item.run_id, item.claim_id, retry_seconds=_retry_delay(item.attempt_count)
                )
        return delivered

    async def _renew_lease(self, item: PlanningGenerationDelivery) -> None:
        """Keep an owned planner attempt exclusive through model and persistence latency."""

        while True:
            await asyncio.sleep(self._lease_renewal_seconds)
            try:
                renewed = await self._store.renew_planning_generation_delivery(item.run_id, item.claim_id)
            except Exception:
                _LOGGER.warning("planning generation lease renewal failed", extra={"run_id": item.run_id}, exc_info=False)
                continue
            if not renewed:
                return

    def cancel(self, run_id: str) -> None:
        """Cancel a local planner request after its durable run is cancelled."""

        task = self._active.get(run_id)
        if task is None:
            return
        self._cancelled.add(run_id)
        task.cancel()

    async def run(self) -> None:
        """Keep planner handoffs recoverable across API restarts and replicas."""

        while True:
            try:
                await self.deliver_once()
            except Exception:
                _LOGGER.warning("planning generation dispatcher pass failed", exc_info=False)
            await asyncio.sleep(self._poll_seconds)


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
                # New workers validate the resolved gate ID themselves. A
                # legacy envelope has no resolved gate set and returns False,
                # in which case the historic update remains the compatibility
                # adapter during the rollout.
                accepted = await self._starter.submit_workflow_gate(
                    item.workflow_id, "plan_scope_review", item.payload
                )
                if not accepted:
                    accepted = await self._starter.submit_plan_approval(item.workflow_id, item.payload)
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
            try:
                await self.deliver_once()
            except Exception:
                # A transient database or gateway failure must not terminate the
                # only background retry loop. Do not log exception text: provider
                # exceptions can embed request headers or connection strings.
                _LOGGER.warning("plan approval outbox delivery pass failed", exc_info=False)
            await asyncio.sleep(self._poll_seconds)


class ImplementationApprovalOutboxDispatcher:
    """Delivers persisted implementation decisions with the same leased semantics."""

    def __init__(self, store: SupervisorStore, starter: RunStarter, poll_seconds: float = 1.0):
        self._store = store
        self._starter = starter
        self._poll_seconds = poll_seconds

    async def deliver_once(self, decision_id: str | None = None, limit: int = 10) -> set[str]:
        """Deliver a bounded set of digest-bound implementation approvals."""

        delivered: set[str] = set()
        pending = await self._store.claim_implementation_approval_deliveries(
            limit=limit, lease_seconds=30, decision_id=decision_id
        )
        for item in pending:
            try:
                accepted = await self._starter.submit_workflow_gate(
                    item.workflow_id, "delivery_review", item.payload
                )
                if not accepted:
                    accepted = await self._starter.submit_implementation_approval(item.workflow_id, item.payload)
            except Exception as error:
                await self._store.release_implementation_approval_delivery(
                    item.decision_id, retry_seconds=_retry_delay(item.attempt_count), error=_error_detail(error)
                )
                continue
            if accepted:
                await self._store.mark_implementation_approval_delivered(item.decision_id)
                delivered.add(item.decision_id)
            else:
                await self._store.release_implementation_approval_delivery(
                    item.decision_id,
                    retry_seconds=_retry_delay(item.attempt_count),
                    error="Temporal workflow did not accept the implementation approval update",
                )
        return delivered

    async def run(self) -> None:
        """Poll until cancelled; leasing keeps multiple API replicas safe."""

        while True:
            try:
                await self.deliver_once()
            except Exception:
                _LOGGER.warning("implementation approval outbox delivery pass failed", exc_info=False)
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

    del error
    return "transient Temporal delivery failure"
