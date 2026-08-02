"""Fail-closed recovery of Supervisor projections after worker interruption."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Protocol

from .supervisor import SupervisorStore
from .temporal import WorkflowOutcomeInspector

_LOGGER = logging.getLogger(__name__)


class ReconciliationObserver(Protocol):
    """Receive aggregate reconciliation metrics without run identifiers."""

    def reconciliation_pass(self, *, inspected: int, repaired: int, failures: int) -> None: ...


class NullReconciliationObserver:
    """Discard metrics when reconciliation is used outside the application runtime."""

    def reconciliation_pass(self, *, inspected: int, repaired: int, failures: int) -> None:
        """Accept aggregate counters without emitting telemetry."""

        del inspected, repaired, failures


class ReconciliationHealth:
    """Track whether the reconciliation loop is making bounded progress."""

    def __init__(self, stall_seconds: int) -> None:
        self._stall_seconds = stall_seconds
        self._running = False
        self._last_pass_at: float | None = None

    def started(self) -> None:
        """Mark the loop as running before its first reconciliation pass."""

        self._running = True
        self._last_pass_at = time.monotonic()

    def completed_pass(self) -> None:
        """Record a completed pass, including a pass that found an unavailable dependency."""

        self._last_pass_at = time.monotonic()

    def stopped(self) -> None:
        """Mark the loop as unavailable after its task exits."""

        self._running = False

    def is_healthy(self) -> bool:
        """Return whether the loop is running and has progressed within its bounded window."""

        return (
            self._running
            and self._last_pass_at is not None
            and time.monotonic() - self._last_pass_at <= self._stall_seconds
        )


class WorkflowProjectionReconciler:
    """Converge only explicit terminal Temporal outcomes into PostgreSQL state."""

    def __init__(
        self,
        store: SupervisorStore,
        inspector: WorkflowOutcomeInspector,
        *,
        poll_seconds: int = 5,
        batch_size: int = 100,
        stall_seconds: int = 30,
        telemetry: ReconciliationObserver | None = None,
    ) -> None:
        self._store = store
        self._inspector = inspector
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size
        self._telemetry = telemetry or NullReconciliationObserver()
        self.health = ReconciliationHealth(stall_seconds)

    async def reconcile_once(self) -> int:
        """Repair at most one bounded batch and return its number of changes."""

        repaired = 0
        inspected = 0
        failures = 0
        for run in await self._store.list_reconcilable_runs(limit=self._batch_size):
            if not run.workflow_id:
                continue
            inspected += 1
            try:
                outcome = await self._inspector.get_terminal_outcome(run.workflow_id)
            except Exception:
                # Temporal unavailability is not workflow failure. Keep the
                # projection unchanged and retry on the next bounded pass.
                _LOGGER.warning(
                    "unable to inspect workflow for reconciliation",
                    extra={"run_id": run.run_id},
                    exc_info=True,
                )
                failures += 1
                continue
            if outcome is None:
                continue
            if await self._store.reconcile_terminal_workflow(
                run_id=run.run_id,
                workflow_id=run.workflow_id,
                outcome=outcome,
            ):
                repaired += 1
        self._telemetry.reconciliation_pass(inspected=inspected, repaired=repaired, failures=failures)
        return repaired

    async def run(self) -> None:
        """Run until application shutdown; each pass is independently safe."""

        self.health.started()
        try:
            while True:
                try:
                    await self.reconcile_once()
                except Exception:
                    _LOGGER.exception("workflow projection reconciliation pass failed")
                    self._telemetry.reconciliation_pass(inspected=0, repaired=0, failures=1)
                finally:
                    self.health.completed_pass()
                await asyncio.sleep(self._poll_seconds)
        finally:
            self.health.stopped()


async def stop_reconciler(task: asyncio.Task[None] | None) -> None:
    """Cancel a running reconciliation loop without masking shutdown errors."""

    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
