"""Fail-closed recovery of Supervisor projections after worker interruption."""

from __future__ import annotations

import asyncio
import logging

from .supervisor import SupervisorStore
from .temporal import WorkflowOutcomeInspector

_LOGGER = logging.getLogger(__name__)
_POLL_SECONDS = 5
_BATCH_SIZE = 100


class WorkflowProjectionReconciler:
    """Converge only explicit terminal Temporal outcomes into PostgreSQL state."""

    def __init__(self, store: SupervisorStore, inspector: WorkflowOutcomeInspector) -> None:
        self._store = store
        self._inspector = inspector

    async def reconcile_once(self) -> int:
        """Repair at most one bounded batch and return its number of changes."""

        repaired = 0
        for run in await self._store.list_reconcilable_runs(limit=_BATCH_SIZE):
            if not run.workflow_id:
                continue
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
                continue
            if outcome is None:
                continue
            if await self._store.reconcile_terminal_workflow(
                run_id=run.run_id,
                workflow_id=run.workflow_id,
                outcome=outcome,
            ):
                repaired += 1
        return repaired

    async def run(self) -> None:
        """Run until application shutdown; each pass is independently safe."""

        while True:
            try:
                await self.reconcile_once()
            except Exception:
                _LOGGER.exception("workflow projection reconciliation pass failed")
            await asyncio.sleep(_POLL_SECONDS)


async def stop_reconciler(task: asyncio.Task[None] | None) -> None:
    """Cancel a running reconciliation loop without masking shutdown errors."""

    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
