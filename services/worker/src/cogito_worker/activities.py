from __future__ import annotations

from temporalio import activity

from .storage import RunStore, now_iso


class WorkerActivities:
    def __init__(self, store: RunStore):
        self._store = store

    @activity.defn
    async def load_plan(self, plan_ref: str) -> dict:
        activity.logger.info("loading plan", extra={"plan_ref": plan_ref})
        return self._store.get_plan(plan_ref)

    @activity.defn
    async def report_status(self, run_id: str, status: str) -> None:
        activity.logger.info("reporting status", extra={"run_id": run_id, "status": status})
        record = self._store.get_status(run_id) or {"run_id": run_id}
        record["status"] = status
        record["updated_at"] = now_iso()
        self._store.put_status(run_id, record)
