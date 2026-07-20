"""Worker-side writes to the API-owned authoritative run-state projection."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


_STATUS_MAP = {
    "claimed": "STARTING",
    "awaiting_plan_approval": "WAITING_FOR_APPROVAL",
    "implementing": "RUNNING",
    "phase_complete": "RUNNING",
    "phase_failed": "FAILED",
    "completed": "SUCCEEDED",
    "failed": "FAILED",
    "rejected": "CANCELLED",
    "revision_requested": "PENDING",
}
_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}
_ALLOWED_TRANSITIONS = {
    "PENDING": {"QUEUED"},
    "QUEUED": {"STARTING", "FAILED", "CANCELLED"},
    "STARTING": {"RUNNING", "WAITING_FOR_APPROVAL", "FAILED", "CANCELLED", "TIMED_OUT"},
    "RUNNING": {"WAITING_FOR_TOOL", "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"},
    "WAITING_FOR_TOOL": {"RUNNING", "FAILED", "CANCELLED", "TIMED_OUT"},
    "WAITING_FOR_APPROVAL": {"RUNNING", "PENDING", "CANCELLED", "TIMED_OUT"},
}


class RunStateReporter(Protocol):
    async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict[str, Any] | None) -> None: ...


class NullRunStateReporter:
    async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict[str, Any] | None) -> None:
        del run_id, status, failure_detail, metadata


class PostgresRunStateReporter:
    def __init__(self, database_url: str):
        self._engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)

    async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict[str, Any] | None) -> None:
        target = _STATUS_MAP.get(status)
        if target is None:
            return
        safe_metadata = {"status": status}
        if metadata and "phase_result" in metadata:
            safe_metadata["phase_result"] = "recorded"
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text("SELECT status FROM agent_runs WHERE run_id = :run_id FOR UPDATE"), {"run_id": run_id}
            )
            row = result.mappings().one_or_none()
            if row is None:
                return
            previous = row["status"]
            # Temporal activities can be retried after a successful database
            # commit. Repeating an identical state must not create a second
            # lifecycle event or mutate a terminal projection.
            if previous == target:
                return
            if previous in _TERMINAL and previous != target:
                return
            if previous != target and target not in _ALLOWED_TRANSITIONS.get(previous, set()):
                return
            now = datetime.now(timezone.utc)
            await connection.execute(
                text(
                    """
                    UPDATE agent_runs
                    SET status = :status, updated_at = :now,
                        last_heartbeat_at = :now,
                        completed_at = CASE WHEN :terminal THEN :now ELSE completed_at END,
                        error_summary = CASE WHEN :error_summary IS NULL THEN error_summary ELSE :error_summary END
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "status": target,
                    "now": now,
                    "terminal": target in _TERMINAL,
                    "error_summary": _safe_error(failure_detail),
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_run_events (event_id, run_id, event_type, from_status, to_status, occurred_at, metadata)
                    VALUES (:event_id, :run_id, 'worker_status', :from_status, :to_status, :occurred_at, CAST(:metadata AS jsonb))
                    """
                ),
                {
                    "event_id": str(uuid.uuid4()), "run_id": run_id, "from_status": previous,
                    "to_status": target, "occurred_at": now, "metadata": json.dumps(safe_metadata),
                },
            )

    async def close(self) -> None:
        await self._engine.dispose()


def _safe_error(value: str | None) -> str | None:
    if not value:
        return None
    return " ".join(value.split())[:4096]
