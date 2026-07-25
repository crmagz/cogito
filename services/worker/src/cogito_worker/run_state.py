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
    "awaiting_implementation_approval": "WAITING_FOR_APPROVAL",
    "finalizing": "RUNNING",
    "implementing": "RUNNING",
    "adversarial_review": "RUNNING",
    "phase_complete": "RUNNING",
    "phase_failed": "FAILED",
    "stopped_with_backup": "TIMED_OUT",
    "completed": "SUCCEEDED",
    "escalated": "SUCCEEDED",
    "failed": "FAILED",
    "rejected": "CANCELLED",
    "revision_requested": "PENDING",
}
_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}
_ALLOWED_TRANSITIONS = {
    "PENDING": {"QUEUED"},
    "QUEUED": {"STARTING", "FAILED", "CANCELLED"},
    "STARTING": {"RUNNING", "WAITING_FOR_APPROVAL", "FAILED", "CANCELLED", "TIMED_OUT"},
    "RUNNING": {
        "WAITING_FOR_TOOL",
        "WAITING_FOR_APPROVAL",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
        "TIMED_OUT",
    },
    "WAITING_FOR_TOOL": {"RUNNING", "FAILED", "CANCELLED", "TIMED_OUT"},
    "WAITING_FOR_APPROVAL": {"RUNNING", "PENDING", "CANCELLED", "TIMED_OUT"},
}


class RunStateReporter(Protocol):
    async def report(
        self,
        run_id: str,
        status: str,
        failure_detail: str | None,
        metadata: dict[str, Any] | None,
    ) -> None: ...


class NullRunStateReporter:
    async def report(
        self,
        run_id: str,
        status: str,
        failure_detail: str | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        del run_id, status, failure_detail, metadata


class PostgresRunStateReporter:
    def __init__(self, database_url: str):
        self._engine: AsyncEngine = create_async_engine(
            database_url, pool_pre_ping=True
        )

    async def report(
        self,
        run_id: str,
        status: str,
        failure_detail: str | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        target = _STATUS_MAP.get(status)
        if target is None:
            return
        safe_metadata = {"status": status}
        if metadata and "phase_result" in metadata:
            safe_metadata["phase_result"] = "recorded"
        implementation_artifact = metadata.get("implementation_artifact") if metadata else None
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text("SELECT status FROM agent_runs WHERE run_id = :run_id FOR UPDATE"),
                {"run_id": run_id},
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
            if previous != target and target not in _ALLOWED_TRANSITIONS.get(
                previous, set()
            ):
                return
            now = datetime.now(timezone.utc)
            await connection.execute(
                text(
                    """
                    UPDATE agent_runs
                    SET status = :status, updated_at = :now,
                        last_heartbeat_at = :now,
                        completed_at = CASE WHEN :terminal THEN :now ELSE completed_at END,
                        error_summary = COALESCE(CAST(:error_summary AS text), error_summary)
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
            if status == "awaiting_implementation_approval":
                if not isinstance(implementation_artifact, dict):
                    raise ValueError("implementation approval requires a frozen artifact")
                ref = implementation_artifact.get("ref")
                digest = implementation_artifact.get("sha256")
                if not isinstance(ref, str) or not isinstance(digest, str):
                    raise ValueError("implementation approval artifact is invalid")
                artifact_result = await connection.execute(
                    text(
                        """
                        UPDATE supervisor_runs
                        SET status = 'awaiting_implementation_approval',
                            implementation_artifact_ref = :ref,
                            implementation_artifact_sha256 = :sha256,
                            implementation_revision = implementation_revision + 1
                        WHERE run_id = :run_id AND status = 'implementing'
                        RETURNING implementation_revision
                        """
                    ),
                    {"run_id": run_id, "ref": ref, "sha256": digest},
                )
                if artifact_result.mappings().one_or_none() is not None:
                    await connection.execute(
                        text(
                            """
                            INSERT INTO supervisor_artifacts (run_id, artifact_type, ref, sha256, created_at)
                            VALUES (:run_id, 'implementation_review', :ref, :sha256, :created_at)
                            ON CONFLICT (run_id, artifact_type, ref) DO NOTHING
                            """
                        ),
                        {"run_id": run_id, "ref": ref, "sha256": digest, "created_at": now},
                    )
            pull_request = metadata.get("pull_request") if metadata else None
            if status == "completed" and isinstance(pull_request, dict) and isinstance(implementation_artifact, dict):
                number = pull_request.get("number")
                url = pull_request.get("url")
                digest = implementation_artifact.get("sha256")
                if isinstance(number, int) and isinstance(url, str) and isinstance(digest, str):
                    repository = _repository_from_pull_request_url(url)
                    if repository is not None:
                        await connection.execute(
                            text(
                                """
                                INSERT INTO implementation_pull_requests (run_id, artifact_sha256, repository, number, url, created_at)
                                VALUES (:run_id, :artifact_sha256, :repository, :number, :url, :created_at)
                                ON CONFLICT (run_id) DO NOTHING
                                """
                            ),
                            {
                                "run_id": run_id,
                                "artifact_sha256": digest,
                                "repository": repository,
                                "number": number,
                                "url": url,
                                "created_at": now,
                            },
                        )
                    await connection.execute(
                        text(
                            """
                            UPDATE supervisor_runs SET status = 'completed'
                            WHERE run_id = :run_id AND status = 'finalizing'
                              AND implementation_artifact_sha256 = :artifact_sha256
                            """
                        ),
                        {"run_id": run_id, "artifact_sha256": digest},
                    )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_run_events (event_id, run_id, event_type, from_status, to_status, occurred_at, metadata)
                    VALUES (:event_id, :run_id, 'worker_status', :from_status, :to_status, :occurred_at, CAST(:metadata AS jsonb))
                    """
                ),
                {
                    "event_id": str(uuid.uuid4()),
                    "run_id": run_id,
                    "from_status": previous,
                    "to_status": target,
                    "occurred_at": now,
                    "metadata": json.dumps(safe_metadata),
                },
            )

    async def close(self) -> None:
        await self._engine.dispose()


def _safe_error(value: str | None) -> str | None:
    if not value:
        return None
    return " ".join(value.split())[:4096]


def _repository_from_pull_request_url(url: str) -> str | None:
    """Extract an owner/repository pair from a GitHub PR URL without accepting arbitrary URLs."""

    from urllib.parse import urlparse

    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if parsed.scheme != "https" or parsed.hostname != "github.com" or len(parts) != 4 or parts[2] != "pull":
        return None
    return f"{parts[0]}/{parts[1]}"
