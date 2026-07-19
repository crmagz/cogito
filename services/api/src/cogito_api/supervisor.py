"""Transactional persistence for the Cogito supervisor control plane."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .models import ArtifactReference, PlanApprovalDecision, PlanConstraints, PlanningRunStatus


@dataclass(frozen=True)
class PlanningRunRecord:
    """Mutable run projection paired with immutable source-artifact identity."""

    run_id: str
    status: PlanningRunStatus
    source_artifact: ArtifactReference
    target_repos: list[str]
    spec_set: str
    constraints: PlanConstraints
    priority: str
    submitted_at: str
    submitted_by: str
    plan_artifact: ArtifactReference | None = None
    planner_model: str | None = None
    workflow_id: str | None = None
    plan_revision: int = 0


@dataclass(frozen=True)
class ApprovalRecord:
    """Immutable human decision and its Temporal delivery state."""

    decision_id: str
    run_id: str
    decision: PlanApprovalDecision
    artifact_sha256: str
    actor_id: str
    created_at: str
    delivered: bool
    plan_revision: int


@dataclass(frozen=True)
class OutboxDelivery:
    """A short-lived lease over an immutable plan approval decision."""

    decision_id: str
    run_id: str
    workflow_id: str
    payload: dict[str, str]
    attempt_count: int


class ApprovalConflictError(Exception):
    """Raised when a decision cannot safely apply to the current run state."""


class SupervisorStore(Protocol):
    """Durable source of truth for supervisor run state."""

    async def create_planning_run(self, record: PlanningRunRecord) -> None: ...

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None: ...

    async def attach_generated_plan(
        self,
        run_id: str,
        plan_artifact: ArtifactReference,
        planner_model: str,
        workflow_id: str,
        expected_plan_revision: int,
    ) -> PlanningRunRecord: ...

    async def record_plan_approval(
        self,
        run_id: str,
        artifact_sha256: str,
        decision: PlanApprovalDecision,
        actor_id: str,
        comment: str | None,
        idempotency_key: str,
        request_sha256: str,
    ) -> ApprovalRecord: ...

    async def mark_plan_approval_delivered(self, decision_id: str) -> None: ...

    async def claim_plan_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]: ...

    async def release_plan_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None: ...


class PostgresSupervisorStore:
    """PostgreSQL implementation of the supervisor-run projection store."""

    def __init__(self, database_url: str):
        self._engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)

    async def create_planning_run(self, record: PlanningRunRecord) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO supervisor_runs (
                        run_id, status, source_artifact_ref, source_artifact_sha256,
                        target_repos, spec_set, constraints, priority, submitted_at, submitted_by
                    ) VALUES (
                        :run_id, :status, :source_artifact_ref, :source_artifact_sha256,
                        CAST(:target_repos AS jsonb), :spec_set, CAST(:constraints AS jsonb),
                        :priority, :submitted_at, :submitted_by
                    )
                    """
                ),
                {
                    "run_id": record.run_id,
                    "status": record.status.value,
                    "source_artifact_ref": record.source_artifact.ref,
                    "source_artifact_sha256": record.source_artifact.sha256,
                    "target_repos": json.dumps(record.target_repos),
                    "spec_set": record.spec_set,
                    "constraints": json.dumps(record.constraints.model_dump(mode="json")),
                    "priority": record.priority,
                    "submitted_at": datetime.fromisoformat(record.submitted_at),
                    "submitted_by": record.submitted_by,
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO supervisor_artifacts (run_id, artifact_type, ref, sha256, created_at)
                    VALUES (:run_id, 'source_spec', :ref, :sha256, :created_at)
                    """
                ),
                {
                    "run_id": record.run_id,
                    "ref": record.source_artifact.ref,
                    "sha256": record.source_artifact.sha256,
                    "created_at": datetime.fromisoformat(record.submitted_at),
                },
            )

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                           target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                           plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision
                    FROM supervisor_runs
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
            row = result.mappings().one_or_none()
        if row is None:
            return None
        return PlanningRunRecord(
            run_id=row["run_id"],
            status=PlanningRunStatus(row["status"]),
            source_artifact=ArtifactReference(
                ref=row["source_artifact_ref"], sha256=row["source_artifact_sha256"]
            ),
            target_repos=list(row["target_repos"]),
            spec_set=row["spec_set"],
            constraints=PlanConstraints.model_validate(row["constraints"]),
            priority=row["priority"],
            submitted_at=row["submitted_at"].isoformat(),
            submitted_by=row["submitted_by"],
            plan_artifact=(
                ArtifactReference(ref=row["plan_artifact_ref"], sha256=row["plan_artifact_sha256"])
                if row["plan_artifact_ref"] is not None
                else None
            ),
            planner_model=row["planner_model"],
            workflow_id=row["active_workflow_id"],
            plan_revision=row["plan_revision"],
        )

    async def attach_generated_plan(
        self,
        run_id: str,
        plan_artifact: ArtifactReference,
        planner_model: str,
        workflow_id: str,
        expected_plan_revision: int,
    ) -> PlanningRunRecord:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET status = 'awaiting_plan_approval',
                        plan_artifact_ref = :plan_artifact_ref,
                        plan_artifact_sha256 = :plan_artifact_sha256,
                        planner_model = :planner_model,
                        active_workflow_id = :workflow_id,
                        plan_revision = plan_revision + 1
                    WHERE run_id = :run_id
                      AND status = 'planning'
                      AND plan_revision = :expected_plan_revision
                    RETURNING run_id, status, source_artifact_ref, source_artifact_sha256,
                              target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                              plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision
                    """
                ),
                {
                    "run_id": run_id,
                    "plan_artifact_ref": plan_artifact.ref,
                    "plan_artifact_sha256": plan_artifact.sha256,
                    "planner_model": planner_model,
                    "workflow_id": workflow_id,
                    "expected_plan_revision": expected_plan_revision,
                },
            )
            row = result.mappings().one_or_none()
            if row is None:
                raise ValueError("planning run is not eligible to accept a generated plan")
            await connection.execute(
                text(
                    """
                    INSERT INTO supervisor_artifacts (run_id, artifact_type, ref, sha256, created_at)
                    VALUES (:run_id, 'plan', :ref, :sha256, now())
                    """
                ),
                {"run_id": run_id, "ref": plan_artifact.ref, "sha256": plan_artifact.sha256},
            )
        return PlanningRunRecord(
            run_id=row["run_id"],
            status=PlanningRunStatus(row["status"]),
            source_artifact=ArtifactReference(
                ref=row["source_artifact_ref"], sha256=row["source_artifact_sha256"]
            ),
            target_repos=list(row["target_repos"]),
            spec_set=row["spec_set"],
            constraints=PlanConstraints.model_validate(row["constraints"]),
            priority=row["priority"],
            submitted_at=row["submitted_at"].isoformat(),
            submitted_by=row["submitted_by"],
            plan_artifact=ArtifactReference(
                ref=row["plan_artifact_ref"], sha256=row["plan_artifact_sha256"]
            ),
            planner_model=row["planner_model"],
            workflow_id=row["active_workflow_id"],
            plan_revision=row["plan_revision"],
        )

    async def record_plan_approval(
        self,
        run_id: str,
        artifact_sha256: str,
        decision: PlanApprovalDecision,
        actor_id: str,
        comment: str | None,
        idempotency_key: str,
        request_sha256: str,
    ) -> ApprovalRecord:
        async with self._engine.begin() as connection:
            run = await connection.execute(
                text(
                    """
                    SELECT status, plan_artifact_sha256, active_workflow_id, plan_revision
                    FROM supervisor_runs
                    WHERE run_id = :run_id
                    FOR UPDATE
                    """
                ),
                {"run_id": run_id},
            )
            run_row = run.mappings().one_or_none()
            if run_row is None:
                raise ApprovalConflictError("planning run does not exist")
            existing = await connection.execute(
                text(
                    """
                    SELECT decision_id, run_id, decision, artifact_sha256, actor_id, created_at, delivered_at,
                           request_sha256, plan_revision
                    FROM plan_approval_decisions
                    WHERE run_id = :run_id
                      AND plan_revision = :plan_revision
                      AND idempotency_key = :idempotency_key
                    """
                ),
                {
                    "run_id": run_id,
                    "plan_revision": run_row["plan_revision"],
                    "idempotency_key": idempotency_key,
                },
            )
            existing_row = existing.mappings().one_or_none()
            if existing_row is not None:
                if existing_row["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with a different decision")
                return _approval_record(existing_row)
            if run_row["status"] != PlanningRunStatus.AWAITING_PLAN_APPROVAL.value:
                raise ApprovalConflictError("planning run is not awaiting plan approval")
            if run_row["plan_artifact_sha256"] != artifact_sha256:
                raise ApprovalConflictError("plan approval artifact digest is stale")
            if not run_row["active_workflow_id"]:
                raise ApprovalConflictError("planning workflow is not available for approval")

            decision_id = str(uuid.uuid4())
            created_at = datetime.now().astimezone()
            await connection.execute(
                text(
                    """
                    INSERT INTO plan_approval_decisions (
                        decision_id, run_id, decision, artifact_sha256, actor_id, comment,
                        idempotency_key, request_sha256, created_at, plan_revision
                    ) VALUES (
                        :decision_id, :run_id, :decision, :artifact_sha256, :actor_id, :comment,
                        :idempotency_key, :request_sha256, :created_at, :plan_revision
                    )
                    """
                ),
                {
                    "decision_id": decision_id,
                    "run_id": run_id,
                    "decision": decision.value,
                    "artifact_sha256": artifact_sha256,
                    "actor_id": actor_id,
                    "comment": comment,
                    "idempotency_key": idempotency_key,
                    "request_sha256": request_sha256,
                    "created_at": created_at,
                    "plan_revision": run_row["plan_revision"],
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO temporal_outbox (decision_id, run_id, workflow_id, payload, created_at)
                    VALUES (:decision_id, :run_id, :workflow_id, CAST(:payload AS jsonb), :created_at)
                    """
                ),
                {
                    "decision_id": decision_id,
                    "run_id": run_id,
                    "workflow_id": run_row["active_workflow_id"],
                    "payload": json.dumps(
                        {
                            "decision_id": decision_id,
                            "artifact_sha256": artifact_sha256,
                            "decision": decision.value,
                        }
                    ),
                    "created_at": created_at,
                },
            )
        return ApprovalRecord(
            decision_id=decision_id,
            run_id=run_id,
            decision=decision,
            artifact_sha256=artifact_sha256,
            actor_id=actor_id,
            created_at=created_at.isoformat(),
            delivered=False,
            plan_revision=run_row["plan_revision"],
        )

    async def mark_plan_approval_delivered(self, decision_id: str) -> None:
        async with self._engine.begin() as connection:
            decision = await connection.execute(
                text(
                    """
                    SELECT run_id, decision, plan_revision FROM plan_approval_decisions
                    WHERE decision_id = :decision_id
                    FOR UPDATE
                    """
                ),
                {"decision_id": decision_id},
            )
            row = decision.mappings().one_or_none()
            if row is None:
                return
            await connection.execute(
                text(
                    """
                    UPDATE plan_approval_decisions SET delivered_at = now()
                    WHERE decision_id = :decision_id AND delivered_at IS NULL
                    """
                ),
                {"decision_id": decision_id},
            )
            await connection.execute(
                text(
                    """
                    UPDATE temporal_outbox
                    SET delivered_at = now(), lease_until = NULL, last_error = NULL
                    WHERE decision_id = :decision_id
                    """
                ),
                {"decision_id": decision_id},
            )
            status = {
                "approve": PlanningRunStatus.IMPLEMENTING.value,
                "reject": PlanningRunStatus.REJECTED.value,
                # A revision decision preserves its immutable audit row, then
                # reopens the run for a replacement plan artifact.
                "request_revision": PlanningRunStatus.PLANNING.value,
            }[row["decision"]]
            await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET status = :status,
                        active_workflow_id = CASE WHEN :status = 'planning' THEN NULL ELSE active_workflow_id END,
                        plan_artifact_ref = CASE WHEN :status = 'planning' THEN NULL ELSE plan_artifact_ref END,
                        plan_artifact_sha256 = CASE WHEN :status = 'planning' THEN NULL ELSE plan_artifact_sha256 END,
                        planner_model = CASE WHEN :status = 'planning' THEN NULL ELSE planner_model END
                    WHERE run_id = :run_id AND plan_revision = :plan_revision
                    """
                ),
                {"status": status, "run_id": row["run_id"], "plan_revision": row["plan_revision"]},
            )

    async def claim_plan_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]:
        """Lease due decisions with SKIP LOCKED so API replicas cannot double-deliver."""

        if limit < 1:
            return []
        filter_sql = "AND decision_id = :decision_id" if decision_id else ""
        parameters: dict[str, object] = {"limit": limit, "lease_seconds": lease_seconds}
        if decision_id:
            parameters["decision_id"] = decision_id
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    f"""
                    SELECT decision_id, run_id, workflow_id, payload, attempt_count
                    FROM temporal_outbox
                    WHERE delivered_at IS NULL
                      AND next_attempt_at <= now()
                      AND (lease_until IS NULL OR lease_until <= now())
                      {filter_sql}
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                    """
                ),
                parameters,
            )
            rows = result.mappings().all()
            for row in rows:
                await connection.execute(
                    text(
                        """
                        UPDATE temporal_outbox
                        SET attempt_count = attempt_count + 1,
                            lease_until = now() + (:lease_seconds * interval '1 second')
                        WHERE decision_id = :decision_id
                        """
                    ),
                    {"decision_id": row["decision_id"], "lease_seconds": lease_seconds},
                )
        return [
            OutboxDelivery(
                decision_id=row["decision_id"],
                run_id=row["run_id"],
                workflow_id=row["workflow_id"],
                payload=dict(row["payload"]),
                attempt_count=int(row["attempt_count"]) + 1,
            )
            for row in rows
        ]

    async def release_plan_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None:
        """Release a failed lease with a bounded diagnostic and retry schedule."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE temporal_outbox
                    SET lease_until = NULL,
                        next_attempt_at = now() + (:retry_seconds * interval '1 second'),
                        last_error = :error
                    WHERE decision_id = :decision_id AND delivered_at IS NULL
                    """
                ),
                {"decision_id": decision_id, "retry_seconds": retry_seconds, "error": error[:1024]},
            )

    async def close(self) -> None:
        """Dispose the pool during application shutdown."""

        await self._engine.dispose()


def _approval_record(row: object) -> ApprovalRecord:
    """Build a typed decision record from a SQLAlchemy mapping row."""

    values = row  # SQLAlchemy RowMapping is intentionally structural here.
    return ApprovalRecord(
        decision_id=values["decision_id"],  # type: ignore[index]
        run_id=values["run_id"],  # type: ignore[index]
        decision=PlanApprovalDecision(values["decision"]),  # type: ignore[index]
        artifact_sha256=values["artifact_sha256"],  # type: ignore[index]
        actor_id=values["actor_id"],  # type: ignore[index]
        created_at=values["created_at"].isoformat(),  # type: ignore[index]
        delivered=values["delivered_at"] is not None,  # type: ignore[index]
        plan_revision=values["plan_revision"],  # type: ignore[index]
    )
