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
                           plan_artifact_ref, plan_artifact_sha256, planner_model
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
        )

    async def attach_generated_plan(
        self,
        run_id: str,
        plan_artifact: ArtifactReference,
        planner_model: str,
    ) -> PlanningRunRecord:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET status = 'awaiting_plan_approval',
                        plan_artifact_ref = :plan_artifact_ref,
                        plan_artifact_sha256 = :plan_artifact_sha256,
                        planner_model = :planner_model
                    WHERE run_id = :run_id
                      AND status = 'planning'
                    RETURNING run_id, status, source_artifact_ref, source_artifact_sha256,
                              target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                              plan_artifact_ref, plan_artifact_sha256, planner_model
                    """
                ),
                {
                    "run_id": run_id,
                    "plan_artifact_ref": plan_artifact.ref,
                    "plan_artifact_sha256": plan_artifact.sha256,
                    "planner_model": planner_model,
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
            existing = await connection.execute(
                text(
                    """
                    SELECT decision_id, run_id, decision, artifact_sha256, actor_id, created_at, delivered_at,
                           request_sha256
                    FROM plan_approval_decisions
                    WHERE run_id = :run_id AND idempotency_key = :idempotency_key
                    """
                ),
                {"run_id": run_id, "idempotency_key": idempotency_key},
            )
            existing_row = existing.mappings().one_or_none()
            if existing_row is not None:
                if existing_row["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with a different decision")
                return _approval_record(existing_row)

            run = await connection.execute(
                text(
                    """
                    SELECT status, plan_artifact_sha256
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
            if run_row["status"] != PlanningRunStatus.AWAITING_PLAN_APPROVAL.value:
                raise ApprovalConflictError("planning run is not awaiting plan approval")
            if run_row["plan_artifact_sha256"] != artifact_sha256:
                raise ApprovalConflictError("plan approval artifact digest is stale")

            decision_id = str(uuid.uuid4())
            created_at = datetime.now().astimezone()
            await connection.execute(
                text(
                    """
                    INSERT INTO plan_approval_decisions (
                        decision_id, run_id, decision, artifact_sha256, actor_id, comment,
                        idempotency_key, request_sha256, created_at
                    ) VALUES (
                        :decision_id, :run_id, :decision, :artifact_sha256, :actor_id, :comment,
                        :idempotency_key, :request_sha256, :created_at
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
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO temporal_outbox (decision_id, run_id, payload, created_at)
                    VALUES (:decision_id, :run_id, CAST(:payload AS jsonb), :created_at)
                    """
                ),
                {
                    "decision_id": decision_id,
                    "run_id": run_id,
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
        )

    async def mark_plan_approval_delivered(self, decision_id: str) -> None:
        async with self._engine.begin() as connection:
            decision = await connection.execute(
                text(
                    """
                    SELECT run_id, decision FROM plan_approval_decisions
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
                text("UPDATE temporal_outbox SET delivered_at = now() WHERE decision_id = :decision_id"),
                {"decision_id": decision_id},
            )
            status = {
                "approve": PlanningRunStatus.IMPLEMENTING.value,
                "reject": PlanningRunStatus.REJECTED.value,
                "request_revision": PlanningRunStatus.REVISION_REQUESTED.value,
            }[row["decision"]]
            await connection.execute(
                text("UPDATE supervisor_runs SET status = :status WHERE run_id = :run_id"),
                {"status": status, "run_id": row["run_id"]},
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
    )
