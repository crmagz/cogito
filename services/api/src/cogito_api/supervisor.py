"""Transactional persistence for the Cogito supervisor control plane."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .models import ArtifactReference, PlanConstraints, PlanningRunStatus


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

    async def close(self) -> None:
        """Dispose the pool during application shutdown."""

        await self._engine.dispose()
