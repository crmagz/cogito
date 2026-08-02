"""Transactional persistence for the Cogito supervisor control plane."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any, Mapping, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .models import (
    AgentRunStatus,
    ArtifactReference,
    ImplementationApprovalDecision,
    PlanApprovalDecision,
    PlanConstraints,
    PlanningRunStatus,
    RegistrationManifest,
    RegistrationReference,
    WorkbenchFeedbackIntent,
)
from .registry import manifest_sha256, registration_reference

_TERMINAL_AGENT_STATUSES = {
    AgentRunStatus.SUCCEEDED.value,
    AgentRunStatus.FAILED.value,
    AgentRunStatus.CANCELLED.value,
    AgentRunStatus.TIMED_OUT.value,
}


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
    implementation_artifact: ArtifactReference | None = None
    implementation_revision: int = 0
    project_id: str | None = None


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
class ImplementationApprovalRecord:
    """Immutable implementation decision and its Temporal delivery state."""

    decision_id: str
    run_id: str
    decision: ImplementationApprovalDecision
    artifact_sha256: str
    actor_id: str
    created_at: str
    delivered: bool
    implementation_revision: int


@dataclass(frozen=True)
class WorkbenchApprovalRecord:
    """Normalized immutable decision history for the scoped Workbench projection."""

    decision_id: str
    run_id: str
    gate: str
    decision: str
    artifact_sha256: str
    actor_id: str
    created_at: str
    delivered: bool


@dataclass(frozen=True)
class WorkbenchFeedbackRecord:
    """Immutable Workbench review context with no execution authority."""

    feedback_id: str
    run_id: str
    intent: WorkbenchFeedbackIntent
    artifact_sha256: str
    stage_id: str
    actor_id: str
    comment: str
    created_at: str


@dataclass(frozen=True)
class OutboxDelivery:
    """A short-lived lease over an immutable plan approval decision."""

    decision_id: str
    run_id: str
    workflow_id: str
    payload: dict[str, str]
    attempt_count: int


@dataclass(frozen=True)
class CoordinationEvent:
    """Immutable, non-secret notification snapshot owned by the Supervisor."""

    event_id: str
    run_id: str
    event_type: str
    occurred_at: str
    gate: str | None
    artifact_ref: str | None
    artifact_sha256: str | None
    decision: str | None
    lifecycle_status: str | None
    payload: dict[str, object]


@dataclass(frozen=True)
class NotificationDelivery:
    """A leased provider-neutral delivery of one immutable coordination event."""

    event: CoordinationEvent
    attempt_count: int


@dataclass(frozen=True)
class AgentRunRecord:
    run_id: str
    root_run_id: str
    parent_run_id: str | None
    agent_name: str
    status: AgentRunStatus
    trace_id: str
    created_at: str
    updated_at: str
    last_heartbeat_at: str | None = None
    worker_id: str | None = None
    result_artifact_uri: str | None = None
    error_summary: str | None = None


class ApprovalConflictError(Exception):
    """Raised when a decision cannot safely apply to the current run state."""


class RegistryConflictError(Exception):
    """Raised when a registry release or pinned run resolution is unsafe."""


def _matches_registration_resolution(
    resolution: Mapping[str, Any], expected: RegistrationReference, policy_revision: str
) -> bool:
    """Return whether durable resolution evidence matches the requested immutable pin."""

    return (
        resolution["registration_id"] == expected.registration_id
        and resolution["registration_version"] == expected.version
        and resolution["manifest_sha256"] == expected.manifest_sha256
        and resolution["component_id"] == expected.component_id
        and resolution["component_version"] == expected.component_version
        and resolution["policy_revision"] == policy_revision
    )


def _planning_run_record(row: Mapping[str, Any]) -> PlanningRunRecord:
    """Materialize a planning-run projection returned by PostgreSQL."""

    return PlanningRunRecord(
        run_id=row["run_id"],
        status=PlanningRunStatus(row["status"]),
        source_artifact=ArtifactReference(ref=row["source_artifact_ref"], sha256=row["source_artifact_sha256"]),
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
        implementation_artifact=(
            ArtifactReference(
                ref=row["implementation_artifact_ref"], sha256=row["implementation_artifact_sha256"]
            )
            if row["implementation_artifact_ref"] is not None
            else None
        ),
        implementation_revision=row["implementation_revision"],
        project_id=row["project_id"],
    )


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

    async def record_implementation_artifact(
        self, run_id: str, artifact: ArtifactReference
    ) -> None: ...

    async def record_implementation_approval(
        self,
        run_id: str,
        artifact_sha256: str,
        decision: ImplementationApprovalDecision,
        actor_id: str,
        comment: str | None,
        idempotency_key: str,
        request_sha256: str,
    ) -> ImplementationApprovalRecord: ...

    async def mark_implementation_approval_delivered(self, decision_id: str) -> None: ...

    async def claim_implementation_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]: ...

    async def release_implementation_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None: ...

    async def create_agent_run(self, record: AgentRunRecord) -> None: ...

    async def get_agent_run(self, run_id: str) -> AgentRunRecord | None: ...

    async def bootstrap_registry(
        self,
        manifests: list[RegistrationManifest],
        policy_revision: str,
        assignments: dict[str, str],
    ) -> None: ...

    async def resolve_run_registration(
        self,
        run_id: str,
        role: str,
        policy_revision: str,
        manifest: RegistrationManifest,
    ) -> RegistrationReference: ...

    async def list_coordination_events(self, run_id: str, *, limit: int = 100) -> list[tuple[CoordinationEvent, bool, int, str | None]]: ...

    async def list_workbench_approvals(self, run_id: str, *, limit: int = 100) -> list[WorkbenchApprovalRecord]: ...

    async def record_workbench_feedback(
        self,
        *,
        run_id: str,
        intent: WorkbenchFeedbackIntent,
        artifact_sha256: str,
        stage_id: str,
        actor_id: str,
        comment: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> WorkbenchFeedbackRecord: ...

    async def list_workbench_feedback(self, run_id: str, *, limit: int = 100) -> list[WorkbenchFeedbackRecord]: ...

    async def list_coordination_runs(self, *, limit: int = 50) -> list[PlanningRunRecord]: ...

    async def list_reconcilable_runs(self, *, limit: int = 100) -> list[PlanningRunRecord]: ...

    async def reconcile_terminal_workflow(self, *, run_id: str, workflow_id: str, outcome: str) -> bool: ...

    async def list_workbench_runs(self, *, project_ids: frozenset[str], limit: int = 50) -> list[PlanningRunRecord]: ...

    async def claim_notification_deliveries(self, *, limit: int, lease_seconds: int) -> list[NotificationDelivery]: ...

    async def mark_notification_delivered(self, event_id: str) -> None: ...

    async def release_notification_delivery(self, event_id: str, *, retry_seconds: int, error: str) -> None: ...


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
                        target_repos, spec_set, constraints, priority, submitted_at, submitted_by, project_id
                    ) VALUES (
                        :run_id, :status, :source_artifact_ref, :source_artifact_sha256,
                        CAST(:target_repos AS jsonb), :spec_set, CAST(:constraints AS jsonb),
                        :priority, :submitted_at, :submitted_by, :project_id
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
                    "project_id": record.project_id,
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
            await self._append_coordination_event(
                connection,
                run_id=record.run_id,
                event_type="specification_recorded",
                artifact=record.source_artifact,
            )
            await self._append_coordination_event(
                connection,
                run_id=record.run_id,
                event_type="planning_started",
                artifact=record.source_artifact,
            )

    async def create_agent_run(self, record: AgentRunRecord) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_runs (
                        run_id, root_run_id, parent_run_id, agent_name, status, trace_id, created_at, updated_at
                    ) VALUES (
                        :run_id, :root_run_id, :parent_run_id, :agent_name, :status, :trace_id,
                        :created_at, :updated_at
                    )
                    """
                ),
                {
                    "run_id": record.run_id,
                    "root_run_id": record.root_run_id,
                    "parent_run_id": record.parent_run_id,
                    "agent_name": record.agent_name,
                    "status": record.status.value,
                    "trace_id": record.trace_id,
                    "created_at": datetime.fromisoformat(record.created_at),
                    "updated_at": datetime.fromisoformat(record.updated_at),
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_run_events (event_id, run_id, event_type, from_status, to_status, occurred_at, metadata)
                    VALUES (:event_id, :run_id, 'run_created', NULL, :status, :occurred_at, CAST('{}' AS jsonb))
                    """
                ),
                {
                    "event_id": str(uuid.uuid4()),
                    "run_id": record.run_id,
                    "status": record.status.value,
                    "occurred_at": datetime.fromisoformat(record.created_at),
                },
            )

    async def get_agent_run(self, run_id: str) -> AgentRunRecord | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, root_run_id, parent_run_id, agent_name, status, trace_id, created_at, updated_at,
                           last_heartbeat_at, worker_id, result_artifact_uri, error_summary
                    FROM agent_runs WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
            row = result.mappings().one_or_none()
        return _agent_run_record(row) if row is not None else None

    async def bootstrap_registry(
        self,
        manifests: list[RegistrationManifest],
        policy_revision: str,
        assignments: dict[str, str],
    ) -> None:
        """Persist immutable releases and one policy revision without rewriting either."""

        async with self._engine.begin() as connection:
            for manifest in manifests:
                manifest_json = manifest.model_dump(mode="json")
                digest = manifest_sha256(manifest)
                await connection.execute(
                    text(
                        """
                        INSERT INTO registry_registrations (
                            registration_id, version, kind, component_id, component_version, lifecycle, maturity,
                            execution_class, owner, input_schema_id, input_schema_version, output_schema_id,
                            output_schema_version, manifest, manifest_sha256, created_at
                        ) VALUES (
                            :registration_id, :version, :kind, :component_id, :component_version, :lifecycle,
                            :maturity, :execution_class, :owner, :input_schema_id, :input_schema_version,
                            :output_schema_id, :output_schema_version, CAST(:manifest AS jsonb), :manifest_sha256,
                            now()
                        ) ON CONFLICT (registration_id, version) DO NOTHING
                        """
                    ),
                    {
                        "registration_id": manifest.registration_id,
                        "version": manifest.version,
                        "kind": manifest.kind.value,
                        "component_id": manifest.component_id,
                        "component_version": manifest.component_version,
                        "lifecycle": manifest.lifecycle.value,
                        "maturity": manifest.maturity.value,
                        "execution_class": manifest.execution_class.value,
                        "owner": manifest.owner,
                        "input_schema_id": manifest.input_schema.schema_id,
                        "input_schema_version": manifest.input_schema.version,
                        "output_schema_id": manifest.output_schema.schema_id,
                        "output_schema_version": manifest.output_schema.version,
                        "manifest": json.dumps(manifest_json, sort_keys=True, separators=(",", ":")),
                        "manifest_sha256": digest,
                    },
                )
                result = await connection.execute(
                    text(
                        """
                        SELECT manifest_sha256 FROM registry_registrations
                        WHERE registration_id = :registration_id AND version = :version
                        """
                    ),
                    {"registration_id": manifest.registration_id, "version": manifest.version},
                )
                existing = result.mappings().one()
                if existing["manifest_sha256"] != digest:
                    raise RegistryConflictError("registration version already exists with different manifest content")
            for manifest in manifests:
                for grant in manifest.grants:
                    await connection.execute(
                        text(
                            """
                            INSERT INTO registry_grants (
                                agent_registration_id, agent_version, tool_registration_id, tool_version, scope
                            ) VALUES (:agent_id, :agent_version, :tool_id, :tool_version, :scope)
                            ON CONFLICT DO NOTHING
                            """
                        ),
                        {
                            "agent_id": manifest.registration_id,
                            "agent_version": manifest.version,
                            "tool_id": grant.tool_id,
                            "tool_version": grant.tool_version,
                            "scope": grant.scope,
                        },
                    )
            await connection.execute(
                text(
                    """
                    INSERT INTO registry_policy_revisions (policy_revision, assignments, created_at)
                    VALUES (:policy_revision, CAST(:assignments AS jsonb), now())
                    ON CONFLICT (policy_revision) DO NOTHING
                    """
                ),
                {"policy_revision": policy_revision, "assignments": json.dumps(assignments, sort_keys=True)},
            )
            policy = await connection.execute(
                text("SELECT assignments FROM registry_policy_revisions WHERE policy_revision = :policy_revision"),
                {"policy_revision": policy_revision},
            )
            if dict(policy.mappings().one()["assignments"]) != assignments:
                raise RegistryConflictError("policy revision already exists with different assignments")

    async def resolve_run_registration(
        self,
        run_id: str,
        role: str,
        policy_revision: str,
        manifest: RegistrationManifest,
    ) -> RegistrationReference:
        """Pin an active policy-selected release once, preserving retries and in-flight runs."""

        expected = registration_reference(role, manifest)
        async with self._engine.begin() as connection:
            existing = await connection.execute(
                text(
                    """
                    SELECT registration_id, registration_version, manifest_sha256, component_id, component_version,
                           policy_revision
                    FROM run_registration_resolutions WHERE run_id = :run_id AND role = :role FOR UPDATE
                    """
                ),
                {"run_id": run_id, "role": role},
            )
            current = existing.mappings().one_or_none()
            if current is not None:
                if not _matches_registration_resolution(current, expected, policy_revision):
                    raise RegistryConflictError("run role is already pinned to a different registration release")
                return expected
            policy = await connection.execute(
                text("SELECT assignments FROM registry_policy_revisions WHERE policy_revision = :policy_revision"),
                {"policy_revision": policy_revision},
            )
            policy_row = policy.mappings().one_or_none()
            if policy_row is None:
                raise RegistryConflictError("registry policy revision is not available")
            assignment = dict(policy_row["assignments"]).get(role)
            if assignment != f"{manifest.registration_id}@{manifest.version}":
                raise RegistryConflictError("registry policy does not select the requested registration release")
            registration = await connection.execute(
                text(
                    """
                    SELECT lifecycle, manifest_sha256, component_id, component_version
                    FROM registry_registrations
                    WHERE registration_id = :registration_id AND version = :version
                    FOR UPDATE
                    """
                ),
                {"registration_id": manifest.registration_id, "version": manifest.version},
            )
            row = registration.mappings().one_or_none()
            if row is None or row["lifecycle"] != "active":
                raise RegistryConflictError("registration release is not active")
            if (
                row["manifest_sha256"] != expected.manifest_sha256
                or row["component_id"] != expected.component_id
                or row["component_version"] != expected.component_version
            ):
                raise RegistryConflictError("registration release does not match its declared manifest")
            inserted = await connection.execute(
                text(
                    """
                    INSERT INTO run_registration_resolutions (
                        run_id, role, registration_id, registration_version, manifest_sha256,
                        component_id, component_version, policy_revision, created_at
                    ) VALUES (
                        :run_id, :role, :registration_id, :registration_version, :manifest_sha256,
                        :component_id, :component_version, :policy_revision, now()
                    ) ON CONFLICT (run_id, role) DO NOTHING
                    RETURNING run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "role": role,
                    "registration_id": expected.registration_id,
                    "registration_version": expected.version,
                    "manifest_sha256": expected.manifest_sha256,
                    "component_id": expected.component_id,
                    "component_version": expected.component_version,
                    "policy_revision": policy_revision,
                },
            )
            if inserted.mappings().one_or_none() is None:
                # `FOR UPDATE` cannot lock a missing row. A concurrent first
                # resolver may have committed the same immutable pin while
                # this transaction was validating it, so converge on that row.
                persisted = await connection.execute(
                    text(
                        """
                        SELECT registration_id, registration_version, manifest_sha256, component_id, component_version,
                               policy_revision
                        FROM run_registration_resolutions WHERE run_id = :run_id AND role = :role
                        """
                    ),
                    {"run_id": run_id, "role": role},
                )
                current = persisted.mappings().one_or_none()
                if current is None or not _matches_registration_resolution(current, expected, policy_revision):
                    raise RegistryConflictError("run role is already pinned to a different registration release")
        return expected

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                           target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                           plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id
                    FROM supervisor_runs
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
            row = result.mappings().one_or_none()
        if row is None:
            return None
        return _planning_run_record(row)

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
                              plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                              implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id
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
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="plan_approval_requested",
                gate="plan",
                artifact=plan_artifact,
            )
        return _planning_run_record(row)

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
                    SELECT status, plan_artifact_ref, plan_artifact_sha256, active_workflow_id, plan_revision
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
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="plan_approval_recorded",
                gate="plan",
                artifact=ArtifactReference(
                    ref=run_row["plan_artifact_ref"], sha256=artifact_sha256
                ),
                decision=decision.value,
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
            clear_current_plan = row["decision"] == "request_revision"
            await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET status = :status,
                        active_workflow_id = CASE WHEN :clear_current_plan THEN NULL ELSE active_workflow_id END,
                        plan_artifact_ref = CASE WHEN :clear_current_plan THEN NULL ELSE plan_artifact_ref END,
                        plan_artifact_sha256 = CASE WHEN :clear_current_plan THEN NULL ELSE plan_artifact_sha256 END,
                        planner_model = CASE WHEN :clear_current_plan THEN NULL ELSE planner_model END
                    WHERE run_id = :run_id AND plan_revision = :plan_revision
                    """
                ),
                {
                    "status": status,
                    "clear_current_plan": clear_current_plan,
                    "run_id": row["run_id"],
                    "plan_revision": row["plan_revision"],
                },
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

    async def record_implementation_artifact(self, run_id: str, artifact: ArtifactReference) -> None:
        """Publish one immutable converged implementation artifact for approval."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
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
                {"run_id": run_id, "ref": artifact.ref, "sha256": artifact.sha256},
            )
            row = result.mappings().one_or_none()
            if row is None:
                # Retries may report the same immutable artifact after the
                # first status activity committed it. A different artifact is
                # never allowed to overwrite a live approval gate.
                current = await connection.execute(
                    text(
                        """
                        SELECT status, implementation_artifact_ref, implementation_artifact_sha256
                        FROM supervisor_runs WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": run_id},
                )
                existing = current.mappings().one_or_none()
                if (
                    existing is None
                    or existing["status"] != PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL.value
                    or existing["implementation_artifact_ref"] != artifact.ref
                    or existing["implementation_artifact_sha256"] != artifact.sha256
                ):
                    raise ApprovalConflictError("implementation artifact cannot be registered for this run")
                return
            await connection.execute(
                text(
                    """
                    INSERT INTO supervisor_artifacts (run_id, artifact_type, ref, sha256, created_at)
                    VALUES (:run_id, 'implementation_review', :ref, :sha256, now())
                    ON CONFLICT (run_id, artifact_type, ref) DO NOTHING
                    """
                ),
                {"run_id": run_id, "ref": artifact.ref, "sha256": artifact.sha256},
            )
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="implementation_approval_requested",
                gate="implementation",
                artifact=artifact,
            )

    async def record_implementation_approval(
        self,
        run_id: str,
        artifact_sha256: str,
        decision: ImplementationApprovalDecision,
        actor_id: str,
        comment: str | None,
        idempotency_key: str,
        request_sha256: str,
    ) -> ImplementationApprovalRecord:
        """Persist a digest-bound implementation decision before Temporal delivery."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT status, implementation_artifact_ref, implementation_artifact_sha256,
                           active_workflow_id, implementation_revision
                    FROM supervisor_runs WHERE run_id = :run_id FOR UPDATE
                    """
                ),
                {"run_id": run_id},
            )
            run = result.mappings().one_or_none()
            if run is None:
                raise ApprovalConflictError("planning run does not exist")
            existing = await connection.execute(
                text(
                    """
                    SELECT decision_id, run_id, decision, artifact_sha256, actor_id, created_at, delivered_at,
                           request_sha256, implementation_revision
                    FROM implementation_approval_decisions
                    WHERE run_id = :run_id AND implementation_revision = :implementation_revision
                      AND idempotency_key = :idempotency_key
                    """
                ),
                {
                    "run_id": run_id,
                    "implementation_revision": run["implementation_revision"],
                    "idempotency_key": idempotency_key,
                },
            )
            existing_row = existing.mappings().one_or_none()
            if existing_row is not None:
                if existing_row["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with a different decision")
                return _implementation_approval_record(existing_row)
            if run["status"] != PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL.value:
                raise ApprovalConflictError("planning run is not awaiting implementation approval")
            if run["implementation_artifact_sha256"] != artifact_sha256:
                raise ApprovalConflictError("implementation approval artifact digest is stale")
            if not run["active_workflow_id"]:
                raise ApprovalConflictError("planning workflow is not available for approval")
            decision_id = str(uuid.uuid4())
            created_at = datetime.now().astimezone()
            await connection.execute(
                text(
                    """
                    INSERT INTO implementation_approval_decisions (
                        decision_id, run_id, decision, artifact_sha256, actor_id, comment,
                        idempotency_key, request_sha256, created_at, implementation_revision
                    ) VALUES (
                        :decision_id, :run_id, :decision, :artifact_sha256, :actor_id, :comment,
                        :idempotency_key, :request_sha256, :created_at, :implementation_revision
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
                    "implementation_revision": run["implementation_revision"],
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO implementation_temporal_outbox (decision_id, run_id, workflow_id, payload, created_at)
                    VALUES (:decision_id, :run_id, :workflow_id, CAST(:payload AS jsonb), :created_at)
                    """
                ),
                {
                    "decision_id": decision_id,
                    "run_id": run_id,
                    "workflow_id": run["active_workflow_id"],
                    "payload": json.dumps(
                        {"decision_id": decision_id, "artifact_sha256": artifact_sha256, "decision": decision.value}
                    ),
                    "created_at": created_at,
                },
            )
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="implementation_approval_recorded",
                gate="implementation",
                artifact=ArtifactReference(
                    ref=run["implementation_artifact_ref"], sha256=artifact_sha256
                ),
                decision=decision.value,
            )
        return ImplementationApprovalRecord(
            decision_id=decision_id,
            run_id=run_id,
            decision=decision,
            artifact_sha256=artifact_sha256,
            actor_id=actor_id,
            created_at=created_at.isoformat(),
            delivered=False,
            implementation_revision=run["implementation_revision"],
        )

    async def mark_implementation_approval_delivered(self, decision_id: str) -> None:
        """Acknowledge delivery and advance only the matching implementation revision."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, decision, implementation_revision FROM implementation_approval_decisions
                    WHERE decision_id = :decision_id FOR UPDATE
                    """
                ),
                {"decision_id": decision_id},
            )
            row = result.mappings().one_or_none()
            if row is None:
                return
            await connection.execute(
                text("UPDATE implementation_approval_decisions SET delivered_at = now() WHERE decision_id = :decision_id AND delivered_at IS NULL"),
                {"decision_id": decision_id},
            )
            await connection.execute(
                text("UPDATE implementation_temporal_outbox SET delivered_at = now(), lease_until = NULL, last_error = NULL WHERE decision_id = :decision_id"),
                {"decision_id": decision_id},
            )
            status = {
                "approve": PlanningRunStatus.FINALIZING.value,
                "reject": PlanningRunStatus.REJECTED.value,
                "request_revision": PlanningRunStatus.IMPLEMENTING.value,
            }[row["decision"]]
            clear_artifact = row["decision"] == "request_revision"
            await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET status = :status,
                        implementation_artifact_ref = CASE WHEN :clear_artifact THEN NULL ELSE implementation_artifact_ref END,
                        implementation_artifact_sha256 = CASE WHEN :clear_artifact THEN NULL ELSE implementation_artifact_sha256 END
                    WHERE run_id = :run_id AND implementation_revision = :implementation_revision
                    """
                ),
                {
                    "run_id": row["run_id"],
                    "status": status,
                    "clear_artifact": clear_artifact,
                    "implementation_revision": row["implementation_revision"],
                },
            )

    async def claim_implementation_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]:
        """Lease due implementation decisions without racing API replicas."""

        return await self._claim_deliveries(
            "implementation_temporal_outbox", limit=limit, lease_seconds=lease_seconds, decision_id=decision_id
        )

    async def release_implementation_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None:
        """Release a failed implementation-delivery lease."""

        await self._release_delivery("implementation_temporal_outbox", decision_id, retry_seconds, error)

    async def _claim_deliveries(
        self, table: str, *, limit: int, lease_seconds: int, decision_id: str | None
    ) -> list[OutboxDelivery]:
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
                    FROM {table}
                    WHERE delivered_at IS NULL AND next_attempt_at <= now()
                      AND (lease_until IS NULL OR lease_until <= now()) {filter_sql}
                    ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT :limit
                    """
                ),
                parameters,
            )
            rows = result.mappings().all()
            for row in rows:
                await connection.execute(
                    text(
                        f"""
                        UPDATE {table}
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

    async def _release_delivery(self, table: str, decision_id: str, retry_seconds: int, error: str) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {table}
                    SET lease_until = NULL, next_attempt_at = now() + (:retry_seconds * interval '1 second'),
                        last_error = :error
                    WHERE decision_id = :decision_id AND delivered_at IS NULL
                    """
                ),
                {"decision_id": decision_id, "retry_seconds": retry_seconds, "error": error[:1024]},
            )

    async def _append_coordination_event(
        self,
        connection: Any,
        *,
        run_id: str,
        event_type: str,
        gate: str | None = None,
        artifact: ArtifactReference | None = None,
        decision: str | None = None,
        lifecycle_status: str | None = None,
        stage_id: str | None = None,
    ) -> None:
        """Append one safe event and its generic notification delivery in the current transaction."""

        artifact_payload = (
            {"ref": artifact.ref, "sha256": artifact.sha256} if artifact is not None and artifact.ref else None
        )
        payload: dict[str, object] = {
            "schema_version": "1.0",
            "event_type": event_type,
            "run_id": run_id,
            "gate": gate,
            "artifact": artifact_payload,
            "decision": decision,
            "lifecycle_status": lifecycle_status,
            "stage_id": stage_id,
            "read_url": f"/api/v1/planning-runs/{run_id}/coordination",
            "action_url": f"/api/v1/coordination/runs/{run_id}/actions/{gate}" if gate else None,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        dedupe_key = sha256(canonical.encode()).hexdigest()
        event_id = str(uuid.uuid4())
        created_at = datetime.now().astimezone()
        result = await connection.execute(
            text(
                """
                INSERT INTO coordination_events (event_id, run_id, event_type, dedupe_key, payload, created_at)
                VALUES (:event_id, :run_id, :event_type, :dedupe_key, CAST(:payload AS jsonb), :created_at)
                ON CONFLICT (dedupe_key) DO NOTHING
                RETURNING event_id
                """
            ),
            {
                "event_id": event_id,
                "run_id": run_id,
                "event_type": event_type,
                "dedupe_key": dedupe_key,
                "payload": canonical,
                "created_at": created_at,
            },
        )
        if result.mappings().one_or_none() is None:
            return
        await connection.execute(
            text(
                """
                INSERT INTO notification_outbox (event_id, sink_id, created_at)
                VALUES (:event_id, 'webhook', :created_at)
                """
            ),
            {"event_id": event_id, "created_at": created_at},
        )

    async def list_coordination_events(
        self, run_id: str, *, limit: int = 100
    ) -> list[tuple[CoordinationEvent, bool, int, str | None]]:
        """Read bounded, newest-first event and reconciliation snapshots for one run."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT e.event_id, e.run_id, e.event_type, e.payload, e.created_at,
                           o.delivered_at, o.attempt_count, o.last_error
                    FROM coordination_events AS e
                    LEFT JOIN notification_outbox AS o USING (event_id)
                    WHERE e.run_id = :run_id
                    ORDER BY e.created_at DESC
                    LIMIT :limit
                    """
                ),
                {"run_id": run_id, "limit": max(1, min(limit, 100))},
            )
            rows = result.mappings().all()
        return [
            (
                _coordination_event(row),
                row["delivered_at"] is not None,
                int(row["attempt_count"] or 0),
                row["last_error"],
            )
            for row in rows
        ]

    async def list_workbench_approvals(self, run_id: str, *, limit: int = 100) -> list[WorkbenchApprovalRecord]:
        """Return one bounded newest-first view across both approval gates."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT decision_id, run_id, 'plan' AS gate, decision, artifact_sha256, actor_id, created_at,
                           delivered_at
                    FROM plan_approval_decisions
                    WHERE run_id = :run_id
                    UNION ALL
                    SELECT decision_id, run_id, 'implementation' AS gate, decision, artifact_sha256, actor_id,
                           created_at, delivered_at
                    FROM implementation_approval_decisions
                    WHERE run_id = :run_id
                    ORDER BY created_at DESC, decision_id DESC
                    LIMIT :limit
                    """
                ),
                {"run_id": run_id, "limit": max(1, min(limit, 100))},
            )
            rows = result.mappings().all()
        return [
            WorkbenchApprovalRecord(
                decision_id=row["decision_id"],
                run_id=row["run_id"],
                gate=row["gate"],
                decision=row["decision"],
                artifact_sha256=row["artifact_sha256"],
                actor_id=row["actor_id"],
                created_at=row["created_at"].isoformat(),
                delivered=row["delivered_at"] is not None,
            )
            for row in rows
        ]

    async def record_workbench_feedback(
        self,
        *,
        run_id: str,
        intent: WorkbenchFeedbackIntent,
        artifact_sha256: str,
        stage_id: str,
        actor_id: str,
        comment: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> WorkbenchFeedbackRecord:
        """Persist one non-executable, digest-bound Workbench review context record."""

        async with self._engine.begin() as connection:
            run_result = await connection.execute(
                text(
                    """
                    SELECT source_artifact_ref, source_artifact_sha256, plan_artifact_ref, plan_artifact_sha256,
                           implementation_artifact_ref, implementation_artifact_sha256
                    FROM supervisor_runs WHERE run_id = :run_id FOR UPDATE
                    """
                ),
                {"run_id": run_id},
            )
            run = run_result.mappings().one_or_none()
            if run is None:
                raise ApprovalConflictError("planning run does not exist")
            existing_result = await connection.execute(
                text("SELECT * FROM workbench_feedback WHERE run_id = :run_id AND idempotency_key = :idempotency_key"),
                {"run_id": run_id, "idempotency_key": idempotency_key},
            )
            existing = existing_result.mappings().one_or_none()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with different feedback")
                return _workbench_feedback_record(existing)
            expected_artifact = {
                "specification": (run["source_artifact_ref"], run["source_artifact_sha256"]),
                "planning": (run["plan_artifact_ref"], run["plan_artifact_sha256"]),
                "plan_approval": (run["plan_artifact_ref"], run["plan_artifact_sha256"]),
                "implementation": (run["implementation_artifact_ref"], run["implementation_artifact_sha256"]),
                "implementation_approval": (run["implementation_artifact_ref"], run["implementation_artifact_sha256"]),
            }.get(stage_id)
            if expected_artifact is None or expected_artifact[0] is None or expected_artifact[1] != artifact_sha256:
                raise ApprovalConflictError("feedback artifact is not authoritative for this stage")
            artifact_ref = expected_artifact[0]
            feedback_id = str(uuid.uuid4())
            created_at = datetime.now().astimezone()
            await connection.execute(
                text(
                    """
                    INSERT INTO workbench_feedback (
                        feedback_id, run_id, intent, artifact_sha256, stage_id, actor_id, comment,
                        idempotency_key, request_sha256, created_at
                    ) VALUES (
                        :feedback_id, :run_id, :intent, :artifact_sha256, :stage_id, :actor_id, :comment,
                        :idempotency_key, :request_sha256, :created_at
                    )
                    """
                ),
                {
                    "feedback_id": feedback_id,
                    "run_id": run_id,
                    "intent": intent.value,
                    "artifact_sha256": artifact_sha256,
                    "stage_id": stage_id,
                    "actor_id": actor_id,
                    "comment": comment.strip(),
                    "idempotency_key": idempotency_key,
                    "request_sha256": request_sha256,
                    "created_at": created_at,
                },
            )
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="workbench_feedback_recorded",
                artifact=ArtifactReference(ref=artifact_ref, sha256=artifact_sha256),
                stage_id=stage_id,
            )
        return WorkbenchFeedbackRecord(
            feedback_id=feedback_id,
            run_id=run_id,
            intent=intent,
            artifact_sha256=artifact_sha256,
            stage_id=stage_id,
            actor_id=actor_id,
            comment=comment.strip(),
            created_at=created_at.isoformat(),
        )

    async def list_workbench_feedback(self, run_id: str, *, limit: int = 100) -> list[WorkbenchFeedbackRecord]:
        """List bounded newest-first immutable Workbench review context records."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT feedback_id, run_id, intent, artifact_sha256, stage_id, actor_id, comment, created_at
                    FROM workbench_feedback WHERE run_id = :run_id
                    ORDER BY created_at DESC, feedback_id DESC LIMIT :limit
                    """
                ),
                {"run_id": run_id, "limit": max(1, min(limit, 100))},
            )
            rows = result.mappings().all()
        return [_workbench_feedback_record(row) for row in rows]

    async def list_coordination_runs(self, *, limit: int = 50) -> list[PlanningRunRecord]:
        """List bounded newest-first planning runs for authenticated coordination clients."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                           target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                           plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id
                    FROM supervisor_runs
                    ORDER BY submitted_at DESC LIMIT :limit
                    """
                ),
                {"limit": max(1, min(limit, 100))},
            )
            rows = result.mappings().all()
        return [_planning_run_record(row) for row in rows]

    async def list_reconcilable_runs(self, *, limit: int = 100) -> list[PlanningRunRecord]:
        """List live workflow projections that can safely be checked for recovery."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                           target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                           plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id
                    FROM supervisor_runs
                    WHERE status IN ('implementing', 'finalizing') AND active_workflow_id IS NOT NULL
                    ORDER BY submitted_at
                    LIMIT :limit
                    """
                ),
                {"limit": max(1, min(limit, 100))},
            )
            rows = result.mappings().all()
        return [_planning_run_record(row) for row in rows]

    async def reconcile_terminal_workflow(self, *, run_id: str, workflow_id: str, outcome: str) -> bool:
        """Atomically repair one stale projection from an exact Temporal result.

        The caller has already established that ``workflow_id`` closed with a
        recognized Cogito result.  Re-checking that workflow identity and both
        current statuses under a lock prevents an old workflow revision from
        overwriting a newer gate or a competing worker report.
        """

        target = {
            "completed": (PlanningRunStatus.COMPLETED.value, AgentRunStatus.SUCCEEDED.value),
            "failed": (PlanningRunStatus.PLANNING_FAILED.value, AgentRunStatus.FAILED.value),
            "stopped_with_backup": (PlanningRunStatus.PLANNING_FAILED.value, AgentRunStatus.TIMED_OUT.value),
        }.get(outcome)
        if target is None:
            return False
        planning_status, agent_status = target
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT s.status AS planning_status, a.status AS agent_status
                    FROM supervisor_runs AS s
                    JOIN agent_runs AS a USING (run_id)
                    WHERE s.run_id = :run_id AND s.active_workflow_id = :workflow_id
                    FOR UPDATE OF s, a
                    """
                ),
                {"run_id": run_id, "workflow_id": workflow_id},
            )
            row = result.mappings().one_or_none()
            if row is None or row["planning_status"] not in {
                PlanningRunStatus.IMPLEMENTING.value,
                PlanningRunStatus.FINALIZING.value,
            }:
                return False
            current_agent_status = row["agent_status"]
            if current_agent_status in _TERMINAL_AGENT_STATUSES and current_agent_status != agent_status:
                return False
            now = datetime.now().astimezone()
            await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs SET status = :planning_status
                    WHERE run_id = :run_id AND active_workflow_id = :workflow_id
                    """
                ),
                {"run_id": run_id, "workflow_id": workflow_id, "planning_status": planning_status},
            )
            if current_agent_status != agent_status:
                await connection.execute(
                    text(
                        """
                        UPDATE agent_runs
                        SET status = :agent_status, updated_at = :now, last_heartbeat_at = :now,
                            completed_at = :now
                        WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": run_id, "agent_status": agent_status, "now": now},
                )
                await connection.execute(
                    text(
                        """
                        INSERT INTO agent_run_events (event_id, run_id, event_type, from_status, to_status, occurred_at, metadata)
                        VALUES (:event_id, :run_id, 'workflow_reconciled', :from_status, :to_status, :occurred_at,
                                CAST(:metadata AS jsonb))
                        """
                    ),
                    {
                        "event_id": str(uuid.uuid4()),
                        "run_id": run_id,
                        "from_status": current_agent_status,
                        "to_status": agent_status,
                        "occurred_at": now,
                        "metadata": json.dumps({"outcome": outcome}),
                    },
                )
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="workflow_reconciled",
                lifecycle_status=agent_status,
            )
        return True

    async def list_workbench_runs(self, *, project_ids: frozenset[str], limit: int = 50) -> list[PlanningRunRecord]:
        """List only runs whose persisted project scope is authorized."""

        if not project_ids:
            return []
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                           target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                           plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id
                    FROM supervisor_runs
                    WHERE project_id = ANY(CAST(:project_ids AS text[]))
                    ORDER BY submitted_at DESC LIMIT :limit
                    """
                ),
                {"project_ids": list(project_ids), "limit": max(1, min(limit, 100))},
            )
            rows = result.mappings().all()
        return [_planning_run_record(row) for row in rows]

    async def claim_notification_deliveries(self, *, limit: int, lease_seconds: int) -> list[NotificationDelivery]:
        """Lease due webhook events without racing API replicas."""

        if limit < 1:
            return []
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT e.event_id, e.run_id, e.event_type, e.payload, e.created_at, o.attempt_count
                    FROM notification_outbox AS o
                    JOIN coordination_events AS e ON e.event_id = o.event_id
                    WHERE o.sink_id = 'webhook' AND o.delivered_at IS NULL AND o.next_attempt_at <= now()
                      AND (o.lease_until IS NULL OR o.lease_until <= now())
                    ORDER BY o.created_at FOR UPDATE OF o SKIP LOCKED LIMIT :limit
                    """
                ),
                {"limit": limit},
            )
            rows = result.mappings().all()
            for row in rows:
                await connection.execute(
                    text(
                        """
                        UPDATE notification_outbox
                        SET attempt_count = attempt_count + 1,
                            lease_until = now() + (:lease_seconds * interval '1 second')
                        WHERE event_id = :event_id AND sink_id = 'webhook'
                        """
                    ),
                    {"event_id": row["event_id"], "lease_seconds": lease_seconds},
                )
        return [
            NotificationDelivery(event=_coordination_event(row), attempt_count=int(row["attempt_count"]) + 1)
            for row in rows
        ]

    async def mark_notification_delivered(self, event_id: str) -> None:
        """Persist one successful webhook acknowledgement without touching workflow state."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE notification_outbox
                    SET delivered_at = now(), lease_until = NULL, last_error = NULL
                    WHERE event_id = :event_id AND sink_id = 'webhook' AND delivered_at IS NULL
                    """
                ),
                {"event_id": event_id},
            )

    async def release_notification_delivery(self, event_id: str, *, retry_seconds: int, error: str) -> None:
        """Release a failed notification lease with a non-secret retry category."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE notification_outbox
                    SET lease_until = NULL,
                        next_attempt_at = now() + (:retry_seconds * interval '1 second'),
                        last_error = :error
                    WHERE event_id = :event_id AND sink_id = 'webhook' AND delivered_at IS NULL
                    """
                ),
                {"event_id": event_id, "retry_seconds": retry_seconds, "error": error[:1024]},
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


def _implementation_approval_record(row: object) -> ImplementationApprovalRecord:
    """Build a typed implementation decision record from a SQLAlchemy mapping row."""

    values = row
    return ImplementationApprovalRecord(
        decision_id=values["decision_id"],  # type: ignore[index]
        run_id=values["run_id"],  # type: ignore[index]
        decision=ImplementationApprovalDecision(values["decision"]),  # type: ignore[index]
        artifact_sha256=values["artifact_sha256"],  # type: ignore[index]
        actor_id=values["actor_id"],  # type: ignore[index]
        created_at=values["created_at"].isoformat(),  # type: ignore[index]
        delivered=values["delivered_at"] is not None,  # type: ignore[index]
        implementation_revision=values["implementation_revision"],  # type: ignore[index]
    )


def _coordination_event(row: object) -> CoordinationEvent:
    """Build a safe immutable event from a SQL mapping row."""

    values = row
    payload = dict(values["payload"])  # type: ignore[index]
    return CoordinationEvent(
        event_id=values["event_id"],  # type: ignore[index]
        run_id=values["run_id"],  # type: ignore[index]
        event_type=values["event_type"],  # type: ignore[index]
        occurred_at=values["created_at"].isoformat(),  # type: ignore[index]
        gate=payload.get("gate") if isinstance(payload.get("gate"), str) else None,
        artifact_ref=(payload.get("artifact") or {}).get("ref") if isinstance(payload.get("artifact"), dict) else None,
        artifact_sha256=(payload.get("artifact") or {}).get("sha256")
        if isinstance(payload.get("artifact"), dict)
        else None,
        decision=payload.get("decision") if isinstance(payload.get("decision"), str) else None,
        lifecycle_status=payload.get("lifecycle_status") if isinstance(payload.get("lifecycle_status"), str) else None,
        payload=payload,
    )


def _workbench_feedback_record(row: object) -> WorkbenchFeedbackRecord:
    """Build one immutable Workbench review context record from a SQL mapping row."""

    values = row
    return WorkbenchFeedbackRecord(
        feedback_id=values["feedback_id"],  # type: ignore[index]
        run_id=values["run_id"],  # type: ignore[index]
        intent=WorkbenchFeedbackIntent(values["intent"]),  # type: ignore[index]
        artifact_sha256=values["artifact_sha256"],  # type: ignore[index]
        stage_id=values["stage_id"],  # type: ignore[index]
        actor_id=values["actor_id"],  # type: ignore[index]
        comment=values["comment"],  # type: ignore[index]
        created_at=values["created_at"].isoformat(),  # type: ignore[index]
    )


def _agent_run_record(row: object) -> AgentRunRecord:
    values = row
    return AgentRunRecord(
        run_id=values["run_id"],  # type: ignore[index]
        root_run_id=values["root_run_id"],  # type: ignore[index]
        parent_run_id=values["parent_run_id"],  # type: ignore[index]
        agent_name=values["agent_name"],  # type: ignore[index]
        status=AgentRunStatus(values["status"]),  # type: ignore[index]
        trace_id=values["trace_id"],  # type: ignore[index]
        created_at=values["created_at"].isoformat(),  # type: ignore[index]
        updated_at=values["updated_at"].isoformat(),  # type: ignore[index]
        last_heartbeat_at=values["last_heartbeat_at"].isoformat() if values["last_heartbeat_at"] else None,  # type: ignore[index]
        worker_id=values["worker_id"],  # type: ignore[index]
        result_artifact_uri=values["result_artifact_uri"],  # type: ignore[index]
        error_summary=values["error_summary"],  # type: ignore[index]
    )
