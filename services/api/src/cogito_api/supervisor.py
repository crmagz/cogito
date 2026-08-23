"""Transactional persistence for the Cogito supervisor control plane."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Mapping, Protocol
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .models import (
    AgentGatewayPolicy,
    AgentGatewayResolution,
    AgentRunStatus,
    ArtifactReference,
    ImplementationApprovalDecision,
    McpBindingPolicy,
    McpToolGrant,
    McpToolSelection,
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
    product_specification_artifact: ArtifactReference | None = None
    product_specification_revision: int = 0
    product_specification_generation_claimed_at: datetime | None = None
    selected_product_specification_artifact: ArtifactReference | None = None
    selected_product_specification_revision: int | None = None
    specification_evaluation_artifact: ArtifactReference | None = None
    specification_evaluation_readiness: str | None = None
    selected_specification_evaluation_artifact: ArtifactReference | None = None


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
    mcp_selection: list[McpToolSelection] | None = None


@dataclass(frozen=True)
class PlanningGenerationDelivery:
    """One leased, durable request to generate or start a plan."""

    run_id: str
    claim_id: str
    attempt_count: int


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
    mcp_selection: list[McpToolSelection] | None = None


@dataclass(frozen=True)
class SpecificationEvaluationWaiverRecord:
    """Approver-visible audit fields for one immutable evaluation exception."""

    artifact_sha256: str
    actor_id: str
    rationale: str
    created_at: str


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
    payload: dict[str, object]
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


@dataclass(frozen=True)
class WorkbenchAgentGatewayRouteRecord:
    """One non-secret persisted policy route for a project-authorized agent release."""

    policy_revision: str
    role: str
    model_alias: str
    max_budget_usd: float
    toolset: str


@dataclass(frozen=True)
class WorkbenchAgentRecord:
    """Safe immutable agent-release facts and its routes for one authorized project."""

    registration_id: str
    registration_version: str
    manifest_sha256: str
    component_id: str
    component_version: str
    lifecycle: str
    maturity: str
    execution_class: str
    owner: str
    capabilities: list[str]
    gateway_routes: list[WorkbenchAgentGatewayRouteRecord]


@dataclass(frozen=True)
class WorkbenchAgentLifecycleTransitionRecord:
    """Status-only lifecycle evidence for a run-role binding."""

    from_status: AgentRunStatus | None
    to_status: AgentRunStatus | None
    occurred_at: str


@dataclass(frozen=True)
class WorkbenchAgentInvocationRecord:
    """Safe run-role binding facts; status remains authoritative only for the root run."""

    run_id: str
    root_run_id: str
    parent_run_id: str | None
    registration_id: str
    registration_version: str
    role: str
    run_lifecycle_status: AgentRunStatus
    workflow_available: bool
    created_at: str
    updated_at: str
    gateway_route: WorkbenchAgentGatewayRouteRecord | None
    mcp_grants: list[McpToolGrant]
    lifecycle_transitions: list[WorkbenchAgentLifecycleTransitionRecord]


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


def _mcp_grant_key(grant: McpToolGrant) -> tuple[str, str, str, str, str, str, str]:
    """Return a stable identity for a pinned MCP tool authorization."""

    return (
        grant.server_id,
        grant.server_version,
        grant.server_manifest_sha256,
        grant.tool_name,
        grant.input_schema_sha256,
        grant.repository_scope or "",
    )


def _gateway_resolution_from_row(row: Mapping[str, Any]) -> AgentGatewayResolution:
    """Materialize one immutable agent gateway route from durable storage."""

    return AgentGatewayResolution(
        policy_revision=row["policy_revision"],
        project_id=row["project_id"],
        role=row["role"],
        registration_id=row["registration_id"],
        registration_version=row["registration_version"],
        manifest_sha256=row["manifest_sha256"],
        model_alias=row["model_alias"],
        max_budget_usd=float(row["max_budget_usd"]),
        toolset=row["toolset"],
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
        product_specification_artifact=(
            ArtifactReference(
                ref=row["product_specification_artifact_ref"],
                sha256=row["product_specification_artifact_sha256"],
            )
            if row.get("product_specification_artifact_ref") is not None
            else None
        ),
        product_specification_revision=int(row.get("product_specification_revision", 0)),
        product_specification_generation_claimed_at=row.get("product_specification_generation_claimed_at"),
        selected_product_specification_artifact=(
            ArtifactReference(
                ref=row["selected_product_specification_artifact_ref"],
                sha256=row["selected_product_specification_artifact_sha256"],
            )
            if row.get("selected_product_specification_artifact_ref") is not None
            else None
        ),
        selected_product_specification_revision=row.get("selected_product_specification_revision"),
        specification_evaluation_artifact=(
            ArtifactReference(
                ref=row["specification_evaluation_artifact_ref"],
                sha256=row["specification_evaluation_artifact_sha256"],
            )
            if row.get("specification_evaluation_artifact_ref") is not None
            else None
        ),
        specification_evaluation_readiness=row.get("specification_evaluation_readiness"),
        selected_specification_evaluation_artifact=(
            ArtifactReference(
                ref=row["selected_specification_evaluation_artifact_ref"],
                sha256=row["selected_specification_evaluation_artifact_sha256"],
            )
            if row.get("selected_specification_evaluation_artifact_ref") is not None
            else None
        ),
    )


class SupervisorStore(Protocol):
    """Durable source of truth for supervisor run state."""

    async def create_planning_run(self, record: PlanningRunRecord) -> None: ...

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None: ...

    async def cancel_planning_run(self, run_id: str) -> PlanningRunRecord: ...

    async def attach_product_specification_draft(
        self,
        run_id: str,
        artifact: ArtifactReference,
        planner_model: str,
        expected_product_specification_revision: int,
        generation_claim: str | None = None,
    ) -> PlanningRunRecord: ...

    async def claim_product_specification_generation(self, run_id: str) -> str | None: ...

    async def release_product_specification_generation(self, run_id: str, generation_claim: str) -> None: ...

    async def record_product_specification_generation_failure(
        self, run_id: str, generation_claim: str, message: str
    ) -> None: ...

    async def attach_product_specification_revision(
        self,
        run_id: str,
        artifact: ArtifactReference,
        expected_product_specification_revision: int,
        parent_artifact_sha256: str,
        actor_id: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> PlanningRunRecord: ...

    async def select_product_specification(
        self,
        run_id: str,
        revision: int,
        artifact_sha256: str,
        actor_id: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> PlanningRunRecord: ...

    async def record_specification_evaluation(
        self,
        run_id: str,
        artifact: ArtifactReference,
        specification_revision: int,
        specification_sha256: str,
        readiness: str, generation_claim: str | None = None,
    ) -> PlanningRunRecord: ...

    async def claim_specification_evaluation_generation(self, run_id: str) -> str | None: ...

    async def release_specification_evaluation_generation(self, run_id: str, generation_claim: str) -> None: ...

    async def waive_specification_evaluation(
        self, *, run_id: str, artifact_sha256: str, actor_id: str, rationale: str,
        idempotency_key: str, request_sha256: str,
    ) -> PlanningRunRecord: ...

    async def attach_generated_plan(
        self,
        run_id: str,
        plan_artifact: ArtifactReference,
        planner_model: str,
        workflow_id: str,
        expected_plan_revision: int,
        expected_product_specification_revision: int | None = None,
        expected_product_specification_sha256: str | None = None,
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
        mcp_selection: list[McpToolSelection] | None = None,
    ) -> ApprovalRecord: ...

    async def get_run_mcp_capabilities(
        self, run_id: str, plan_revision: int
    ) -> tuple[list[McpToolSelection], list[McpToolSelection] | None, bool]: ...

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

    async def claim_planning_generation_deliveries(
        self, *, limit: int, lease_seconds: int
    ) -> list[PlanningGenerationDelivery]: ...

    async def release_planning_generation_delivery(
        self, run_id: str, claim_id: str, *, retry_seconds: int
    ) -> None: ...

    async def renew_planning_generation_delivery(self, run_id: str, claim_id: str) -> bool: ...

    async def record_planning_agent_terminal(
        self, run_id: str, claim_id: str, *, succeeded: bool, error_summary: str | None = None
    ) -> None: ...

    async def list_workbench_agents(
        self, *, project_id: str, policy_revision: str, limit: int = 50
    ) -> list[WorkbenchAgentRecord]: ...

    async def get_workbench_agent(
        self, *, project_id: str, policy_revision: str, registration_id: str, registration_version: str
    ) -> WorkbenchAgentRecord | None: ...

    async def list_workbench_agent_invocations(
        self, *, project_id: str, registration_id: str, registration_version: str, limit: int = 50
    ) -> list[WorkbenchAgentInvocationRecord]: ...

    async def get_workbench_agent_invocation(
        self, *, project_id: str, run_id: str, role: str
    ) -> WorkbenchAgentInvocationRecord | None: ...

    async def bootstrap_registry(
        self,
        manifests: list[RegistrationManifest],
        policy_revision: str,
        assignments: dict[str, str],
        mcp_policy: McpBindingPolicy | None = None,
    ) -> None: ...

    async def resolve_run_registration(
        self,
        run_id: str,
        role: str,
        policy_revision: str,
        manifest: RegistrationManifest,
    ) -> RegistrationReference: ...

    async def resolve_run_mcp_tools(
        self,
        run_id: str,
        role: str,
        project_id: str,
        policy_revision: str,
        target_repositories: list[str] | None = None,
        target_repository_scopes: Mapping[str, str] | None = None,
    ) -> list[McpToolGrant]: ...

    async def bootstrap_agent_gateway_policy(self, policy: AgentGatewayPolicy) -> None: ...

    async def resolve_run_agent_gateway(
        self,
        run_id: str,
        role: str,
        project_id: str,
        registration: RegistrationReference,
        policy: AgentGatewayPolicy,
    ) -> AgentGatewayResolution: ...

    async def list_coordination_events(self, run_id: str, *, limit: int = 100) -> list[tuple[CoordinationEvent, bool, int, str | None]]: ...

    async def list_workbench_approvals(self, run_id: str, *, limit: int = 100) -> list[WorkbenchApprovalRecord]: ...

    async def get_specification_evaluation_waiver(
        self, run_id: str, artifact_sha256: str
    ) -> SpecificationEvaluationWaiverRecord | None: ...

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

    async def claim_planning_generation_deliveries(
        self, *, limit: int, lease_seconds: int
    ) -> list[PlanningGenerationDelivery]:
        """Lease accepted planning work so restarts and replicas cannot lose it."""

        if limit < 1:
            return []
        now = datetime.now().astimezone()
        deliveries: list[PlanningGenerationDelivery] = []
        async with self._engine.begin() as connection:
            candidates = await connection.execute(
                text(
                    """
                    SELECT s.run_id, a.status, a.planning_generation_attempt_count
                    FROM supervisor_runs AS s
                    JOIN agent_runs AS a USING (run_id)
                    WHERE s.selected_product_specification_artifact_ref IS NOT NULL
                      AND s.selected_specification_evaluation_artifact_ref IS NOT NULL
                      AND (
                        (s.status = 'planning' AND s.plan_artifact_ref IS NULL)
                        OR (s.status = 'awaiting_plan_approval' AND a.planning_generation_retry_at IS NOT NULL)
                      )
                      AND (a.planning_generation_claim IS NULL OR a.planning_generation_claimed_at < :expired_at)
                      AND a.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
                      AND (a.planning_generation_retry_at IS NULL OR a.planning_generation_retry_at <= :now)
                    ORDER BY s.submitted_at
                    FOR UPDATE OF s, a SKIP LOCKED
                    LIMIT :limit
                    """
                ),
                {"now": now, "expired_at": now - timedelta(seconds=lease_seconds), "limit": limit},
            )
            for row in candidates.mappings():
                claim_id = str(uuid.uuid4())
                await connection.execute(
                    text(
                        """
                        UPDATE agent_runs
                        SET status = 'RUNNING', planning_generation_claim = :claim_id,
                            planning_generation_claimed_at = :now, planning_generation_retry_at = NULL,
                            planning_generation_attempt_count = planning_generation_attempt_count + 1,
                            updated_at = :now, last_heartbeat_at = :now
                        WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": row["run_id"], "claim_id": claim_id, "now": now},
                )
                if row["status"] != AgentRunStatus.RUNNING.value:
                    await connection.execute(
                        text(
                            """
                            INSERT INTO agent_run_events (event_id, run_id, event_type, from_status, to_status, occurred_at, metadata)
                            VALUES (:event_id, :run_id, 'planner_started', :from_status, 'RUNNING', :occurred_at, CAST('{}' AS jsonb))
                            """
                        ),
                        {"event_id": str(uuid.uuid4()), "run_id": row["run_id"], "from_status": row["status"], "occurred_at": now},
                    )
                    await self._append_coordination_event(connection, run_id=row["run_id"], event_type="planning_agent_started")
                deliveries.append(
                    PlanningGenerationDelivery(
                        run_id=row["run_id"], claim_id=claim_id,
                        attempt_count=int(row["planning_generation_attempt_count"]) + 1,
                    )
                )
        return deliveries

    async def release_planning_generation_delivery(
        self, run_id: str, claim_id: str, *, retry_seconds: int
    ) -> None:
        """Release a transient planner-start failure for bounded retry."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE agent_runs
                    SET status = 'QUEUED', planning_generation_claim = NULL,
                        planning_generation_claimed_at = NULL,
                        planning_generation_retry_at = now() + (:retry_seconds * interval '1 second'),
                        updated_at = now()
                    WHERE run_id = :run_id AND planning_generation_claim = :claim_id
                    """
                ),
                {"run_id": run_id, "claim_id": claim_id, "retry_seconds": retry_seconds},
            )

    async def renew_planning_generation_delivery(self, run_id: str, claim_id: str) -> bool:
        """Extend an active planner lease while its bounded attempt is still running."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE agent_runs
                    SET planning_generation_claimed_at = now(), updated_at = now(), last_heartbeat_at = now()
                    WHERE run_id = :run_id AND planning_generation_claim = :claim_id AND status = 'RUNNING'
                    RETURNING run_id
                    """
                ),
                {"run_id": run_id, "claim_id": claim_id},
            )
            return result.scalar_one_or_none() is not None

    async def record_planning_agent_terminal(
        self, run_id: str, claim_id: str, *, succeeded: bool, error_summary: str | None = None
    ) -> None:
        """Persist one terminal planner result and never leave a failed attempt live."""

        # This root agent record becomes the implementation worker's status
        # carrier after plan approval. A successful planning handoff therefore
        # waits at the plan gate instead of becoming terminal.
        target = AgentRunStatus.WAITING_FOR_APPROVAL.value if succeeded else AgentRunStatus.FAILED.value
        now = datetime.now().astimezone()
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE agent_runs
                    SET status = :target, updated_at = :now, last_heartbeat_at = :now,
                        completed_at = CASE WHEN :succeeded THEN completed_at ELSE :now END,
                        planning_generation_claim = NULL, planning_generation_claimed_at = NULL,
                        planning_generation_retry_at = NULL,
                        error_summary = CASE
                            WHEN CAST(:error_summary AS text) IS NULL THEN error_summary
                            ELSE CAST(:error_summary AS text)
                        END
                    WHERE run_id = :run_id AND planning_generation_claim = :claim_id
                      AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
                    RETURNING status
                    """
                ),
                {"run_id": run_id, "claim_id": claim_id, "target": target, "now": now, "succeeded": succeeded, "error_summary": error_summary},
            )
            if result.scalar_one_or_none() is None:
                return
            if not succeeded:
                await connection.execute(
                    text("UPDATE supervisor_runs SET status = 'planning_failed' WHERE run_id = :run_id AND status IN ('planning', 'awaiting_plan_approval')"),
                    {"run_id": run_id},
                )
                await self._append_coordination_event(
                    connection, run_id=run_id, event_type="planning_agent_failed", lifecycle_status="FAILED"
                )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_run_events (event_id, run_id, event_type, from_status, to_status, occurred_at, metadata)
                    VALUES (:event_id, :run_id, 'planner_terminal', 'RUNNING', :target, :occurred_at, CAST('{}' AS jsonb))
                    """
                ),
                {"event_id": str(uuid.uuid4()), "run_id": run_id, "target": target, "occurred_at": now},
            )

    async def list_workbench_agents(
        self, *, project_id: str, policy_revision: str, limit: int = 50
    ) -> list[WorkbenchAgentRecord]:
        """List project-visible agent releases, including historical pinned releases."""

        rows = await self._workbench_agent_rows(
            project_id=project_id,
            policy_revision=policy_revision,
            limit=max(1, min(limit, 100)),
        )
        return _workbench_agent_records(rows)

    async def get_workbench_agent(
        self, *, project_id: str, policy_revision: str, registration_id: str, registration_version: str
    ) -> WorkbenchAgentRecord | None:
        """Return one project-visible agent release without exposing its full manifest."""

        rows = await self._workbench_agent_rows(
            project_id=project_id,
            policy_revision=policy_revision,
            registration_id=registration_id,
            registration_version=registration_version,
            limit=100,
        )
        records = _workbench_agent_records(rows)
        return records[0] if records else None

    async def _workbench_agent_rows(
        self,
        *,
        project_id: str,
        policy_revision: str,
        registration_id: str | None = None,
        registration_version: str | None = None,
        limit: int = 100,
    ) -> list[Mapping[str, Any]]:
        """Read only allow-listed agent inventory columns from immutable policy and registry tables."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    WITH project_routes AS (
                        SELECT policy.policy_revision, binding.role, binding.registration_id,
                               binding.registration_version, binding.model_alias,
                               binding.max_budget_usd, binding.toolset
                        FROM registry_agent_gateway_policy_revisions AS policy
                        CROSS JOIN LATERAL jsonb_to_recordset(policy.bindings) AS binding(
                            role text,
                            registration_id text,
                            registration_version text,
                            project_ids jsonb,
                            model_alias text,
                            max_budget_usd numeric,
                            toolset text
                        )
                        WHERE policy.policy_revision = :policy_revision
                          AND binding.project_ids ? CAST(:project_id AS text)
                    ), historical_releases AS (
                        SELECT DISTINCT
                               COALESCE(resolution.registration_id, root_resolution.registration_id) AS registration_id,
                               COALESCE(resolution.registration_version, root_resolution.registration_version) AS registration_version
                        FROM agent_runs AS run
                        JOIN agent_runs AS root ON root.run_id = run.root_run_id
                        LEFT JOIN run_agent_gateway_resolutions AS gateway ON gateway.run_id = run.run_id
                        LEFT JOIN run_registration_resolutions AS resolution
                          ON resolution.run_id = run.run_id
                         AND (gateway.role IS NULL OR resolution.role = gateway.role)
                        LEFT JOIN run_registration_resolutions AS root_resolution
                          ON root_resolution.run_id = root.run_id
                         AND root_resolution.role = COALESCE(gateway.role, resolution.role)
                        LEFT JOIN supervisor_runs AS workflow ON workflow.run_id = run.root_run_id
                        WHERE (gateway.project_id = :project_id OR (gateway.run_id IS NULL AND workflow.project_id = :project_id))
                          AND COALESCE(resolution.registration_id, root_resolution.registration_id) IS NOT NULL
                    ), inventory_releases AS (
                        SELECT route.registration_id, route.registration_version
                        FROM project_routes AS route
                        JOIN registry_registrations AS registration
                          ON registration.registration_id = route.registration_id
                         AND registration.version = route.registration_version
                        WHERE registration.kind = 'agent' AND registration.lifecycle = 'active'
                        UNION
                        SELECT registration_id, registration_version FROM historical_releases
                    ), limited_releases AS (
                        SELECT registration_id, registration_version
                        FROM inventory_releases
                        WHERE (CAST(:registration_id AS text) IS NULL OR registration_id = :registration_id)
                          AND (CAST(:registration_version AS text) IS NULL OR registration_version = :registration_version)
                        GROUP BY registration_id, registration_version
                        ORDER BY registration_id, registration_version
                        LIMIT :limit
                    )
                    SELECT registration.registration_id, registration.version AS registration_version,
                           registration.manifest_sha256, registration.component_id, registration.component_version,
                           registration.lifecycle, registration.maturity, registration.execution_class, registration.owner,
                           COALESCE(registration.manifest -> 'capabilities', '[]'::jsonb) AS capabilities,
                           route.policy_revision, route.role, route.model_alias, route.max_budget_usd, route.toolset
                    FROM limited_releases AS release
                    JOIN registry_registrations AS registration
                      ON registration.registration_id = release.registration_id
                     AND registration.version = release.registration_version
                    LEFT JOIN project_routes AS route
                      ON route.registration_id = release.registration_id
                     AND route.registration_version = release.registration_version
                    WHERE registration.kind = 'agent'
                    ORDER BY registration.registration_id, registration.version, route.policy_revision NULLS LAST, route.role NULLS LAST
                    """
                ),
                {
                    "project_id": project_id,
                    "policy_revision": policy_revision,
                    "registration_id": registration_id,
                    "registration_version": registration_version,
                    "limit": limit,
                },
            )
            return list(result.mappings().all())

    async def list_workbench_agent_invocations(
        self, *, project_id: str, registration_id: str, registration_version: str, limit: int = 50
    ) -> list[WorkbenchAgentInvocationRecord]:
        """List bounded newest-first run-role bindings only when their immutable route is project scoped."""

        rows = await self._workbench_agent_invocation_rows(
            project_id=project_id,
            registration_id=registration_id,
            registration_version=registration_version,
            limit=limit,
        )
        return [_workbench_agent_invocation_record(row) for row in rows]

    async def get_workbench_agent_invocation(
        self, *, project_id: str, run_id: str, role: str
    ) -> WorkbenchAgentInvocationRecord | None:
        """Return one safe project-scoped run-role binding and status-only lifecycle evidence."""

        rows = await self._workbench_agent_invocation_rows(
            project_id=project_id,
            registration_id=None,
            registration_version=None,
            run_id=run_id,
            role=role,
            limit=1,
        )
        if not rows:
            return None
        record = _workbench_agent_invocation_record(rows[0])
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT from_status, to_status, occurred_at
                    FROM agent_run_events
                    WHERE run_id = :run_id
                    ORDER BY occurred_at ASC, event_id ASC
                    LIMIT 100
                    """
                ),
                {"run_id": run_id},
            )
            transitions = [
                WorkbenchAgentLifecycleTransitionRecord(
                    from_status=AgentRunStatus(row["from_status"]) if row["from_status"] is not None else None,
                    to_status=AgentRunStatus(row["to_status"]) if row["to_status"] is not None else None,
                    occurred_at=row["occurred_at"].isoformat(),
                )
                for row in result.mappings().all()
            ]
        return WorkbenchAgentInvocationRecord(
            **{**record.__dict__, "lifecycle_transitions": transitions}
        )

    async def _workbench_agent_invocation_rows(
        self,
        *,
        project_id: str,
        registration_id: str | None,
        registration_version: str | None,
        limit: int,
        run_id: str | None = None,
        role: str | None = None,
    ) -> list[Mapping[str, Any]]:
        """Read one project-scoped run-role binding projection without selecting unsafe run fields."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run.run_id, run.root_run_id, run.parent_run_id, root.status AS run_lifecycle_status,
                           (workflow.run_id IS NOT NULL) AS workflow_available,
                           run.created_at, run.updated_at,
                           COALESCE(resolution.registration_id, root_resolution.registration_id) AS registration_id,
                           COALESCE(resolution.registration_version, root_resolution.registration_version) AS registration_version,
                           COALESCE(gateway.role, resolution.role, root_resolution.role) AS role,
                           gateway.policy_revision AS gateway_policy_revision,
                           gateway.model_alias, gateway.max_budget_usd, gateway.toolset,
                           COALESCE((
                               SELECT jsonb_agg(jsonb_build_object(
                                   'server_id', mcp_grant.server_registration_id,
                                   'server_version', mcp_grant.server_version,
                                   'server_manifest_sha256', mcp_grant.server_manifest_sha256,
                                   'tool_name', mcp_grant.tool_name,
                                   'input_schema_sha256', mcp_grant.input_schema_sha256,
                                   'repository_scope', mcp_grant.repository_scope
                               ) ORDER BY mcp_grant.server_registration_id, mcp_grant.server_version, mcp_grant.tool_name)
                               FROM run_mcp_tool_resolutions AS mcp_grant
                               WHERE mcp_grant.run_id = run.run_id
                                 AND mcp_grant.role = COALESCE(gateway.role, resolution.role, root_resolution.role)
                           ), '[]'::jsonb) AS mcp_grants
                    FROM agent_runs AS run
                    JOIN agent_runs AS root ON root.run_id = run.root_run_id
                    LEFT JOIN run_agent_gateway_resolutions AS gateway
                      ON gateway.run_id = run.run_id
                    LEFT JOIN run_registration_resolutions AS resolution
                      ON resolution.run_id = run.run_id
                     AND (gateway.role IS NULL OR resolution.role = gateway.role)
                    LEFT JOIN run_registration_resolutions AS root_resolution
                      ON root_resolution.run_id = root.run_id
                     AND root_resolution.role = COALESCE(gateway.role, resolution.role)
                    LEFT JOIN supervisor_runs AS workflow ON workflow.run_id = run.root_run_id
                    WHERE (gateway.project_id = :project_id OR (gateway.run_id IS NULL AND workflow.project_id = :project_id))
                      AND COALESCE(resolution.registration_id, root_resolution.registration_id) IS NOT NULL
                      AND (CAST(:registration_id AS text) IS NULL OR COALESCE(resolution.registration_id, root_resolution.registration_id) = :registration_id)
                      AND (CAST(:registration_version AS text) IS NULL OR COALESCE(resolution.registration_version, root_resolution.registration_version) = :registration_version)
                      AND (CAST(:run_id AS text) IS NULL OR run.run_id = :run_id)
                      AND (CAST(:role AS text) IS NULL OR COALESCE(gateway.role, resolution.role, root_resolution.role) = :role)
                    ORDER BY run.created_at DESC, run.run_id DESC, COALESCE(gateway.role, resolution.role, root_resolution.role) ASC
                    LIMIT :limit
                    """
                ),
                {
                    "project_id": project_id,
                    "registration_id": registration_id,
                    "registration_version": registration_version,
                    "run_id": run_id,
                    "role": role,
                    "limit": max(1, min(limit, 100)),
                },
            )
            return list(result.mappings().all())

    async def bootstrap_registry(
        self,
        manifests: list[RegistrationManifest],
        policy_revision: str,
        assignments: dict[str, str],
        mcp_policy: McpBindingPolicy | None = None,
    ) -> None:
        """Persist immutable releases and one policy revision without rewriting either."""

        mcp_policy = mcp_policy or McpBindingPolicy(policy_revision=policy_revision)
        if mcp_policy.policy_revision != policy_revision:
            raise RegistryConflictError("MCP policy revision does not match the registry policy revision")
        mcp_bindings = mcp_policy.model_dump(mode="json")["bindings"]
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
                    INSERT INTO registry_policy_revisions (policy_revision, assignments, mcp_bindings, created_at)
                    VALUES (:policy_revision, CAST(:assignments AS jsonb), CAST(:mcp_bindings AS jsonb), now())
                    ON CONFLICT (policy_revision) DO NOTHING
                    """
                ),
                {
                    "policy_revision": policy_revision,
                    "assignments": json.dumps(assignments, sort_keys=True),
                    "mcp_bindings": json.dumps(mcp_bindings, sort_keys=True),
                },
            )
            policy = await connection.execute(
                text(
                    "SELECT assignments, mcp_bindings FROM registry_policy_revisions "
                    "WHERE policy_revision = :policy_revision"
                ),
                {"policy_revision": policy_revision},
            )
            persisted_policy = policy.mappings().one()
            if (
                dict(persisted_policy["assignments"]) != assignments
                or list(persisted_policy["mcp_bindings"]) != mcp_bindings
            ):
                raise RegistryConflictError("policy revision already exists with different assignments")

    async def bootstrap_agent_gateway_policy(self, policy: AgentGatewayPolicy) -> None:
        """Persist one immutable agent routing policy without rewriting it."""

        bindings = policy.model_dump(mode="json")["bindings"]
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO registry_agent_gateway_policy_revisions (policy_revision, bindings, created_at)
                    VALUES (:policy_revision, CAST(:bindings AS jsonb), now())
                    ON CONFLICT (policy_revision) DO NOTHING
                    """
                ),
                {
                    "policy_revision": policy.policy_revision,
                    "bindings": json.dumps(bindings, sort_keys=True),
                },
            )
            persisted = await connection.execute(
                text(
                    "SELECT bindings FROM registry_agent_gateway_policy_revisions "
                    "WHERE policy_revision = :policy_revision"
                ),
                {"policy_revision": policy.policy_revision},
            )
            row = persisted.mappings().one()
            if list(row["bindings"]) != bindings:
                raise RegistryConflictError("agent gateway policy revision already exists with different bindings")

    async def resolve_run_agent_gateway(
        self,
        run_id: str,
        role: str,
        project_id: str,
        registration: RegistrationReference,
        policy: AgentGatewayPolicy,
    ) -> AgentGatewayResolution:
        """Pin one project-authorized LiteLLM route before a role can execute."""

        async with self._engine.begin() as connection:
            existing = await connection.execute(
                text(
                    """
                    SELECT policy_revision, project_id, role, registration_id, registration_version, manifest_sha256,
                           model_alias, max_budget_usd, toolset
                    FROM run_agent_gateway_resolutions
                    WHERE run_id = :run_id AND role = :role
                    FOR UPDATE
                    """
                ),
                {"run_id": run_id, "role": role},
            )
            current = existing.mappings().one_or_none()
            if current is not None:
                route = _gateway_resolution_from_row(current)
                if route.project_id != project_id:
                    raise RegistryConflictError("run role is already pinned to a different project route")
                return route

            persisted = await connection.execute(
                text(
                    "SELECT bindings FROM registry_agent_gateway_policy_revisions "
                    "WHERE policy_revision = :policy_revision"
                ),
                {"policy_revision": policy.policy_revision},
            )
            policy_row = persisted.mappings().one_or_none()
            if policy_row is None:
                raise RegistryConflictError("agent gateway policy revision is not available")
            durable_policy = AgentGatewayPolicy.model_validate(
                {"policy_revision": policy.policy_revision, "bindings": list(policy_row["bindings"])}
            )
            binding = next(
                (
                    candidate
                    for candidate in durable_policy.bindings
                    if candidate.role == role and project_id in candidate.project_ids
                ),
                None,
            )
            if binding is None:
                raise RegistryConflictError("agent gateway policy does not authorize the requested role and project")
            if (
                binding.registration_id != registration.registration_id
                or binding.registration_version != registration.version
            ):
                raise RegistryConflictError("agent gateway policy does not select the pinned registration release")
            registered = await connection.execute(
                text(
                    """
                    SELECT lifecycle, manifest_sha256
                    FROM registry_registrations
                    WHERE registration_id = :registration_id AND version = :version
                    FOR UPDATE
                    """
                ),
                {"registration_id": registration.registration_id, "version": registration.version},
            )
            registered_row = registered.mappings().one_or_none()
            if registered_row is None or registered_row["lifecycle"] != "active":
                raise RegistryConflictError("agent gateway registration release is not active")
            if registered_row["manifest_sha256"] != registration.manifest_sha256:
                raise RegistryConflictError("agent gateway registration release does not match its declared manifest")
            route = AgentGatewayResolution(
                policy_revision=durable_policy.policy_revision,
                project_id=project_id,
                role=role,
                registration_id=registration.registration_id,
                registration_version=registration.version,
                manifest_sha256=registration.manifest_sha256,
                model_alias=binding.model_alias,
                max_budget_usd=binding.max_budget_usd,
                toolset=binding.toolset,
            )
            inserted = await connection.execute(
                text(
                    """
                    INSERT INTO run_agent_gateway_resolutions (
                        run_id, role, project_id, registration_id, registration_version, manifest_sha256,
                        policy_revision, model_alias, max_budget_usd, toolset, created_at
                    ) VALUES (
                        :run_id, :role, :project_id, :registration_id, :registration_version, :manifest_sha256,
                        :policy_revision, :model_alias, :max_budget_usd, :toolset, now()
                    ) ON CONFLICT (run_id, role) DO NOTHING
                    RETURNING run_id
                    """
                ),
                {"run_id": run_id, **route.model_dump()},
            )
            if inserted.mappings().one_or_none() is not None:
                return route
            concurrent = await connection.execute(
                text(
                    """
                    SELECT policy_revision, project_id, role, registration_id, registration_version, manifest_sha256,
                           model_alias, max_budget_usd, toolset
                    FROM run_agent_gateway_resolutions
                    WHERE run_id = :run_id AND role = :role
                    """
                ),
                {"run_id": run_id, "role": role},
            )
            current = concurrent.mappings().one_or_none()
            if current is None:
                raise RegistryConflictError("agent gateway route could not be persisted")
            route = _gateway_resolution_from_row(current)
            if route.project_id != project_id:
                raise RegistryConflictError("run role is already pinned to a different project route")
            return route

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

    async def resolve_run_mcp_tools(
        self,
        run_id: str,
        role: str,
        project_id: str,
        policy_revision: str,
        target_repositories: list[str] | None = None,
        target_repository_scopes: Mapping[str, str] | None = None,
    ) -> list[McpToolGrant]:
        """Pin project-scoped MCP tool authority from the immutable policy revision."""

        async with self._engine.begin() as connection:
            existing = await connection.execute(
                text(
                    """
                    SELECT server_registration_id, server_version, server_manifest_sha256,
                           tool_name, input_schema_sha256, repository_scope, policy_revision
                    FROM run_mcp_tool_resolutions
                    WHERE run_id = :run_id AND role = :role
                    FOR UPDATE
                    """
                ),
                {"run_id": run_id, "role": role},
            )
            persisted = existing.mappings().all()
            if persisted:
                if len({item["policy_revision"] for item in persisted}) != 1:
                    raise RegistryConflictError("run MCP tools have inconsistent policy revisions")
                return sorted(
                    [
                        McpToolGrant(
                            server_id=item["server_registration_id"],
                            server_version=item["server_version"],
                            server_manifest_sha256=item["server_manifest_sha256"],
                            tool_name=item["tool_name"],
                            input_schema_sha256=item["input_schema_sha256"],
                            repository_scope=item["repository_scope"],
                        )
                        for item in persisted
                    ],
                    key=_mcp_grant_key,
                )
            policy = await connection.execute(
                text(
                    "SELECT mcp_bindings FROM registry_policy_revisions "
                    "WHERE policy_revision = :policy_revision"
                ),
                {"policy_revision": policy_revision},
            )
            policy_row = policy.mappings().one_or_none()
            if policy_row is None:
                raise RegistryConflictError("registry policy revision is not available")
            bindings = McpBindingPolicy.model_validate(
                {"policy_revision": policy_revision, "bindings": list(policy_row["mcp_bindings"])}
            )
            grants: list[McpToolGrant] = []
            for binding in bindings.bindings:
                if binding.role != role or project_id not in binding.project_ids:
                    continue
                if not _binding_targets_a_run_repository(
                    binding.server_id,
                    binding.server_version,
                    target_repositories or [],
                    target_repository_scopes or {},
                ):
                    continue
                repository_scope = (target_repository_scopes or {}).get(
                    f"{binding.server_id}@{binding.server_version}"
                )
                if repository_scope is not None:
                    repository_scope = repository_scope.casefold()
                registration = await connection.execute(
                    text(
                        """
                        SELECT lifecycle, manifest_sha256, manifest
                        FROM registry_registrations
                        WHERE registration_id = :registration_id AND version = :version
                        FOR UPDATE
                        """
                    ),
                    {"registration_id": binding.server_id, "version": binding.server_version},
                )
                row = registration.mappings().one_or_none()
                if row is None or row["lifecycle"] != "active":
                    raise RegistryConflictError("MCP server release is not active")
                server = RegistrationManifest.model_validate(dict(row["manifest"]))
                if manifest_sha256(server) != row["manifest_sha256"]:
                    raise RegistryConflictError("MCP server release does not match its declared manifest")
                tool_schemas = {tool.name: tool.input_schema_sha256 for tool in server.mcp_tools}
                for tool_name in binding.tools:
                    input_schema_sha256 = tool_schemas.get(tool_name)
                    if input_schema_sha256 is None:
                        raise RegistryConflictError("MCP policy references an unavailable server tool")
                    grants.append(
                        McpToolGrant(
                            server_id=binding.server_id,
                            server_version=binding.server_version,
                            server_manifest_sha256=row["manifest_sha256"],
                            tool_name=tool_name,
                            input_schema_sha256=input_schema_sha256,
                            repository_scope=repository_scope,
                        )
                    )
            expected = sorted(grants, key=_mcp_grant_key)
            for grant in expected:
                await connection.execute(
                    text(
                        """
                        INSERT INTO run_mcp_tool_resolutions (
                            run_id, role, server_registration_id, server_version, server_manifest_sha256,
                            tool_name, input_schema_sha256, repository_scope, policy_revision, created_at
                        ) VALUES (
                            :run_id, :role, :server_id, :server_version, :server_manifest_sha256,
                            :tool_name, :input_schema_sha256, :repository_scope, :policy_revision, now()
                        ) ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "run_id": run_id,
                        "role": role,
                        "server_id": grant.server_id,
                        "server_version": grant.server_version,
                        "server_manifest_sha256": grant.server_manifest_sha256,
                        "tool_name": grant.tool_name,
                        "input_schema_sha256": grant.input_schema_sha256,
                        "repository_scope": grant.repository_scope,
                        "policy_revision": policy_revision,
                    },
                )
            return expected

    async def get_run_mcp_capabilities(
        self, run_id: str, plan_revision: int
    ) -> tuple[list[McpToolSelection], list[McpToolSelection] | None, bool]:
        """Return durable pins and the current revision's immutable selection without re-resolving policy."""

        async with self._engine.connect() as connection:
            pins = await connection.execute(
                text(
                    """
                    SELECT role, server_registration_id, server_version, server_manifest_sha256,
                           tool_name, input_schema_sha256, repository_scope
                    FROM run_mcp_tool_resolutions
                    WHERE run_id = :run_id AND role = 'developer'
                    ORDER BY role, server_registration_id, server_version, server_manifest_sha256,
                             tool_name, input_schema_sha256, repository_scope
                    """
                ),
                {"run_id": run_id},
            )
            decision = await connection.execute(
                text(
                    """
                    SELECT decision, mcp_selection
                    FROM plan_approval_decisions
                    WHERE run_id = :run_id AND plan_revision = :plan_revision
                    ORDER BY created_at DESC, decision_id DESC
                    LIMIT 1
                    """
                ),
                {"run_id": run_id, "plan_revision": plan_revision},
            )
            selection_row = decision.mappings().one_or_none()
        return (
            [_mcp_tool_selection(row) for row in pins.mappings().all()],
            _mcp_selection_from_json(selection_row["mcp_selection"]) if selection_row is not None else None,
            selection_row is not None and selection_row["decision"] == PlanApprovalDecision.APPROVE.value,
        )

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                           target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                           plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                           product_specification_artifact_ref, product_specification_artifact_sha256,
                           product_specification_revision, product_specification_generation_claimed_at,
                           selected_product_specification_artifact_ref,
                           selected_product_specification_artifact_sha256, selected_product_specification_revision,
                           specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                           specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                           selected_specification_evaluation_artifact_sha256
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

    async def cancel_planning_run(self, run_id: str) -> PlanningRunRecord:
        """Terminally stop a run before any generated plan can be executed."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET status = 'cancelled'
                    WHERE run_id = :run_id AND status = 'planning' AND plan_artifact_ref IS NULL
                    RETURNING run_id
                    """
                ),
                {"run_id": run_id},
            )
            if result.scalar_one_or_none() is not None:
                await connection.execute(
                    text(
                        """
                        UPDATE agent_runs
                        SET status = 'CANCELLED', planning_generation_claim = NULL,
                            planning_generation_claimed_at = NULL, planning_generation_retry_at = NULL,
                            updated_at = now(), completed_at = now()
                        WHERE run_id = :run_id AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
                        """
                    ),
                    {"run_id": run_id},
                )
                await self._append_coordination_event(
                    connection,
                    run_id=run_id,
                    event_type="planning_cancelled",
                    lifecycle_status=PlanningRunStatus.CANCELLED.value,
                )
        record = await self.get_planning_run(run_id)
        if record is None or record.status is not PlanningRunStatus.CANCELLED:
            raise ValueError("planning run is not eligible for cancellation")
        return record

    async def attach_product_specification_revision(
        self,
        run_id: str,
        artifact: ArtifactReference,
        expected_product_specification_revision: int,
        parent_artifact_sha256: str,
        actor_id: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> PlanningRunRecord:
        """Append a complete human-reviewed replacement and clear stale selected input."""

        async with self._engine.begin() as connection:
            existing_result = await connection.execute(
                text(
                    """SELECT revision, request_sha256 FROM product_specification_revision_decisions
                           WHERE run_id = :run_id AND idempotency_key = :idempotency_key"""
                ),
                {"run_id": run_id, "idempotency_key": idempotency_key},
            )
            existing = existing_result.mappings().one_or_none()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with different product specification revision")
                return (await self.get_planning_run(run_id))  # type: ignore[return-value]
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET product_specification_artifact_ref = :artifact_ref,
                        product_specification_artifact_sha256 = :artifact_sha256,
                        product_specification_revision = product_specification_revision + 1,
                        selected_product_specification_artifact_ref = NULL,
                        selected_product_specification_artifact_sha256 = NULL,
                        selected_product_specification_revision = NULL,
                        specification_evaluation_artifact_ref = NULL,
                        specification_evaluation_artifact_sha256 = NULL,
                        specification_evaluation_readiness = NULL,
                        selected_specification_evaluation_artifact_ref = NULL,
                        selected_specification_evaluation_artifact_sha256 = NULL,
                        specification_evaluation_generation_claim = NULL,
                        specification_evaluation_generation_claimed_at = NULL
                    WHERE run_id = :run_id
                      AND status = 'planning'
                      AND product_specification_revision = :expected_revision
                      AND product_specification_artifact_sha256 = :parent_artifact_sha256
                    RETURNING run_id, status, source_artifact_ref, source_artifact_sha256,
                              target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                              plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                              implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                              product_specification_artifact_ref, product_specification_artifact_sha256,
                              product_specification_revision, selected_product_specification_artifact_ref,
                              selected_product_specification_artifact_sha256, selected_product_specification_revision,
                              specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                              specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                              selected_specification_evaluation_artifact_sha256
                    """
                ),
                {"run_id": run_id, "artifact_ref": artifact.ref, "artifact_sha256": artifact.sha256,
                 "expected_revision": expected_product_specification_revision,
                 "parent_artifact_sha256": parent_artifact_sha256},
            )
            row = result.mappings().one_or_none()
            if row is None:
                raise ValueError("planning run is not eligible to accept this product specification revision")
            revision = row["product_specification_revision"]
            await connection.execute(
                text("""INSERT INTO product_specification_revisions (run_id, revision, artifact_ref, artifact_sha256, planner_model, created_at)
                         VALUES (:run_id, :revision, :artifact_ref, :artifact_sha256, 'human-authored', now())"""),
                {"run_id": run_id, "revision": revision, "artifact_ref": artifact.ref, "artifact_sha256": artifact.sha256},
            )
            await connection.execute(
                text("""INSERT INTO product_specification_revision_decisions
                         (decision_id, run_id, revision, parent_artifact_sha256, actor_id, idempotency_key, request_sha256, created_at)
                         VALUES (:decision_id, :run_id, :revision, :parent_artifact_sha256, :actor_id, :idempotency_key, :request_sha256, now())"""),
                {"decision_id": str(uuid.uuid4()), "run_id": run_id, "revision": revision,
                 "parent_artifact_sha256": parent_artifact_sha256, "actor_id": actor_id,
                 "idempotency_key": idempotency_key, "request_sha256": request_sha256},
            )
            await self._append_coordination_event(connection, run_id=run_id, event_type="product_specification_revised", artifact=artifact)
        return _planning_run_record(row)

    async def record_specification_evaluation(
        self,
        run_id: str,
        artifact: ArtifactReference,
        specification_revision: int,
        specification_sha256: str,
        readiness: str,
        generation_claim: str | None = None,
    ) -> PlanningRunRecord:
        """Attach one immutable evaluation only when its exact spec revision is still current."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET specification_evaluation_artifact_ref = :artifact_ref,
                        specification_evaluation_artifact_sha256 = :artifact_sha256,
                        specification_evaluation_readiness = :readiness,
                        specification_evaluation_generation_claim = NULL,
                        specification_evaluation_generation_claimed_at = NULL
                    WHERE run_id = :run_id
                      AND status = 'planning'
                      AND product_specification_revision = :specification_revision
                      AND product_specification_artifact_sha256 = :specification_sha256
                      AND specification_evaluation_artifact_ref IS NULL
                      AND (CAST(:generation_claim AS text) IS NULL OR specification_evaluation_generation_claim = :generation_claim)
                    """
                ),
                {
                    "run_id": run_id,
                    "artifact_ref": artifact.ref,
                    "artifact_sha256": artifact.sha256,
                    "readiness": readiness,
                    "specification_revision": specification_revision,
                    "specification_sha256": specification_sha256,
                    "generation_claim": generation_claim,
                },
            )
            if result.rowcount == 0:
                current = await self.get_planning_run(run_id)
                if current is not None and current.specification_evaluation_artifact is not None:
                    return current
                raise ValueError("product specification changed while evaluation was generated")
            await connection.execute(
                text(
                    """
                    INSERT INTO specification_evaluations (
                        run_id, specification_revision, specification_sha256, artifact_ref, artifact_sha256, readiness, created_at
                    ) VALUES (
                        :run_id, :specification_revision, :specification_sha256, :artifact_ref, :artifact_sha256, :readiness, now()
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "specification_revision": specification_revision,
                    "specification_sha256": specification_sha256,
                    "artifact_ref": artifact.ref,
                    "artifact_sha256": artifact.sha256,
                    "readiness": readiness,
                },
            )
            await self._append_coordination_event(
                connection, run_id=run_id, event_type="specification_evaluated", artifact=artifact
            )
        return (await self.get_planning_run(run_id))  # type: ignore[return-value]

    async def claim_specification_evaluation_generation(self, run_id: str) -> str | None:
        """Serialize immutable evaluation creation and recover an abandoned lease."""

        claim = str(uuid.uuid4())
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text("""UPDATE supervisor_runs
                         SET specification_evaluation_generation_claim = :claim,
                             specification_evaluation_generation_claimed_at = now()
                         WHERE run_id = :run_id AND status = 'planning'
                           AND product_specification_artifact_ref IS NOT NULL
                           AND specification_evaluation_artifact_ref IS NULL
                           AND (specification_evaluation_generation_claim IS NULL
                                OR specification_evaluation_generation_claimed_at IS NULL
                                OR specification_evaluation_generation_claimed_at < now() - interval '15 minutes')
                         RETURNING specification_evaluation_generation_claim"""),
                {"run_id": run_id, "claim": claim},
            )
            row = result.mappings().one_or_none()
        return row["specification_evaluation_generation_claim"] if row is not None else None

    async def release_specification_evaluation_generation(self, run_id: str, generation_claim: str) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                text("""UPDATE supervisor_runs SET specification_evaluation_generation_claim = NULL,
                                                   specification_evaluation_generation_claimed_at = NULL
                        WHERE run_id = :run_id AND specification_evaluation_generation_claim = :claim"""),
                {"run_id": run_id, "claim": generation_claim},
            )

    async def waive_specification_evaluation(
        self, *, run_id: str, artifact_sha256: str, actor_id: str, rationale: str,
        idempotency_key: str, request_sha256: str,
    ) -> PlanningRunRecord:
        """Persist an approver-owned exception for exactly one failing evaluation."""

        async with self._engine.begin() as connection:
            existing = (await connection.execute(
                text("""SELECT request_sha256 FROM specification_evaluation_waivers
                         WHERE run_id = :run_id AND idempotency_key = :idempotency_key"""),
                {"run_id": run_id, "idempotency_key": idempotency_key},
            )).mappings().one_or_none()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with different evaluation waiver")
                return (await self.get_planning_run(run_id))  # type: ignore[return-value]
            result = await connection.execute(
                text("""
                    UPDATE supervisor_runs
                    SET specification_evaluation_readiness = 'waived'
                    WHERE run_id = :run_id AND status = 'planning'
                      AND specification_evaluation_artifact_sha256 = :artifact_sha256
                      AND specification_evaluation_readiness = 'needs_revision'
                    RETURNING specification_evaluation_artifact_ref
                """),
                {"run_id": run_id, "artifact_sha256": artifact_sha256},
            )
            row = result.mappings().one_or_none()
            if row is None:
                raise ValueError("evaluation is not eligible for waiver")
            await connection.execute(
                text("""INSERT INTO specification_evaluation_waivers
                         (decision_id, run_id, artifact_sha256, actor_id, rationale, idempotency_key, request_sha256, created_at)
                         VALUES (:decision_id, :run_id, :artifact_sha256, :actor_id, :rationale, :idempotency_key, :request_sha256, now())"""),
                {"decision_id": str(uuid.uuid4()), "run_id": run_id, "artifact_sha256": artifact_sha256,
                 "actor_id": actor_id, "rationale": rationale, "idempotency_key": idempotency_key,
                 "request_sha256": request_sha256},
            )
            await self._append_coordination_event(connection, run_id=run_id, event_type="specification_evaluation_waived")
        return (await self.get_planning_run(run_id))  # type: ignore[return-value]

    async def select_product_specification(
        self,
        run_id: str,
        revision: int,
        artifact_sha256: str,
        actor_id: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> PlanningRunRecord:
        """Atomically bind one displayed immutable specification revision as the only planning input."""

        async with self._engine.begin() as connection:
            existing_result = await connection.execute(
                text(
                    """
                    SELECT revision, artifact_sha256, request_sha256
                    FROM product_specification_selection_decisions
                    WHERE run_id = :run_id AND idempotency_key = :idempotency_key
                    """
                ),
                {"run_id": run_id, "idempotency_key": idempotency_key},
            )
            existing = existing_result.mappings().one_or_none()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise ApprovalConflictError("idempotency key was reused with different product specification selection")
                current_result = await connection.execute(
                    text(
                        """SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                                  target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                                  plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                                  implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                                  product_specification_artifact_ref, product_specification_artifact_sha256,
                                  product_specification_revision, selected_product_specification_artifact_ref,
                                  selected_product_specification_artifact_sha256, selected_product_specification_revision,
                                  specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                                  specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                                  selected_specification_evaluation_artifact_sha256
                           FROM supervisor_runs WHERE run_id = :run_id"""
                    ),
                    {"run_id": run_id},
                )
                current = current_result.mappings().one()
                return _planning_run_record(current)
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs AS run
                    SET selected_product_specification_artifact_ref = revision.artifact_ref,
                        selected_product_specification_artifact_sha256 = revision.artifact_sha256,
                        selected_product_specification_revision = revision.revision,
                        selected_specification_evaluation_artifact_ref = run.specification_evaluation_artifact_ref,
                        selected_specification_evaluation_artifact_sha256 = run.specification_evaluation_artifact_sha256
                    FROM product_specification_revisions AS revision
                    WHERE run.run_id = :run_id
                      AND revision.run_id = run.run_id
                      AND revision.revision = :revision
                      AND revision.artifact_sha256 = :artifact_sha256
                      AND run.status = 'planning'
                      AND run.selected_product_specification_revision IS NULL
                      AND run.product_specification_revision = :revision
                      AND run.product_specification_artifact_sha256 = :artifact_sha256
                      -- Evaluation is mandatory evidence, but an explicit human approval
                      -- owns the decision to proceed when it records findings.
                      AND run.specification_evaluation_artifact_sha256 IS NOT NULL
                    RETURNING run.run_id, run.status, run.source_artifact_ref, run.source_artifact_sha256,
                              run.target_repos, run.spec_set, run.constraints, run.priority, run.submitted_at, run.submitted_by,
                              run.plan_artifact_ref, run.plan_artifact_sha256, run.planner_model, run.active_workflow_id, run.plan_revision,
                              run.implementation_artifact_ref, run.implementation_artifact_sha256, run.implementation_revision, run.project_id,
                              run.product_specification_artifact_ref, run.product_specification_artifact_sha256,
                              run.product_specification_revision, run.selected_product_specification_artifact_ref,
                              run.selected_product_specification_artifact_sha256, run.selected_product_specification_revision,
                              run.specification_evaluation_artifact_ref, run.specification_evaluation_artifact_sha256,
                              run.specification_evaluation_readiness, run.selected_specification_evaluation_artifact_ref,
                              run.selected_specification_evaluation_artifact_sha256
                    """
                ),
                {"run_id": run_id, "revision": revision, "artifact_sha256": artifact_sha256},
            )
            row = result.mappings().one_or_none()
            if row is None:
                existing = await connection.execute(
                    text(
                        """
                        SELECT run_id, status, source_artifact_ref, source_artifact_sha256,
                               target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                               plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                               implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                               product_specification_artifact_ref, product_specification_artifact_sha256,
                               product_specification_revision, selected_product_specification_artifact_ref,
                               selected_product_specification_artifact_sha256, selected_product_specification_revision,
                               specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                               specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                               selected_specification_evaluation_artifact_sha256
                        FROM supervisor_runs WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": run_id},
                )
                current = existing.mappings().one_or_none()
                if (
                    current is not None
                    and current["selected_product_specification_revision"] == revision
                    and current["selected_product_specification_artifact_sha256"] == artifact_sha256
                ):
                    return _planning_run_record(current)
                raise ValueError("planning run is not eligible to select this product specification")
            artifact = ArtifactReference(
                ref=row["selected_product_specification_artifact_ref"],
                sha256=row["selected_product_specification_artifact_sha256"],
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO product_specification_selection_decisions (
                        decision_id, run_id, revision, artifact_sha256, actor_id, idempotency_key, request_sha256, created_at
                    ) VALUES (
                        :decision_id, :run_id, :revision, :artifact_sha256, :actor_id, :idempotency_key, :request_sha256, now()
                    )
                    """
                ),
                {
                    "decision_id": str(uuid.uuid4()), "run_id": run_id, "revision": revision,
                    "artifact_sha256": artifact_sha256, "actor_id": actor_id,
                    "idempotency_key": idempotency_key, "request_sha256": request_sha256,
                },
            )
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="product_specification_selected",
                artifact=artifact,
            )
        return _planning_run_record(row)

    async def attach_product_specification_draft(
        self,
        run_id: str,
        artifact: ArtifactReference,
        planner_model: str,
        expected_product_specification_revision: int,
        generation_claim: str | None = None,
    ) -> PlanningRunRecord:
        """Atomically retain one generated product specification draft for a planning run."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    UPDATE supervisor_runs
                    SET product_specification_artifact_ref = :artifact_ref,
                        product_specification_artifact_sha256 = :artifact_sha256,
                        product_specification_revision = product_specification_revision + 1,
                        product_specification_generation_claim = NULL,
                        product_specification_generation_claimed_at = NULL
                    WHERE run_id = :run_id
                      AND status = 'planning'
                      AND product_specification_revision = :expected_product_specification_revision
                      AND (CAST(:generation_claim AS text) IS NULL OR product_specification_generation_claim = :generation_claim)
                    RETURNING run_id, status, source_artifact_ref, source_artifact_sha256,
                              target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                              plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                              implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                              product_specification_artifact_ref, product_specification_artifact_sha256,
                              product_specification_revision, selected_product_specification_artifact_ref,
                              selected_product_specification_artifact_sha256, selected_product_specification_revision,
                              specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                              specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                              selected_specification_evaluation_artifact_sha256
                    """
                ),
                {
                    "run_id": run_id,
                    "artifact_ref": artifact.ref,
                    "artifact_sha256": artifact.sha256,
                    "expected_product_specification_revision": expected_product_specification_revision,
                    "generation_claim": generation_claim,
                },
            )
            row = result.mappings().one_or_none()
            if row is None:
                raise ValueError("planning run is not eligible to accept a product specification draft")
            await connection.execute(
                text(
                    """
                    INSERT INTO product_specification_revisions (
                        run_id, revision, artifact_ref, artifact_sha256, planner_model, created_at
                    ) VALUES (
                        :run_id, :revision, :artifact_ref, :artifact_sha256, :planner_model, now()
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "revision": row["product_specification_revision"],
                    "artifact_ref": artifact.ref,
                    "artifact_sha256": artifact.sha256,
                    "planner_model": planner_model,
                },
            )
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="product_specification_draft_created",
                artifact=artifact,
            )
        return _planning_run_record(row)

    async def claim_product_specification_generation(self, run_id: str) -> str | None:
        """Reserve draft generation so a concurrent model response cannot create an orphan revision object."""

        claim = str(uuid.uuid4())
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """UPDATE supervisor_runs SET product_specification_generation_claim = :claim,
                                                   product_specification_generation_claimed_at = now()
                       WHERE run_id = :run_id AND status = 'planning'
                         AND product_specification_artifact_ref IS NULL
                         AND (
                             product_specification_generation_claim IS NULL
                             OR product_specification_generation_claimed_at IS NULL
                             OR product_specification_generation_claimed_at < now() - interval '15 minutes'
                         )
                       RETURNING product_specification_generation_claim"""
                ),
                {"run_id": run_id, "claim": claim},
            )
            row = result.mappings().one_or_none()
        return row["product_specification_generation_claim"] if row is not None else None

    async def release_product_specification_generation(self, run_id: str, generation_claim: str) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                text("""UPDATE supervisor_runs SET product_specification_generation_claim = NULL,
                                                     product_specification_generation_claimed_at = NULL
                        WHERE run_id = :run_id AND product_specification_generation_claim = :claim"""),
                {"run_id": run_id, "claim": generation_claim},
            )

    async def record_product_specification_generation_failure(
        self, run_id: str, generation_claim: str, message: str
    ) -> None:
        """Record a retryable draft-generation failure without changing run eligibility."""

        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT run_id FROM supervisor_runs
                    WHERE run_id = :run_id AND status = 'planning'
                      AND product_specification_artifact_ref IS NULL
                      AND product_specification_generation_claim = :generation_claim
                    """
                ),
                {"run_id": run_id, "generation_claim": generation_claim},
            )
            if result.scalar_one_or_none() is None:
                return
            await self._append_coordination_event(
                connection,
                run_id=run_id,
                event_type="product_specification_generation_failed",
                stage_id="product_specification",
                message=message,
                attempt_id=generation_claim,
            )

    async def attach_generated_plan(
        self,
        run_id: str,
        plan_artifact: ArtifactReference,
        planner_model: str,
        workflow_id: str,
        expected_plan_revision: int,
        expected_product_specification_revision: int | None = None,
        expected_product_specification_sha256: str | None = None,
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
                      AND (
                          :expected_product_specification_revision IS NULL
                          OR (
                              selected_product_specification_revision = :expected_product_specification_revision
                              AND selected_product_specification_artifact_sha256 = :expected_product_specification_sha256
                          )
                      )
                    RETURNING run_id, status, source_artifact_ref, source_artifact_sha256,
                              target_repos, spec_set, constraints, priority, submitted_at, submitted_by,
                              plan_artifact_ref, plan_artifact_sha256, planner_model, active_workflow_id, plan_revision,
                              implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                              product_specification_artifact_ref, product_specification_artifact_sha256,
                              product_specification_revision, selected_product_specification_artifact_ref,
                              selected_product_specification_artifact_sha256, selected_product_specification_revision,
                              specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                              specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                              selected_specification_evaluation_artifact_sha256
                    """
                ),
                {
                    "run_id": run_id,
                    "plan_artifact_ref": plan_artifact.ref,
                    "plan_artifact_sha256": plan_artifact.sha256,
                    "planner_model": planner_model,
                    "workflow_id": workflow_id,
                    "expected_plan_revision": expected_plan_revision,
                    "expected_product_specification_revision": expected_product_specification_revision,
                    "expected_product_specification_sha256": expected_product_specification_sha256,
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
            if expected_product_specification_revision is not None:
                await connection.execute(
                    text(
                        """
                        INSERT INTO plan_product_specification_bindings (
                            run_id, plan_revision, product_specification_revision, artifact_ref, artifact_sha256, created_at
                        ) VALUES (
                            :run_id, :plan_revision, :product_specification_revision, :artifact_ref, :artifact_sha256, now()
                        )
                        """
                    ),
                    {
                        "run_id": run_id,
                        "plan_revision": row["plan_revision"],
                        "product_specification_revision": expected_product_specification_revision,
                        "artifact_ref": row["selected_product_specification_artifact_ref"],
                        "artifact_sha256": expected_product_specification_sha256,
                    },
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
        mcp_selection: list[McpToolSelection] | None = None,
    ) -> ApprovalRecord:
        mcp_selection = _canonical_mcp_selection(mcp_selection)
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
                           request_sha256, plan_revision, mcp_selection
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
            existing_decision = await connection.execute(
                text(
                    """
                    SELECT decision_id
                    FROM plan_approval_decisions
                    WHERE run_id = :run_id AND plan_revision = :plan_revision
                    LIMIT 1
                    """
                ),
                {"run_id": run_id, "plan_revision": run_row["plan_revision"]},
            )
            if existing_decision.mappings().one_or_none() is not None:
                raise ApprovalConflictError("a plan approval decision is already recorded for this revision")
            if run_row["status"] != PlanningRunStatus.AWAITING_PLAN_APPROVAL.value:
                raise ApprovalConflictError("planning run is not awaiting plan approval")
            if run_row["plan_artifact_sha256"] != artifact_sha256:
                raise ApprovalConflictError("plan approval artifact digest is stale")
            if not run_row["active_workflow_id"]:
                raise ApprovalConflictError("planning workflow is not available for approval")
            await _require_mcp_selection_subset(connection, run_id, mcp_selection)

            decision_id = str(uuid.uuid4())
            created_at = datetime.now().astimezone()
            selection_json = _mcp_selection_json(mcp_selection)
            outbox_payload: dict[str, object] = {
                "decision_id": decision_id,
                "artifact_sha256": artifact_sha256,
                "decision": decision.value,
            }
            if selection_json is not None:
                outbox_payload["mcp_selection"] = selection_json
            await connection.execute(
                text(
                    """
                    INSERT INTO plan_approval_decisions (
                        decision_id, run_id, decision, artifact_sha256, actor_id, comment,
                        idempotency_key, request_sha256, created_at, plan_revision, mcp_selection
                    ) VALUES (
                        :decision_id, :run_id, :decision, :artifact_sha256, :actor_id, :comment,
                        :idempotency_key, :request_sha256, :created_at, :plan_revision, CAST(:mcp_selection AS jsonb)
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
                    "mcp_selection": json.dumps(selection_json) if selection_json is not None else None,
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
                    "payload": json.dumps(outbox_payload),
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
            mcp_selection=mcp_selection,
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
        message: str | None = None,
        attempt_id: str | None = None,
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
            "message": message[:512] if message else None,
            "attempt_id": attempt_id,
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
                           delivered_at, mcp_selection
                    FROM plan_approval_decisions
                    WHERE run_id = :run_id
                    UNION ALL
                    SELECT decision_id, run_id, 'implementation' AS gate, decision, artifact_sha256, actor_id,
                           created_at, delivered_at, NULL::jsonb AS mcp_selection
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
                mcp_selection=_mcp_selection_from_json(row["mcp_selection"]),
            )
            for row in rows
        ]

    async def get_specification_evaluation_waiver(
        self, run_id: str, artifact_sha256: str
    ) -> SpecificationEvaluationWaiverRecord | None:
        """Return the latest bounded exception record for its exact evaluation digest."""

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """SELECT artifact_sha256, actor_id, rationale, created_at
                       FROM specification_evaluation_waivers
                       WHERE run_id = :run_id AND artifact_sha256 = :artifact_sha256
                       ORDER BY created_at DESC, decision_id DESC LIMIT 1"""
                ),
                {"run_id": run_id, "artifact_sha256": artifact_sha256},
            )
            row = result.mappings().one_or_none()
        if row is None:
            return None
        return SpecificationEvaluationWaiverRecord(
            artifact_sha256=row["artifact_sha256"],
            actor_id=row["actor_id"],
            rationale=row["rationale"],
            created_at=row["created_at"].isoformat(),
        )

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
                    SELECT source_artifact_ref, source_artifact_sha256,
                           product_specification_artifact_ref, product_specification_artifact_sha256,
                           plan_artifact_ref, plan_artifact_sha256,
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
                "product_specification": (
                    run["product_specification_artifact_ref"],
                    run["product_specification_artifact_sha256"],
                ),
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
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                           product_specification_artifact_ref, product_specification_artifact_sha256,
                           product_specification_revision, product_specification_generation_claimed_at,
                           selected_product_specification_artifact_ref,
                           selected_product_specification_artifact_sha256, selected_product_specification_revision,
                           specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                           specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                           selected_specification_evaluation_artifact_sha256
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
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                           product_specification_artifact_ref, product_specification_artifact_sha256,
                           product_specification_revision, selected_product_specification_artifact_ref,
                           selected_product_specification_artifact_sha256, selected_product_specification_revision,
                           specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                           specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                           selected_specification_evaluation_artifact_sha256
                    FROM supervisor_runs
                    WHERE status IN ('awaiting_plan_approval', 'implementing', 'finalizing')
                      AND active_workflow_id IS NOT NULL
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
            "stopped_with_backup": (PlanningRunStatus.IMPLEMENTATION_FAILED.value, AgentRunStatus.TIMED_OUT.value),
        }.get(outcome)
        if target is None and outcome != "failed":
            return False
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
                PlanningRunStatus.AWAITING_PLAN_APPROVAL.value,
                PlanningRunStatus.IMPLEMENTING.value,
                PlanningRunStatus.FINALIZING.value,
            }:
                return False
            if outcome == "failed":
                target = (
                    PlanningRunStatus.PLANNING_FAILED.value
                    if row["planning_status"] == PlanningRunStatus.AWAITING_PLAN_APPROVAL.value
                    else PlanningRunStatus.IMPLEMENTATION_FAILED.value,
                    AgentRunStatus.FAILED.value,
                )
            assert target is not None
            planning_status, agent_status = target
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
                           implementation_artifact_ref, implementation_artifact_sha256, implementation_revision, project_id,
                           product_specification_artifact_ref, product_specification_artifact_sha256,
                           product_specification_revision, selected_product_specification_artifact_ref,
                           selected_product_specification_artifact_sha256, selected_product_specification_revision,
                           specification_evaluation_artifact_ref, specification_evaluation_artifact_sha256,
                           specification_evaluation_readiness, selected_specification_evaluation_artifact_ref,
                           selected_specification_evaluation_artifact_sha256
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
        mcp_selection=_mcp_selection_from_json(values["mcp_selection"]),  # type: ignore[index]
    )


def _mcp_tool_selection(row: Mapping[str, Any]) -> McpToolSelection:
    """Materialize one exact selection identity from durable resolution columns."""

    return McpToolSelection(
        role=row["role"],
        server_id=row["server_registration_id"],
        server_version=row["server_version"],
        server_manifest_sha256=row["server_manifest_sha256"],
        tool_name=row["tool_name"],
        input_schema_sha256=row["input_schema_sha256"],
        repository_scope=row["repository_scope"],
    )


def _mcp_selection_json(selection: list[McpToolSelection] | None) -> list[dict[str, str]] | None:
    """Serialize a canonical selection as non-secret durable JSON."""

    return [item.model_dump(mode="json") for item in selection] if selection is not None else None


def _mcp_selection_from_json(value: object) -> list[McpToolSelection] | None:
    """Validate persisted selection JSON before exposing it to approval or Workbench callers."""

    if value is None:
        return None
    if not isinstance(value, list):
        raise ApprovalConflictError("persisted MCP selection is invalid")
    return _canonical_mcp_selection([McpToolSelection.model_validate(item) for item in value])


def _canonical_mcp_selection(selection: list[McpToolSelection] | None) -> list[McpToolSelection] | None:
    """Canonicalize store inputs so retries always carry one exact selection."""

    if selection is None:
        return None
    if len({item.key() for item in selection}) != len(selection):
        raise ApprovalConflictError("MCP selection grants must be unique")
    return sorted(selection, key=McpToolSelection.key)


def _binding_targets_a_run_repository(
    server_id: str,
    server_version: str,
    target_repositories: list[str],
    target_repository_scopes: Mapping[str, str],
) -> bool:
    """Allow an MCP release only when its configured repository is in this run."""

    scope = target_repository_scopes.get(f"{server_id}@{server_version}")
    if scope is None:
        return True
    return scope.casefold() in {
        repository_id
        for target in target_repositories
        if (repository_id := _github_repository_id(target)) is not None
    }


def _github_repository_id(target: str) -> str | None:
    """Return a canonical GitHub owner/repository identity for an immutable target URL."""

    parsed = urlparse(target)
    if parsed.scheme != "https" or parsed.hostname != "github.com" or parsed.username or parsed.password:
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    repository = parts[1]
    if repository.casefold().endswith(".git"):
        repository = repository[:-4]
    return f"{parts[0]}/{repository}".casefold() if repository else None


async def _require_mcp_selection_subset(
    connection, run_id: str, selection: list[McpToolSelection] | None
) -> None:  # type: ignore[no-untyped-def]
    """Reject an approval selection that would expand a run's already-pinned policy grants."""

    if selection is None:
        return
    result = await connection.execute(
        text(
            """
            SELECT role, server_registration_id, server_version, server_manifest_sha256,
                   tool_name, input_schema_sha256, repository_scope
            FROM run_mcp_tool_resolutions
            WHERE run_id = :run_id AND role = 'developer'
            """
        ),
        {"run_id": run_id},
    )
    available = {_mcp_tool_selection(row).key() for row in result.mappings().all()}
    if any(item.key() not in available for item in selection):
        raise ApprovalConflictError("MCP selection is not a subset of the run's pinned policy grants")


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


def _workbench_agent_records(rows: list[Mapping[str, Any]]) -> list[WorkbenchAgentRecord]:
    """Group project routes under immutable releases, including historical releases."""

    grouped: dict[tuple[str, str], WorkbenchAgentRecord] = {}
    for row in rows:
        key = (row["registration_id"], row["registration_version"])
        route = (
            WorkbenchAgentGatewayRouteRecord(
                policy_revision=row["policy_revision"],
                role=row["role"],
                model_alias=row["model_alias"],
                max_budget_usd=float(row["max_budget_usd"]),
                toolset=row["toolset"],
            )
            if row["policy_revision"] is not None
            else None
        )
        current = grouped.get(key)
        if current is None:
            grouped[key] = WorkbenchAgentRecord(
                registration_id=row["registration_id"],
                registration_version=row["registration_version"],
                manifest_sha256=row["manifest_sha256"],
                component_id=row["component_id"],
                component_version=row["component_version"],
                lifecycle=row["lifecycle"],
                maturity=row["maturity"],
                execution_class=row["execution_class"],
                owner=row["owner"],
                capabilities=_json_string_list(row["capabilities"]),
                gateway_routes=[route] if route is not None else [],
            )
        elif route is not None and route not in current.gateway_routes:
            grouped[key] = WorkbenchAgentRecord(
                **{**current.__dict__, "gateway_routes": [*current.gateway_routes, route]}
            )
    return list(grouped.values())


def _workbench_agent_invocation_record(row: Mapping[str, Any]) -> WorkbenchAgentInvocationRecord:
    """Materialize a narrow run-role projection without copying unsafe agent-run columns."""

    return WorkbenchAgentInvocationRecord(
        run_id=row["run_id"],
        root_run_id=row["root_run_id"],
        parent_run_id=row["parent_run_id"],
        registration_id=row["registration_id"],
        registration_version=row["registration_version"],
        role=row["role"],
        run_lifecycle_status=AgentRunStatus(row["run_lifecycle_status"]),
        workflow_available=bool(row["workflow_available"]),
        created_at=row["created_at"].isoformat(),
        updated_at=row["updated_at"].isoformat(),
        gateway_route=(
            WorkbenchAgentGatewayRouteRecord(
                policy_revision=row["gateway_policy_revision"],
                role=row["role"],
                model_alias=row["model_alias"],
                max_budget_usd=float(row["max_budget_usd"]),
                toolset=row["toolset"],
            )
            if row["gateway_policy_revision"] is not None
            else None
        ),
        mcp_grants=[McpToolGrant.model_validate(item) for item in _json_list(row["mcp_grants"])],
        lifecycle_transitions=[],
    )


def _json_string_list(value: object) -> list[str]:
    """Return a persisted JSON string array or fail closed on malformed projection data."""

    items = _json_list(value)
    if not all(isinstance(item, str) for item in items):
        raise RegistryConflictError("persisted agent capabilities are invalid")
    return items


def _json_list(value: object) -> list[object]:
    """Normalize JSONB values returned by supported SQLAlchemy drivers."""

    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        raise RegistryConflictError("persisted agent operations projection is invalid")
    return value
