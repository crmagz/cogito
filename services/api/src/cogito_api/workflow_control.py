"""Platform-owned configuration and deterministic workflow admission.

This is an API control-plane store, not a Kubernetes operator.  Versions are
immutable once published; a project binding selects the allowed template and
repository context, and product managers can submit only ``SpecificationIntake``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .models import (
    ArtifactSchema,
    ModelTier,
    PlanConstraints,
    ProjectWorkflowBinding,
    ResolvedWorkflow,
    WorkflowConfigurationState,
    WorkflowGateDefinition,
    WorkflowPhaseDefinition,
    WorkflowPolicy,
    WorkflowTemplate,
)


class WorkflowConfigurationError(ValueError):
    """A configuration is missing, incompatible, or attempts a rewrite."""


def configuration_ref(identifier: str, version: str) -> str:
    return f"{identifier}@{version}"


def canonical_configuration_bytes(value: object) -> bytes:
    payload = value.model_dump(mode="json") if hasattr(value, "model_dump") else value
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _split_ref(reference: str) -> tuple[str, str]:
    identifier, separator, version = reference.rpartition("@")
    if not separator or not identifier or not version:
        raise WorkflowConfigurationError("workflow configuration references must use id@version")
    return identifier, version


@dataclass(frozen=True)
class WorkflowAdmission:
    binding: ProjectWorkflowBinding
    template: WorkflowTemplate
    policy: WorkflowPolicy


class WorkflowConfigurationStore(Protocol):
    async def bootstrap_defaults(self, *, project_id: str, constraints: PlanConstraints) -> None: ...

    async def put_template(self, template: WorkflowTemplate, *, actor: str) -> None: ...

    async def put_policy(self, policy: WorkflowPolicy, *, actor: str) -> None: ...

    async def put_binding(self, binding: ProjectWorkflowBinding, *, actor: str) -> None: ...

    async def get_template(self, reference: str) -> WorkflowTemplate | None: ...

    async def get_policy(self, reference: str) -> WorkflowPolicy | None: ...

    async def get_binding(self, project_id: str) -> ProjectWorkflowBinding | None: ...

    async def put_run_resolution(self, resolution: ResolvedWorkflow) -> None: ...

    async def get_run_resolution(self, run_id: str) -> ResolvedWorkflow | None: ...


def default_template() -> WorkflowTemplate:
    """Return the baseline governed graph every installation starts with."""

    return WorkflowTemplate(
        id="software_delivery",
        version="1.0.0",
        default_policy_ref="platform_standard@1.0.0",
        phases=[
            WorkflowPhaseDefinition(
                id="product_specification", kind="product_specification", agent_role="planner",
                permitted_tiers=[ModelTier.BALANCED, ModelTier.COMPLEX],
                output_schemas=[ArtifactSchema(schema_id="product_specification", version="2")],
            ),
            WorkflowPhaseDefinition(
                id="implementation_plan", kind="implementation_plan", agent_role="planner",
                depends_on=["product_specification"], permitted_tiers=[ModelTier.BALANCED, ModelTier.COMPLEX],
                output_schemas=[ArtifactSchema(schema_id="ai_plan", version="1")],
            ),
            WorkflowPhaseDefinition(
                id="implementation", kind="implementation", agent_role="developer",
                depends_on=["implementation_plan"], permitted_tiers=[ModelTier.COMPLEX],
                output_schemas=[ArtifactSchema(schema_id="implementation", version="1")],
            ),
        ],
        required_gates=[
            WorkflowGateDefinition(
                id="product_specification_review", approver_roles=["workflow_approver"],
                required_artifacts=[ArtifactSchema(schema_id="product_specification", version="2")],
                permitted_decisions=["approve", "request_revision", "reject"],
            ),
            WorkflowGateDefinition(
                id="plan_scope_review", approver_roles=["workflow_approver"],
                required_artifacts=[ArtifactSchema(schema_id="ai_plan", version="1")],
                permitted_decisions=["approve", "request_revision", "reject"],
            ),
            WorkflowGateDefinition(
                id="delivery_review", approver_roles=["workflow_approver"],
                required_artifacts=[ArtifactSchema(schema_id="implementation", version="1")],
                permitted_decisions=["approve", "request_revision", "reject"],
            ),
        ],
    )


def default_policy(project_id: str, constraints: PlanConstraints) -> WorkflowPolicy:
    return WorkflowPolicy(
        id="platform_standard", version="1.0.0", project_ids=[project_id], max_constraints=constraints,
        model_tier_profiles=[
            # These are logical aliases. The agent gateway remains the only
            # place that holds provider credentials and must be no broader.
            {"id": "fast", "version": "1.0.0", "tier": "fast", "model_alias": "fast", "max_budget_usd": min(1.0, constraints.max_cost_usd), "max_turns_per_phase": min(100, constraints.max_turns_per_phase)},
            {"id": "balanced", "version": "1.0.0", "tier": "balanced", "model_alias": "balanced", "max_budget_usd": min(5.0, constraints.max_cost_usd), "max_turns_per_phase": min(200, constraints.max_turns_per_phase)},
            {"id": "complex", "version": "1.0.0", "tier": "complex", "model_alias": "complex", "max_budget_usd": min(25.0, constraints.max_cost_usd), "max_turns_per_phase": min(500, constraints.max_turns_per_phase)},
        ],
        mandatory_phase_ids=["product_specification", "implementation_plan", "implementation"],
        required_gate_ids=["product_specification_review", "plan_scope_review", "delivery_review"],
    )


def validate_admission(binding: ProjectWorkflowBinding, template: WorkflowTemplate, policy: WorkflowPolicy) -> WorkflowAdmission:
    if binding.policy_ref is not None and binding.policy_ref != configuration_ref(policy.id, policy.version):
        raise WorkflowConfigurationError("project binding policy does not match its resolved policy")
    if binding.policy_ref is None and template.default_policy_ref != configuration_ref(policy.id, policy.version):
        raise WorkflowConfigurationError("workflow template default policy is not available")
    if binding.project_id not in policy.project_ids:
        raise WorkflowConfigurationError("workflow policy is not authorized for this project")
    phase_ids = {phase.id for phase in template.phases}
    gate_ids = {gate.id for gate in template.required_gates}
    missing_phases = set(policy.mandatory_phase_ids) - phase_ids
    missing_gates = set(policy.required_gate_ids) - gate_ids
    if missing_phases or missing_gates:
        raise WorkflowConfigurationError("workflow template does not satisfy mandatory policy phases and gates")
    tier_profiles = {profile.tier: profile for profile in policy.model_tier_profiles}
    for phase in template.phases:
        if phase.id not in policy.mandatory_phase_ids:
            continue
        permitted_profiles = [tier_profiles[tier] for tier in phase.permitted_tiers]
        if not any(
            profile.max_budget_usd <= binding.constraints.max_cost_usd
            and profile.max_turns_per_phase <= binding.constraints.max_turns_per_phase
            for profile in permitted_profiles
        ):
            raise WorkflowConfigurationError(
                f"project binding constraints cannot fund mandatory phase '{phase.id}'"
            )
    return WorkflowAdmission(binding=binding, template=template, policy=policy)


class InMemoryWorkflowConfigurationStore:
    def __init__(self) -> None:
        self.templates: dict[str, WorkflowTemplate] = {}
        self.policies: dict[str, WorkflowPolicy] = {}
        self.bindings: dict[str, ProjectWorkflowBinding] = {}
        self.resolutions: dict[str, ResolvedWorkflow] = {}

    async def bootstrap_defaults(self, *, project_id: str, constraints: PlanConstraints) -> None:
        await self.put_policy(default_policy(project_id, constraints), actor="bootstrap")
        await self.put_template(default_template(), actor="bootstrap")

    async def put_template(self, template: WorkflowTemplate, *, actor: str) -> None:
        reference = configuration_ref(template.id, template.version)
        existing = self.templates.get(reference)
        if existing is not None and existing != template:
            raise WorkflowConfigurationError("workflow template versions are immutable")
        self.templates[reference] = template

    async def put_policy(self, policy: WorkflowPolicy, *, actor: str) -> None:
        reference = configuration_ref(policy.id, policy.version)
        existing = self.policies.get(reference)
        if existing is not None and existing != policy:
            raise WorkflowConfigurationError("workflow policy versions are immutable")
        self.policies[reference] = policy

    async def put_binding(self, binding: ProjectWorkflowBinding, *, actor: str) -> None:
        self.bindings[binding.project_id] = binding

    async def get_template(self, reference: str) -> WorkflowTemplate | None:
        return self.templates.get(reference)

    async def get_policy(self, reference: str) -> WorkflowPolicy | None:
        return self.policies.get(reference)

    async def get_binding(self, project_id: str) -> ProjectWorkflowBinding | None:
        return self.bindings.get(project_id)

    async def put_run_resolution(self, resolution: ResolvedWorkflow) -> None:
        existing = self.resolutions.get(resolution.run_id)
        if existing is not None and existing != resolution:
            raise WorkflowConfigurationError("run workflow resolution is immutable")
        self.resolutions[resolution.run_id] = resolution

    async def get_run_resolution(self, run_id: str) -> ResolvedWorkflow | None:
        return self.resolutions.get(run_id)

    async def close(self) -> None:
        return None


class PostgresWorkflowConfigurationStore:
    """Postgres implementation with immutable documents and mutable bindings."""

    def __init__(self, database_url: str):
        self._engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)

    async def bootstrap_defaults(self, *, project_id: str, constraints: PlanConstraints) -> None:
        await self.put_policy(default_policy(project_id, constraints), actor="bootstrap")
        await self.put_template(default_template(), actor="bootstrap")

    async def _put(self, kind: str, identifier: str, version: str, value: object, actor: str) -> None:
        payload = canonical_configuration_bytes(value).decode()
        digest = sha256(payload.encode()).hexdigest()
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text("SELECT payload_sha256 FROM workflow_configuration_versions WHERE kind = :kind AND identifier = :identifier AND version = :version"),
                {"kind": kind, "identifier": identifier, "version": version},
            )
            existing = result.scalar_one_or_none()
            if existing is not None:
                if existing != digest:
                    raise WorkflowConfigurationError(f"{kind} versions are immutable")
                return
            await connection.execute(
                text("""INSERT INTO workflow_configuration_versions
                    (kind, identifier, version, state, payload, payload_sha256, created_by, created_at)
                    VALUES (:kind, :identifier, :version, :state, CAST(:payload AS jsonb), :digest, :actor, :created_at)"""),
                {"kind": kind, "identifier": identifier, "version": version, "state": WorkflowConfigurationState.PUBLISHED.value,
                 "payload": payload, "digest": digest, "actor": actor, "created_at": datetime.now(timezone.utc)},
            )

    async def put_template(self, template: WorkflowTemplate, *, actor: str) -> None:
        await self._put("template", template.id, template.version, template, actor)

    async def put_policy(self, policy: WorkflowPolicy, *, actor: str) -> None:
        await self._put("policy", policy.id, policy.version, policy, actor)

    async def put_binding(self, binding: ProjectWorkflowBinding, *, actor: str) -> None:
        payload = canonical_configuration_bytes(binding).decode()
        async with self._engine.begin() as connection:
            await connection.execute(
                text("""INSERT INTO project_workflow_bindings (project_id, payload, updated_by, updated_at)
                    VALUES (:project_id, CAST(:payload AS jsonb), :actor, :updated_at)
                    ON CONFLICT (project_id) DO UPDATE SET payload = EXCLUDED.payload, updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at"""),
                {"project_id": binding.project_id, "payload": payload, "actor": actor, "updated_at": datetime.now(timezone.utc)},
            )

    async def _get(self, kind: str, reference: str, model: type[WorkflowTemplate] | type[WorkflowPolicy]):
        identifier, version = _split_ref(reference)
        async with self._engine.connect() as connection:
            row = (await connection.execute(
                text("SELECT payload FROM workflow_configuration_versions WHERE kind = :kind AND identifier = :identifier AND version = :version AND state = :state"),
                {"kind": kind, "identifier": identifier, "version": version, "state": WorkflowConfigurationState.PUBLISHED.value},
            )).mappings().one_or_none()
        return model.model_validate(row["payload"]) if row is not None else None

    async def get_template(self, reference: str) -> WorkflowTemplate | None:
        return await self._get("template", reference, WorkflowTemplate)

    async def get_policy(self, reference: str) -> WorkflowPolicy | None:
        return await self._get("policy", reference, WorkflowPolicy)

    async def get_binding(self, project_id: str) -> ProjectWorkflowBinding | None:
        async with self._engine.connect() as connection:
            row = (await connection.execute(
                text("SELECT payload FROM project_workflow_bindings WHERE project_id = :project_id"), {"project_id": project_id}
            )).mappings().one_or_none()
        return ProjectWorkflowBinding.model_validate(row["payload"]) if row is not None else None

    async def put_run_resolution(self, resolution: ResolvedWorkflow) -> None:
        payload = canonical_configuration_bytes(resolution).decode()
        digest = sha256(payload.encode()).hexdigest()
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text("SELECT payload_sha256 FROM run_workflow_resolutions WHERE run_id = :run_id"), {"run_id": resolution.run_id}
            )
            existing = result.scalar_one_or_none()
            if existing is not None:
                if existing != digest:
                    raise WorkflowConfigurationError("run workflow resolution is immutable")
                return
            await connection.execute(
                text("""INSERT INTO run_workflow_resolutions (run_id, payload, payload_sha256, created_at)
                    VALUES (:run_id, CAST(:payload AS jsonb), :digest, :created_at)"""),
                {"run_id": resolution.run_id, "payload": payload, "digest": digest, "created_at": datetime.now(timezone.utc)},
            )

    async def get_run_resolution(self, run_id: str) -> ResolvedWorkflow | None:
        async with self._engine.connect() as connection:
            row = (await connection.execute(
                text("SELECT payload FROM run_workflow_resolutions WHERE run_id = :run_id"), {"run_id": run_id}
            )).mappings().one_or_none()
        return ResolvedWorkflow.model_validate(row["payload"]) if row is not None else None

    async def close(self) -> None:
        await self._engine.dispose()
