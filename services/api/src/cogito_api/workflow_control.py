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
    WorkflowAdmissionSnapshot,
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

    async def create_template_draft(self, template: WorkflowTemplate, *, actor: str) -> None: ...

    async def create_policy_draft(self, policy: WorkflowPolicy, *, actor: str) -> None: ...

    async def transition_template(self, reference: str, target: WorkflowConfigurationState, *, actor: str) -> WorkflowConfigurationState: ...

    async def transition_policy(self, reference: str, target: WorkflowConfigurationState, *, actor: str) -> WorkflowConfigurationState: ...

    async def get_template(self, reference: str) -> WorkflowTemplate | None: ...

    async def get_policy(self, reference: str) -> WorkflowPolicy | None: ...

    async def get_template_configuration(
        self, reference: str
    ) -> tuple[WorkflowTemplate, WorkflowConfigurationState] | None: ...

    async def get_policy_configuration(
        self, reference: str
    ) -> tuple[WorkflowPolicy, WorkflowConfigurationState] | None: ...

    async def get_binding(self, project_id: str) -> ProjectWorkflowBinding | None: ...

    async def put_run_resolution(self, resolution: ResolvedWorkflow) -> None: ...

    async def get_run_resolution(self, run_id: str) -> ResolvedWorkflow | None: ...

    async def put_run_admission(self, admission: WorkflowAdmissionSnapshot) -> None: ...

    async def get_run_admission(self, run_id: str) -> WorkflowAdmissionSnapshot | None: ...


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
                # A revision requires a complete replacement specification,
                # and rejection is an explicit cancellation command. Keeping
                # this gate to the executable promotion command prevents a
                # decision token from being mistaken for either action.
                permitted_decisions=["approve"],
                separation_of_duties=False,
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


def validate_template_policy(template: WorkflowTemplate, policy: WorkflowPolicy) -> None:
    """Reject a published template/policy pair the runtime cannot admit."""

    phase_ids = {phase.id for phase in template.phases}
    gate_ids = {gate.id for gate in template.required_gates}
    missing_phases = set(policy.mandatory_phase_ids) - phase_ids
    missing_gates = set(policy.required_gate_ids) - gate_ids
    if missing_phases or missing_gates:
        raise WorkflowConfigurationError("workflow template does not satisfy mandatory policy phases and gates")
    tier_profiles = {profile.tier for profile in policy.model_tier_profiles}
    for phase in template.phases:
        if not set(phase.permitted_tiers).issubset(tier_profiles):
            raise WorkflowConfigurationError(
                f"workflow template phase '{phase.id}' references a model tier unavailable in its policy"
            )


def validate_admission(binding: ProjectWorkflowBinding, template: WorkflowTemplate, policy: WorkflowPolicy) -> WorkflowAdmission:
    if binding.policy_ref is not None and binding.policy_ref != configuration_ref(policy.id, policy.version):
        raise WorkflowConfigurationError("project binding policy does not match its resolved policy")
    if binding.policy_ref is None and template.default_policy_ref != configuration_ref(policy.id, policy.version):
        raise WorkflowConfigurationError("workflow template default policy is not available")
    if binding.project_id not in policy.project_ids:
        raise WorkflowConfigurationError("workflow policy is not authorized for this project")
    validate_template_policy(template, policy)
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
        self.admissions: dict[str, WorkflowAdmissionSnapshot] = {}
        self.template_states: dict[str, WorkflowConfigurationState] = {}
        self.policy_states: dict[str, WorkflowConfigurationState] = {}
        self.lifecycle_events: list[tuple[str, str, WorkflowConfigurationState, WorkflowConfigurationState, str]] = []

    async def bootstrap_defaults(self, *, project_id: str, constraints: PlanConstraints) -> None:
        if await self.get_policy_configuration("platform_standard@1.0.0") is None:
            await self.put_policy(default_policy(project_id, constraints), actor="bootstrap")
        if await self.get_template_configuration("software_delivery@1.0.0") is None:
            await self.put_template(default_template(), actor="bootstrap")

    async def put_template(self, template: WorkflowTemplate, *, actor: str) -> None:
        reference = configuration_ref(template.id, template.version)
        existing = self.templates.get(reference)
        if existing is not None:
            if existing != template:
                raise WorkflowConfigurationError("workflow template versions are immutable")
            if self.template_states.get(reference) is not WorkflowConfigurationState.PUBLISHED:
                raise WorkflowConfigurationError("workflow template draft must use the lifecycle transition endpoint")
            return
        self.templates[reference] = template
        self.template_states[reference] = WorkflowConfigurationState.PUBLISHED

    async def put_policy(self, policy: WorkflowPolicy, *, actor: str) -> None:
        reference = configuration_ref(policy.id, policy.version)
        existing = self.policies.get(reference)
        if existing is not None:
            if existing != policy:
                raise WorkflowConfigurationError("workflow policy versions are immutable")
            if self.policy_states.get(reference) is not WorkflowConfigurationState.PUBLISHED:
                raise WorkflowConfigurationError("workflow policy draft must use the lifecycle transition endpoint")
            return
        self.policies[reference] = policy
        self.policy_states[reference] = WorkflowConfigurationState.PUBLISHED

    async def put_binding(self, binding: ProjectWorkflowBinding, *, actor: str) -> None:
        self.bindings[binding.project_id] = binding

    async def get_template(self, reference: str) -> WorkflowTemplate | None:
        return self.templates.get(reference) if self.template_states.get(reference) is WorkflowConfigurationState.PUBLISHED else None

    async def get_policy(self, reference: str) -> WorkflowPolicy | None:
        return self.policies.get(reference) if self.policy_states.get(reference) is WorkflowConfigurationState.PUBLISHED else None

    async def get_template_configuration(
        self, reference: str
    ) -> tuple[WorkflowTemplate, WorkflowConfigurationState] | None:
        template = self.templates.get(reference)
        state = self.template_states.get(reference)
        return (template, state) if template is not None and state is not None else None

    async def get_policy_configuration(
        self, reference: str
    ) -> tuple[WorkflowPolicy, WorkflowConfigurationState] | None:
        policy = self.policies.get(reference)
        state = self.policy_states.get(reference)
        return (policy, state) if policy is not None and state is not None else None

    async def create_template_draft(self, template: WorkflowTemplate, *, actor: str) -> None:
        del actor
        reference = configuration_ref(template.id, template.version)
        if reference in self.templates:
            raise WorkflowConfigurationError("workflow template versions are immutable")
        self.templates[reference] = template
        self.template_states[reference] = WorkflowConfigurationState.DRAFT

    async def create_policy_draft(self, policy: WorkflowPolicy, *, actor: str) -> None:
        del actor
        reference = configuration_ref(policy.id, policy.version)
        if reference in self.policies:
            raise WorkflowConfigurationError("workflow policy versions are immutable")
        self.policies[reference] = policy
        self.policy_states[reference] = WorkflowConfigurationState.DRAFT

    async def _transition(
        self, kind: str, reference: str, target: WorkflowConfigurationState, *, actor: str
    ) -> WorkflowConfigurationState:
        states = self.template_states if kind == "template" else self.policy_states
        values = self.templates if kind == "template" else self.policies
        current = states.get(reference)
        if reference not in values or current is None:
            raise WorkflowConfigurationError(f"workflow {kind} version does not exist")
        allowed = {
            WorkflowConfigurationState.DRAFT: {WorkflowConfigurationState.VALIDATED, WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.VALIDATED: {WorkflowConfigurationState.PUBLISHED, WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.PUBLISHED: {WorkflowConfigurationState.DEPRECATED, WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.DEPRECATED: {WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.REVOKED: set(),
        }
        if target not in allowed[current]:
            raise WorkflowConfigurationError(f"workflow {kind} cannot transition from {current.value} to {target.value}")
        states[reference] = target
        self.lifecycle_events.append((kind, reference, current, target, actor))
        return target

    async def transition_template(
        self, reference: str, target: WorkflowConfigurationState, *, actor: str
    ) -> WorkflowConfigurationState:
        return await self._transition("template", reference, target, actor=actor)

    async def transition_policy(
        self, reference: str, target: WorkflowConfigurationState, *, actor: str
    ) -> WorkflowConfigurationState:
        return await self._transition("policy", reference, target, actor=actor)

    async def get_binding(self, project_id: str) -> ProjectWorkflowBinding | None:
        return self.bindings.get(project_id)

    async def put_run_resolution(self, resolution: ResolvedWorkflow) -> None:
        existing = self.resolutions.get(resolution.run_id)
        if existing is not None and existing != resolution:
            raise WorkflowConfigurationError("run workflow resolution is immutable")
        self.resolutions[resolution.run_id] = resolution

    async def get_run_resolution(self, run_id: str) -> ResolvedWorkflow | None:
        return self.resolutions.get(run_id)

    async def put_run_admission(self, admission: WorkflowAdmissionSnapshot) -> None:
        existing = self.admissions.get(admission.run_id)
        if existing is not None and existing != admission:
            raise WorkflowConfigurationError("run workflow admission is immutable")
        self.admissions[admission.run_id] = admission

    async def get_run_admission(self, run_id: str) -> WorkflowAdmissionSnapshot | None:
        return self.admissions.get(run_id)

    async def close(self) -> None:
        return None


class PostgresWorkflowConfigurationStore:
    """Postgres implementation with immutable documents and mutable bindings."""

    def __init__(self, database_url: str):
        self._engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)

    async def bootstrap_defaults(self, *, project_id: str, constraints: PlanConstraints) -> None:
        if await self.get_policy_configuration("platform_standard@1.0.0") is None:
            await self.put_policy(default_policy(project_id, constraints), actor="bootstrap")
        if await self.get_template_configuration("software_delivery@1.0.0") is None:
            await self.put_template(default_template(), actor="bootstrap")

    async def _put(
        self,
        kind: str,
        identifier: str,
        version: str,
        value: object,
        actor: str,
        state: WorkflowConfigurationState = WorkflowConfigurationState.PUBLISHED,
    ) -> None:
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
                if state is not WorkflowConfigurationState.PUBLISHED:
                    raise WorkflowConfigurationError(f"workflow {kind} version already exists")
                current_state = (await connection.execute(
                    text("SELECT state FROM workflow_configuration_versions WHERE kind = :kind AND identifier = :identifier AND version = :version"),
                    {"kind": kind, "identifier": identifier, "version": version},
                )).scalar_one()
                if current_state != WorkflowConfigurationState.PUBLISHED.value:
                    raise WorkflowConfigurationError(f"workflow {kind} draft must use the lifecycle transition endpoint")
                return
            await connection.execute(
                text("""INSERT INTO workflow_configuration_versions
                    (kind, identifier, version, state, payload, payload_sha256, created_by, created_at)
                    VALUES (:kind, :identifier, :version, :state, CAST(:payload AS jsonb), :digest, :actor, :created_at)"""),
                {"kind": kind, "identifier": identifier, "version": version, "state": state.value,
                 "payload": payload, "digest": digest, "actor": actor, "created_at": datetime.now(timezone.utc)},
            )

    async def put_template(self, template: WorkflowTemplate, *, actor: str) -> None:
        await self._put("template", template.id, template.version, template, actor)

    async def put_policy(self, policy: WorkflowPolicy, *, actor: str) -> None:
        await self._put("policy", policy.id, policy.version, policy, actor)

    async def create_template_draft(self, template: WorkflowTemplate, *, actor: str) -> None:
        await self._put(
            "template", template.id, template.version, template, actor, WorkflowConfigurationState.DRAFT
        )

    async def create_policy_draft(self, policy: WorkflowPolicy, *, actor: str) -> None:
        await self._put("policy", policy.id, policy.version, policy, actor, WorkflowConfigurationState.DRAFT)

    async def _transition(
        self, kind: str, reference: str, target: WorkflowConfigurationState, *, actor: str
    ) -> WorkflowConfigurationState:
        identifier, version = _split_ref(reference)
        allowed = {
            WorkflowConfigurationState.DRAFT: {WorkflowConfigurationState.VALIDATED, WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.VALIDATED: {WorkflowConfigurationState.PUBLISHED, WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.PUBLISHED: {WorkflowConfigurationState.DEPRECATED, WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.DEPRECATED: {WorkflowConfigurationState.REVOKED},
            WorkflowConfigurationState.REVOKED: set(),
        }
        async with self._engine.begin() as connection:
            row = (await connection.execute(
                text("SELECT state FROM workflow_configuration_versions WHERE kind = :kind AND identifier = :identifier AND version = :version FOR UPDATE"),
                {"kind": kind, "identifier": identifier, "version": version},
            )).mappings().one_or_none()
            if row is None:
                raise WorkflowConfigurationError(f"workflow {kind} version does not exist")
            current = WorkflowConfigurationState(row["state"])
            if target not in allowed[current]:
                raise WorkflowConfigurationError(f"workflow {kind} cannot transition from {current.value} to {target.value}")
            await connection.execute(
                text("UPDATE workflow_configuration_versions SET state = :target WHERE kind = :kind AND identifier = :identifier AND version = :version"),
                {"target": target.value, "kind": kind, "identifier": identifier, "version": version},
            )
            await connection.execute(
                text("""INSERT INTO workflow_configuration_events
                    (event_id, kind, identifier, version, from_state, to_state, actor_id, created_at)
                    VALUES (:event_id, :kind, :identifier, :version, :from_state, :to_state, :actor_id, :created_at)"""),
                {
                    "event_id": sha256(
                        f"{kind}:{identifier}:{version}:{current.value}:{target.value}:{actor}:{datetime.now(timezone.utc).isoformat()}".encode()
                    ).hexdigest(),
                    "kind": kind,
                    "identifier": identifier,
                    "version": version,
                    "from_state": current.value,
                    "to_state": target.value,
                    "actor_id": actor,
                    "created_at": datetime.now(timezone.utc),
                },
            )
        return target

    async def transition_template(
        self, reference: str, target: WorkflowConfigurationState, *, actor: str
    ) -> WorkflowConfigurationState:
        return await self._transition("template", reference, target, actor=actor)

    async def transition_policy(
        self, reference: str, target: WorkflowConfigurationState, *, actor: str
    ) -> WorkflowConfigurationState:
        return await self._transition("policy", reference, target, actor=actor)

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

    async def _get_configuration(self, kind: str, reference: str, model: type[WorkflowTemplate] | type[WorkflowPolicy]):
        identifier, version = _split_ref(reference)
        async with self._engine.connect() as connection:
            row = (await connection.execute(
                text("SELECT payload, state FROM workflow_configuration_versions WHERE kind = :kind AND identifier = :identifier AND version = :version"),
                {"kind": kind, "identifier": identifier, "version": version},
            )).mappings().one_or_none()
        if row is None:
            return None
        return model.model_validate(row["payload"]), WorkflowConfigurationState(row["state"])

    async def get_template_configuration(
        self, reference: str
    ) -> tuple[WorkflowTemplate, WorkflowConfigurationState] | None:
        return await self._get_configuration("template", reference, WorkflowTemplate)

    async def get_policy_configuration(
        self, reference: str
    ) -> tuple[WorkflowPolicy, WorkflowConfigurationState] | None:
        return await self._get_configuration("policy", reference, WorkflowPolicy)

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

    async def put_run_admission(self, admission: WorkflowAdmissionSnapshot) -> None:
        payload = canonical_configuration_bytes(admission).decode()
        digest = sha256(payload.encode()).hexdigest()
        async with self._engine.begin() as connection:
            existing = (await connection.execute(
                text("SELECT payload_sha256 FROM run_workflow_admissions WHERE run_id = :run_id"), {"run_id": admission.run_id}
            )).scalar_one_or_none()
            if existing is not None:
                if existing != digest:
                    raise WorkflowConfigurationError("run workflow admission is immutable")
                return
            await connection.execute(
                text("""INSERT INTO run_workflow_admissions (run_id, payload, payload_sha256, created_at)
                    VALUES (:run_id, CAST(:payload AS jsonb), :digest, :created_at)"""),
                {"run_id": admission.run_id, "payload": payload, "digest": digest, "created_at": datetime.now(timezone.utc)},
            )

    async def get_run_admission(self, run_id: str) -> WorkflowAdmissionSnapshot | None:
        async with self._engine.connect() as connection:
            row = (await connection.execute(
                text("SELECT payload FROM run_workflow_admissions WHERE run_id = :run_id"), {"run_id": run_id}
            )).mappings().one_or_none()
        return WorkflowAdmissionSnapshot.model_validate(row["payload"]) if row is not None else None

    async def close(self) -> None:
        await self._engine.dispose()
