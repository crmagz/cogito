from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from cogito_api.models import (
    AgentGatewayPolicy,
    AgentGatewayResolution,
    AgentRunStatus,
    AiPlan,
    ArtifactReference,
    ImplementationApprovalDecision,
    PlanApprovalDecision,
    PlanningRunStatus,
    ProductSpecification,
    SpecificationEvaluation,
    McpBindingPolicy,
    McpToolGrant,
    McpToolSelection,
    RegistrationManifest,
    RegistrationReference,
    RunEnvelope,
    WorkbenchFeedbackIntent,
)
from cogito_api.registry import manifest_sha256, registration_reference
from cogito_api.planner import PlanningContext, ProductSpecificationContext
from cogito_api.storage import (
    PlanSnapshot,
    plan_snapshot_bytes,
    product_specification_bytes,
    specification_evaluation_bytes,
    source_specification_bytes,
)
from cogito_api.supervisor import (
    AgentRunRecord,
    ApprovalConflictError,
    ApprovalRecord,
    CoordinationEvent,
    ImplementationApprovalRecord,
    NotificationDelivery,
    OutboxDelivery,
    PlanningRunRecord,
    WorkbenchAgentGatewayRouteRecord,
    WorkbenchAgentInvocationRecord,
    WorkbenchAgentLifecycleTransitionRecord,
    WorkbenchAgentRecord,
    WorkbenchApprovalRecord,
    WorkbenchFeedbackRecord,
    RegistryConflictError,
    _binding_targets_a_run_repository,
)


class InMemoryPlanStore:
    def __init__(self) -> None:
        self.plans: dict[str, AiPlan] = {}
        self.statuses: dict[str, dict] = {}
        self.source_specifications: dict[str, str] = {}
        self.product_specifications: dict[tuple[str, int], ProductSpecification] = {}
        self.specification_evaluations: dict[tuple[str, int], SpecificationEvaluation] = {}
        self.artifacts: dict[str, bytes] = {}

    def put_plan(self, run_id: str, plan: AiPlan) -> PlanSnapshot:
        self.plans[run_id] = plan
        from hashlib import sha256

        return PlanSnapshot(
            ref=f"s3://plans/plans/{run_id}/plan.json",
            sha256=sha256(plan_snapshot_bytes(plan)).hexdigest(),
        )

    def put_planning_plan(self, run_id: str, revision: int, plan: AiPlan) -> PlanSnapshot:
        from hashlib import sha256

        self.plans[run_id] = plan
        digest = sha256(plan_snapshot_bytes(plan)).hexdigest()
        return PlanSnapshot(
            ref=f"s3://plans/plans/{run_id}/revisions/{revision}/{digest}/plan.json",
            sha256=digest,
        )

    def put_status(self, run_id: str, status: dict) -> None:
        self.statuses[run_id] = status

    def get_status(self, run_id: str) -> dict | None:
        return self.statuses.get(run_id)

    def put_source_specification(self, run_id: str, initial_specification: str) -> ArtifactReference:
        from hashlib import sha256

        self.source_specifications[run_id] = initial_specification
        return ArtifactReference(
            ref=f"s3://plan-snapshots/runs/{run_id}/source-spec.json",
            sha256=sha256(source_specification_bytes(initial_specification)).hexdigest(),
        )

    def put_product_specification(
        self, run_id: str, revision: int, specification: ProductSpecification
    ) -> ArtifactReference:
        from hashlib import sha256

        self.product_specifications[(run_id, revision)] = specification
        data = product_specification_bytes(specification)
        digest = sha256(data).hexdigest()
        return ArtifactReference(
            ref=f"s3://plan-snapshots/runs/{run_id}/product-specifications/{revision}/{digest}/specification.json",
            sha256=digest,
        )

    def put_specification_evaluation(
        self, run_id: str, specification_revision: int, evaluation: SpecificationEvaluation
    ) -> ArtifactReference:
        from hashlib import sha256

        self.specification_evaluations[(run_id, specification_revision)] = evaluation
        data = specification_evaluation_bytes(evaluation)
        digest = sha256(data).hexdigest()
        return ArtifactReference(
            ref=f"s3://plan-snapshots/runs/{run_id}/specification-evaluations/{specification_revision}/{digest}/evaluation.json",
            sha256=digest,
        )

    def put_artifact(self, ref: str, content: bytes) -> ArtifactReference:
        """Store a test-only immutable artifact with its matching digest."""

        from hashlib import sha256

        self.artifacts[ref] = content
        return ArtifactReference(ref=ref, sha256=sha256(content).hexdigest())

    def get_source_specification(self, source_artifact_ref: str) -> str:
        run_id = source_artifact_ref.split("/")[4]
        return self.source_specifications[run_id]

    def get_verified_artifact(self, artifact: ArtifactReference, *, max_bytes: int) -> bytes:
        if artifact.ref in self.artifacts:
            body = self.artifacts[artifact.ref]
        elif "/source-spec.json" in artifact.ref:
            run_id = artifact.ref.split("/")[4]
            body = source_specification_bytes(self.source_specifications[run_id])
        elif "/product-specifications/" in artifact.ref:
            parts = artifact.ref.split("/")
            run_id = parts[4]
            revision = int(parts[6])
            body = product_specification_bytes(self.product_specifications[(run_id, revision)])
        elif "/specification-evaluations/" in artifact.ref:
            parts = artifact.ref.split("/")
            run_id = parts[4]
            revision = int(parts[6])
            body = specification_evaluation_bytes(self.specification_evaluations[(run_id, revision)])
        else:
            run_id = artifact.ref.split("/")[4]
            body = plan_snapshot_bytes(self.plans[run_id])
        if len(body) > max_bytes:
            raise ValueError("artifact exceeds the Workbench evidence limit")
        from hashlib import sha256

        if sha256(body).hexdigest() != artifact.sha256:
            raise ValueError("artifact digest does not match its immutable reference")
        return body


class InMemorySupervisorStore:
    def __init__(self) -> None:
        self.planning_runs: dict[str, PlanningRunRecord] = {}
        self.product_specification_generation_claims: dict[str, str] = {}
        self.product_specification_generation_claimed_at: dict[str, datetime] = {}
        self.specification_evaluation_generation_claims: dict[str, str] = {}
        self.plan_product_specification_bindings: dict[tuple[str, int], tuple[int, ArtifactReference]] = {}
        self.approvals: dict[tuple[str, int, str], ApprovalRecord] = {}
        self.approval_request_hashes: dict[tuple[str, int, str], str] = {}
        self.outbox: dict[str, OutboxDelivery] = {}
        self.leased_decision_ids: set[str] = set()
        self.agent_runs: dict[str, AgentRunRecord] = {}
        self.agent_run_lifecycle_transitions: dict[str, list[WorkbenchAgentLifecycleTransitionRecord]] = {}
        self.implementation_approvals: dict[tuple[str, int, str], ImplementationApprovalRecord] = {}
        self.implementation_request_hashes: dict[tuple[str, int, str], str] = {}
        self.implementation_outbox: dict[str, OutboxDelivery] = {}
        self.registrations: dict[tuple[str, str], RegistrationManifest] = {}
        self.registry_policies: dict[str, dict[str, str]] = {}
        self.registry_mcp_policies: dict[str, McpBindingPolicy] = {}
        self.registry_agent_gateway_policies: dict[str, AgentGatewayPolicy] = {}
        self.run_registration_resolutions: dict[tuple[str, str], RegistrationReference] = {}
        self.run_mcp_tool_resolutions: dict[tuple[str, str], list[McpToolGrant]] = {}
        self.run_agent_gateway_resolutions: dict[tuple[str, str], AgentGatewayResolution] = {}
        self.coordination_events: dict[str, CoordinationEvent] = {}
        self.notification_deliveries: dict[str, tuple[bool, int, str | None]] = {}
        self.leased_notification_event_ids: set[str] = set()
        self.workbench_feedback: dict[tuple[str, str], WorkbenchFeedbackRecord] = {}
        self.workbench_feedback_request_hashes: dict[tuple[str, str], str] = {}

    async def create_agent_run(self, record: AgentRunRecord) -> None:
        self.agent_runs[record.run_id] = record

    async def get_agent_run(self, run_id: str) -> AgentRunRecord | None:
        return self.agent_runs.get(run_id)

    async def list_workbench_agents(
        self, *, project_id: str, policy_revision: str, limit: int = 50
    ) -> list[WorkbenchAgentRecord]:
        records = self._workbench_agents(project_id, policy_revision)
        return records[: max(1, min(limit, 100))]

    async def get_workbench_agent(
        self, *, project_id: str, policy_revision: str, registration_id: str, registration_version: str
    ) -> WorkbenchAgentRecord | None:
        return next(
            (
                record
                for record in self._workbench_agents(project_id, policy_revision)
                if record.registration_id == registration_id and record.registration_version == registration_version
            ),
            None,
        )

    def _workbench_agents(self, project_id: str, policy_revision: str) -> list[WorkbenchAgentRecord]:
        grouped: dict[tuple[str, str], WorkbenchAgentRecord] = {}
        policy = self.registry_agent_gateway_policies.get(policy_revision)
        if policy is not None:
            for binding in policy.bindings:
                if project_id not in binding.project_ids:
                    continue
                manifest = self.registrations.get((binding.registration_id, binding.registration_version))
                if manifest is None or manifest.kind.value != "agent" or manifest.lifecycle.value != "active":
                    continue
                key = (manifest.registration_id, manifest.version)
                route = WorkbenchAgentGatewayRouteRecord(
                    policy_revision=policy.policy_revision,
                    role=binding.role,
                    model_alias=binding.model_alias,
                    max_budget_usd=binding.max_budget_usd,
                    toolset=binding.toolset,
                )
                current = grouped.get(key)
                if current is None:
                    grouped[key] = WorkbenchAgentRecord(
                        registration_id=manifest.registration_id,
                        registration_version=manifest.version,
                        manifest_sha256=manifest_sha256(manifest),
                        component_id=manifest.component_id,
                        component_version=manifest.component_version,
                        lifecycle=manifest.lifecycle.value,
                        maturity=manifest.maturity.value,
                        execution_class=manifest.execution_class.value,
                        owner=manifest.owner,
                        capabilities=list(manifest.capabilities),
                        gateway_routes=[route],
                    )
                elif route not in current.gateway_routes:
                    grouped[key] = replace(current, gateway_routes=[*current.gateway_routes, route])
        for invocation in self._workbench_agent_invocations(project_id, None, None):
            key = (invocation.registration_id, invocation.registration_version)
            if key in grouped:
                continue
            manifest = self.registrations.get(key)
            if manifest is None or manifest.kind.value != "agent":
                continue
            grouped[key] = WorkbenchAgentRecord(
                registration_id=manifest.registration_id,
                registration_version=manifest.version,
                manifest_sha256=manifest_sha256(manifest),
                component_id=manifest.component_id,
                component_version=manifest.component_version,
                lifecycle=manifest.lifecycle.value,
                maturity=manifest.maturity.value,
                execution_class=manifest.execution_class.value,
                owner=manifest.owner,
                capabilities=list(manifest.capabilities),
                gateway_routes=[],
            )
        return sorted(grouped.values(), key=lambda item: (item.registration_id, item.registration_version))

    async def list_workbench_agent_invocations(
        self, *, project_id: str, registration_id: str, registration_version: str, limit: int = 50
    ) -> list[WorkbenchAgentInvocationRecord]:
        records = self._workbench_agent_invocations(project_id, registration_id, registration_version)
        return records[: max(1, min(limit, 100))]

    async def get_workbench_agent_invocation(
        self, *, project_id: str, run_id: str, role: str
    ) -> WorkbenchAgentInvocationRecord | None:
        return next(
            (
                record
                for record in self._workbench_agent_invocations(project_id, None, None)
                if record.run_id == run_id and record.role == role
            ),
            None,
        )

    def _workbench_agent_invocations(
        self, project_id: str, registration_id: str | None, registration_version: str | None
    ) -> list[WorkbenchAgentInvocationRecord]:
        records: list[WorkbenchAgentInvocationRecord] = []
        binding_keys = set(self.run_registration_resolutions) | set(self.run_agent_gateway_resolutions)
        for run_id, role in binding_keys:
            run = self.agent_runs.get(run_id)
            if run is None:
                continue
            resolution = self.run_registration_resolutions.get((run_id, role)) or self.run_registration_resolutions.get((run.root_run_id, role))
            if resolution is None:
                continue
            route = self.run_agent_gateway_resolutions.get((run_id, role))
            planning_run = self.planning_runs.get(run.root_run_id)
            historical_project = planning_run.project_id if planning_run is not None else None
            if (
                (route is not None and route.project_id != project_id)
                or (route is None and historical_project != project_id)
                or (registration_id is not None and resolution.registration_id != registration_id)
                or (registration_version is not None and resolution.version != registration_version)
            ):
                continue
            root_run = self.agent_runs.get(run.root_run_id)
            if root_run is None:
                continue
            records.append(
                WorkbenchAgentInvocationRecord(
                    run_id=run.run_id,
                    root_run_id=run.root_run_id,
                    parent_run_id=run.parent_run_id,
                    registration_id=resolution.registration_id,
                    registration_version=resolution.version,
                    role=role,
                    run_lifecycle_status=root_run.status,
                    workflow_available=run.root_run_id in self.planning_runs,
                    created_at=run.created_at,
                    updated_at=run.updated_at,
                    gateway_route=(
                        WorkbenchAgentGatewayRouteRecord(
                            policy_revision=route.policy_revision,
                            role=role,
                            model_alias=route.model_alias,
                            max_budget_usd=route.max_budget_usd,
                            toolset=route.toolset,
                        )
                        if route is not None
                        else None
                    ),
                    mcp_grants=list(self.run_mcp_tool_resolutions.get((run_id, role), [])),
                    lifecycle_transitions=list(self.agent_run_lifecycle_transitions.get(run_id, [])),
                )
            )
        return sorted(records, key=lambda item: (item.created_at, item.run_id, item.role), reverse=True)

    async def bootstrap_registry(
        self,
        manifests: list[RegistrationManifest],
        policy_revision: str,
        assignments: dict[str, str],
        mcp_policy: McpBindingPolicy | None = None,
    ) -> None:
        mcp_policy = mcp_policy or McpBindingPolicy(policy_revision=policy_revision)
        if mcp_policy.policy_revision != policy_revision:
            raise RegistryConflictError("MCP policy revision does not match the registry policy revision")
        for manifest in manifests:
            key = (manifest.registration_id, manifest.version)
            existing = self.registrations.get(key)
            if existing is not None and manifest_sha256(existing) != manifest_sha256(manifest):
                raise RegistryConflictError("registration version already exists with different manifest content")
            self.registrations[key] = manifest
        existing_policy = self.registry_policies.get(policy_revision)
        if existing_policy is not None and existing_policy != assignments:
            raise RegistryConflictError("policy revision already exists with different assignments")
        self.registry_policies[policy_revision] = dict(assignments)
        existing_mcp_policy = self.registry_mcp_policies.get(policy_revision)
        if existing_mcp_policy is not None and existing_mcp_policy != mcp_policy:
            raise RegistryConflictError("policy revision already exists with different MCP bindings")
        self.registry_mcp_policies[policy_revision] = mcp_policy

    async def bootstrap_agent_gateway_policy(self, policy: AgentGatewayPolicy) -> None:
        existing = self.registry_agent_gateway_policies.get(policy.policy_revision)
        if existing is not None and existing != policy:
            raise RegistryConflictError("agent gateway policy revision already exists with different bindings")
        self.registry_agent_gateway_policies[policy.policy_revision] = policy

    async def resolve_run_agent_gateway(
        self,
        run_id: str,
        role: str,
        project_id: str,
        registration: RegistrationReference,
        policy: AgentGatewayPolicy,
    ) -> AgentGatewayResolution:
        key = (run_id, role)
        existing = self.run_agent_gateway_resolutions.get(key)
        if existing is not None:
            if existing.project_id != project_id:
                raise RegistryConflictError("run role is already pinned to a different project route")
            return existing
        durable_policy = self.registry_agent_gateway_policies.get(policy.policy_revision)
        if durable_policy is None:
            raise RegistryConflictError("agent gateway policy revision is not available")
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
        registered = self.registrations.get((registration.registration_id, registration.version))
        if registered is None or registered.lifecycle.value != "active":
            raise RegistryConflictError("agent gateway registration release is not active")
        if manifest_sha256(registered) != registration.manifest_sha256:
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
        self.run_agent_gateway_resolutions[key] = route
        return route

    async def resolve_run_registration(
        self,
        run_id: str,
        role: str,
        policy_revision: str,
        manifest: RegistrationManifest,
    ) -> RegistrationReference:
        expected = registration_reference(role, manifest)
        key = (run_id, role)
        existing = self.run_registration_resolutions.get(key)
        if existing is not None:
            if existing != expected:
                raise RegistryConflictError("run role is already pinned to a different registration release")
            return existing
        policy = self.registry_policies.get(policy_revision)
        if policy is None:
            raise RegistryConflictError("registry policy revision is not available")
        if policy.get(role) != f"{manifest.registration_id}@{manifest.version}":
            raise RegistryConflictError("registry policy does not select the requested registration release")
        registered = self.registrations.get((manifest.registration_id, manifest.version))
        if registered is None or registered.lifecycle.value != "active":
            raise RegistryConflictError("registration release is not active")
        if manifest_sha256(registered) != expected.manifest_sha256:
            raise RegistryConflictError("registration release does not match its declared manifest")
        self.run_registration_resolutions[key] = expected
        return expected

    async def resolve_run_mcp_tools(
        self,
        run_id: str,
        role: str,
        project_id: str,
        policy_revision: str,
        target_repositories: list[str] | None = None,
        target_repository_scopes: dict[str, str] | None = None,
    ) -> list[McpToolGrant]:
        key = (run_id, role)
        existing = self.run_mcp_tool_resolutions.get(key)
        if existing is not None:
            return existing
        policy = self.registry_mcp_policies.get(policy_revision)
        if policy is None:
            raise RegistryConflictError("registry policy revision is not available")
        expected: list[McpToolGrant] = []
        for binding in policy.bindings:
            if binding.role != role or project_id not in binding.project_ids:
                continue
            if not _binding_targets_a_run_repository(
                binding.server_id,
                binding.server_version,
                target_repositories or [],
                target_repository_scopes or {},
            ):
                continue
            server = self.registrations.get((binding.server_id, binding.server_version))
            if server is None or server.lifecycle.value != "active":
                raise RegistryConflictError("MCP server release is not active")
            tool_schemas = {tool.name: tool.input_schema_sha256 for tool in server.mcp_tools}
            for tool_name in binding.tools:
                input_schema_sha256 = tool_schemas.get(tool_name)
                if input_schema_sha256 is None:
                    raise RegistryConflictError("MCP policy references an unavailable server tool")
                expected.append(
                    McpToolGrant(
                        server_id=binding.server_id,
                        server_version=binding.server_version,
                        server_manifest_sha256=manifest_sha256(server),
                        tool_name=tool_name,
                        input_schema_sha256=input_schema_sha256,
                        repository_scope=(
                            (target_repository_scopes or {})
                            .get(f"{binding.server_id}@{binding.server_version}", "")
                            .casefold()
                            or None
                        ),
                    )
                )
        self.run_mcp_tool_resolutions[key] = expected
        return expected

    async def create_planning_run(self, record: PlanningRunRecord) -> None:
        self.planning_runs[record.run_id] = record
        self._append_coordination_event(
            record.run_id, "specification_recorded", artifact=record.source_artifact
        )
        self._append_coordination_event(record.run_id, "planning_started", artifact=record.source_artifact)

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None:
        return self.planning_runs.get(run_id)

    async def attach_product_specification_draft(
        self,
        run_id: str,
        artifact: ArtifactReference,
        planner_model: str,
        expected_product_specification_revision: int,
        generation_claim: str | None = None,
    ) -> PlanningRunRecord:
        del planner_model
        record = self.planning_runs[run_id]
        if generation_claim is not None and self.product_specification_generation_claims.get(run_id) != generation_claim:
            raise ValueError("planning run is not eligible to accept a product specification draft")
        if (
            record.status is not PlanningRunStatus.PLANNING
            or record.product_specification_revision != expected_product_specification_revision
        ):
            raise ValueError("planning run is not eligible to accept a product specification draft")
        updated = PlanningRunRecord(
            **{
                **record.__dict__,
                "product_specification_artifact": artifact,
                "product_specification_revision": record.product_specification_revision + 1,
            }
        )
        self.planning_runs[run_id] = updated
        self.product_specification_generation_claims.pop(run_id, None)
        self.product_specification_generation_claimed_at.pop(run_id, None)
        self._append_coordination_event(run_id, "product_specification_draft_created", artifact=artifact)
        return updated

    async def claim_product_specification_generation(self, run_id: str) -> str | None:
        record = self.planning_runs[run_id]
        claimed_at = self.product_specification_generation_claimed_at.get(run_id)
        if (
            record.status is not PlanningRunStatus.PLANNING
            or record.product_specification_artifact is not None
            or (
                claimed_at is not None
                and claimed_at >= datetime.now(timezone.utc) - timedelta(minutes=15)
                and run_id in self.product_specification_generation_claims
            )
        ):
            return None
        claim = f"claim-{run_id}"
        self.product_specification_generation_claims[run_id] = claim
        self.product_specification_generation_claimed_at[run_id] = datetime.now(timezone.utc)
        return claim

    async def release_product_specification_generation(self, run_id: str, generation_claim: str) -> None:
        if self.product_specification_generation_claims.get(run_id) == generation_claim:
            self.product_specification_generation_claims.pop(run_id, None)
            self.product_specification_generation_claimed_at.pop(run_id, None)

    async def record_specification_evaluation(
        self,
        run_id: str,
        artifact: ArtifactReference,
        specification_revision: int,
        specification_sha256: str,
        readiness: str,
        generation_claim: str | None = None,
    ) -> PlanningRunRecord:
        record = self.planning_runs[run_id]
        if generation_claim is not None and self.specification_evaluation_generation_claims.get(run_id) != generation_claim:
            raise ValueError("specification evaluation generation claim was lost")
        if (
            record.status is not PlanningRunStatus.PLANNING
            or record.product_specification_revision != specification_revision
            or record.product_specification_artifact is None
            or record.product_specification_artifact.sha256 != specification_sha256
        ):
            raise ValueError("product specification changed while evaluation was generated")
        if record.specification_evaluation_artifact is not None:
            return record
        updated = replace(
            record,
            specification_evaluation_artifact=artifact,
            specification_evaluation_readiness=readiness,
        )
        self.planning_runs[run_id] = updated
        self.specification_evaluation_generation_claims.pop(run_id, None)
        self._append_coordination_event(run_id, "specification_evaluated", artifact=artifact)
        return updated

    async def claim_specification_evaluation_generation(self, run_id: str) -> str | None:
        record = self.planning_runs[run_id]
        if record.specification_evaluation_artifact is not None or run_id in self.specification_evaluation_generation_claims:
            return None
        claim = f"evaluation-claim-{run_id}"
        self.specification_evaluation_generation_claims[run_id] = claim
        return claim

    async def release_specification_evaluation_generation(self, run_id: str, generation_claim: str) -> None:
        if self.specification_evaluation_generation_claims.get(run_id) == generation_claim:
            self.specification_evaluation_generation_claims.pop(run_id, None)

    async def waive_specification_evaluation(
        self, *, run_id: str, artifact_sha256: str, actor_id: str, rationale: str,
        idempotency_key: str, request_sha256: str,
    ) -> PlanningRunRecord:
        del actor_id, rationale, idempotency_key, request_sha256
        record = self.planning_runs[run_id]
        if (
            record.status is not PlanningRunStatus.PLANNING
            or record.specification_evaluation_artifact is None
            or record.specification_evaluation_artifact.sha256 != artifact_sha256
            or record.specification_evaluation_readiness != "needs_revision"
        ):
            raise ValueError("evaluation is not eligible for waiver")
        updated = replace(record, specification_evaluation_readiness="waived")
        self.planning_runs[run_id] = updated
        self._append_coordination_event(run_id, "specification_evaluation_waived")
        return updated

    async def select_product_specification(
        self, run_id: str, revision: int, artifact_sha256: str, actor_id: str, idempotency_key: str, request_sha256: str
    ) -> PlanningRunRecord:
        del actor_id, idempotency_key, request_sha256
        record = self.planning_runs[run_id]
        artifact = record.product_specification_artifact
        if (
            record.status is not PlanningRunStatus.PLANNING
            or artifact is None
            or record.product_specification_revision != revision
            or artifact.sha256 != artifact_sha256
        ):
            if (
                record.selected_product_specification_revision == revision
                and record.selected_product_specification_artifact is not None
                and record.selected_product_specification_artifact.sha256 == artifact_sha256
            ):
                return record
            raise ValueError("planning run is not eligible to select this product specification")
        updated = PlanningRunRecord(
            **{
                **record.__dict__,
                "selected_product_specification_artifact": artifact,
                "selected_product_specification_revision": revision,
                "selected_specification_evaluation_artifact": record.specification_evaluation_artifact,
            }
        )
        self.planning_runs[run_id] = updated
        self._append_coordination_event(run_id, "product_specification_selected", artifact=artifact)
        return updated

    async def attach_product_specification_revision(
        self, run_id: str, artifact: ArtifactReference, expected_product_specification_revision: int,
        parent_artifact_sha256: str, actor_id: str, idempotency_key: str, request_sha256: str,
    ) -> PlanningRunRecord:
        del actor_id, idempotency_key, request_sha256
        record = self.planning_runs[run_id]
        if (
            record.status is not PlanningRunStatus.PLANNING
            or record.product_specification_revision != expected_product_specification_revision
            or record.product_specification_artifact is None
            or record.product_specification_artifact.sha256 != parent_artifact_sha256
        ):
            raise ValueError("planning run is not eligible to accept this product specification revision")
        updated = PlanningRunRecord(
            **{
                **record.__dict__, "product_specification_artifact": artifact,
                "product_specification_revision": record.product_specification_revision + 1,
                "selected_product_specification_artifact": None,
                "selected_product_specification_revision": None,
                "specification_evaluation_artifact": None,
                "specification_evaluation_readiness": None,
                "selected_specification_evaluation_artifact": None,
            }
        )
        self.planning_runs[run_id] = updated
        self._append_coordination_event(run_id, "product_specification_revised", artifact=artifact)
        return updated

    async def record_workbench_feedback(
        self, *, run_id: str, intent: WorkbenchFeedbackIntent, artifact_sha256: str, stage_id: str,
        actor_id: str, comment: str, idempotency_key: str, request_sha256: str,
    ) -> WorkbenchFeedbackRecord:
        run = self.planning_runs.get(run_id)
        key = (run_id, idempotency_key)
        existing = self.workbench_feedback.get(key)
        if existing is not None:
            if self.workbench_feedback_request_hashes[key] != request_sha256:
                raise ApprovalConflictError("idempotency key was reused with different feedback")
            return existing
        expected_artifact = {
            "specification": run.source_artifact if run is not None else None,
            "product_specification": run.product_specification_artifact if run is not None else None,
            "planning": run.plan_artifact if run is not None else None,
            "plan_approval": run.plan_artifact if run is not None else None,
            "implementation": run.implementation_artifact if run is not None else None,
            "implementation_approval": run.implementation_artifact if run is not None else None,
        }.get(stage_id)
        if expected_artifact is None or expected_artifact.sha256 != artifact_sha256:
            raise ApprovalConflictError("feedback artifact is not authoritative for this stage")
        record = WorkbenchFeedbackRecord(
            feedback_id=f"feedback-{len(self.workbench_feedback) + 1}", run_id=run_id, intent=intent,
            artifact_sha256=artifact_sha256, stage_id=stage_id, actor_id=actor_id, comment=comment.strip(),
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self.workbench_feedback[key] = record
        self.workbench_feedback_request_hashes[key] = request_sha256
        self._append_coordination_event(
            run_id,
            "workbench_feedback_recorded",
            artifact=expected_artifact,
            stage_id=stage_id,
        )
        return record

    async def list_workbench_feedback(self, run_id: str, *, limit: int = 100) -> list[WorkbenchFeedbackRecord]:
        return list(reversed([item for item in self.workbench_feedback.values() if item.run_id == run_id]))[:limit]

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
        record = self.planning_runs[run_id]
        if (
            record.status.value != "planning"
            or record.plan_revision != expected_plan_revision
            or (
                expected_product_specification_revision is not None
                and (
                    record.selected_product_specification_revision != expected_product_specification_revision
                    or record.selected_product_specification_artifact is None
                    or record.selected_product_specification_artifact.sha256 != expected_product_specification_sha256
                )
            )
        ):
            raise ValueError("planning run is not eligible to accept a generated plan")
        updated = PlanningRunRecord(
            run_id=record.run_id,
            status=PlanningRunStatus.AWAITING_PLAN_APPROVAL,
            source_artifact=record.source_artifact,
            target_repos=record.target_repos,
            spec_set=record.spec_set,
            constraints=record.constraints,
            priority=record.priority,
            submitted_at=record.submitted_at,
            submitted_by=record.submitted_by,
            plan_artifact=plan_artifact,
            planner_model=planner_model,
            workflow_id=workflow_id,
            plan_revision=record.plan_revision + 1,
            project_id=record.project_id,
            product_specification_artifact=record.product_specification_artifact,
            product_specification_revision=record.product_specification_revision,
            selected_product_specification_artifact=record.selected_product_specification_artifact,
            selected_product_specification_revision=record.selected_product_specification_revision,
            specification_evaluation_artifact=record.specification_evaluation_artifact,
            specification_evaluation_readiness=record.specification_evaluation_readiness,
            selected_specification_evaluation_artifact=record.selected_specification_evaluation_artifact,
        )
        self.planning_runs[run_id] = updated
        if expected_product_specification_revision is not None and record.selected_product_specification_artifact is not None:
            self.plan_product_specification_bindings[(run_id, updated.plan_revision)] = (
                expected_product_specification_revision,
                record.selected_product_specification_artifact,
            )
        self._append_coordination_event(
            run_id,
            "plan_approval_requested",
            gate="plan",
            artifact=plan_artifact,
        )
        return updated

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
        if mcp_selection is not None:
            if len({item.key() for item in mcp_selection}) != len(mcp_selection):
                raise ApprovalConflictError("MCP selection grants must be unique")
            mcp_selection = sorted(mcp_selection, key=McpToolSelection.key)
        run = self.planning_runs.get(run_id)
        revision = run.plan_revision if run is not None else 0
        approval_key = (run_id, revision, idempotency_key)
        existing = self.approvals.get(approval_key)
        if existing is not None:
            if self.approval_request_hashes[approval_key] != request_sha256:
                raise ApprovalConflictError("idempotency key was reused with a different decision")
            return existing
        if run is None or run.status is not PlanningRunStatus.AWAITING_PLAN_APPROVAL:
            raise ApprovalConflictError("planning run is not awaiting plan approval")
        if any(item.run_id == run_id and item.plan_revision == revision for item in self.approvals.values()):
            raise ApprovalConflictError("a plan approval decision is already recorded for this revision")
        if run.plan_artifact is None or run.plan_artifact.sha256 != artifact_sha256:
            raise ApprovalConflictError("plan approval artifact digest is stale")
        if mcp_selection is not None:
            available = {
                (
                    role,
                    grant.server_id,
                    grant.server_version,
                    grant.server_manifest_sha256,
                    grant.tool_name,
                    grant.input_schema_sha256,
                    grant.repository_scope or "",
                )
                for (resolved_run_id, role), grants in self.run_mcp_tool_resolutions.items()
                if resolved_run_id == run_id and role == "developer"
                for grant in grants
            }
            if any(item.key() not in available for item in mcp_selection):
                raise ApprovalConflictError("MCP selection is not a subset of the run's pinned policy grants")
        record = ApprovalRecord(
            decision_id=f"decision-{len(self.approvals) + 1}",
            run_id=run_id,
            decision=decision,
            artifact_sha256=artifact_sha256,
            actor_id=actor_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            delivered=False,
            plan_revision=run.plan_revision,
            mcp_selection=mcp_selection,
        )
        self.approvals[approval_key] = record
        self.approval_request_hashes[approval_key] = request_sha256
        self.outbox[record.decision_id] = OutboxDelivery(
            decision_id=record.decision_id,
            run_id=record.run_id,
            workflow_id=run.workflow_id or "",
            payload={
                "decision_id": record.decision_id,
                "artifact_sha256": record.artifact_sha256,
                "decision": record.decision.value,
            }
            | (
                {"mcp_selection": [item.model_dump(mode="json") for item in mcp_selection]}
                if mcp_selection is not None
                else {}
            ),
            attempt_count=0,
        )
        self._append_coordination_event(
            run_id,
            "plan_approval_recorded",
            gate="plan",
            artifact=run.plan_artifact,
            decision=decision.value,
        )
        return record

    async def mark_plan_approval_delivered(self, decision_id: str) -> None:
        for key, record in self.approvals.items():
            if record.decision_id == decision_id:
                self.approvals[key] = ApprovalRecord(
                    decision_id=record.decision_id,
                    run_id=record.run_id,
                    decision=record.decision,
                    artifact_sha256=record.artifact_sha256,
                    actor_id=record.actor_id,
                    created_at=record.created_at,
                    delivered=True,
                    plan_revision=record.plan_revision,
                    mcp_selection=record.mcp_selection,
                )
                run = self.planning_runs[record.run_id]
                if run.plan_revision != record.plan_revision:
                    self.outbox.pop(decision_id, None)
                    self.leased_decision_ids.discard(decision_id)
                    return
                status = {
                    PlanApprovalDecision.APPROVE: PlanningRunStatus.IMPLEMENTING,
                    PlanApprovalDecision.REJECT: PlanningRunStatus.REJECTED,
                    PlanApprovalDecision.REQUEST_REVISION: PlanningRunStatus.PLANNING,
                }[record.decision]
                self.planning_runs[record.run_id] = PlanningRunRecord(
                    run_id=run.run_id,
                    status=status,
                    source_artifact=run.source_artifact,
                    target_repos=run.target_repos,
                    spec_set=run.spec_set,
                    constraints=run.constraints,
                    priority=run.priority,
                    submitted_at=run.submitted_at,
                    submitted_by=run.submitted_by,
                    plan_artifact=None if status is PlanningRunStatus.PLANNING else run.plan_artifact,
                    planner_model=None if status is PlanningRunStatus.PLANNING else run.planner_model,
                    workflow_id=None if status is PlanningRunStatus.PLANNING else run.workflow_id,
                    plan_revision=run.plan_revision,
                    project_id=run.project_id,
                    product_specification_artifact=run.product_specification_artifact,
                    product_specification_revision=run.product_specification_revision,
                    selected_product_specification_artifact=run.selected_product_specification_artifact,
                    selected_product_specification_revision=run.selected_product_specification_revision,
                    specification_evaluation_artifact=run.specification_evaluation_artifact,
                    specification_evaluation_readiness=run.specification_evaluation_readiness,
                    selected_specification_evaluation_artifact=run.selected_specification_evaluation_artifact,
                )
                self.outbox.pop(decision_id, None)
                self.leased_decision_ids.discard(decision_id)
                return

    async def claim_plan_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]:
        del lease_seconds
        claimed: list[OutboxDelivery] = []
        for item in self.outbox.values():
            if decision_id and item.decision_id != decision_id:
                continue
            if item.decision_id in self.leased_decision_ids:
                continue
            self.leased_decision_ids.add(item.decision_id)
            updated = OutboxDelivery(
                decision_id=item.decision_id,
                run_id=item.run_id,
                workflow_id=item.workflow_id,
                payload=item.payload,
                attempt_count=item.attempt_count + 1,
            )
            self.outbox[item.decision_id] = updated
            claimed.append(updated)
            if len(claimed) == limit:
                break
        return claimed

    async def release_plan_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None:
        del retry_seconds, error
        self.leased_decision_ids.discard(decision_id)

    async def record_implementation_artifact(self, run_id: str, artifact: ArtifactReference) -> None:
        run = self.planning_runs[run_id]
        if run.status is PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL:
            if run.implementation_artifact != artifact:
                raise ApprovalConflictError("implementation artifact cannot be registered for this run")
            return
        if run.status is not PlanningRunStatus.IMPLEMENTING:
            raise ApprovalConflictError("implementation artifact cannot be registered for this run")
        self.planning_runs[run_id] = PlanningRunRecord(
            **{**run.__dict__, "status": PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL,
               "implementation_artifact": artifact, "implementation_revision": run.implementation_revision + 1}
        )
        self._append_coordination_event(
            run_id,
            "implementation_approval_requested",
            gate="implementation",
            artifact=artifact,
        )

    async def record_implementation_approval(
        self, run_id: str, artifact_sha256: str, decision: ImplementationApprovalDecision,
        actor_id: str, comment: str | None, idempotency_key: str, request_sha256: str,
    ) -> ImplementationApprovalRecord:
        run = self.planning_runs.get(run_id)
        revision = run.implementation_revision if run is not None else 0
        key = (run_id, revision, idempotency_key)
        existing = self.implementation_approvals.get(key)
        if existing is not None:
            if self.implementation_request_hashes[key] != request_sha256:
                raise ApprovalConflictError("idempotency key was reused with a different decision")
            return existing
        if run is None or run.status is not PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL:
            raise ApprovalConflictError("planning run is not awaiting implementation approval")
        if run.implementation_artifact is None or run.implementation_artifact.sha256 != artifact_sha256:
            raise ApprovalConflictError("implementation approval artifact digest is stale")
        record = ImplementationApprovalRecord(
            decision_id=f"implementation-decision-{len(self.implementation_approvals) + 1}", run_id=run_id,
            decision=decision, artifact_sha256=artifact_sha256, actor_id=actor_id,
            created_at=datetime.now(timezone.utc).isoformat(), delivered=False, implementation_revision=revision,
        )
        self.implementation_approvals[key] = record
        self.implementation_request_hashes[key] = request_sha256
        self.implementation_outbox[record.decision_id] = OutboxDelivery(
            decision_id=record.decision_id, run_id=run_id, workflow_id=run.workflow_id or "",
            payload={"decision_id": record.decision_id, "artifact_sha256": artifact_sha256, "decision": decision.value},
            attempt_count=0,
        )
        self._append_coordination_event(
            run_id,
            "implementation_approval_recorded",
            gate="implementation",
            artifact=run.implementation_artifact,
            decision=decision.value,
        )
        return record

    async def mark_implementation_approval_delivered(self, decision_id: str) -> None:
        for key, record in self.implementation_approvals.items():
            if record.decision_id != decision_id:
                continue
            self.implementation_approvals[key] = ImplementationApprovalRecord(
                **{**record.__dict__, "delivered": True}
            )
            run = self.planning_runs[record.run_id]
            status = {
                ImplementationApprovalDecision.APPROVE: PlanningRunStatus.FINALIZING,
                ImplementationApprovalDecision.REJECT: PlanningRunStatus.REJECTED,
                ImplementationApprovalDecision.REQUEST_REVISION: PlanningRunStatus.IMPLEMENTING,
            }[record.decision]
            self.planning_runs[record.run_id] = PlanningRunRecord(
                **{**run.__dict__, "status": status,
                   "implementation_artifact": None if record.decision is ImplementationApprovalDecision.REQUEST_REVISION else run.implementation_artifact}
            )
            self.implementation_outbox.pop(decision_id, None)
            self.leased_decision_ids.discard(decision_id)
            return

    async def claim_implementation_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]:
        del lease_seconds
        claimed: list[OutboxDelivery] = []
        for item in self.implementation_outbox.values():
            if decision_id and item.decision_id != decision_id or item.decision_id in self.leased_decision_ids:
                continue
            self.leased_decision_ids.add(item.decision_id)
            updated = OutboxDelivery(**{**item.__dict__, "attempt_count": item.attempt_count + 1})
            self.implementation_outbox[item.decision_id] = updated
            claimed.append(updated)
            if len(claimed) == limit:
                break
        return claimed

    async def release_implementation_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None:
        del retry_seconds, error
        self.leased_decision_ids.discard(decision_id)

    def _append_coordination_event(
        self,
        run_id: str,
        event_type: str,
        *,
        gate: str | None = None,
        artifact: ArtifactReference | None = None,
        decision: str | None = None,
        lifecycle_status: str | None = None,
        stage_id: str | None = None,
    ) -> None:
        payload = {
            "schema_version": "1.0",
            "event_type": event_type,
            "run_id": run_id,
            "gate": gate,
            "artifact": {"ref": artifact.ref, "sha256": artifact.sha256} if artifact and artifact.ref else None,
            "decision": decision,
            "lifecycle_status": lifecycle_status,
            "stage_id": stage_id,
            "read_url": f"/api/v1/planning-runs/{run_id}/coordination",
            "action_url": f"/api/v1/coordination/runs/{run_id}/actions/{gate}" if gate else None,
        }
        import hashlib
        import json

        event_id = f"notification-{len(self.coordination_events) + 1}"
        dedupe_key = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if any(existing.payload.get("dedupe_key") == dedupe_key for existing in self.coordination_events.values()):
            return
        payload["dedupe_key"] = dedupe_key
        self.coordination_events[event_id] = CoordinationEvent(
            event_id=event_id,
            run_id=run_id,
            event_type=event_type,
            occurred_at=datetime.now(timezone.utc).isoformat(),
            gate=gate,
            artifact_ref=artifact.ref if artifact and artifact.ref else None,
            artifact_sha256=artifact.sha256 if artifact and artifact.ref else None,
            decision=decision,
            lifecycle_status=lifecycle_status,
            payload=payload,
        )
        self.notification_deliveries[event_id] = (False, 0, None)

    async def list_coordination_events(self, run_id: str, *, limit: int = 100):  # type: ignore[no-untyped-def]
        events = [event for event in self.coordination_events.values() if event.run_id == run_id]
        events.sort(key=lambda item: item.occurred_at, reverse=True)
        return [
            (event, *self.notification_deliveries[event.event_id])
            for event in events[:limit]
        ]

    async def list_workbench_approvals(self, run_id: str, *, limit: int = 100) -> list[WorkbenchApprovalRecord]:
        """Provide the same normalized immutable approval history as PostgreSQL."""

        records = [
            WorkbenchApprovalRecord(
                decision_id=item.decision_id,
                run_id=item.run_id,
                gate="plan",
                decision=item.decision.value,
                artifact_sha256=item.artifact_sha256,
                actor_id=item.actor_id,
                created_at=item.created_at,
                delivered=item.delivered,
                mcp_selection=item.mcp_selection,
            )
            for item in self.approvals.values()
            if item.run_id == run_id
        ] + [
            WorkbenchApprovalRecord(
                decision_id=item.decision_id,
                run_id=item.run_id,
                gate="implementation",
                decision=item.decision.value,
                artifact_sha256=item.artifact_sha256,
                actor_id=item.actor_id,
                created_at=item.created_at,
                delivered=item.delivered,
            )
            for item in self.implementation_approvals.values()
            if item.run_id == run_id
        ]
        return sorted(records, key=lambda item: (item.created_at, item.decision_id), reverse=True)[:max(1, min(limit, 100))]

    async def get_run_mcp_capabilities(
        self, run_id: str, plan_revision: int
    ) -> tuple[list[McpToolSelection], list[McpToolSelection] | None, bool]:
        pins = sorted(
            [
                McpToolSelection(
                    role=role,
                    server_id=grant.server_id,
                    server_version=grant.server_version,
                    server_manifest_sha256=grant.server_manifest_sha256,
                    tool_name=grant.tool_name,
                    input_schema_sha256=grant.input_schema_sha256,
                    repository_scope=grant.repository_scope,
                )
                for (resolved_run_id, role), grants in self.run_mcp_tool_resolutions.items()
                if resolved_run_id == run_id and role == "developer"
                for grant in grants
            ],
            key=McpToolSelection.key,
        )
        decisions = [
            item for item in self.approvals.values() if item.run_id == run_id and item.plan_revision == plan_revision
        ]
        decisions.sort(key=lambda item: (item.created_at, item.decision_id), reverse=True)
        return (
            pins,
            decisions[0].mcp_selection if decisions else None,
            bool(decisions) and decisions[0].decision is PlanApprovalDecision.APPROVE,
        )

    async def list_coordination_runs(self, *, limit: int = 50) -> list[PlanningRunRecord]:
        return sorted(self.planning_runs.values(), key=lambda item: item.submitted_at, reverse=True)[:limit]

    async def list_reconcilable_runs(self, *, limit: int = 100) -> list[PlanningRunRecord]:
        return [
            record
            for record in self.planning_runs.values()
            if record.status in {PlanningRunStatus.IMPLEMENTING, PlanningRunStatus.FINALIZING} and record.workflow_id
        ][:limit]

    async def reconcile_terminal_workflow(self, *, run_id: str, workflow_id: str, outcome: str) -> bool:
        record = self.planning_runs.get(run_id)
        agent = self.agent_runs.get(run_id)
        targets = {
            "completed": (PlanningRunStatus.COMPLETED, AgentRunStatus.SUCCEEDED),
            "failed": (PlanningRunStatus.PLANNING_FAILED, AgentRunStatus.FAILED),
            "stopped_with_backup": (PlanningRunStatus.PLANNING_FAILED, AgentRunStatus.TIMED_OUT),
        }
        target = targets.get(outcome)
        if (
            record is None
            or agent is None
            or target is None
            or record.workflow_id != workflow_id
            or record.status not in {PlanningRunStatus.IMPLEMENTING, PlanningRunStatus.FINALIZING}
        ):
            return False
        terminal = {AgentRunStatus.SUCCEEDED, AgentRunStatus.FAILED, AgentRunStatus.CANCELLED, AgentRunStatus.TIMED_OUT}
        if agent.status in terminal and agent.status is not target[1]:
            return False
        now = datetime.now(timezone.utc).isoformat()
        self.planning_runs[run_id] = replace(record, status=target[0])
        self.agent_runs[run_id] = replace(
            agent,
            status=target[1],
            updated_at=now,
            last_heartbeat_at=now,
        )
        self._append_coordination_event(run_id, "workflow_reconciled", lifecycle_status=target[1].value)
        return True

    async def list_workbench_runs(self, *, project_ids: frozenset[str], limit: int = 50) -> list[PlanningRunRecord]:
        return [
            record
            for record in sorted(self.planning_runs.values(), key=lambda item: item.submitted_at, reverse=True)
            if record.project_id in project_ids
        ][:limit]

    async def claim_notification_deliveries(self, *, limit: int, lease_seconds: int) -> list[NotificationDelivery]:
        del lease_seconds
        claimed = []
        for event_id, (delivered, attempts, error) in self.notification_deliveries.items():
            if delivered or event_id in self.leased_notification_event_ids:
                continue
            self.leased_notification_event_ids.add(event_id)
            self.notification_deliveries[event_id] = (delivered, attempts + 1, error)
            claimed.append(NotificationDelivery(self.coordination_events[event_id], attempts + 1))
            if len(claimed) == limit:
                break
        return claimed

    async def mark_notification_delivered(self, event_id: str) -> None:
        _, attempts, _ = self.notification_deliveries[event_id]
        self.notification_deliveries[event_id] = (True, attempts, None)
        self.leased_notification_event_ids.discard(event_id)

    async def release_notification_delivery(self, event_id: str, *, retry_seconds: int, error: str) -> None:
        del retry_seconds
        delivered, attempts, _ = self.notification_deliveries[event_id]
        self.notification_deliveries[event_id] = (delivered, attempts, error)
        self.leased_notification_event_ids.discard(event_id)


class FakePlanner:
    def __init__(self, plan: AiPlan, product_specification: ProductSpecification | None = None) -> None:
        self.plan = plan
        self.product_specification = product_specification
        self.contexts: list[PlanningContext] = []
        self.gateways: list[AgentGatewayResolution] = []
        self.product_specification_contexts: list[ProductSpecificationContext] = []

    async def generate(self, context: PlanningContext, gateway: AgentGatewayResolution) -> AiPlan:
        self.contexts.append(context)
        self.gateways.append(gateway)
        return self.plan

    async def generate_product_specification(
        self, context: ProductSpecificationContext, gateway: AgentGatewayResolution
    ) -> ProductSpecification:
        if self.product_specification is None:
            def source(statement_id: str, requirement_ids: list[str] | None = None) -> dict[str, object]:
                return {"id": statement_id, "text": statement_id.replace("-", " "), "kind": "source", "source_segment_ids": ["source-1"], "requirement_ids": requirement_ids or []}

            self.product_specification = ProductSpecification.model_validate(
                {
                    "schema_version": 2,
                    "title": source("title"), "problem_statement": source("problem"),
                    "desired_outcomes": [source("outcome")], "actors": [source("actor")],
                    "in_scope": [source("in-scope")], "out_of_scope": [source("out-of-scope")],
                    "functional_requirements": [source("functional-1"), source("functional-2")],
                    "acceptance_criteria": [source("acceptance-1", ["functional-1"]), source("acceptance-2", ["functional-2"])],
                    "personas": [source("persona")], "user_journeys": [source("journey")],
                    "constraints": [source("constraint")], "dependencies": [source("dependency")],
                }
            )
        self.product_specification_contexts.append(context)
        self.gateways.append(gateway)
        return self.product_specification


class FakeRunStarter:
    def __init__(self) -> None:
        self.started_runs: list[RunEnvelope] = []
        self.plan_approvals: list[tuple[str, dict[str, object]]] = []
        self.implementation_approvals: list[tuple[str, dict[str, str]]] = []
        self.approval_error: Exception | None = None
        self.approval_result = True
        self.start_error: Exception | None = None

    async def start_run(self, envelope: RunEnvelope) -> None:
        if self.start_error is not None:
            raise self.start_error
        if any((run.workflow_id or run.run_id) == (envelope.workflow_id or envelope.run_id) for run in self.started_runs):
            return
        self.started_runs.append(envelope)

    async def submit_plan_approval(self, workflow_id: str, decision: dict[str, object]) -> bool:
        self.plan_approvals.append((workflow_id, decision))
        if self.approval_error is not None:
            raise self.approval_error
        return self.approval_result

    async def submit_implementation_approval(self, workflow_id: str, decision: dict[str, str]) -> bool:
        self.implementation_approvals.append((workflow_id, decision))
        if self.approval_error is not None:
            raise self.approval_error
        return self.approval_result
