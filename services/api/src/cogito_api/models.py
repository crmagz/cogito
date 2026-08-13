from __future__ import annotations

import json
from enum import Enum, StrEnum
from math import isfinite
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, model_validator


# Workbench evidence retrieval is capped at 100 KB. Keep immutable product
# specifications below that boundary so every stored revision remains reviewable.
MAX_PRODUCT_SPECIFICATION_BYTES = 96 * 1024


class ReviewProfile(str, Enum):
    STRICT = "strict"
    STANDARD = "standard"
    MINIMAL = "minimal"


class PlanPhase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(description="Unique phase identifier (e.g., 'phase-1')")
    name: str = Field(description="Human-readable phase name")
    description: str = Field(description="What this phase accomplishes")
    tasks: list[str] = Field(description="Ordered list of concrete tasks")
    acceptance_criteria: list[str] = Field(
        description="Conditions that must be true when the phase is complete"
    )
    verification: list[str] = Field(
        description="Commands or checks to run after phase execution"
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description="Phase IDs that must complete before this one starts",
    )


class PlanConstraints(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_wall_clock_minutes: int = Field(default=60)
    max_cost_usd: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    max_review_rounds: int = Field(default=3)
    max_turns_per_phase: int = Field(default=200)
    backup_reserve_turns: int = Field(
        default=25,
        ge=20,
        le=30,
        description=(
            "Turns (20-30) reserved to commit and push partial progress before a ceiling forces a stop, "
            "so productive work is never lost."
        ),
    )

    @model_validator(mode="after")
    def productive_turn_budget_exceeds_reserve(self) -> "PlanConstraints":
        """Require at least one productive turn after the recovery reserve."""

        if self.max_turns_per_phase <= self.backup_reserve_turns:
            raise ValueError("max_turns_per_phase must exceed backup_reserve_turns")
        return self


class AiPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(description="Brief title of the work")
    summary: str = Field(description="What problem this plan solves and why")
    target_repos: list[str] = Field(
        min_length=1,
        max_length=10,
        description="Pinned HTTPS repository references in URL#commit-sha form",
    )
    spec_set: str = Field(
        min_length=1,
        max_length=256,
        description="Spec set reference with immutable archive digest (e.g., 'typescript-backend@v2.1#sha256=<digest>')",
    )
    phases: list[PlanPhase] = Field(description="Ordered execution phases")
    constraints: PlanConstraints = Field(description="Execution limits")
    review_profile: ReviewProfile = Field(
        default=ReviewProfile.STANDARD, description="How strict the review loop is"
    )


class ProductSpecificationStatementKind(StrEnum):
    """Whether a product-specification statement is sourced or explicitly uncertain."""

    SOURCE = "source"
    ASSUMPTION = "assumption"
    QUESTION = "question"


class ProductSpecificationStatement(BaseModel):
    """One traceable statement in a structured product specification."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Stable statement identifier within this immutable specification",
    )
    text: str = Field(
        min_length=1,
        max_length=10_000,
        description="Bounded product statement, requirement, criterion, assumption, risk, or question",
    )
    kind: ProductSpecificationStatementKind = Field(
        description="Whether the statement is grounded in intake, an assumption, or an unresolved question",
    )
    source_segment_ids: list[str] = Field(
        default_factory=list,
        max_length=32,
        description="Explicit immutable intake segments supporting a source-grounded statement",
    )

    @model_validator(mode="after")
    def validate_provenance(self) -> "ProductSpecificationStatement":
        """Require source references only for claims grounded in the immutable intake."""

        if len(set(self.source_segment_ids)) != len(self.source_segment_ids):
            raise ValueError("product specification source segment IDs must be unique")
        if self.kind is ProductSpecificationStatementKind.SOURCE and not self.source_segment_ids:
            raise ValueError("source-grounded product specification statements require a source segment")
        if self.kind is not ProductSpecificationStatementKind.SOURCE and self.source_segment_ids:
            raise ValueError("assumptions and questions cannot claim source segments")
        return self


class ProductSpecification(BaseModel):
    """Strict, evidence-labelled product contract produced before implementation planning."""

    model_config = ConfigDict(extra="forbid")

    title: ProductSpecificationStatement = Field(description="Traceable concise name for the proposed outcome")
    problem_statement: ProductSpecificationStatement = Field(description="Traceable problem to solve")
    desired_outcomes: list[ProductSpecificationStatement] = Field(
        min_length=1,
        max_length=64,
        description="Traceable expected product outcomes",
    )
    actors: list[ProductSpecificationStatement] = Field(
        min_length=1,
        max_length=64,
        description="Traceable affected users or systems",
    )
    in_scope: list[ProductSpecificationStatement] = Field(
        min_length=1,
        max_length=128,
        description="Traceable work included in the proposed feature",
    )
    out_of_scope: list[ProductSpecificationStatement] = Field(
        min_length=1,
        max_length=128,
        description="Traceable work deliberately excluded from the proposed feature",
    )
    functional_requirements: list[ProductSpecificationStatement] = Field(
        min_length=1,
        max_length=256,
        description="Traceable functional requirements for the future implementation plan",
    )
    non_functional_requirements: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=256,
        description="Traceable quality, security, performance, or operability requirements",
    )
    acceptance_criteria: list[ProductSpecificationStatement] = Field(
        min_length=1,
        max_length=256,
        description="Traceable observable conditions required for feature acceptance",
    )
    assumptions: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=128,
        description="Explicit assumptions requiring later human confirmation",
    )
    risks: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=128,
        description="Traceable risks or explicitly assumed risks",
    )
    unresolved_questions: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=128,
        description="Explicit questions that must not be treated as settled requirements",
    )

    @model_validator(mode="after")
    def validate_statement_kinds(self) -> "ProductSpecification":
        """Keep uncertain material visibly separate from claimed requirements."""

        statements = [
            self.title,
            self.problem_statement,
            *self.desired_outcomes,
            *self.actors,
            *self.in_scope,
            *self.out_of_scope,
            *self.functional_requirements,
            *self.non_functional_requirements,
            *self.acceptance_criteria,
            *self.assumptions,
            *self.risks,
            *self.unresolved_questions,
        ]
        if len({statement.id for statement in statements}) != len(statements):
            raise ValueError("product specification statement IDs must be unique")
        factual = [
            self.title,
            self.problem_statement,
            *self.desired_outcomes,
            *self.actors,
            *self.in_scope,
            *self.out_of_scope,
            *self.functional_requirements,
            *self.non_functional_requirements,
            *self.acceptance_criteria,
            *self.risks,
        ]
        if any(statement.kind is ProductSpecificationStatementKind.QUESTION for statement in factual):
            raise ValueError("product requirements and risks cannot be unresolved questions")
        if any(statement.kind is not ProductSpecificationStatementKind.ASSUMPTION for statement in self.assumptions):
            raise ValueError("product specification assumptions must be labelled assumptions")
        if any(statement.kind is not ProductSpecificationStatementKind.QUESTION for statement in self.unresolved_questions):
            raise ValueError("product specification unresolved questions must be labelled questions")
        serialized = json.dumps(
            self.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(serialized) > MAX_PRODUCT_SPECIFICATION_BYTES:
            raise ValueError("product specification exceeds the 96 KiB evidence limit")
        return self


class RunSubmission(BaseModel):
    plan: AiPlan
    dry_run: bool = Field(
        default=False, description="Validate the plan without persisting or queuing it"
    )
    priority: str = Field(default="normal")


class PlanningRunStatus(StrEnum):
    """Authoritative lifecycle states for a supervisor planning run."""

    PLANNING = "planning"
    AWAITING_PLAN_APPROVAL = "awaiting_plan_approval"
    IMPLEMENTING = "implementing"
    AWAITING_IMPLEMENTATION_APPROVAL = "awaiting_implementation_approval"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    PLANNING_FAILED = "planning_failed"
    REJECTED = "rejected"
    REVISION_REQUESTED = "revision_requested"


class AgentRunStatus(StrEnum):
    """Canonical lifecycle state independent from planning approval state."""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    WAITING_FOR_TOOL = "WAITING_FOR_TOOL"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


class PlanApprovalDecision(StrEnum):
    """Human decision permitted at the plan-approval gate."""

    APPROVE = "approve"
    REJECT = "reject"
    REQUEST_REVISION = "request_revision"


class ImplementationApprovalDecision(StrEnum):
    """Human decision permitted for a converged implementation artifact."""

    APPROVE = "approve"
    REJECT = "reject"
    REQUEST_REVISION = "request_revision"


class PlanningRunSubmission(BaseModel):
    """Input used to create a human-gated planning run."""

    initial_specification: str = Field(
        min_length=1,
        max_length=100_000,
        description="Untrusted work specification from which the planner will produce a normalized plan",
    )
    target_repos: list[str] = Field(
        min_length=1,
        max_length=10,
        description="Pinned HTTPS repository references in URL#commit-sha form",
    )
    spec_set: str = Field(
        min_length=1,
        max_length=256,
        description="Spec set reference with immutable archive digest",
    )
    constraints: PlanConstraints = Field(
        default_factory=PlanConstraints,
        description="Hard limits that the future generated plan must satisfy",
    )
    priority: str = Field(default="normal", description="Scheduling priority for the planning run")
    dry_run: bool = Field(
        default=False,
        description="Validate the planning request without persisting an artifact or run record",
    )


class ArtifactReference(BaseModel):
    """Immutable object-store identity for a supervisor artifact."""

    ref: str = Field(description="Object store URI of the immutable artifact")
    sha256: str = Field(description="SHA-256 digest of the canonical artifact bytes")


class RegistrationKind(StrEnum):
    """The independently versioned capability class selected by the Supervisor."""

    AGENT = "agent"
    TOOL = "tool"
    MCP_SERVER = "mcp_server"


class RegistrationLifecycle(StrEnum):
    """Selectability state for one immutable registration release."""

    DRAFT = "draft"
    ACTIVE = "active"
    DISABLED = "disabled"
    REVOKED = "revoked"


class ComponentMaturity(StrEnum):
    """Product and operational maturity for a monorepo SDLC component."""

    INCUBATING = "incubating"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class ExecutionClass(StrEnum):
    """Execution shape for a component without granting it control-plane authority."""

    ADAPTER = "adapter"
    WORKER_SERVICE = "worker_service"
    ISOLATED_JOB = "isolated_job"


class ArtifactSchema(BaseModel):
    """Versioned artifact contract accepted or emitted by a registration."""

    schema_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Stable artifact schema identifier",
    )
    version: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$",
        description="Compatible artifact schema version",
    )


class ToolGrant(BaseModel):
    """A pinned tool release and the minimum scope granted to an agent release."""

    tool_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Registered tool identifier",
    )
    tool_version: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$",
        description="Pinned registered tool version",
    )
    scope: str = Field(
        min_length=1,
        max_length=256,
        description="Bounded non-secret capability scope enforced by the broker",
    )


class McpTransport(StrEnum):
    """Transport allowed for a governed MCP server release."""

    HTTP = "http"


class McpToolDefinition(BaseModel):
    """Non-secret, immutable contract for one MCP tool."""

    name: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
    )
    input_schema_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the versioned tool input schema, never the tool input itself.",
    )


class McpBinding(BaseModel):
    """Explicit, non-secret policy grant from an agent role to one MCP release."""

    role: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    server_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    server_version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    tools: list[str] = Field(min_length=1, max_length=32)
    project_ids: list[str] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def validate_unique_tools(self) -> "McpBinding":
        """Reject wildcard or duplicate tool selection before it becomes authority."""

        if any(not tool or tool == "*" for tool in self.tools) or len(set(self.tools)) != len(self.tools):
            raise ValueError("MCP binding tools must be unique explicit tool names")
        if len(set(self.project_ids)) != len(self.project_ids):
            raise ValueError("MCP binding project IDs must be unique")
        return self


class McpBindingPolicy(BaseModel):
    """Immutable reviewed MCP authorization policy loaded with the component catalog."""

    policy_revision: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    bindings: list[McpBinding] = Field(default_factory=list, max_length=128)

    @model_validator(mode="after")
    def validate_unique_bindings(self) -> "McpBindingPolicy":
        """Require one policy entry for each role/server release pair."""

        keys = [(binding.role, binding.server_id, binding.server_version) for binding in self.bindings]
        if len(set(keys)) != len(keys):
            raise ValueError("MCP policy contains duplicate role/server bindings")
        return self


class AgentGatewayBinding(BaseModel):
    """Explicit non-secret policy binding for one agent role and project."""

    role: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    registration_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    registration_version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    project_ids: list[str] = Field(min_length=1, max_length=32, description="Projects eligible to resolve this agent")
    model_alias: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="LiteLLM model alias permitted for the resolved invocation",
    )
    max_budget_usd: float = Field(gt=0, description="Maximum LiteLLM budget for one resolved invocation")
    toolset: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Non-secret LiteLLM toolset label recorded with the resolved route",
    )

    @model_validator(mode="after")
    def validate_unique_projects(self) -> "AgentGatewayBinding":
        """Require each project to appear only once in one binding."""

        if not isfinite(self.max_budget_usd):
            raise ValueError("agent gateway binding max budget must be finite")
        if len(set(self.project_ids)) != len(self.project_ids):
            raise ValueError("agent gateway binding project IDs must be unique")
        return self


class AgentGatewayPolicy(BaseModel):
    """Immutable reviewed routing policy for registered agents."""

    policy_revision: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    bindings: list[AgentGatewayBinding] = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def validate_unique_role_project_bindings(self) -> "AgentGatewayPolicy":
        """Reject ambiguous agent-route selection for one role and project."""

        keys = [(binding.role, project_id) for binding in self.bindings for project_id in binding.project_ids]
        if len(set(keys)) != len(keys):
            raise ValueError("agent gateway policy contains duplicate role/project bindings")
        return self


class AgentGatewayResolution(BaseModel):
    """Pinned gateway limits selected for one run role without a credential."""

    policy_revision: str = Field(description="Immutable agent gateway policy revision")
    project_id: str = Field(description="Project scope that authorized the route")
    role: str = Field(description="Supervisor role alias authorized by the route")
    registration_id: str = Field(description="Pinned registered agent identifier")
    registration_version: str = Field(description="Pinned registered agent version")
    manifest_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Canonical digest of the selected non-secret agent manifest",
    )
    model_alias: str = Field(description="Only LiteLLM model alias permitted for this agent")
    max_budget_usd: float = Field(gt=0, description="Maximum spend allowed for this agent invocation")
    toolset: str = Field(description="Non-secret toolset label pinned with the gateway route")


class McpToolGrant(BaseModel):
    """Pinned tool-level authority for one MCP server release."""

    server_id: str = Field(description="Immutable registered MCP server identifier")
    server_version: str = Field(description="Immutable registered MCP server version")
    server_manifest_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Canonical digest of the registered MCP server manifest",
    )
    tool_name: str = Field(description="Explicit MCP tool allow-listed for the role")
    input_schema_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Pinned digest of the tool input-schema contract",
    )
    repository_scope: str | None = Field(
        default=None,
        description="Immutable repository identity required by this grant, when the connector is repository-scoped",
    )


class McpToolSelection(BaseModel):
    """One exact run-pinned MCP grant selected by an approver."""

    model_config = ConfigDict(extra="forbid")

    role: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    server_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    server_version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    server_manifest_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    tool_name: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    input_schema_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    repository_scope: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
        description="Immutable repository identity for a repository-scoped connector grant",
    )

    def key(self) -> tuple[str, str, str, str, str, str, str]:
        """Return the canonical identity used for equality and ordering."""

        return (
            self.role,
            self.server_id,
            self.server_version,
            self.server_manifest_sha256,
            self.tool_name,
            self.input_schema_sha256,
            self.repository_scope or "",
        )


def _canonical_mcp_selection(selection: list[McpToolSelection] | None) -> list[McpToolSelection] | None:
    """Sort a selection and reject duplicate exact grants before request hashing."""

    if selection is None:
        return None
    if len({item.key() for item in selection}) != len(selection):
        raise ValueError("MCP selection grants must be unique")
    return sorted(selection, key=McpToolSelection.key)


class RegistrationManifest(BaseModel):
    """Declarative, non-secret definition of an immutable component release."""

    registration_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Stable agent or tool registration identifier",
    )
    version: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$",
        description="Immutable semantic registration version",
    )
    kind: RegistrationKind = Field(description="Whether this release is an agent or tool")
    component_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Monorepo component that owns this release",
    )
    component_version: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$",
        description="Immutable component release version",
    )
    lifecycle: RegistrationLifecycle = Field(description="Whether new runs may select this release")
    maturity: ComponentMaturity = Field(description="Component product and operational maturity")
    execution_class: ExecutionClass = Field(description="How the capability is executed")
    owner: str = Field(min_length=1, max_length=256, description="Owning team or service identity")
    input_schema: ArtifactSchema = Field(description="Immutable artifact contract consumed by the release")
    output_schema: ArtifactSchema = Field(description="Immutable artifact contract emitted by the release")
    capabilities: list[str] = Field(
        min_length=1,
        max_length=32,
        description="Declared non-secret operations exposed by the release",
    )
    grants: list[ToolGrant] = Field(
        default_factory=list,
        max_length=32,
        description="Pinned tool grants available to this release",
    )
    mcp_transport: McpTransport | None = Field(
        default=None,
        description="MCP transport for an MCP server release only",
    )
    mcp_endpoint: str | None = Field(
        default=None,
        max_length=2048,
        description="Non-secret logical internal MCP endpoint for an MCP server release only",
    )
    mcp_endpoint_template: str | None = Field(
        default=None,
        max_length=2048,
        description="Deployment-scoped internal MCP endpoint template using {scope_sha256_12} for a scoped release",
    )
    mcp_tools: list[McpToolDefinition] = Field(
        default_factory=list,
        max_length=128,
        description="Named MCP tools exposed by an MCP server release",
    )
    quality_gates: list[str] = Field(
        min_length=1,
        max_length=32,
        description="Required contract, safety, and operational gates for the component",
    )

    @model_validator(mode="after")
    def validate_kind_grants(self) -> "RegistrationManifest":
        """Reject tool grants on tool definitions and duplicate capability declarations."""

        if self.kind in {RegistrationKind.TOOL, RegistrationKind.MCP_SERVER} and self.grants:
            raise ValueError("tool and MCP server registrations cannot grant other tools")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("registration capabilities must be unique")
        grant_keys = [(grant.tool_id, grant.tool_version, grant.scope) for grant in self.grants]
        if len(set(grant_keys)) != len(grant_keys):
            raise ValueError("registration grants must be unique")
        if self.kind is RegistrationKind.MCP_SERVER:
            if self.mcp_transport is None or not (self.mcp_endpoint or self.mcp_endpoint_template) or not self.mcp_tools:
                raise ValueError("MCP server registrations require transport, endpoint, and tools")
            if self.mcp_endpoint is not None and self.mcp_endpoint_template is not None:
                raise ValueError("MCP server registrations may define an endpoint or endpoint template, not both")
            endpoint = self.mcp_endpoint or self.mcp_endpoint_template
            assert endpoint is not None
            if self.mcp_endpoint_template is not None:
                placeholder = "{scope_sha256_12}"
                if endpoint.count(placeholder) != 1:
                    raise ValueError("MCP endpoint templates require exactly one {scope_sha256_12} placeholder")
                endpoint = endpoint.replace(placeholder, "scope")
            parsed = urlparse(endpoint)
            if parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password or parsed.query:
                raise ValueError("MCP server endpoint must be an internal HTTP URL without credentials or query")
            tool_names = [tool.name for tool in self.mcp_tools]
            if len(set(tool_names)) != len(tool_names):
                raise ValueError("MCP server tool names must be unique")
        elif self.mcp_transport is not None or self.mcp_endpoint is not None or self.mcp_endpoint_template is not None or self.mcp_tools:
            raise ValueError("only MCP server registrations may define MCP transport, endpoint, or tools")
        return self


class RegistrationReference(BaseModel):
    """Audit-safe identity of the exact registration release selected for a run."""

    role: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_-]{0,127}$",
        description="Supervisor role alias resolved for this run",
    )
    registration_id: str = Field(description="Immutable selected registration identifier")
    version: str = Field(description="Immutable selected registration version")
    manifest_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="SHA-256 digest of canonical non-secret manifest bytes",
    )
    component_id: str = Field(description="Owning monorepo component identifier")
    component_version: str = Field(description="Owning immutable component release version")
    grants: list[ToolGrant] = Field(default_factory=list, description="Pinned tool releases and scopes for this role")
    mcp_grants: list[McpToolGrant] = Field(
        default_factory=list,
        description="Pinned MCP server tools authorized for this role and run project",
    )
    gateway: AgentGatewayResolution | None = Field(
        default=None,
        description="Pinned LiteLLM route and budget selected for this role",
    )


class PlanningRunResponse(BaseModel):
    """Accepted planning-run response returned to API callers."""

    run_id: str = Field(description="Stable planning run identifier")
    status: PlanningRunStatus = Field(description="Authoritative initial lifecycle state")
    source_artifact: ArtifactReference = Field(description="Immutable submitted specification")
    product_specification_artifact: ArtifactReference | None = Field(
        default=None,
        description="Latest immutable structured product specification draft when available",
    )
    product_specification_revision: int = Field(
        default=0,
        ge=0,
        description="Latest immutable product specification draft revision",
    )
    selected_product_specification_artifact: ArtifactReference | None = Field(
        default=None,
        description="Immutable product specification explicitly selected as the planning input",
    )
    selected_product_specification_revision: int | None = Field(
        default=None,
        ge=1,
        description="Selected immutable product specification revision when planning is authorized",
    )
    plan_artifact: ArtifactReference | None = Field(
        default=None, description="Immutable generated plan when planning has completed"
    )
    implementation_artifact: ArtifactReference | None = Field(
        default=None, description="Frozen implementation and review evidence when approval is pending"
    )
    submitted_at: str = Field(description="ISO 8601 submission timestamp")


class ProductSpecificationSelectionRequest(BaseModel):
    """Digest-bound explicit promotion of one reviewed product-specification draft."""

    model_config = ConfigDict(extra="forbid")

    revision: int = Field(ge=1, description="Immutable product specification revision displayed to the operator")
    artifact_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the displayed immutable product specification artifact",
    )


class ProductSpecificationRevisionRequest(BaseModel):
    """Strict human-authored revision anchored to the displayed current draft."""

    model_config = ConfigDict(extra="forbid")

    expected_product_specification_revision: int = Field(
        ge=1, description="Displayed latest revision used to reject stale edits"
    )
    parent_artifact_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the displayed latest product specification",
    )
    specification: ProductSpecification = Field(description="Complete reviewed replacement specification")


class AgentRunResponse(BaseModel):
    """Authoritative lifecycle projection for a submitted run."""

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


class PlanApprovalRequest(BaseModel):
    """Digest-bound human decision submitted to the plan-approval gate."""

    decision: PlanApprovalDecision = Field(description="Human approval, rejection, or revision request")
    artifact_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the exact generated plan being reviewed",
    )
    comment: str | None = Field(
        default=None,
        max_length=10_000,
        description="Required rationale for rejection or revision requests",
    )
    mcp_selection: list[McpToolSelection] | None = Field(
        default=None,
        max_length=128,
        description=(
            "Optional exact subset of the run-pinned MCP grants. Null preserves the run's pinned grants; "
            "an empty list explicitly selects no MCP grants."
        ),
    )

    @model_validator(mode="after")
    def require_comment_for_non_approval(self) -> "PlanApprovalRequest":
        """Ensure non-approval decisions carry durable reviewer context."""

        if self.decision is not PlanApprovalDecision.APPROVE and not (self.comment and self.comment.strip()):
            raise ValueError("comment is required when rejecting or requesting revision")
        if self.decision is not PlanApprovalDecision.APPROVE and self.mcp_selection is not None:
            raise ValueError("MCP selection is allowed only when approving a plan")
        self.mcp_selection = _canonical_mcp_selection(self.mcp_selection)
        return self


class PlanApprovalResponse(BaseModel):
    """Auditable result of an accepted idempotent plan decision."""

    decision_id: str = Field(description="Immutable decision identifier")
    run_id: str = Field(description="Planning run identifier")
    decision: PlanApprovalDecision = Field(description="Recorded plan decision")
    artifact_sha256: str = Field(description="Digest reviewed by the human")
    actor_id: str = Field(description="Authenticated reviewer subject")
    delivered: bool = Field(description="Whether Temporal accepted the decision update")
    created_at: str = Field(description="ISO 8601 decision timestamp")
    mcp_selection: list[McpToolSelection] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
        description="Immutable canonical MCP selection recorded with this decision",
    )


class ImplementationApprovalRequest(BaseModel):
    """Digest-bound human decision submitted after implementation review converges."""

    decision: ImplementationApprovalDecision = Field(
        description="Human approval, rejection, or revision request for the frozen implementation"
    )
    artifact_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the exact implementation and review artifact being reviewed",
    )
    comment: str | None = Field(
        default=None,
        max_length=10_000,
        description="Required rationale for rejection or revision requests",
    )

    @model_validator(mode="after")
    def require_comment_for_non_approval(self) -> "ImplementationApprovalRequest":
        """Ensure non-approval decisions retain reviewer context."""

        if self.decision is not ImplementationApprovalDecision.APPROVE and not (self.comment and self.comment.strip()):
            raise ValueError("comment is required when rejecting or requesting revision")
        return self


class ImplementationApprovalResponse(BaseModel):
    """Auditable result of an accepted idempotent implementation decision."""

    decision_id: str = Field(description="Immutable decision identifier")
    run_id: str = Field(description="Planning run identifier")
    decision: ImplementationApprovalDecision = Field(description="Recorded implementation decision")
    artifact_sha256: str = Field(description="Digest reviewed by the human")
    actor_id: str = Field(description="Authenticated reviewer subject")
    delivered: bool = Field(description="Whether Temporal accepted the decision update")
    created_at: str = Field(description="ISO 8601 decision timestamp")


class CoordinationGate(StrEnum):
    """Human gate represented by a provider-neutral coordination event."""

    PLAN = "plan"
    IMPLEMENTATION = "implementation"


class WorkbenchArtifactKind(StrEnum):
    """Server-owned evidence kinds available to an authorized Workbench."""

    SOURCE = "source"
    PRODUCT_SPECIFICATION = "product_specification"
    PLAN = "plan"
    IMPLEMENTATION = "implementation"


class WorkbenchStageState(StrEnum):
    """Truthful lifecycle state for one Workbench workflow-map node."""

    COMPLETED = "completed"
    IN_PROGRESS = "in_progress"
    AWAITING_OPERATOR = "awaiting_operator"
    NEEDS_REVISION = "needs_revision"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"


class WorkbenchStageAvailability(StrEnum):
    """Whether a stage value is backed by a durable authoritative source."""

    AUTHORITATIVE = "authoritative"
    UNAVAILABLE = "unavailable"


class WorkbenchWorkflowNodeType(StrEnum):
    """Semantic node role for the Workbench relay graph."""

    AGENT = "agent"
    GATE = "gate"
    QUEUE = "queue"


class WorkbenchWorkflowEdgeStyle(StrEnum):
    """Rendering class for a durable graph dependency."""

    SOLID = "solid"
    DASHED = "dashed"


class WorkbenchWorkflowEdgeEmphasis(StrEnum):
    """Relative visual prominence for a durable graph dependency."""

    PRIMARY = "primary"
    SECONDARY = "secondary"


class WorkbenchStageSummary(BaseModel):
    """Server-owned state for one visible Workflow Map node."""

    stage_id: str = Field(description="Stable workflow-map node identifier")
    label: str = Field(description="Operator-facing workflow-map node label")
    state: WorkbenchStageState = Field(description="Explicit lifecycle state for this node")
    availability: WorkbenchStageAvailability = Field(description="Whether the state is durably authoritative")
    reason: str = Field(description="Bounded explanation for the displayed state")
    artifact_kind: WorkbenchArtifactKind | None = Field(
        default=None, description="Verified evidence kind associated with this node when available"
    )


class WorkbenchWorkflowNode(WorkbenchStageSummary):
    """A typed, server-owned relay graph node without presentation coordinates."""

    node_type: WorkbenchWorkflowNodeType = Field(description="Semantic role used to render the relay node")


class WorkbenchWorkflowEdge(BaseModel):
    """One directed, server-owned dependency in the relay graph."""

    source_node_id: str = Field(description="Stable upstream node identifier")
    target_node_id: str = Field(description="Stable downstream node identifier")
    style: WorkbenchWorkflowEdgeStyle = Field(default=WorkbenchWorkflowEdgeStyle.SOLID, description="Authoritative edge rendering hint")
    emphasis: WorkbenchWorkflowEdgeEmphasis = Field(default=WorkbenchWorkflowEdgeEmphasis.PRIMARY, description="Authoritative edge emphasis hint")


class WorkbenchWorkflowGraph(BaseModel):
    """A per-run graph contract; layout remains a client presentation concern."""

    nodes: list[WorkbenchWorkflowNode] = Field(default_factory=list)
    edges: list[WorkbenchWorkflowEdge] = Field(default_factory=list)


class WorkbenchArtifactSummary(BaseModel):
    """Immutable evidence identity that deliberately omits its object-store location."""

    kind: WorkbenchArtifactKind
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class WorkbenchApprovalSummary(BaseModel):
    """Immutable operator decision history shown only to scoped approvers."""

    decision_id: str
    gate: CoordinationGate
    decision: str
    artifact_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    actor_id: str
    created_at: str
    delivered: bool
    mcp_selection: list[McpToolSelection] | None = Field(default=None, exclude_if=lambda value: value is None)


class WorkbenchMcpCapabilityState(StrEnum):
    """Server-owned capability selection state for an approver's Workbench view."""

    AWAITING_PLAN_APPROVAL = "awaiting_plan_approval"
    APPROVED = "approved"
    NOT_APPLICABLE = "not_applicable"


class WorkbenchMcpCapabilities(BaseModel):
    """Approver-only exact pins and immutable selection for one planning run."""

    state: WorkbenchMcpCapabilityState
    pinned_grants: list[McpToolSelection] = Field(default_factory=list)
    selected_grants: list[McpToolSelection] | None = None
    invocation_evidence_available: bool = False


class WorkbenchBudgetSummary(BaseModel):
    """Server-owned run limits; actual spend is unavailable until frozen evidence exists."""

    max_cost_usd: float
    max_wall_clock_minutes: int
    max_review_rounds: int
    actual_cost_usd: float | None = None
    turns_used: int | None = None


class WorkbenchExecutionSummary(BaseModel):
    """Bounded, typed facts extracted from the immutable implementation artifact."""

    phase_count: int
    succeeded_phase_count: int
    failed_phase_count: int
    verification_passed: int
    verification_failed: int
    review_status: str | None = None
    validation_status: str | None = None


class WorkbenchExternalLink(BaseModel):
    """A server-owned external destination, never a browser-provided URL."""

    kind: str
    label: str
    url: str


class WorkbenchTimelineEvent(BaseModel):
    """Bounded, scope-filtered lifecycle event safe for Workbench rendering."""

    event_id: str = Field(description="Immutable coordination event identifier")
    event_type: str = Field(description="Versioned lifecycle or approval event kind")
    occurred_at: str = Field(description="Authoritative ISO 8601 event timestamp")
    stage_id: str | None = Field(
        default=None,
        description="Server-owned Workflow Map stage attribution when the event has a canonical stage",
    )
    stage_ids: list[str] = Field(
        default_factory=list,
        description="All server-owned Workflow Map stages materially affected by this lifecycle transition",
    )
    gate: CoordinationGate | None = Field(default=None, description="Approval gate when the event concerns one")
    artifact_sha256: str | None = Field(
        default=None, pattern=r"^[a-f0-9]{64}$", description="Immutable artifact digest when available"
    )
    decision: PlanApprovalDecision | None = Field(default=None, description="Recorded gate decision when available")
    lifecycle_status: AgentRunStatus | None = Field(default=None, description="Persisted lifecycle status when available")
    delivered: bool = Field(description="Whether the configured delivery sink acknowledged the event")
    delivery_attempt_count: int = Field(ge=0, description="Bounded reconciliation attempt count")


class WorkbenchTimelineResponse(BaseModel):
    """One bounded timeline page with a representation revision."""

    items: list[WorkbenchTimelineEvent] = Field(description="Newest-first scoped Workbench timeline events")
    revision: str = Field(description="Opaque ETag-compatible representation revision")


class WorkbenchRunResponse(BaseModel):
    """Scope-filtered, authoritative run projection for the Operator Workbench."""

    run_id: str
    project_id: str
    status: PlanningRunStatus
    submitted_at: str
    workflow_id: str | None = Field(default=None, description="Authoritative workflow execution identity when available")
    product_specification_revision: int = Field(default=0, ge=0)
    selected_product_specification_revision: int | None = Field(default=None, ge=1)
    stages: list[WorkbenchStageSummary] = Field(default_factory=list, description="Ordered authoritative Workflow Map nodes")
    workflow_graph: WorkbenchWorkflowGraph = Field(default_factory=WorkbenchWorkflowGraph, description="Typed relay graph for this run")
    active_gate: CoordinationGate | None = None
    artifacts: list[WorkbenchArtifactSummary] = Field(default_factory=list)
    abilities: list[str] = Field(default_factory=list)
    workflow: list[str] = Field(default_factory=list, description="Authoritative ordered stage and gate labels")
    budget: WorkbenchBudgetSummary
    approval_history_available: bool = False
    approval_history: list[WorkbenchApprovalSummary] = Field(default_factory=list)
    mcp_capabilities: WorkbenchMcpCapabilities | None = None
    execution: WorkbenchExecutionSummary | None = None
    external_links: list[WorkbenchExternalLink] = Field(default_factory=list)


class WorkbenchProjectResponse(BaseModel):
    """A project the authenticated principal may select in the Workbench."""

    project_id: str = Field(min_length=1, description="Server-authorized project identifier")


class WorkbenchProjectListResponse(BaseModel):
    """Bounded Workbench project inventory for the authenticated principal."""

    items: list[WorkbenchProjectResponse] = Field(description="Projects authorized for the current principal")


class WorkbenchRunListResponse(BaseModel):
    """Bounded Workbench inventory with a representation revision."""

    items: list[WorkbenchRunResponse]
    revision: str


class WorkbenchEvidenceResponse(BaseModel):
    """Bounded verified evidence safe to render as text or structured JSON."""

    kind: WorkbenchArtifactKind
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_type: str
    content: str


class WorkbenchFeedbackIntent(StrEnum):
    """Non-executable Workbench review-context types."""

    NOTE = "note"


class WorkbenchFeedbackStage(StrEnum):
    """Current server-owned stages eligible for immutable review context."""

    SPECIFICATION = "specification"
    PRODUCT_SPECIFICATION = "product_specification"
    PLANNING = "planning"
    PLAN_APPROVAL = "plan_approval"
    IMPLEMENTATION = "implementation"
    IMPLEMENTATION_APPROVAL = "implementation_approval"


class WorkbenchFeedbackRequest(BaseModel):
    """Append-only feedback bound to one displayed immutable artifact."""

    intent: WorkbenchFeedbackIntent = Field(default=WorkbenchFeedbackIntent.NOTE, description="Non-executable feedback intent")
    artifact_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$", description="Displayed immutable artifact digest")
    stage_id: WorkbenchFeedbackStage = Field(description="Authoritative stage receiving the feedback")
    comment: str = Field(min_length=1, max_length=10_000, description="Bounded operator note that is never agent input")


class WorkbenchFeedbackResponse(BaseModel):
    """Immutable receipt for one accepted review-context record."""

    feedback_id: str = Field(description="Immutable feedback identifier")
    run_id: str = Field(description="Authoritative run identifier")
    intent: WorkbenchFeedbackIntent = Field(description="Recorded non-executable intent")
    artifact_sha256: str = Field(pattern=r"^[a-f0-9]{64}$", description="Digest bound to the note")
    stage_id: str = Field(description="Authoritative stage bound to the note")
    actor_id: str = Field(description="Authenticated note author")
    comment: str = Field(description="Recorded bounded operator note")
    created_at: str = Field(description="ISO 8601 immutable creation timestamp")


class WorkbenchFeedbackListResponse(BaseModel):
    """Bounded newest-first notes visible in one authorized dossier."""

    items: list[WorkbenchFeedbackResponse] = Field(description="Immutable review-context records")


class CoordinationApprovalActionRequest(BaseModel):
    """Normalized authenticated approval action for a future operator client."""

    decision: PlanApprovalDecision = Field(description="Decision to record for the selected approval gate")
    artifact_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the exact immutable artifact being approved",
    )
    comment: str | None = Field(
        default=None,
        max_length=10_000,
        description="Required durable rationale for rejection or revision requests",
    )
    mcp_selection: list[McpToolSelection] | None = Field(
        default=None,
        max_length=128,
        description="Optional exact MCP subset for a plan-approval action",
    )

    @model_validator(mode="after")
    def require_comment_for_non_approval(self) -> "CoordinationApprovalActionRequest":
        """Keep normalized approval actions equivalent to existing gate requests."""

        if self.decision is not PlanApprovalDecision.APPROVE and not (self.comment and self.comment.strip()):
            raise ValueError("comment is required when rejecting or requesting revision")
        if self.decision is not PlanApprovalDecision.APPROVE and self.mcp_selection is not None:
            raise ValueError("MCP selection is allowed only when approving a plan")
        self.mcp_selection = _canonical_mcp_selection(self.mcp_selection)
        return self


class CoordinationArtifactReference(BaseModel):
    """Safe immutable artifact identity surfaced to coordination clients."""

    ref: str = Field(description="Immutable artifact reference; never includes artifact contents")
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$", description="SHA-256 digest of the immutable artifact")


class CoordinationDeliveryResponse(BaseModel):
    """Bounded reconciliation state for one outbound notification."""

    delivered: bool = Field(description="Whether the configured sink acknowledged this event")
    attempt_count: int = Field(ge=0, description="Number of leased delivery attempts")
    last_error: str | None = Field(default=None, description="Bounded non-secret failure category")


class CoordinationEventResponse(BaseModel):
    """Provider-neutral authoritative event safe for authenticated operator reads."""

    event_id: str = Field(description="Immutable coordination event identifier")
    event_type: str = Field(description="Versioned safe event kind")
    run_id: str = Field(description="Authoritative Cogito run identifier")
    occurred_at: str = Field(description="ISO 8601 authoritative event timestamp")
    gate: CoordinationGate | None = Field(default=None, description="Approval gate when applicable")
    artifact: CoordinationArtifactReference | None = Field(
        default=None, description="Exact immutable artifact identity when applicable"
    )
    decision: PlanApprovalDecision | None = Field(default=None, description="Recorded approval decision when applicable")
    lifecycle_status: AgentRunStatus | None = Field(
        default=None, description="Canonical lifecycle state when the event is a status transition"
    )
    delivery: CoordinationDeliveryResponse = Field(description="Notification reconciliation state")


class CoordinationRunResponse(BaseModel):
    """Privileged coordination summary from Supervisor-owned state."""

    run_id: str = Field(description="Authoritative Cogito run identifier")
    status: PlanningRunStatus = Field(description="Authoritative planning-run state")
    submitted_at: str = Field(description="ISO 8601 run submission timestamp")
    plan_artifact: CoordinationArtifactReference | None = Field(
        default=None, description="Current immutable plan identity when available"
    )
    implementation_artifact: CoordinationArtifactReference | None = Field(
        default=None, description="Current immutable implementation identity when available"
    )
    active_gate: CoordinationGate | None = Field(default=None, description="Gate currently awaiting an operator action")
    events: list[CoordinationEventResponse] = Field(
        default_factory=list, description="Bounded newest-first coordination event history"
    )


class CoordinationRunListResponse(BaseModel):
    """Bounded authenticated run queue for a future Operator Workbench."""

    items: list[CoordinationRunResponse] = Field(description="Newest-first authoritative coordination summaries")


class RunEnvelope(BaseModel):
    run_id: str
    plan_ref: str = Field(description="Object store path of the immutable plan snapshot")
    plan_sha256: str = Field(description="SHA-256 digest of the canonical plan snapshot")
    spec_ref: str = Field(description="Spec set reference to resolve at execution time")
    target_repos: list[str]
    constraints: PlanConstraints
    priority: str = Field(default="normal")
    submitted_at: str = Field(description="ISO 8601 timestamp")
    submitted_by: str = Field(description="Identity of the submitter")
    workflow_id: str | None = Field(
        default=None,
        description="Temporal workflow identity; differs from run_id for revised plans",
    )
    requires_plan_approval: bool = Field(
        default=False,
        description="Whether the workflow must wait for a digest-bound plan decision before execution",
    )
    requires_implementation_approval: bool = Field(
        default=False,
        description="Whether converged implementation evidence must receive a human decision before finalization",
    )
    registry_resolutions: list[RegistrationReference] = Field(
        default_factory=list,
        description="Pinned non-secret registry releases selected for this run",
    )
    traceparent: str | None = Field(default=None, max_length=512)
    tracestate: str | None = Field(default=None, max_length=4096)


class Violation(BaseModel):
    field: str
    message: str
