from __future__ import annotations

import json
from enum import Enum, IntEnum, StrEnum
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
    requirement_ids: list[str] = Field(
        default_factory=list,
        description="Stable selected-specification requirement IDs implemented by this phase",
    )
    verification_references: list[str] = Field(
        default_factory=list,
        description="Requirement IDs verified by this phase's checks",
    )
    risk_notes: list[str] = Field(default_factory=list, description="Bounded delivery risks for this phase")
    rollback_notes: list[str] = Field(default_factory=list, description="Bounded rollback considerations for this phase")
    requirement_assignments: list["RequirementAssignment"] = Field(
        default_factory=list,
        description="Owner, support, and verification relationships for selected requirements",
    )

    @model_validator(mode="after")
    def validate_requirement_traceability_shape(self) -> "PlanPhase":
        if len(set(self.requirement_ids)) != len(self.requirement_ids):
            raise ValueError("plan phase requirement IDs must be unique")
        if len(set(self.verification_references)) != len(self.verification_references):
            raise ValueError("plan phase verification references must be unique")
        assignment_keys = [(item.requirement_id, item.relationship) for item in self.requirement_assignments]
        if len(set(assignment_keys)) != len(assignment_keys):
            raise ValueError("plan phase requirement assignments must be unique by requirement and relationship")
        return self


class PlanConstraints(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_wall_clock_minutes: int = Field(default=50)
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


class SpecificationIntake(BaseModel):
    """The only product-manager authored input to a governed workflow run."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    objective: str = Field(min_length=1, max_length=10_000)
    actors: list[str] = Field(min_length=1, max_length=64)
    desired_outcomes: list[str] = Field(min_length=1, max_length=128)
    scope_in: list[str] = Field(min_length=1, max_length=256)
    scope_out: list[str] = Field(default_factory=list, max_length=256)
    acceptance_expectations: list[str] = Field(min_length=1, max_length=256)
    constraints: list[str] = Field(default_factory=list, max_length=256)
    unknowns: list[str] = Field(default_factory=list, max_length=256)
    repository_candidates: list["RepositoryCandidate"] = Field(
        default_factory=list,
        max_length=32,
        description="Repositories the product manager already believes are relevant; relationships are discovered server-side",
    )
    discovery_preference: "RepositoryDiscoveryPreference" = Field(
        default="supplied_first",
        validate_default=True,
        description="Whether discovery may expand beyond the product manager's supplied repository candidates",
    )

    @model_validator(mode="after")
    def validate_non_blank_values(self) -> "SpecificationIntake":
        fields = (
            self.actors,
            self.desired_outcomes,
            self.scope_in,
            self.scope_out,
            self.acceptance_expectations,
            self.constraints,
            self.unknowns,
        )
        if not self.objective.strip() or any(not value.strip() for field in fields for value in field):
            raise ValueError("specification intake values must be non-blank")
        repository_ids = [candidate.repository_id for candidate in self.repository_candidates]
        if len(set(repository_ids)) != len(repository_ids):
            raise ValueError("repository candidates must be unique")
        return self


class RepositoryDiscoveryPreference(StrEnum):
    """Bounded product-manager hint; platform policy still controls actual discovery authority."""

    SUPPLIED_ONLY = "supplied_only"
    SUPPLIED_FIRST = "supplied_first"
    EXPAND_IF_NEEDED = "expand_if_needed"


class RepositoryCandidate(BaseModel):
    """A simple repository identifier supplied without technical relationship mapping."""

    model_config = ConfigDict(extra="forbid")

    repository_id: str = Field(min_length=1, max_length=256)
    note: str | None = Field(default=None, min_length=1, max_length=1_000)

    @model_validator(mode="after")
    def validate_repository_id(self) -> "RepositoryCandidate":
        if not self.repository_id.strip() or self.repository_id != self.repository_id.strip():
            raise ValueError("repository candidate identifier must be non-blank and trimmed")
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
    specification_evaluation_sha256: str | None = Field(
        default=None,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the exact specification evaluation that authorized planning",
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
    requirement_ids: list[str] = Field(
        default_factory=list,
        max_length=256,
        description="Requirement IDs that this acceptance criterion verifies",
    )

    @model_validator(mode="after")
    def validate_provenance(self) -> "ProductSpecificationStatement":
        """Require source references only for claims grounded in the immutable intake."""

        if len(set(self.source_segment_ids)) != len(self.source_segment_ids):
            raise ValueError("product specification source segment IDs must be unique")
        if len(set(self.requirement_ids)) != len(self.requirement_ids):
            raise ValueError("product specification requirement IDs must be unique")
        if self.kind is ProductSpecificationStatementKind.SOURCE and not self.source_segment_ids:
            raise ValueError("source-grounded product specification statements require a source segment")
        if self.kind is not ProductSpecificationStatementKind.SOURCE and self.source_segment_ids:
            raise ValueError("assumptions and questions cannot claim source segments")
        return self


class ProductSpecification(BaseModel):
    """Strict, evidence-labelled product contract produced before implementation planning."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(
        default=1,
        ge=1,
        le=2,
        description="Product specification contract version; evaluation requires version 2",
    )

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
    personas: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=64,
        description="Version 2 personas whose needs are addressed by the journeys",
    )
    user_journeys: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=128,
        description="Version 2 user or system journeys covered by the contract",
    )
    constraints: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=128,
        description="Version 2 delivery or product constraints",
    )
    dependencies: list[ProductSpecificationStatement] = Field(
        default_factory=list,
        max_length=128,
        description="Version 2 external dependencies or explicit absence thereof",
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
            *self.personas,
            *self.user_journeys,
            *self.constraints,
            *self.dependencies,
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
            *self.personas,
            *self.user_journeys,
            *self.constraints,
            *self.dependencies,
        ]
        if any(statement.kind is not ProductSpecificationStatementKind.SOURCE for statement in factual):
            raise ValueError("claimed product requirements and risks must be source-grounded")
        requirement_ids = set(self.requirement_ids)
        invalid_acceptance_references = {
            requirement_id
            for criterion in self.acceptance_criteria
            for requirement_id in criterion.requirement_ids
            if requirement_id not in requirement_ids
        }
        if invalid_acceptance_references:
            raise ValueError(
                "acceptance criteria reference unknown requirement IDs: "
                + ", ".join(sorted(invalid_acceptance_references))
            )
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

    def validate_source_segment_ids(self, known_source_segment_ids: set[str]) -> None:
        """Reject source claims that do not belong to this run's immutable intake."""

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
            *self.personas,
            *self.user_journeys,
            *self.constraints,
            *self.dependencies,
        ]
        invalid_statement_ids = [
            statement.id
            for statement in statements
            if not set(statement.source_segment_ids).issubset(known_source_segment_ids)
        ]
        if invalid_statement_ids:
            raise ValueError(
                "product specification cited unknown source segments: "
                + ", ".join(sorted(invalid_statement_ids))
            )

    @property
    def requirement_ids(self) -> list[str]:
        """Return the stable requirement IDs that a generated plan must cover."""

        return [
            *(statement.id for statement in self.functional_requirements),
            *(statement.id for statement in self.non_functional_requirements),
        ]


class SpecificationEvaluationReadiness(StrEnum):
    READY = "ready"
    NEEDS_REVISION = "needs_revision"
    WAIVED = "waived"


class SpecificationRiskTier(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class SpecificationEvaluationFindingKind(StrEnum):
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"
    CONFLICTING = "conflicting"
    UNVERIFIABLE = "unverifiable"


class SpecificationEvaluationFinding(BaseModel):
    """One bounded, non-mutating gap in a product specification."""

    model_config = ConfigDict(extra="forbid")

    kind: SpecificationEvaluationFindingKind
    message: str = Field(min_length=1, max_length=1_000)
    requirement_ids: list[str] = Field(default_factory=list, max_length=256)

    @model_validator(mode="after")
    def validate_references(self) -> "SpecificationEvaluationFinding":
        if len(set(self.requirement_ids)) != len(self.requirement_ids):
            raise ValueError("evaluation finding requirement IDs must be unique")
        return self


class SpecificationEvaluationCoverage(BaseModel):
    """Exact requirement coverage claimed by the immutable evaluation."""

    model_config = ConfigDict(extra="forbid")

    covered_requirement_ids: list[str] = Field(default_factory=list)
    uncovered_requirement_ids: list[str] = Field(default_factory=list)
    deferred_requirement_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_partitions(self) -> "SpecificationEvaluationCoverage":
        values = self.covered_requirement_ids + self.uncovered_requirement_ids + self.deferred_requirement_ids
        if len(set(values)) != len(values):
            raise ValueError("evaluation coverage requirement IDs must appear in one bucket")
        return self


class SpecificationEvaluation(BaseModel):
    """Immutable, digest-bound assessment of one product-specification revision."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1)
    specification_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    specification_revision: int = Field(ge=1)
    readiness: SpecificationEvaluationReadiness
    risk_tier: SpecificationRiskTier
    findings: list[SpecificationEvaluationFinding] = Field(default_factory=list)
    coverage: SpecificationEvaluationCoverage
    required_decisions: list[str] = Field(default_factory=list, max_length=64)
    generated_at: str = Field(min_length=1)
    generator_version: str = Field(min_length=1, max_length=128)


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
    CANCELLED = "cancelled"


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


class WorkflowRunSubmission(BaseModel):
    """The deliberately small product-manager request for a governed run.

    Repository, specification-set, policy, model tier, MCP grants, and hard
    execution ceilings are selected by the platform-owned project binding.
    """

    model_config = ConfigDict(extra="forbid")

    specification: SpecificationIntake
    priority: str = Field(default="normal", min_length=1, max_length=32)
    dry_run: bool = False


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


class WorkflowConfigurationState(StrEnum):
    """Lifecycle for an immutable platform-controlled configuration version."""

    DRAFT = "draft"
    VALIDATED = "validated"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    REVOKED = "revoked"


class ModelTier(StrEnum):
    """Logical model tier selected by workflow policy, never by product intake."""

    FAST = "fast"
    BALANCED = "balanced"
    COMPLEX = "complex"


class WorkflowRequirementRelationship(StrEnum):
    """A phase's bounded relationship to one accepted requirement."""

    OWNS = "owns"
    SUPPORTS = "supports"
    VERIFIES = "verifies"


class RequirementAssignment(BaseModel):
    """Traceable requirement work without conflating delivery and verification."""

    model_config = ConfigDict(extra="forbid")

    requirement_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    relationship: WorkflowRequirementRelationship
    acceptance_criterion_ids: list[str] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def validate_unique_criteria(self) -> "RequirementAssignment":
        if len(set(self.acceptance_criterion_ids)) != len(self.acceptance_criterion_ids):
            raise ValueError("requirement assignment acceptance criterion IDs must be unique")
        return self


class WorkflowGateDefinition(BaseModel):
    """A mandatory, artifact-bound human decision in a template."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    approver_roles: list[str] = Field(min_length=1, max_length=16)
    required_artifacts: list[ArtifactSchema] = Field(min_length=1, max_length=16)
    permitted_decisions: list[str] = Field(min_length=1, max_length=8)
    separation_of_duties: bool = True

    @model_validator(mode="after")
    def validate_gate_shape(self) -> "WorkflowGateDefinition":
        if len(set(self.approver_roles)) != len(self.approver_roles):
            raise ValueError("workflow gate approver roles must be unique")
        if len(set(self.permitted_decisions)) != len(self.permitted_decisions):
            raise ValueError("workflow gate decisions must be unique")
        return self


class WorkflowGateDecisionRequest(BaseModel):
    """A schema-gate decision bound to one immutable artifact.

    The public contract intentionally does not expose a worker, model, or
    policy selector.  ``gate_id`` comes from the URL and is checked against
    the run's immutable resolved workflow before a transition is attempted.
    ``artifact_revision`` is used only by the product-specification gate,
    whose artifact family is revisioned before planning begins.
    """

    model_config = ConfigDict(extra="forbid")

    decision: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    artifact_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    artifact_revision: int | None = Field(default=None, ge=1)
    comment: str | None = Field(default=None, max_length=10_000)
    mcp_selection: list["McpToolSelection"] | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def validate_comment_and_mcp_selection(self) -> "WorkflowGateDecisionRequest":
        if self.decision != "approve" and not (self.comment and self.comment.strip()):
            raise ValueError("comment is required when rejecting or requesting revision")
        if self.decision != "approve" and self.mcp_selection is not None:
            raise ValueError("MCP selection is allowed only when approving a gate")
        self.mcp_selection = _canonical_mcp_selection(self.mcp_selection)
        return self


class WorkflowPhaseActivation(BaseModel):
    """A deliberately small, non-executable condition for an optional phase."""

    model_config = ConfigDict(extra="forbid")

    kind: str = Field(pattern=r"^(always|intake_constraint)$")
    value: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_activation_shape(self) -> "WorkflowPhaseActivation":
        if (self.kind == "intake_constraint") != (self.value is not None):
            raise ValueError("intake_constraint activation requires a value; always activation does not")
        return self


class WorkflowPhaseDefinition(BaseModel):
    """A declarative phase that can be selected only by a resolved workflow."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    kind: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    agent_role: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    depends_on: list[str] = Field(default_factory=list, max_length=64)
    permitted_tiers: list[ModelTier] = Field(min_length=1, max_length=3)
    input_schemas: list[ArtifactSchema] = Field(default_factory=list, max_length=32)
    output_schemas: list[ArtifactSchema] = Field(default_factory=list, max_length=32)
    capability_profile_refs: list[str] = Field(default_factory=list, max_length=16)
    activation: WorkflowPhaseActivation | None = None
    opt_in: bool = False

    @model_validator(mode="after")
    def validate_phase_shape(self) -> "WorkflowPhaseDefinition":
        if self.id in self.depends_on or len(set(self.depends_on)) != len(self.depends_on):
            raise ValueError("workflow phase dependencies must be unique and cannot include the phase itself")
        if len(set(self.permitted_tiers)) != len(self.permitted_tiers):
            raise ValueError("workflow phase model tiers must be unique")
        if len(set(self.capability_profile_refs)) != len(self.capability_profile_refs):
            raise ValueError("workflow phase capability profile references must be unique")
        if self.opt_in and self.activation is None:
            raise ValueError("optional workflow phases require a typed activation condition")
        return self


class WorkflowTemplate(BaseModel):
    """Published workflow graph with a required immutable default policy."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    default_policy_ref: str = Field(min_length=1, max_length=192)
    phases: list[WorkflowPhaseDefinition] = Field(min_length=1, max_length=128)
    required_gates: list[WorkflowGateDefinition] = Field(min_length=3, max_length=32)
    capability_profiles: list["CapabilityProfile"] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def validate_template_shape(self) -> "WorkflowTemplate":
        if len({phase.id for phase in self.phases}) != len(self.phases):
            raise ValueError("workflow template phase IDs must be unique")
        if len({gate.id for gate in self.required_gates}) != len(self.required_gates):
            raise ValueError("workflow template gate IDs must be unique")
        required = {"product_specification_review", "plan_scope_review", "delivery_review"}
        if not required.issubset({gate.id for gate in self.required_gates}):
            raise ValueError("workflow template must define product, plan, and delivery review gates")
        profile_refs = {f"{profile.id}@{profile.version}": profile for profile in self.capability_profiles}
        if len(profile_refs) != len(self.capability_profiles):
            raise ValueError("workflow template capability profile references must be unique")
        for phase in self.phases:
            for reference in phase.capability_profile_refs:
                profile = profile_refs.get(reference)
                if profile is None or profile.role != phase.agent_role:
                    raise ValueError("workflow phase must reference a compatible capability profile")
        return self


class WorkflowPolicy(BaseModel):
    """Platform-controlled limits and mandatory phase/gate rules for a template."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    project_ids: list[str] = Field(min_length=1, max_length=32)
    max_constraints: PlanConstraints
    model_tier_profiles: list["ModelTierProfile"] = Field(
        min_length=3,
        max_length=3,
        description="Published fast, balanced, and complex model ceilings for this policy",
    )
    mandatory_phase_ids: list[str] = Field(default_factory=list, max_length=128)
    required_gate_ids: list[str] = Field(default_factory=list, max_length=32)
    enforce_separation_of_duties: bool = True

    @model_validator(mode="after")
    def validate_policy_shape(self) -> "WorkflowPolicy":
        if len(set(self.project_ids)) != len(self.project_ids):
            raise ValueError("workflow policy project IDs must be unique")
        if len(set(self.mandatory_phase_ids)) != len(self.mandatory_phase_ids):
            raise ValueError("workflow policy mandatory phase IDs must be unique")
        if len(set(self.required_gate_ids)) != len(self.required_gate_ids):
            raise ValueError("workflow policy required gate IDs must be unique")
        if {profile.tier for profile in self.model_tier_profiles} != set(ModelTier):
            raise ValueError("workflow policy must define fast, balanced, and complex model tier profiles")
        if any(
            profile.max_budget_usd > self.max_constraints.max_cost_usd
            or profile.max_turns_per_phase > self.max_constraints.max_turns_per_phase
            for profile in self.model_tier_profiles
        ):
            raise ValueError("workflow model tier profile exceeds policy constraints")
        return self


class ModelTierProfile(BaseModel):
    """Non-secret platform mapping from a logical tier to permitted model ceilings."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    tier: ModelTier
    model_alias: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    max_budget_usd: float = Field(gt=0)
    max_turns_per_phase: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_finite_budget(self) -> "ModelTierProfile":
        if not isfinite(self.max_budget_usd):
            raise ValueError("model tier profile budget must be finite")
        return self


class CapabilityProfile(BaseModel):
    """A named narrowing profile over registered agent/MCP authority."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    role: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    allowed_mcp_tools: list[str] = Field(default_factory=list, max_length=128)

    @model_validator(mode="after")
    def validate_explicit_tools(self) -> "CapabilityProfile":
        if any(not tool or tool == "*" for tool in self.allowed_mcp_tools) or len(set(self.allowed_mcp_tools)) != len(
            self.allowed_mcp_tools
        ):
            raise ValueError("capability profile tools must be unique explicit names")
        return self


class ProjectWorkflowBinding(BaseModel):
    """Platform-owned selection of the only template a project may use by default."""

    model_config = ConfigDict(extra="forbid")

    project_id: str = Field(min_length=1, max_length=128)
    template_ref: str = Field(min_length=1, max_length=192)
    policy_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=192,
        description="Optional platform override; otherwise the template default is mandatory",
    )
    target_repos: list[str] = Field(min_length=1, max_length=10)
    spec_set: str = Field(min_length=1, max_length=256)
    constraints: PlanConstraints = Field(default_factory=PlanConstraints)


class ResolvedWorkflowPhase(BaseModel):
    """One active or skipped phase with the effective platform-selected tier."""

    model_config = ConfigDict(extra="forbid")

    id: str
    active: bool
    activation_reason: str = Field(min_length=1, max_length=1_000)
    agent_role: str
    model_tier: ModelTier
    capability_profile_refs: list[str] = Field(default_factory=list)


class ResolvedWorkflow(BaseModel):
    """Immutable execution contract compiled from policy, template, and run artifacts."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    run_id: str = Field(min_length=1, max_length=64)
    project_id: str = Field(min_length=1, max_length=128)
    template_ref: str = Field(min_length=1, max_length=192)
    policy_ref: str = Field(min_length=1, max_length=192)
    model_tier_profile_refs: list[str] = Field(default_factory=list)
    capability_profile_refs: list[str] = Field(default_factory=list)
    source_artifact: ArtifactReference
    product_specification_artifact: ArtifactReference
    specification_evaluation_artifact: ArtifactReference
    plan_artifact: ArtifactReference
    gates: list[WorkflowGateDefinition] = Field(min_length=3)
    phases: list[ResolvedWorkflowPhase] = Field(min_length=1)
    effective_constraints: PlanConstraints

    @model_validator(mode="after")
    def validate_resolution_shape(self) -> "ResolvedWorkflow":
        if len({gate.id for gate in self.gates}) != len(self.gates):
            raise ValueError("resolved workflow gate IDs must be unique")
        if len({phase.id for phase in self.phases}) != len(self.phases):
            raise ValueError("resolved workflow phase IDs must be unique")
        return self


class WorkflowAdmissionSnapshot(BaseModel):
    """Immutable pre-execution authority used while product review is active.

    A full ``ResolvedWorkflow`` cannot exist until product, evaluation, and
    plan artifacts have been created. This smaller snapshot pins the template
    and policy at submission time so the first mandatory gate is governed by
    the same authority as the later worker contract.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    run_id: str = Field(min_length=1, max_length=64)
    project_id: str = Field(min_length=1, max_length=128)
    template_ref: str = Field(min_length=1, max_length=192)
    policy_ref: str = Field(min_length=1, max_length=192)
    gates: list[WorkflowGateDefinition] = Field(min_length=3, max_length=32)
    effective_constraints: PlanConstraints

    @model_validator(mode="after")
    def validate_snapshot_gates(self) -> "WorkflowAdmissionSnapshot":
        if len({gate.id for gate in self.gates}) != len(self.gates):
            raise ValueError("workflow admission gate IDs must be unique")
        return self


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
    specification_evaluation_artifact: ArtifactReference | None = Field(
        default=None, description="Latest immutable evaluation for the latest product specification revision"
    )
    specification_evaluation_readiness: SpecificationEvaluationReadiness | None = Field(
        default=None, description="Readiness established by the latest immutable evaluation"
    )
    selected_specification_evaluation_artifact: ArtifactReference | None = Field(
        default=None, description="Evaluation immutably selected with the planning input"
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


class ProductSpecificationAcceptanceRequest(ProductSpecificationSelectionRequest):
    """Digest-bound request to validate and accept one displayed product specification."""


class ProductSpecificationAcceptanceOutcome(StrEnum):
    """The operator-visible result of accepting a product specification."""

    ACCEPTED = "accepted"
    NEEDS_REFINEMENT = "needs_refinement"


class ProductSpecificationAcceptanceResponse(PlanningRunResponse):
    """Authoritative state after validating and conditionally selecting a specification."""

    outcome: ProductSpecificationAcceptanceOutcome


class SpecificationEvaluationWaiverRequest(BaseModel):
    """An explicit, auditable exception for a failing immutable evaluation."""

    model_config = ConfigDict(extra="forbid")

    artifact_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the displayed evaluation being waived",
    )
    rationale: str = Field(min_length=1, max_length=2_000, description="Why the named evaluation finding is accepted")

    @model_validator(mode="after")
    def require_nonblank_rationale(self) -> "SpecificationEvaluationWaiverRequest":
        if not self.rationale.strip():
            raise ValueError("waiver rationale must contain non-whitespace text")
        return self


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
    SPECIFICATION_EVALUATION = "specification_evaluation"
    PLAN = "plan"
    IMPLEMENTATION = "implementation"


class WorkbenchStageState(StrEnum):
    """Truthful lifecycle state for one Workbench workflow-map node."""

    COMPLETED = "completed"
    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    AWAITING_OPERATOR = "awaiting_operator"
    NEEDS_REVISION = "needs_revision"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"
    CANCELLED = "cancelled"


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


class WorkbenchActionId(StrEnum):
    """Stable operator action identifiers rendered from the server-owned projection."""

    GENERATE_PRODUCT_SPECIFICATION = "generate_product_specification"
    ACCEPT_PRODUCT_SPECIFICATION = "accept_product_specification"
    REFINE_PRODUCT_SPECIFICATION = "refine_product_specification"
    GENERATE_PLAN = "generate_plan"
    CANCEL_PLANNING_RUN = "cancel_planning_run"


class WorkbenchActionSummary(BaseModel):
    """One permitted action with copy supplied by the workflow authority."""

    action_id: WorkbenchActionId
    stage_id: str
    label: str
    description: str
    requires_confirmation: bool = False


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


class WorkbenchSpecificationEvaluationWaiverSummary(BaseModel):
    """Bounded approver-only audit record for a waived evaluation."""

    artifact_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    actor_id: str
    rationale: str
    created_at: str


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
    specification_evaluation_readiness: SpecificationEvaluationReadiness | None = Field(
        default=None, description="Latest immutable evaluation readiness for the displayed specification"
    )
    specification_evaluation_sha256: str | None = Field(
        default=None,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the latest immutable evaluation for the displayed specification",
    )
    selected_specification_evaluation_sha256: str | None = Field(
        default=None,
        pattern=r"^[a-f0-9]{64}$",
        description="Evaluation digest selected with the product specification for planning",
    )
    available_actions: list[WorkbenchActionSummary] = Field(
        default_factory=list,
        description="Server-authorized operator actions for the current immutable workflow state",
    )
    stages: list[WorkbenchStageSummary] = Field(default_factory=list, description="Ordered authoritative Workflow Map nodes")
    workflow_graph: WorkbenchWorkflowGraph = Field(default_factory=WorkbenchWorkflowGraph, description="Typed relay graph for this run")
    active_gate: CoordinationGate | None = None
    artifacts: list[WorkbenchArtifactSummary] = Field(default_factory=list)
    abilities: list[str] = Field(default_factory=list)
    workflow: list[str] = Field(default_factory=list, description="Authoritative ordered stage and gate labels")
    budget: WorkbenchBudgetSummary
    approval_history_available: bool = False
    approval_history: list[WorkbenchApprovalSummary] = Field(default_factory=list)
    specification_evaluation_waiver: WorkbenchSpecificationEvaluationWaiverSummary | None = None
    mcp_capabilities: WorkbenchMcpCapabilities | None = None
    execution: WorkbenchExecutionSummary | None = None
    failure_summary: str | None = Field(
        default=None,
        max_length=4096,
        description="Bounded sanitized reason for a terminal workflow failure when available",
    )
    external_links: list[WorkbenchExternalLink] = Field(default_factory=list)


class WorkbenchProjectResponse(BaseModel):
    """A project the authenticated principal may select in the Workbench."""

    project_id: str = Field(min_length=1, description="Server-authorized project identifier")


class WorkbenchProjectListResponse(BaseModel):
    """Bounded Workbench project inventory for the authenticated principal."""

    items: list[WorkbenchProjectResponse] = Field(description="Projects authorized for the current principal")


class WorkbenchAgentEvidenceState(StrEnum):
    """Whether a requested agent-operation evidence category is safe and available to render."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    REDACTED = "redacted"


class WorkbenchAgentGatewayRoute(BaseModel):
    """Non-secret immutable gateway route facts for one policy-authorized agent release."""

    policy_revision: str
    role: str
    model_alias: str
    max_budget_usd: float = Field(gt=0)
    toolset: str


class WorkbenchAgentSummary(BaseModel):
    """Safe immutable identity and project-authorized routes for one agent release."""

    registration_id: str
    registration_version: str
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    component_id: str
    component_version: str
    lifecycle: RegistrationLifecycle
    maturity: ComponentMaturity
    execution_class: ExecutionClass
    owner: str
    capabilities: list[str] = Field(default_factory=list)
    gateway_routes: list[WorkbenchAgentGatewayRoute] = Field(default_factory=list)


class WorkbenchAgentListResponse(BaseModel):
    """Bounded project-scoped agent inventory with an ETag-compatible revision."""

    items: list[WorkbenchAgentSummary] = Field(default_factory=list)
    revision: str


class WorkbenchAgentInvocationEvidence(BaseModel):
    """Availability-only evidence summary that never transports raw invocation material."""

    lifecycle: WorkbenchAgentEvidenceState = WorkbenchAgentEvidenceState.AVAILABLE
    actual_cost: WorkbenchAgentEvidenceState = WorkbenchAgentEvidenceState.UNAVAILABLE
    turns_used: WorkbenchAgentEvidenceState = WorkbenchAgentEvidenceState.UNAVAILABLE
    result_artifact: WorkbenchAgentEvidenceState = WorkbenchAgentEvidenceState.REDACTED
    failure_detail: WorkbenchAgentEvidenceState = WorkbenchAgentEvidenceState.REDACTED
    mcp_invocation_outcome: WorkbenchAgentEvidenceState = WorkbenchAgentEvidenceState.UNAVAILABLE


class WorkbenchAgentLifecycleTransition(BaseModel):
    """Safe status-only run lifecycle transition associated with a run-role binding."""

    from_status: AgentRunStatus | None = None
    to_status: AgentRunStatus | None = None
    occurred_at: str


class WorkbenchAgentInvocationSummary(BaseModel):
    """Safe project-scoped run-role binding summary; lifecycle is authoritative only for the run."""

    run_id: str
    root_run_id: str
    parent_run_id: str | None = None
    registration_id: str
    registration_version: str
    role: str
    run_lifecycle_status: AgentRunStatus = Field(
        description="Authoritative root-run lifecycle status; not a role-specific agent outcome"
    )
    workflow_available: bool = Field(
        default=False,
        description="Whether the root run has a Workbench workflow page that the operator may open",
    )
    created_at: str
    updated_at: str
    gateway_route: WorkbenchAgentGatewayRoute | None = Field(
        default=None,
        description="Pinned gateway route when this historical invocation was recorded after gateway routing was introduced",
    )


class WorkbenchAgentInvocationListResponse(BaseModel):
    """Bounded newest-first invocation-binding inventory with an ETag-compatible revision."""

    items: list[WorkbenchAgentInvocationSummary] = Field(default_factory=list)
    revision: str


class WorkbenchAgentInvocationResponse(WorkbenchAgentInvocationSummary):
    """Safe run-role binding detail including exact MCP pins and status-only lifecycle evidence."""

    mcp_grants: list[McpToolGrant] = Field(default_factory=list)
    lifecycle_transitions: list[WorkbenchAgentLifecycleTransition] = Field(default_factory=list)
    evidence: WorkbenchAgentInvocationEvidence = Field(default_factory=WorkbenchAgentInvocationEvidence)


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
    specification_evaluation_sha256: str | None = Field(
        default=None,
        pattern=r"^[a-f0-9]{64}$",
        description="Evaluation digest that authorized the immutable plan snapshot",
    )
    specification_requirement_ids: list[str] = Field(
        default_factory=list,
        description="Exact selected-specification requirements that must be traceable in the plan",
    )
    registry_resolutions: list[RegistrationReference] = Field(
        default_factory=list,
        description="Pinned non-secret registry releases selected for this run",
    )
    workflow_template_ref: str | None = Field(
        default=None,
        max_length=192,
        description="Pinned platform workflow template selected at run admission",
    )
    workflow_policy_ref: str | None = Field(
        default=None,
        max_length=192,
        description="Pinned platform workflow policy selected at run admission",
    )
    workflow_resolution_ref: str | None = Field(
        default=None,
        max_length=1024,
        description="Immutable object-store reference for the resolved workflow",
    )
    workflow_resolution_sha256: str | None = Field(
        default=None,
        pattern=r"^[a-f0-9]{64}$",
        description="Digest of the immutable resolved workflow persisted by the API control plane",
    )
    workflow_required_gate_ids: list[str] = Field(
        default_factory=list,
        description="Mandatory gate identities copied from the immutable resolved workflow",
    )
    traceparent: str | None = Field(default=None, max_length=512)
    tracestate: str | None = Field(default=None, max_length=4096)


class Violation(BaseModel):
    field: str
    message: str
