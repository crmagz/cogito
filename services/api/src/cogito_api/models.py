from __future__ import annotations

from enum import Enum, StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


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
    quality_gates: list[str] = Field(
        min_length=1,
        max_length=32,
        description="Required contract, safety, and operational gates for the component",
    )

    @model_validator(mode="after")
    def validate_kind_grants(self) -> "RegistrationManifest":
        """Reject tool grants on tool definitions and duplicate capability declarations."""

        if self.kind is RegistrationKind.TOOL and self.grants:
            raise ValueError("tool registrations cannot grant other tools")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("registration capabilities must be unique")
        grant_keys = [(grant.tool_id, grant.tool_version, grant.scope) for grant in self.grants]
        if len(set(grant_keys)) != len(grant_keys):
            raise ValueError("registration grants must be unique")
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


class PlanningRunResponse(BaseModel):
    """Accepted planning-run response returned to API callers."""

    run_id: str = Field(description="Stable planning run identifier")
    status: PlanningRunStatus = Field(description="Authoritative initial lifecycle state")
    source_artifact: ArtifactReference = Field(description="Immutable submitted specification")
    plan_artifact: ArtifactReference | None = Field(
        default=None, description="Immutable generated plan when planning has completed"
    )
    implementation_artifact: ArtifactReference | None = Field(
        default=None, description="Frozen implementation and review evidence when approval is pending"
    )
    submitted_at: str = Field(description="ISO 8601 submission timestamp")


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

    @model_validator(mode="after")
    def require_comment_for_non_approval(self) -> "PlanApprovalRequest":
        """Ensure non-approval decisions carry durable reviewer context."""

        if self.decision is not PlanApprovalDecision.APPROVE and not (self.comment and self.comment.strip()):
            raise ValueError("comment is required when rejecting or requesting revision")
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
    stages: list[WorkbenchStageSummary] = Field(default_factory=list, description="Ordered authoritative Workflow Map nodes")
    workflow_graph: WorkbenchWorkflowGraph = Field(default_factory=WorkbenchWorkflowGraph, description="Typed relay graph for this run")
    active_gate: CoordinationGate | None = None
    artifacts: list[WorkbenchArtifactSummary] = Field(default_factory=list)
    abilities: list[str] = Field(default_factory=list)
    workflow: list[str] = Field(default_factory=list, description="Authoritative ordered stage and gate labels")
    budget: WorkbenchBudgetSummary
    approval_history_available: bool = False
    approval_history: list[WorkbenchApprovalSummary] = Field(default_factory=list)
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
    """Non-executable product-owner feedback types."""

    NOTE = "note"


class WorkbenchFeedbackStage(StrEnum):
    """Current server-owned stages eligible for product-owner notes."""

    SPECIFICATION = "specification"
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
    """Immutable receipt for one accepted product-owner note."""

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

    items: list[WorkbenchFeedbackResponse] = Field(description="Immutable product-owner notes")


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

    @model_validator(mode="after")
    def require_comment_for_non_approval(self) -> "CoordinationApprovalActionRequest":
        """Keep normalized approval actions equivalent to existing gate requests."""

        if self.decision is not PlanApprovalDecision.APPROVE and not (self.comment and self.comment.strip()):
            raise ValueError("comment is required when rejecting or requesting revision")
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
