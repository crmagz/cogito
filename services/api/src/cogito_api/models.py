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
    max_cost_usd: float = Field(default=5.0)
    max_review_rounds: int = Field(default=3)
    max_turns_per_phase: int = Field(default=200)
    backup_reserve_turns: int = Field(
        default=25,
        description="Turns (20-30) reserved to commit and push partial progress before a ceiling forces a stop, so productive work is never lost.",
    )


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
    PLANNING_FAILED = "planning_failed"
    REJECTED = "rejected"
    REVISION_REQUESTED = "revision_requested"


class PlanApprovalDecision(StrEnum):
    """Human decision permitted at the plan-approval gate."""

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


class PlanningRunResponse(BaseModel):
    """Accepted planning-run response returned to API callers."""

    run_id: str = Field(description="Stable planning run identifier")
    status: PlanningRunStatus = Field(description="Authoritative initial lifecycle state")
    source_artifact: ArtifactReference = Field(description="Immutable submitted specification")
    plan_artifact: ArtifactReference | None = Field(
        default=None, description="Immutable generated plan when planning has completed"
    )
    submitted_at: str = Field(description="ISO 8601 submission timestamp")


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
    requires_plan_approval: bool = Field(
        default=False,
        description="Whether the workflow must wait for a digest-bound plan decision before execution",
    )


class Violation(BaseModel):
    field: str
    message: str
