from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class ReviewProfile(str, Enum):
    STRICT = "strict"
    STANDARD = "standard"
    MINIMAL = "minimal"


class PlanPhase(BaseModel):
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
    max_wall_clock_minutes: int = Field(default=60)
    max_cost_usd: float = Field(default=5.0)
    max_review_rounds: int = Field(default=3)
    max_turns_per_phase: int = Field(default=200)
    backup_reserve_turns: int = Field(
        default=25,
        description="Turns (20-30) reserved to commit and push partial progress before a ceiling forces a stop, so productive work is never lost.",
    )


class AiPlan(BaseModel):
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


class Violation(BaseModel):
    field: str
    message: str
