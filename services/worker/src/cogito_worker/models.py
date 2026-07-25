from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RunEnvelope:
    run_id: str
    plan_ref: str
    spec_ref: str
    plan_sha256: str = ""
    target_repos: list[str] = field(default_factory=list)
    priority: str = "normal"
    submitted_at: str = ""
    submitted_by: str = ""
    requires_plan_approval: bool = False
    requires_implementation_approval: bool = False
    traceparent: str | None = None
    tracestate: str | None = None


@dataclass
class RunResult:
    run_id: str
    status: str


@dataclass(frozen=True)
class ImplementationArtifact:
    """Immutable object-store identity for the converged implementation evidence."""

    ref: str
    sha256: str


@dataclass
class ExecutionWorkspace:
    """Identifies the per-run execution pod and its private workspace."""

    run_id: str
    job_name: str
    workspace_root: str
    repositories: list[str] = field(default_factory=list)
    repository_origins: dict[str, str] = field(default_factory=dict)
    base_commits: dict[str, str] = field(default_factory=dict)
    run_key_secret: str = ""
    run_git_secret: str = ""


@dataclass(frozen=True)
class PlanPhase:
    """A validated phase from the immutable plan snapshot."""

    id: str
    name: str
    description: str
    tasks: list[str]
    acceptance_criteria: list[str]
    verification: list[str]
    depends_on: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, value: object) -> "PlanPhase":
        """Parse the fields needed by the worker without trusting plan JSON."""

        if not isinstance(value, dict):
            raise ValueError("plan phase must be an object")
        required_strings = ("id", "name", "description")
        if not all(isinstance(value.get(field), str) and value[field].strip() for field in required_strings):
            raise ValueError("plan phase is missing a required string field")
        list_fields = ("tasks", "acceptance_criteria", "verification")
        if not all(
            isinstance(value.get(field), list)
            and value[field]
            and all(isinstance(item, str) and item.strip() for item in value[field])
            for field in list_fields
        ):
            raise ValueError("plan phase requires non-empty tasks, acceptance criteria, and verification commands")
        depends_on = value.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(isinstance(item, str) and item.strip() for item in depends_on):
            raise ValueError("plan phase dependencies must be a list of non-empty phase IDs")
        return cls(
            id=value["id"],
            name=value["name"],
            description=value["description"],
            tasks=value["tasks"],
            acceptance_criteria=value["acceptance_criteria"],
            verification=value["verification"],
            depends_on=depends_on,
        )


@dataclass(frozen=True)
class VerificationResult:
    """Bounded evidence from one approved verification command."""

    command: str
    passed: bool
    output: str


@dataclass(frozen=True)
class PhaseResult:
    """Durable execution evidence for one phase."""

    phase_id: str
    branch_name: str
    succeeded: bool
    turns_used: int | None
    cost_usd: float | None
    changed_files: list[str]
    commits: dict[str, str]
    verification: list[VerificationResult]
    summary: str
    outcome: str = "completed"
    ceiling: str | None = None

    def metadata(self) -> dict[str, Any]:
        """Return JSON-compatible metadata safe to persist in the run status."""

        return asdict(self)


@dataclass(frozen=True)
class PhaseExecutionRequest:
    """Inputs required to run one approved phase in an isolated workspace."""

    phase: PlanPhase
    workspace: ExecutionWorkspace
    max_turns: int
    timeout_seconds: int
    backup_reserve_turns: int = 25

    traceparent: str | None = None
    tracestate: str | None = None


@dataclass(frozen=True)
class BackupExecutionRequest:
    """Inputs for deterministic, non-productive recovery of one stopped phase."""

    phase: PlanPhase
    workspace: ExecutionWorkspace
    ceiling: str
    timeout_seconds: int


@dataclass(frozen=True)
class ExecutionRequest:
    """Non-secret inputs that initialize one isolated execution workspace."""

    run_id: str
    spec_ref: str
    target_repos: list[str]
    execution_timeout_seconds: int = 0
    max_cost_usd: float = 0.0
    run_key_secret: str = ""
    run_git_secret: str = ""


@dataclass(frozen=True)
class ResolvedSpecFile:
    """A generic spec file selected from an immutable spec-set archive."""

    path: str
    content: str
    priority: str


@dataclass(frozen=True)
class ResolvedSpecSet:
    """The generic, always-on portion of one versioned spec set."""

    ref: str
    files: list[ResolvedSpecFile]


@dataclass(frozen=True)
class ReviewFinding:
    """One bounded, classified observation produced by a read-only reviewer."""

    severity: str
    lens: str
    model: str
    file: str
    line: int | None
    description: str
    evidence: str | None = None
    suggested_fix: str | None = None
    verified: bool | None = None

    def metadata(self) -> dict[str, Any]:
        """Return JSON-safe evidence without exposing reviewer credentials."""

        return asdict(self)


@dataclass(frozen=True)
class ReviewRequest:
    """Read-only inputs for one adversarial review round."""

    workspace: ExecutionWorkspace
    phase_results: list[dict[str, Any]]
    round_number: int
    review_profile: str


@dataclass(frozen=True)
class ReviewResult:
    """Findings from all independent reviewer lenses in one round."""

    findings: list[ReviewFinding]

    def metadata(self) -> list[dict[str, Any]]:
        """Return JSON-safe finding evidence."""

        return [finding.metadata() for finding in self.findings]


@dataclass(frozen=True)
class ReviewRevisionRequest:
    """Bounded developer revision request for verified blocking findings only."""

    workspace: ExecutionWorkspace
    findings: list[ReviewFinding]
    phases: list[PlanPhase]
    max_turns: int
    timeout_seconds: int


@dataclass(frozen=True)
class ReviewRevisionResult:
    """Evidence that the developer addressed one verified review round."""

    succeeded: bool
    summary: str
    commits: dict[str, str]
    changed_files: list[str]
    verification: list[VerificationResult]
