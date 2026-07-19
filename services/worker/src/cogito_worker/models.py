from __future__ import annotations

from dataclasses import dataclass, field


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


@dataclass
class RunResult:
    run_id: str
    status: str


@dataclass
class ExecutionWorkspace:
    """Identifies the per-run execution pod and its private workspace."""

    run_id: str
    job_name: str
    workspace_root: str


@dataclass(frozen=True)
class ExecutionRequest:
    """Non-secret inputs that initialize one isolated execution workspace."""

    run_id: str
    spec_ref: str
    target_repos: list[str]


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
