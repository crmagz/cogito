from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunEnvelope:
    run_id: str
    plan_ref: str
    spec_ref: str
    target_repos: list[str] = field(default_factory=list)
    priority: str = "normal"
    submitted_at: str = ""
    submitted_by: str = ""


@dataclass
class RunResult:
    run_id: str
    status: str
