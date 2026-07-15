from __future__ import annotations

from .config import Settings
from .models import PlanConstraints, PlanPhase, Violation

_WHITE, _GRAY, _BLACK = 0, 1, 2


def validate_phase_dag(phases: list[PlanPhase]) -> list[Violation]:
    violations: list[Violation] = []
    ids = {phase.id for phase in phases}
    for i, phase in enumerate(phases):
        for dep in phase.depends_on:
            if dep not in ids:
                violations.append(
                    Violation(
                        field=f"phases[{i}].depends_on",
                        message=f"references non-existent phase '{dep}'",
                    )
                )
    if violations:
        # Cycle detection assumes every depends_on reference resolves to a real phase.
        return violations

    graph = {phase.id: phase.depends_on for phase in phases}
    color = {phase_id: _WHITE for phase_id in graph}

    def visit(node: str) -> str | None:
        color[node] = _GRAY
        for dep in graph[node]:
            if color[dep] == _GRAY:
                return dep
            if color[dep] == _WHITE:
                found = visit(dep)
                if found:
                    return found
        color[node] = _BLACK
        return None

    for phase in phases:
        if color[phase.id] == _WHITE:
            cycle_node = visit(phase.id)
            if cycle_node:
                violations.append(
                    Violation(
                        field="phases",
                        message=f"dependency cycle detected involving phase '{cycle_node}'",
                    )
                )
                break

    return violations


def validate_constraints(constraints: PlanConstraints, settings: Settings) -> list[Violation]:
    checks = [
        ("constraints.max_wall_clock_minutes", constraints.max_wall_clock_minutes, settings.max_wall_clock_minutes),
        ("constraints.max_cost_usd", constraints.max_cost_usd, settings.max_cost_usd),
        ("constraints.max_review_rounds", constraints.max_review_rounds, settings.max_review_rounds),
        ("constraints.max_turns_per_phase", constraints.max_turns_per_phase, settings.max_turns_per_phase),
    ]
    return [
        Violation(field=field, message=f"{value} exceeds system-configured maximum of {limit}")
        for field, value, limit in checks
        if value > limit
    ]
