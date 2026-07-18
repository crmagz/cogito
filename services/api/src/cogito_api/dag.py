from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit

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


_COMMIT_PATTERN = re.compile(r"[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?")
_SPEC_REF_PATTERN = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]*@[A-Za-z0-9][A-Za-z0-9._-]*#sha256=[0-9a-fA-F]{64}"
)


def validate_target_repositories(target_repos: list[str], allowed_hosts: tuple[str, ...]) -> list[Violation]:
    """Require pinned, credential-free HTTPS repository references from approved hosts."""

    violations: list[Violation] = []
    for index, repository in enumerate(target_repos):
        try:
            parsed = urlsplit(repository)
            host = parsed.hostname
            is_ip_address = host is not None and _is_ip_address(host)
        except ValueError:
            parsed = None
            host = None
            is_ip_address = False
        if (
            parsed is None
            or parsed.scheme != "https"
            or not host
            or is_ip_address
            or not _host_is_allowed(host, allowed_hosts)
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.strip("/")
            or parsed.query
            or not _COMMIT_PATTERN.fullmatch(parsed.fragment)
        ):
            violations.append(
                Violation(
                    field=f"target_repos[{index}]",
                    message="must be a credential-free HTTPS URL from an approved host, pinned with a 40- or 64-character commit SHA fragment",
                )
            )
    return violations


def validate_spec_reference(spec_ref: str) -> list[Violation]:
    """Require the spec archive digest that makes a named spec set reproducible."""

    if _SPEC_REF_PATTERN.fullmatch(spec_ref):
        return []
    return [
        Violation(
            field="spec_set",
            message="must use name@version#sha256=<64 lowercase-or-uppercase hex characters>",
        )
    ]


def _host_is_allowed(host: str, allowed_hosts: tuple[str, ...]) -> bool:
    normalized_host = host.lower().rstrip(".")
    for allowed_host in allowed_hosts:
        normalized_allowed = allowed_host.lower().rstrip(".")
        if normalized_allowed.startswith("*."):
            suffix = normalized_allowed[1:]
            if normalized_host.endswith(suffix) and normalized_host != suffix[1:]:
                return True
        elif normalized_host == normalized_allowed:
            return True
    return False


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True
