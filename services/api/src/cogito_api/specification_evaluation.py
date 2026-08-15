"""Deterministic, non-mutating evaluation of product-specification revisions."""

from __future__ import annotations

import re

from .models import (
    ProductSpecification,
    SpecificationEvaluation,
    SpecificationEvaluationCoverage,
    SpecificationEvaluationFinding,
    SpecificationEvaluationFindingKind,
    SpecificationEvaluationReadiness,
    SpecificationRiskTier,
)

EVALUATOR_VERSION = "deterministic-v1"


def evaluate_specification(
    specification: ProductSpecification, *, specification_sha256: str, specification_revision: int
) -> SpecificationEvaluation:
    """Return reproducible readiness facts without rewriting the specification.

    Version 1 is intentionally display-only: historical artifacts remain
    reviewable but require a version-2 revision before they can authorize a
    new plan.  Version 2's mandatory structural sections are checked here so
    failures are explicit evidence rather than silent evaluator repair.
    """

    findings: list[SpecificationEvaluationFinding] = []
    decisions: list[str] = []
    requirements = specification.requirement_ids

    if specification.schema_version < 2:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.MISSING,
                message="A version 2 product specification is required before planning.",
            )
        )
    else:
        for field, label in (
            (specification.personas, "personas"),
            (specification.user_journeys, "user journeys"),
            (specification.constraints, "constraints"),
            (specification.dependencies, "dependencies"),
        ):
            if not field:
                findings.append(
                    SpecificationEvaluationFinding(
                        kind=SpecificationEvaluationFindingKind.MISSING,
                        message=f"The version 2 specification has no {label}.",
                    )
                )

    if not requirements:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.MISSING,
                message="The specification has no functional or non-functional requirements.",
            )
        )
    covered_requirement_ids = {
        requirement_id
        for criterion in specification.acceptance_criteria
        for requirement_id in criterion.requirement_ids
    }
    uncovered_requirement_ids = sorted(set(requirements) - covered_requirement_ids)
    if not specification.acceptance_criteria:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.UNVERIFIABLE,
                message="The specification has no measurable acceptance criteria.",
                requirement_ids=requirements,
            )
        )
    elif uncovered_requirement_ids:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.UNVERIFIABLE,
                message="Some requirements have no linked acceptance criterion.",
                requirement_ids=uncovered_requirement_ids,
            )
        )
    if specification.unresolved_questions:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.AMBIGUOUS,
                message="The specification contains unresolved questions.",
            )
        )
        decisions.extend(question.text for question in specification.unresolved_questions)
    if specification.assumptions:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.AMBIGUOUS,
                message="The specification contains unconfirmed assumptions.",
            )
        )
        decisions.extend(assumption.text for assumption in specification.assumptions)

    conflicting_requirement_ids = _find_conflicting_requirement_ids(
        specification.functional_requirements + specification.non_functional_requirements
    )
    if conflicting_requirement_ids:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.CONFLICTING,
                message="Requirements contain directly contradictory statements.",
                requirement_ids=conflicting_requirement_ids,
            )
        )

    high_risk = any("security" in risk.text.lower() or "data loss" in risk.text.lower() for risk in specification.risks)
    risk_tier = SpecificationRiskTier.HIGH if high_risk else SpecificationRiskTier.MEDIUM if specification.risks else SpecificationRiskTier.LOW
    readiness = SpecificationEvaluationReadiness.READY if not findings else SpecificationEvaluationReadiness.NEEDS_REVISION
    return SpecificationEvaluation(
        specification_sha256=specification_sha256,
        specification_revision=specification_revision,
        readiness=readiness,
        risk_tier=risk_tier,
        findings=findings,
        coverage=SpecificationEvaluationCoverage(
            covered_requirement_ids=sorted(covered_requirement_ids),
            uncovered_requirement_ids=uncovered_requirement_ids,
        ),
        required_decisions=decisions,
        # Creation time is recorded by the supervisor's immutable audit row.
        # Keep the canonical evaluation payload itself revision-deterministic.
        generated_at="1970-01-01T00:00:00+00:00",
        generator_version=EVALUATOR_VERSION,
    )


def _find_conflicting_requirement_ids(statements: list[object]) -> list[str]:
    """Detect simple explicit polarity conflicts without inventing semantic facts."""

    normalized: dict[str, tuple[bool, str]] = {}
    conflicts: set[str] = set()
    for statement in statements:
        text = getattr(statement, "text", "").lower()
        statement_id = getattr(statement, "id", "")
        has_not = bool(re.search(r"\b(not|never|must not|shall not)\b", text))
        base = re.sub(r"\b(not|never|must not|shall not)\b", "", text)
        base = re.sub(r"[^a-z0-9]+", " ", base).strip()
        if not base:
            continue
        previous = normalized.get(base)
        if previous is not None and previous[0] != has_not:
            conflicts.update((previous[1], statement_id))
        else:
            normalized[base] = (has_not, statement_id)
    return sorted(conflicts)


def validate_plan_traceability(specification: ProductSpecification, requirement_ids_by_phase: list[list[str]]) -> None:
    """Reject plans that omit, duplicate, or invent selected requirement IDs."""

    known = set(specification.requirement_ids)
    empty_phases = [str(index + 1) for index, phase_ids in enumerate(requirement_ids_by_phase) if not phase_ids]
    if empty_phases:
        raise ValueError("each plan phase must cover at least one requirement ID: " + ", ".join(empty_phases))
    referenced = [requirement_id for phase_ids in requirement_ids_by_phase for requirement_id in phase_ids]
    unknown = set(referenced) - known
    if unknown:
        raise ValueError("plan references unknown requirement IDs: " + ", ".join(sorted(unknown)))
    seen: set[str] = set()
    duplicates: set[str] = set()
    for requirement_id in referenced:
        if requirement_id in seen:
            duplicates.add(requirement_id)
        seen.add(requirement_id)
    if duplicates:
        raise ValueError("plan references requirement IDs more than once: " + ", ".join(sorted(duplicates)))
    missing = known - set(referenced)
    if missing:
        raise ValueError("plan does not cover requirement IDs: " + ", ".join(sorted(missing)))
