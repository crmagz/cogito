"""Deterministic, non-mutating evaluation of product-specification revisions."""

from __future__ import annotations

from datetime import datetime, timezone

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
    if not specification.acceptance_criteria:
        findings.append(
            SpecificationEvaluationFinding(
                kind=SpecificationEvaluationFindingKind.UNVERIFIABLE,
                message="The specification has no measurable acceptance criteria.",
                requirement_ids=requirements,
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
            covered_requirement_ids=requirements if readiness is SpecificationEvaluationReadiness.READY else [],
            uncovered_requirement_ids=[] if readiness is SpecificationEvaluationReadiness.READY else requirements,
        ),
        required_decisions=decisions,
        generated_at=datetime.now(timezone.utc).isoformat(),
        generator_version=EVALUATOR_VERSION,
    )


def validate_plan_traceability(specification: ProductSpecification, requirement_ids_by_phase: list[list[str]]) -> None:
    """Reject plans that omit, duplicate, or invent selected requirement IDs."""

    known = set(specification.requirement_ids)
    referenced = [requirement_id for phase_ids in requirement_ids_by_phase for requirement_id in phase_ids]
    unknown = set(referenced) - known
    if unknown:
        raise ValueError("plan references unknown requirement IDs: " + ", ".join(sorted(unknown)))
    duplicates = {item for item in referenced if referenced.count(item) > 1}
    if duplicates:
        raise ValueError("plan references requirement IDs more than once: " + ", ".join(sorted(duplicates)))
    missing = known - set(referenced)
    if missing:
        raise ValueError("plan does not cover requirement IDs: " + ", ".join(sorted(missing)))
