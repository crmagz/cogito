from __future__ import annotations

from cogito_api.models import ProductSpecification
from cogito_api.specification_evaluation import evaluate_specification, validate_plan_traceability


def test_v2_specification_evaluation_is_ready_and_digest_bound(valid_product_specification: dict) -> None:
    specification = ProductSpecification.model_validate(valid_product_specification)

    evaluation = evaluate_specification(
        specification, specification_sha256="a" * 64, specification_revision=3
    )

    assert evaluation.readiness.value == "ready"
    assert evaluation.specification_sha256 == "a" * 64
    assert evaluation.specification_revision == 3
    assert evaluation.coverage.covered_requirement_ids == ["functional-1"]


def test_legacy_or_ambiguous_specification_needs_revision(valid_product_specification: dict) -> None:
    valid_product_specification["schema_version"] = 1
    valid_product_specification["unresolved_questions"] = [
        {"id": "question-1", "text": "What rate applies?", "kind": "question", "source_segment_ids": []}
    ]

    evaluation = evaluate_specification(
        ProductSpecification.model_validate(valid_product_specification),
        specification_sha256="b" * 64,
        specification_revision=1,
    )

    assert evaluation.readiness.value == "needs_revision"
    assert {finding.kind.value for finding in evaluation.findings} == {"missing", "ambiguous"}


def test_traceability_rejects_unknown_duplicate_and_uncovered_requirements(valid_product_specification: dict) -> None:
    specification = ProductSpecification.model_validate(valid_product_specification)

    for phases, message in (
        ([[]], "does not cover"),
        ([["unknown"]], "unknown"),
        ([["functional-1"], ["functional-1"]], "more than once"),
    ):
        try:
            validate_plan_traceability(specification, phases)
        except ValueError as error:
            assert message in str(error)
        else:
            raise AssertionError("invalid traceability was accepted")
