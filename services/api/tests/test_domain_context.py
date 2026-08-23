from __future__ import annotations

import pytest

from cogito_api.domain_context import DomainContextFrontMatter, parse_domain_context, render_domain_context
from cogito_api.models import SpecificationIntake


def _metadata() -> DomainContextFrontMatter:
    return DomainContextFrontMatter.model_validate(
        {
            "domain_id": "checkout",
            "repository_id": "orders-api",
            "role": "order_service",
            "owners": ["commerce"],
            "relationships": [
                {"repository_id": "storefront", "kind": "api_contract", "direction": "inbound"}
            ],
            "last_assessed_commit": "a" * 40,
        }
    )


def test_domain_context_is_one_markdown_file_with_a_deterministic_mermaid_region() -> None:
    document = render_domain_context(_metadata(), "## Notes\n\nHuman-maintained operational context.")

    parsed = parse_domain_context(document)

    assert parsed.metadata.domain_id == "checkout"
    assert "flowchart LR" in document
    assert "Human-maintained operational context." in document


def test_domain_context_rejects_a_hand_edited_generated_graph() -> None:
    document = render_domain_context(_metadata(), "")

    with pytest.raises(ValueError, match="does not match"):
        parse_domain_context(
            document.replace("repo_storefront -->|api_contract| repo_orders_api", "repo_storefront -->|invented_relationship| repo_orders_api")
        )


def test_product_manager_can_supply_known_repositories_without_relationships() -> None:
    intake = SpecificationIntake.model_validate(
        {
            "objective": "Add a checkout receipt.",
            "actors": ["buyer"],
            "desired_outcomes": ["A buyer receives a receipt."],
            "scope_in": ["Receipt generation"],
            "acceptance_expectations": ["The receipt includes an order ID."],
            "repository_candidates": [
                {"repository_id": "storefront"},
                {"repository_id": "orders-api", "note": "Likely order contract change."},
            ],
            "discovery_preference": "supplied_first",
        }
    )

    assert [candidate.repository_id for candidate in intake.repository_candidates] == ["storefront", "orders-api"]
