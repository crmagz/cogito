Feature: Product specification refinement
  Product intake becomes a structured draft without repository or MCP authority.

  Scenario: A planner draft distinguishes sourced requirements from uncertainty
    Given a source-grounded product specification response
    When the planner refines the product intake
    Then the structured draft preserves source provenance and unresolved questions
    And the planner request has no tool or repository authority

  Scenario: An authorized operator retains one immutable product-specification draft
    Given a planning intake awaiting a product specification draft
    When the authorized operator generates the product specification draft
    Then the planning run exposes its immutable product specification draft
    And retrying the draft generation returns the existing draft

  Scenario: An authorized operator records a validated revised product specification
    Given a planning intake awaiting a product specification draft
    When the authorized operator generates the product specification draft
    And the authorized operator records a revised product specification
    Then the planning run exposes the new immutable product specification revision
