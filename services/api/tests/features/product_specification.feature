Feature: Product specification refinement
  Product intake becomes a structured draft without repository or MCP authority.

  Scenario: A planner draft distinguishes sourced requirements from uncertainty
    Given a source-grounded product specification response
    When the planner refines the product intake
    Then the structured draft preserves source provenance and unresolved questions
    And the planner request has no tool or repository authority
