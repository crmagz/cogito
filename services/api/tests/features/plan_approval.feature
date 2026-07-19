Feature: Human plan approval
  Product operators review the exact generated plan before any execution workspace exists.

  Scenario: An approved plan begins execution
    Given a generated plan is awaiting approval
    When the authorized operator approves the current plan
    Then the approval is delivered exactly once to the workflow
    And the run enters implementing state

  Scenario: A rejected plan never begins execution
    Given a generated plan is awaiting approval
    When the authorized operator rejects the current plan with a reason
    Then the approval is delivered exactly once to the workflow
    And the run enters rejected state

  Scenario: A stale approval cannot authorize execution
    Given a generated plan is awaiting approval
    When the authorized operator approves a stale plan digest
    Then the approval request is rejected as a conflict
