Feature: execution ceiling validation
  Scenario: reserve leaves productive agent turns
    Given a valid planning request
    When the reserve consumes every allowed agent turn
    Then the planning request is rejected before it is persisted
