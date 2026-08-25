Feature: Audited invocation log evidence
  Product operators can inspect bounded, redacted output only for an authorized stage invocation.

  Scenario: An operator opens output for an audited stage invocation
    Given a planning run has a logged stage invocation
    When the authorized operator opens that audit event's output
    Then the operator receives only the bounded redacted output for that invocation
    And another audit event does not expose a raw output stream
