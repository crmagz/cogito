# Agent Completion Standard

## Completion and remediation

An agent must not report an implementation goal as complete until the approved
acceptance criteria and every approved verification command have passed in the
same clean execution workspace that will be delivered.

When an implementation, verification, review, or delivery check fails, the
responsible agent must inspect the concrete failure, remediate it within its
authorized scope, rerun all affected approved verification commands, commit the
correction, and leave the feature branch clean. A failure may be reported as
terminal only when the agent has exhausted its authorized remediation path or a
concrete external dependency prevents completion; in either case the report
must name that blocker and the next required action.

Every platform defect correction must include a regression test that reproduces
the prior failure and proves the corrected behavior. Do not substitute an
unverified implementation summary for executable evidence.
