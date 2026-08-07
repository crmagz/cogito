# Cogito SDLC Components

Each directory below is an independently refinable SDLC capability. Its
`component.json` is the non-secret, versioned registration contract consumed
by the Supervisor registry. The component keeps its own implementation,
fixtures, tests, operating notes, and quality gates close to that contract as
it matures.

The component catalog does not grant authority. The Supervisor remains the
only policy, approval, credential-broker, audit, retry, and lifecycle
authority. Components receive only immutable artifact references and scoped
broker capabilities selected for a run.

`agent_gateway_policy.json` is the separately versioned, non-secret policy
that selects an agent release, LiteLLM model alias, budget ceiling, and
toolset label for each project and active LLM role. It is an allow-list: a
catalog release cannot execute merely because it is registered, and a policy
route cannot grant a tool that the selected agent release does not declare.
This first substrate routes the planner and developer paths; separate reviewer,
validator, environment-test, and publishing adapters must be added before a
route is declared for those roles.

## Lifecycles

Component maturity describes the operational state of the capability:

`incubating` → `active` → `deprecated` → `retired`

Registration lifecycle describes whether a particular immutable release can be
selected for a new run:

`draft` → `active` → `disabled` / `revoked`

These lifecycles are intentionally separate. Disabling a release blocks new
selection; a run that already recorded a pinned release keeps that identity so
its audit trail and retry behavior cannot drift.

## Execution Classes

Initial Phase 12 releases use `adapter` to wrap established behavior in the
API/worker. A future immutable release may use `worker_service` or
`isolated_job` after its component-specific contract, security, operations,
and compatibility gates pass. Changing execution class never gives a component
independent approval, credential, audit, or policy authority.

## Initial Capability Map

| Area | Agent | Brokered tools |
|---|---|---|
| Planning / SDD | `planner` | `planning_model` |
| Software delivery | `developer` | `execution_workspace`, `developer_harness` |
| Adversarial review | `reviewer` | `execution_workspace`, `review_model` |
| Validation management | `validator` | `validation_runner` |
| Ephemeral environments | `ephemeral_environment_tester` | `ephemeral_environment` |
| Git / release management | `pull_request_publisher` | `github_publisher` |
