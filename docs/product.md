# Cogito product overview

Cogito is an agentic-development control plane. A developer submits a plan and
the repositories and standards that apply to it; Cogito validates the request,
persists an immutable execution snapshot, and orchestrates its execution in an
isolated workspace. The intended outcome is a reviewed pull request rather
than an ungoverned autonomous code change.

## Who it is for

Cogito is for engineering teams that want to use coding agents without losing
the controls expected of a production delivery system: scoped credentials,
explicit approvals, durable workflow state, isolated execution, and artifacts
that can be reviewed after the fact.

## Current capability boundary

Today, Cogito provides the production foundation for that workflow:

- A FastAPI API accepts and validates plan, repository, DAG, and constraint
  inputs.
- Plans are persisted to object storage and snapshotted before execution.
- A Temporal worker coordinates the durable run lifecycle.
- Each execution uses a short-lived Kubernetes Job in a dedicated execution
  namespace with a constrained service account and network policy.
- MinIO is used locally; production deployment is configured for external S3
  and PostgreSQL.
- LiteLLM is deployed as the model gateway and is the future control point for
  model routing, virtual keys, toolsets, MCP servers, A2A registration, and
  spend tracking.

The first governed planning path is now present: an initial work specification
is stored as an immutable artifact, the Supervisor can generate a normalized
plan through an API-only LiteLLM virtual key, and Temporal waits at a
digest-bound human plan-approval gate before it provisions an execution
workspace. A requested revision clears the active plan, creates a new
revisioned artifact and Temporal workflow even when the plan content is
byte-identical, and scopes idempotency to that revision. Prior artifacts and
decisions remain auditable, but cannot authorize the replacement plan.
Approval decisions are authenticated, idempotent, and retained as audit
records. They are placed in a leased, retrying transactional outbox before
delivery to Temporal, so a transient control-plane failure cannot turn an
approval into a lost instruction. A persisted plan's workflow start is also
safe to retry without regenerating the plan. Local kind uses an explicit
development credential; production requires OIDC bearer-token validation.

The Helm chart declares three stable role policies: `planner`, `developer`,
and `reviewer`. Each names one model alias, a positive LiteLLM virtual-key
budget ceiling and reset period, and a toolset label. Keys are provisioned by
trusted secret-management infrastructure into distinct Kubernetes Secrets;
the chart never creates or aggregates them. Only the planner Secret is mounted
by the API today. Developer and reviewer runtimes do not yet exist, so their
keys are deliberately not mounted anywhere. No MCP server is registered in
this release, which means every role has zero tool authority. When MCP arrives,
the provisioning layer must map the role's toolset label to an explicit LiteLLM
MCP-server and per-tool allow-list—semantic discovery may narrow that list but
must never expand it.

LiteLLM tier definitions also carry explicit positive per-token input and
output costs. This is essential: a virtual-key budget is meaningful only when
the gateway can calculate spend. The shipped values reflect the current listed
rates for the configured Bedrock models; operators must update those values for
their provider, region, and pricing changes before promotion.

Multi-phase implementation is available through a pinned Claude Code runtime.
The worker validates a stable topological order from each approved phase's
`depends_on` list and runs every ready phase in the same isolated workspace and
feature branch. It records the CLI's turn count and gateway-reported cost,
changed files, verification output, and resulting commit SHA in durable run
metadata. The feature branch is published only after every approved
verification command passes.

Execution has hard, explicit stop behavior. `max_wall_clock_minutes` is a
run-wide productive-work deadline, while `max_turns_per_phase` includes a
required 20–30-turn recovery reserve. The worker passes only the productive
portion to Claude Code. On a known turns, local wall-clock, or LiteLLM budget
ceiling, it does not make another model call: it stages existing work, creates
a deterministic backup commit if needed, validates the approved branch and
origin again, and pushes the feature branch before recording
`stopped_with_backup`. Unknown 429 responses and ordinary failures fail
closed. A backup that cannot be committed or published is a failed run rather
than a successful stop. Each execution Job receives that approved wall-clock
budget plus the bounded recovery allowance; the Job still cannot exceed the
operator-configured execution deadline.

Before a Job is created, the worker uses its separate, operator-provisioned
LiteLLM management credential to mint one opaque, short-lived, model-limited
run key whose `max_budget` equals the immutable plan cost. The key is stored in
a labelled run Secret and only that Secret (plus the repository-scoped Git
credential) reaches the execution pod. The management credential never enters
the Job, Temporal history, prompts, logs, or status metadata. The worker's
authority is limited to the run-specific workspace lifecycle and command
channel; execution pods receive no Kubernetes service account token.

Delegated A2A sub-agents, semantic tool discovery, MCP tool execution,
adversarial implementation review, the final implementation gate, and the
operator UI are deliberately not represented as completed features yet. They
are the next product layers on top of this execution substrate, not implicit
behavior hidden in the current worker.

## Target agentic ecosystem

The planned ecosystem keeps policy and execution boundaries explicit:

| Layer | Responsibility |
|---|---|
| Supervisor | Custom FastAPI service that owns sessions, budgets, approvals, and delegation through LiteLLM's OpenAI-compatible API. |
| LiteLLM | Model routing, MCP registry and execution, virtual keys, stable-role toolsets, A2A registry, logging, and spend tracking. |
| Tool discovery | Stable roles use LiteLLM toolsets; a semantic filter narrows dynamically relevant tools without granting new authority. |
| Tool runtimes | One MCP server per meaningful security or deployment boundary, with only the access that boundary needs. |
| Sub-agents | Separate A2A services, each with its own restricted LiteLLM key and toolset for least privilege and separate accounting. |

The Supervisor makes policy decisions. LiteLLM enforces gateway-level model,
tool, and accounting controls. Workers and sub-agents remain in the execution
plane, where their credentials and network reach can be narrowly constrained.

## Architecture

```text
Developer / CI
     |
     v
API -- validates request --> immutable plan snapshot (S3 / MinIO)
     |                                    |
     v                                    v
Temporal workflow <------------------ Cogito worker
     |
     v
isolated Kubernetes Job --> workspace artifacts --> reviewed pull request

LiteLLM sits beside the control plane as the model and tool gateway.
```

The API owns request validation and the externally visible run lifecycle. The
worker owns durable orchestration. Runtime Jobs receive only the credentials
and network access needed for their task. This separation is intentional: an
agent runtime must not gain control-plane credentials merely because it runs a
task.

## Deployment posture

Cogito ships as an umbrella Helm chart. The local defaults include PostgreSQL,
Temporal, and MinIO for an end-to-end developer environment. Production values
disable the local data services, require external persistence, and require
immutable image digests. See the [release guide](releases.md) for the artifact
and promotion model.
