# Operations

## Health and readiness

`/healthz` reports that the API process is alive. `/readyz` reports that
startup completed and, when enabled, the workflow-projection reconciliation
loop is making progress within `api.reconciliation.stallSeconds`.

The worker becomes ready only after it connects to Temporal and initializes
its worker runtime. Its readiness probe reads a container-local sentinel; it
does not expose an HTTP listener or use object-store, database, or provider
credentials as a liveness condition.

## Workflow projection recovery

The API reconciler checks only `implementing` and `finalizing` projections
that have an active workflow ID. It changes a projection only when Temporal
reports a completed workflow with a recognized Cogito result. The database
update verifies the active workflow ID and current states under a row lock.

Waiting approvals, live workflows, failed or cancelled Temporal workflows,
unknown results, and stale workflow revisions remain unchanged. The reconciler
does not create workflows, approve work, or access Kubernetes resources.

Set `api.reconciliation.enabled: false` before reverting a reconciliation
change. Preserve the resulting audit events; do not rewrite prior projections.

## Observability and incident response

When OTLP metrics are enabled, the API exports aggregate reconciliation pass,
inspection, repair, and failure counters. These metrics intentionally have no
run, workflow, artifact, or error-text labels. Use the structured API log's
`run_id` only for an individual incident investigation.

## GitHub read-only MCP connector

`cogito-platform` owns the GitHub connector and its incident response. It is
disabled by default. If it cannot reach GitHub, first verify the connector's
readiness, its GitHub App installation access to the configured allow-list, and
the environment-owned egress proxy. Do not copy the App private key, GitHub App
JWT, or installation token into a shell, an execution Job, or a LiteLLM Secret.
Connector failures are bounded gateway outcomes; inspect the provider and proxy
logs under their existing secret-redaction controls.

If `/readyz` remains non-ready, first inspect API startup and reconciliation
logs, then verify Temporal reachability. If a run projection is stale, compare
the active workflow ID in the Workbench with Temporal before any manual action.
Do not delete execution Jobs, Pods, or Secrets as part of projection recovery;
the worker owns their lifecycle.
