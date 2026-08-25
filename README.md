# Cogito

An agentic development platform. Submit a plan, point it at your repos and coding standards, and get back a reviewed pull request.

```
Specifications + Repositories + Plan  →  Reviewed PR
```

Cogito is designed to turn a reviewed plan into an isolated, observable execution
run. Read the [product overview](docs/product.md) for the current capability
boundary and [release guide](docs/releases.md) for the independently versioned
API, worker, and Helm chart artifacts.

Operational health, reconciliation boundaries, and incident response are
documented in the [operations guide](docs/operations.md).

The specification lifecycle and its centralized Workbench operator workflow
are documented in the [specification evaluation lifecycle guide](docs/specification-evaluation-lifecycle.md)
and the durable [project context](docs/CONTEXT.md).

## Helm Chart

Cogito deploys as an umbrella Helm chart with the following components:

| Component | Subchart | Purpose |
|-----------|----------|---------|
| PostgreSQL | `bitnami/postgresql` | Persistence for Temporal (default + visibility stores) |
| Temporal | `temporalio/temporal` | Durable workflow orchestration |
| MinIO | `minio/minio` | Object storage for plans and artifacts |
| API | local template (`services/api`) | Plan submission REST API: schema/DAG/constraint validation, plan storage |
| Worker | local template (`services/worker`) | Temporal workflow worker: loads persisted plans and reports run status |

### Local Kind observability

`charts/values-kind-observability.yaml` connects Cogito to the local Grafana
OSS platform. The platform is deliberately composed from independent Helm
releases, because Helm dependencies always inherit their parent's namespace.
The Kind profile retains MinIO data on a 5 Gi PVC and uses the same
in-cluster MinIO service as an S3-compatible store for Loki, Tempo, and Mimir.
It is intentionally ClusterIP-only: it creates no Ingress or gateway
requirement.

Install Cogito as `cogito` in the `cogito` namespace:

```sh
helm upgrade --install cogito charts/ \
  -f charts/values.yaml \
  -f charts/values-kind-observability.yaml \
  --namespace cogito --create-namespace
```

Then install the separately namespaced observability platform:

```sh
KUBE_CONTEXT=kind-cogito-observability \
  deploy/observability/kind/install.sh install
```

Open Grafana without an ingress controller:

```sh
kubectl -n grafana port-forward service/grafana 3000:80
```

The default local Grafana administrator password is managed by the Grafana
release Secret; retrieve it rather than placing it in values. See the
[Kind platform contract](deploy/observability/kind/README.md) for the release
and namespace map.
The Kind profile is for local development only: production must provide
separate buckets, credentials, retention, and workload-identity policy.

### Production

`charts/values-production.yaml` is a non-deployable contract template, not a
set of credentials or environment values. Before deploying, an environment
owner must supply a private values file with immutable image digests, real
external database and object-store endpoints, TLS/ingress, and OIDC issuer,
audience, and JWKS values. They must also provision these outside Helm:

- a LiteLLM Secret containing `LITELLM_MASTER_KEY` and the selected provider's
  key, or a provider workload identity attached to the LiteLLM ServiceAccount;
- separate, least-privilege LiteLLM role-key Secrets for planner, developer,
  and reviewers;
- external database, object-store, and GitHub pull-request credential Secrets;
- a GitHub App Secret for execution workspaces (`app-id`, `installation-id`, and
  `private-key`), whose installation has Contents read/write access only to the
  repositories the implementation agent may clone and push. Existing releases
  are safely clamped to a 50-minute implementation limit during upgrade because
  GitHub installation tokens expire after one hour; regenerate any pending plan
  whose approved duration is longer;
- when enabling the GitHub read-only MCP connector: a separate GitHub App
  Secret (`app-id`, `installation-id`, and `private-key`), an explicit
  single-repository allow-list, and an egress-proxy NetworkPolicy peer;
- the Workbench's environment-owned OIDC session relay.

The chart never reads a local cloud profile or creates provider credentials.
After the owner has supplied those inputs, render and review the manifest
before applying it:

```bash
helm template cogito charts/ \
  -f charts/values.yaml \
  -f charts/values-production.yaml \
  -f /secure/path/cogito-production.yaml
```

Then use the same files with `helm upgrade --install`. See
[values.yaml](charts/values.yaml) for the full configuration contract and
[values-production.yaml](charts/values-production.yaml) for the public
template.

### Execution egress capabilities

Execution Jobs are default-deny for network egress. A developer run that
checks out a repository needs an explicit `worker.execution.networkPolicy.gitEgress`
peer list on TCP/443; use the Git provider's maintained CIDRs or an
environment-owned egress proxy. Do not grant a catch-all Internet CIDR.

`worker.execution.networkPolicy.additionalEgress` provides named, port-scoped
rules when an enabled agent capability requires direct access to an approved
service. This is intentionally an operator deployment decision rather than an
automatic consequence of an agent or MCP toggle. A chart cannot safely turn a
hostname into a Kubernetes NetworkPolicy peer, and most MCPs should keep their
network access inside their own service boundary instead of extending execution
Job egress.

## Full local Kind E2E

The Phase 13 test creates its own uniquely versioned, immutable spec fixture,
then validates planning, both approval gates, execution, and cleanup against
the selected Kind release without replacing its deployed API or worker images:

```sh
COGITO_E2E_ENABLED=1 COGITO_E2E_CONFIRM=1 \
  uv run --project services/api pytest -q -m kind_e2e \
  services/api/tests/integration/test_kind_e2e_phase13.py
```

It uses operator-managed provider and fixture-repository credentials, so run
it only against a disposable local Kind environment. Provision the selected
provider identity in the configured LiteLLM Kubernetes Secret before starting;
Kind does not establish cloud workload identity. Do not pass provider
credentials, a cloud profile, or a token to the launcher. The test checks only
that the required Secret keys exist and never prints their values. If the
gateway is disabled, the test enables LiteLLM for its duration and restores its
original setting during cleanup.

The test creates its own uniquely versioned, immutable fixture unless
`COGITO_E2E_SPEC_REF` supplies one. The fixture repository credential must
already be present in `cogito-github-pull-request`, and the worker needs a
GitHub App Secret containing `app-id`, `installation-id`, and `private-key`
in `cogito-github-app` (or `COGITO_E2E_GITHUB_APP_SECRET`). Set
`COGITO_E2E_CONTEXT`, `COGITO_E2E_TIMEOUT_SECONDS`, `COGITO_E2E_TARGET_REPO`,
or `COGITO_E2E_VALUES_FILE` to select a disposable environment; none of those
options source credentials from the local machine.

### Governed MCP Kind E2E

This opt-in test does not invoke a model or modify a repository. It verifies
the persisted MCP policy, compatibility with the established agent policy, a
revocation-safe retry of a pinned grant, a real worker-issued LiteLLM run key,
the allowed tool call, the denied-tool case, and temporary Secret cleanup. The
selected release must have `mcp.enabled=true`.

When disabled, governed MCP emits no grants and normal agent runs continue
without an MCP dependency. When enabled, the worker validates each grant
against its exact server version, manifest digest, and allowed tool set before
issuing a LiteLLM run key. The chart restricts the credentialless backing
service to LiteLLM ingress with a Kubernetes `NetworkPolicy`; the cluster CNI
must enforce NetworkPolicies for that isolation to be effective.

The optional GitHub connector is a separate, read-only MCP service. It mints a
short-lived GitHub App installation token limited to its one configured
repository and Contents, Issues, and Pull requests read permissions. Its App
private key is mounted only into the connector pod; it accepts LiteLLM ingress
only and can reach GitHub only through the required environment-owned egress
proxy. A run receives its GitHub tool grants only when its immutable target
repositories include that configured repository. It is owned by
`cogito-platform` for maintenance and incident response.

```sh
COGITO_E2E_ENABLED=1 \
  uv run --project services/api pytest -q -m kind_e2e \
  services/api/tests/integration/test_kind_e2e_governed_mcp.py
```

### Agent Operations Kind E2E

This API-read verification creates and removes an isolated agent-route record inside the deployed API pod, then verifies the Agent Operations inventory, release detail, immutable invocation history, and safe invocation-detail projections through the public API. It relies on the catalog and gateway policy already bootstrapped by the deployed API; it does not mutate registry or policy rows, invoke a model, start a worker execution, or modify a repository.

```sh
COGITO_E2E_ENABLED=1 \
  uv run --project services/api pytest -q -m kind_e2e \
  services/api/tests/integration/test_kind_e2e_phase18_agent_operations.py
```

Set `COGITO_E2E_KEEP_RUNS=1` to retain the seeded standalone agent-run binding
for direct API inspection; the test prints its retained run ID. It deliberately
has no `supervisor_runs` workflow, so Workbench correctly labels its workflow
view unavailable. Use a real planning workflow and its matching agent run for
the Workbench navigation E2E.
