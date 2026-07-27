# Cogito

An agentic development platform. Submit a plan, point it at your repos and coding standards, and get back a reviewed pull request.

```
Specifications + Repositories + Plan  →  Reviewed PR
```

Cogito is designed to turn a reviewed plan into an isolated, observable execution
run. Read the [product overview](docs/product.md) for the current capability
boundary and [release guide](docs/releases.md) for the independently versioned
API, worker, and Helm chart artifacts.

## Helm Chart

Cogito deploys as an umbrella Helm chart with the following components:

| Component | Subchart | Purpose |
|-----------|----------|---------|
| PostgreSQL | `bitnami/postgresql` | Persistence for Temporal (default + visibility stores) |
| Temporal | `temporalio/temporal` | Durable workflow orchestration |
| MinIO | `minio/minio` | Object storage for plans and artifacts |
| API | local template (`services/api`) | Plan submission REST API: schema/DAG/constraint validation, plan storage |
| Worker | local template (`services/worker`) | Temporal workflow worker: loads persisted plans and reports run status |

### Production

Disable in-cluster PostgreSQL and MinIO, point Temporal at RDS and S3:

```bash
helm upgrade --install cogito charts/ \
  -f charts/values.yaml \
  -f charts/values-production.yaml
```

See [values.yaml](charts/values.yaml) for all configurable parameters and [values-production.yaml](charts/values-production.yaml) for a production overlay example.

## Full local Kind E2E

The Phase 13 test creates its own uniquely versioned, immutable spec fixture,
then validates planning, both approval gates, execution, and cleanup against
the selected Kind release without replacing its deployed API or worker images:

```sh
COGITO_E2E_CONFIRM=1 bash .claude/scripts/run-kind-e2e-phase13.sh
```

It uses configured provider and fixture-repository credentials, so run it only
against the disposable local Kind environment. If the gateway is disabled, the
test enables LiteLLM for its duration and restores its original setting during
cleanup. The configured provider key must be valid. Set `COGITO_E2E_CONTEXT`,
`COGITO_E2E_TIMEOUT_SECONDS`, or `COGITO_E2E_TARGET_REPO` to override the
defaults when needed.

To route the local E2E through AWS Bedrock using a short-lived named AWS
profile instead of a direct provider key, opt in explicitly:

```sh
COGITO_E2E_CONFIRM=1 COGITO_E2E_AWS_PROFILE=<named-aws-profile> \
  bash .claude/scripts/run-kind-e2e-phase13.sh
```

This refreshes only the AWS credential fields in the existing local LiteLLM
Secret, selects the local Bedrock overlay, and never prints or commits values.

If `GITHUB_TOKEN`, `GH_TOKEN`, or `COGITO_E2E_GITHUB_TOKEN` is exported, the
launcher safely refreshes the local PR-publisher Secret and verifies it can
list the fixture repository before starting the workflow.
