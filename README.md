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
