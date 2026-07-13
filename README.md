# Cogito

An agentic development platform. Submit a plan, point it at your repos and coding standards, and get back a reviewed pull request.

```
Specifications + Repositories + Plan  →  Reviewed PR
```

## Helm Chart

Cogito deploys as an umbrella Helm chart with the following components:

| Component | Subchart | Purpose |
|-----------|----------|---------|
| PostgreSQL | `bitnami/postgresql` | Persistence for Temporal (default + visibility stores) |
| Temporal | `temporalio/temporal` | Durable workflow orchestration |
| MinIO | `minio/minio` | Object storage for plans and artifacts |

### Local Development

Requires [kind](https://kind.sigs.k8s.io/), [Helm](https://helm.sh/), and [Docker](https://www.docker.com/).

```bash
make up        # Create kind cluster, load images, install chart
make status    # Show pod and helm release status
make port-forward  # Forward all services to localhost
make down      # Uninstall chart (keeps cluster)
make destroy   # Delete the kind cluster
```

| Service | Local URL |
|---------|-----------|
| Temporal UI | http://localhost:8080 |
| Temporal gRPC | localhost:7233 |
| MinIO Console | http://localhost:9001 |
| MinIO API | localhost:9000 |

### Production

Disable in-cluster PostgreSQL and MinIO, point Temporal at RDS and S3:

```bash
helm upgrade --install cogito charts/ \
  -f charts/values.yaml \
  -f charts/values-production.yaml
```

See [values.yaml](charts/values.yaml) for all configurable parameters and [values-production.yaml](charts/values-production.yaml) for a production overlay example.
