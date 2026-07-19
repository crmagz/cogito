# Releases and artifact promotion

Cogito has three independently versioned deliverables. Their Git tags are
namespaced so a release is unambiguous in both Git history and GitHub Releases.

| Deliverable | Git tag | Published artifact |
|---|---|---|
| API | `api/vX.Y.Z` | `ghcr.io/crmagz/cogito-api:vX.Y.Z` |
| Worker | `worker/vX.Y.Z` | `ghcr.io/crmagz/cogito-worker:vX.Y.Z` |
| Helm chart | `chart/vX.Y.Z` | `oci://ghcr.io/crmagz/charts/cogito --version X.Y.Z` |

The API and worker are independently deployable. The chart is the deployable
composition: it selects the exact image digests that are promoted into an
environment. An image tag is useful for discovery, but production values must
use its immutable digest.

## Semantic version policy

On every push to `main`, the release workflow evaluates each component against
its own latest tag and its own source paths:

| Component | Paths that affect its version |
|---|---|
| API | `services/api`, `.python-version` |
| Worker | `services/worker`, `.python-version` |
| Chart | `charts` |

Conventional Commit subjects determine the bump: `feat` is minor;
`fix`, `perf`, `refactor`, `build`, and `revert` are patch; `!`, a
`BREAKING CHANGE:`, or a `BREAKING-CHANGE:` footer is major. `docs` and
`chore` changes do not release an artifact. The first feature release starts
at `0.1.0`; promote to `1.0.0` only when the corresponding component has a
stable public contract.

Cogito’s release automation uses Forge’s `releases/v1` compatibility channel
for semantic versioning and idempotent GitHub Releases. Each job supplies its
component pathset to both actions, so the version and release notes cover the
same commits and a `feat(worker)` cannot change the API’s version. The Forge
major tag receives backward-compatible updates; maintainers who need an exact
Forge revision can instead pin `releases/v1.0.0`. The image-build actions are
pinned directly in this repository so their complete dependency chain is
immutable.

## Release flow

1. The `Release` workflow calculates the component version from the commit
   range after its matching tag.
2. For API and worker releases it builds multi-architecture (`linux/amd64` and
   `linux/arm64`) images, publishes them to GHCR, reuses a registry-backed
   Buildx cache, and attaches BuildKit provenance and SBOM attestations.
3. For chart releases it packages the chart with the calculated version and
   publishes the OCI artifact to GHCR.
4. Forge creates the matching annotated Git tag and GitHub Release only after
   the artifact publish succeeds. The action is idempotent for safe reruns.

The workflow requires GitHub Actions to have `contents: write` and
`packages: write`, which are declared in the workflow. The first run creates
the initial component releases; a manual `workflow_dispatch` is available if
an initial or retried release is needed.

## Promoting a deployment

Release each image first, verify it, and record its manifest digest. Update the
production chart values with those digests, review the chart change, and then
release the chart. This makes the chart release the auditable environment
contract while still allowing API and worker image releases to move at their
own cadence.

```bash
helm upgrade --install cogito oci://ghcr.io/crmagz/charts/cogito \
  --version X.Y.Z \
  -f production-values.yaml
```

Do not use `latest` in an environment promotion. It is overwritten by each
component release and is intentionally unsuitable as a deployment identity.
