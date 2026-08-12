# Environment release bundles

A release bundle is an environment-owned JSON document validated against
[`schema.json`](./schema.json). It pins a deployable package version, immutable
component registration and manifest digest, and OCI image digest together.

Bundles are not component registrations and never contain credentials. Publish
the package image first, obtain its registry digest, and then use that exact
digest in the production Helm values and the environment's release bundle.

[`github-readonly-mcp.example.json`](./github-readonly-mcp.example.json) is
schema- and cross-reference-validated in CI. It demonstrates the exact
package, registry, and manifest pin shape but uses a non-deployable example
digest. An environment owner must copy it outside this repository and replace
the digest with the published image digest before deploying.
