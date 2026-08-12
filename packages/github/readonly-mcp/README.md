# GitHub read-only MCP

This independently versioned GitHub-domain package runs Cogito's bounded
GitHub App-backed read-only MCP server. It owns the connector source, tests,
image build, and release lifecycle; the platform retains registry, policy,
grant, and gateway enforcement.

The package accepts only a single allow-listed repository and requests only
Contents, Issues, and Pull requests read permissions. Its Kubernetes wiring is
defined by the Cogito chart: the App key is mounted only into this Pod, ingress
is limited to LiteLLM, and GitHub traffic must use the environment-owned proxy.

Run its isolated test suite with:

```sh
uv run --project packages/github/readonly-mcp pytest -q packages/github/readonly-mcp/tests
```
