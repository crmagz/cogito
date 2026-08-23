---
schema_version: 1
domain_id: cogito_platform
repository_id: cogito
role: workflow_orchestration_platform
owners:
  - platform-engineering
relationships: []
last_assessed_commit: e043f66b89400e9c49be75fbc5165a9156b2e6e4
---

# cogito domain context

## Domain graph

<!-- cogito:generated:domain-graph:start -->
```mermaid
flowchart LR
  repo_cogito[cogito]
```
<!-- cogito:generated:domain-graph:end -->

## Notes

Cogito is the API-native workflow control plane. It resolves product
specifications, policies, gates, and capability boundaries into immutable
execution contracts for the worker. Repository-domain discovery is read-only;
any update to this document is proposed through a governed pull request.
