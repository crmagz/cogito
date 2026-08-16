# Cogito context

Cogito is the authoritative workflow service behind the Workbench operator
console. It persists immutable, content-addressed artifacts and exposes a
server-owned run projection; the browser must not infer or advance workflow
state locally.

## Specification and workflow contract

Each product-specification revision is an immutable artifact. A revision
request must include the expected revision and parent artifact digest so Cogito
can reject stale updates. A new revision clears the prior selection and
evaluation pointers. Evaluation, selection, planning, and approval decisions
remain bound to the precise artifact provenance recorded by Cogito.

Workflow decisions are durable and audit-backed. The API, not Workbench,
authorizes approve, refinement, and cancellation actions and determines the
current phase. Agent phase load and completion updates are included in the
workflow activity projection used by Workbench's centralized audit log.

## Workbench integration

Workbench's centralized workflow-specification workspace is one review form
for a run. It displays the active phase, the current source and
product-specification references with their SHA-256 digests, the editable
product-specification JSON, permitted workflow actions, and the centralized
audit activity. A JSON change is confirmed before it is sent to Cogito and
becomes a new immutable revision.

Workbench preserves a dirty specification draft if the authoritative projection
advances concurrently, marks it stale, and requires an explicit reload before
submission. That client protection complements—rather than replaces—Cogito's
revision/digest preconditions. Full evidence views for evaluation and plan
artifacts remain on their dedicated Workbench detail routes.

See [the specification evaluation lifecycle](specification-evaluation-lifecycle.md)
for endpoints, state transitions, and local validation commands.
