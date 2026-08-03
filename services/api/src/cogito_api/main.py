from __future__ import annotations

import json
import uuid
import asyncio
import secrets
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from hashlib import sha256
from math import isfinite
from urllib.parse import urlparse

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from minio import Minio
from opentelemetry.context import attach, detach
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .auth import ApprovalAuthenticator
from .config import Settings, load_settings
from .dag import validate_constraints, validate_phase_dag, validate_spec_reference, validate_target_repositories
from .models import (
    AgentRunResponse,
    AgentRunStatus,
    ArtifactReference,
    CoordinationApprovalActionRequest,
    CoordinationArtifactReference,
    CoordinationDeliveryResponse,
    CoordinationEventResponse,
    CoordinationGate,
    CoordinationRunListResponse,
    CoordinationRunResponse,
    ImplementationApprovalDecision,
    ImplementationApprovalRequest,
    ImplementationApprovalResponse,
    PlanApprovalDecision,
    PlanApprovalRequest,
    PlanApprovalResponse,
    PlanningRunResponse,
    PlanningRunStatus,
    PlanningRunSubmission,
    RunEnvelope,
    RunSubmission,
    Violation,
    WorkbenchArtifactKind,
    WorkbenchArtifactSummary,
    WorkbenchApprovalSummary,
    WorkbenchBudgetSummary,
    WorkbenchEvidenceResponse,
    WorkbenchExecutionSummary,
    WorkbenchExternalLink,
    WorkbenchFeedbackRequest,
    WorkbenchFeedbackListResponse,
    WorkbenchFeedbackResponse,
    WorkbenchProjectListResponse,
    WorkbenchProjectResponse,
    WorkbenchRunListResponse,
    WorkbenchRunResponse,
    WorkbenchStageAvailability,
    WorkbenchStageState,
    WorkbenchStageSummary,
    WorkbenchWorkflowEdge,
    WorkbenchWorkflowGraph,
    WorkbenchWorkflowNode,
    WorkbenchWorkflowNodeType,
    WorkbenchTimelineEvent,
    WorkbenchTimelineResponse,
)
from .outbox import ImplementationApprovalOutboxDispatcher, PlanApprovalOutboxDispatcher, stop_dispatcher
from .notifications import NotificationOutboxDispatcher, notification_sink, stop_notification_dispatcher
from .observability import Telemetry, TelemetrySettings
from .planner import LiteLLMPlanner, Planner, PlannerError, PlanningContext
from .reconciliation import ReconciliationHealth, WorkflowProjectionReconciler, stop_reconciler
from .storage import MinioPlanStore, PlanStore, PlanStoreUnavailableError
from .registry import RegistryAuthorizationError, load_component_catalog, require_tool
from .supervisor import (
    AgentRunRecord,
    ApprovalConflictError,
    PlanningRunRecord,
    PostgresSupervisorStore,
    RegistryConflictError,
    SupervisorStore,
)
from .temporal import RunStarter, TemporalRunStarter


class PlanValidationError(Exception):
    def __init__(self, violations: list[Violation]):
        self.violations = violations


def _violation_response(violations: list[Violation]) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"error": "validation_failed", "violations": [v.model_dump() for v in violations]},
    )


def _schema_violations(exc: RequestValidationError) -> list[Violation]:
    violations = []
    for error in exc.errors():
        field_path = ".".join(str(p) for p in error["loc"] if p != "body")
        violations.append(Violation(field=field_path or "body", message=error["msg"]))
    return violations


class TraceRequestMiddleware:
    """Record request telemetry without converting no-content responses to streams."""

    def __init__(self, app: ASGIApp, telemetry: Telemetry):
        self.app = app
        self.telemetry = telemetry

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        parent = self.telemetry.extract(dict(request.headers))
        token = attach(parent)

        async def send_response(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                self.telemetry.request(request.method, status_code)
                self.telemetry.event("cogito.api.response", {"http.response.status_code": str(status_code)})
            await send(message)

        try:
            with self.telemetry.span("cogito.api.request", {"http.request.method": request.method}):
                await self.app(scope, receive, send_response)
        finally:
            detach(token)


class ApplicationReadiness:
    """Represent startup completion and required background-loop progress."""

    def __init__(self, reconciliation_health: ReconciliationHealth | None) -> None:
        self._reconciliation_health = reconciliation_health
        self._started = False

    def started(self) -> None:
        """Mark application initialization as complete."""

        self._started = True

    def stopped(self) -> None:
        """Mark the application unavailable during shutdown."""

        self._started = False

    def is_ready(self) -> bool:
        """Return whether startup completed and the required loop is progressing."""

        return self._started and (
            self._reconciliation_health is None or self._reconciliation_health.is_healthy()
        )


def create_app(
    store: PlanStore | None = None,
    settings: Settings | None = None,
    starter: RunStarter | None = None,
    supervisor_store: SupervisorStore | None = None,
    planner: Planner | None = None,
) -> FastAPI:
    settings = settings or load_settings()
    store = store or MinioPlanStore(
        Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        ),
        settings.plans_bucket,
        settings.plan_snapshots_bucket,
        settings.plan_snapshot_retention_days,
    )
    starter = starter or TemporalRunStarter(
        settings.temporal_host, settings.temporal_namespace, settings.temporal_task_queue
    )
    supervisor_store = supervisor_store or PostgresSupervisorStore(settings.supervisor_database_url)
    planner = planner or LiteLLMPlanner(settings)
    catalog = load_component_catalog(Path(settings.registry_catalog_path))
    agents = {item.registration_id: item for item in catalog.components if item.kind.value == "agent"}
    policy_revision = "phase12_initial"
    assignments = {role: f"{manifest.registration_id}@{manifest.version}" for role, manifest in agents.items()}
    telemetry = Telemetry(TelemetrySettings.from_environment())
    authenticator = ApprovalAuthenticator(settings)

    dispatcher = PlanApprovalOutboxDispatcher(supervisor_store, starter)
    implementation_dispatcher = ImplementationApprovalOutboxDispatcher(supervisor_store, starter)
    sink = notification_sink(settings)
    notification_dispatcher = NotificationOutboxDispatcher(supervisor_store, sink) if sink is not None else None
    reconciler = (
        WorkflowProjectionReconciler(
            supervisor_store,
            starter,
            poll_seconds=settings.reconciliation_poll_seconds,
            batch_size=settings.reconciliation_batch_size,
            stall_seconds=settings.reconciliation_stall_seconds,
            telemetry=telemetry,
        )
        if settings.reconciliation_enabled and callable(getattr(starter, "get_terminal_outcome", None))
        else None
    )
    readiness = ApplicationReadiness(reconciler.health if reconciler is not None else None)

    async def resolve_roles(run_id: str, roles: list[str]):
        await supervisor_store.bootstrap_registry(catalog.components, policy_revision, assignments)
        try:
            return [
                await supervisor_store.resolve_run_registration(run_id, role, policy_revision, agents[role])
                for role in roles
            ]
        except KeyError as error:
            raise RegistryConflictError("registry policy does not define the requested role") from error

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # Validate and persist the non-secret catalog before the API reports
        # ready. Deferring this until the first submitted run makes a broken
        # migration or policy look healthy and delays a safe failure boundary.
        await supervisor_store.bootstrap_registry(catalog.components, policy_revision, assignments)
        delivery_task = asyncio.create_task(dispatcher.run())
        implementation_delivery_task = asyncio.create_task(implementation_dispatcher.run())
        notification_delivery_task = (
            asyncio.create_task(notification_dispatcher.run()) if notification_dispatcher is not None else None
        )
        reconciliation_task = asyncio.create_task(reconciler.run()) if reconciler is not None else None
        readiness.started()
        try:
            yield
        finally:
            readiness.stopped()
            await stop_dispatcher(delivery_task)
            await stop_dispatcher(implementation_delivery_task)
            await stop_notification_dispatcher(notification_delivery_task)
            await stop_reconciler(reconciliation_task)
            telemetry.shutdown()
            close = getattr(supervisor_store, "close", None)
            if close is not None:
                await close()

    app = FastAPI(title="Cogito API", lifespan=lifespan)
    app.add_middleware(TraceRequestMiddleware, telemetry=telemetry)

    @app.exception_handler(RequestValidationError)
    async def handle_schema_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _violation_response(_schema_violations(exc))

    @app.exception_handler(PlanValidationError)
    async def handle_plan_error(request: Request, exc: PlanValidationError) -> JSONResponse:
        return _violation_response(exc.violations)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        """Report whether required startup and reconciliation work is progressing."""

        if readiness.is_ready():
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ready"})
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"status": "not_ready"})

    @app.post("/api/v1/runs")
    async def submit_run(submission: RunSubmission) -> JSONResponse:
        plan = submission.plan
        violations = (
            validate_phase_dag(plan.phases)
            + validate_constraints(plan.constraints, settings)
            + validate_target_repositories(plan.target_repos, settings.allowed_git_hosts)
            + validate_spec_reference(plan.spec_set)
        )
        if violations:
            raise PlanValidationError(violations)

        run_id = str(uuid.uuid4())

        if submission.dry_run:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"run_id": run_id, "status": "validated", "dry_run": True},
            )

        submitted_at = datetime.now(timezone.utc).isoformat()
        await supervisor_store.create_agent_run(
            AgentRunRecord(
                run_id=run_id,
                root_run_id=run_id,
                parent_run_id=None,
                agent_name="supervisor",
                status=AgentRunStatus.QUEUED,
                trace_id=telemetry.trace_id() or secrets.token_hex(16),
                created_at=submitted_at,
                updated_at=submitted_at,
            )
        )
        try:
            resolutions = await resolve_roles(
                run_id, ["planner", "developer", "reviewer", "validator", "ephemeral_environment_tester", "pull_request_publisher"]
            )
        except RegistryConflictError as error:
            raise HTTPException(status_code=503, detail="registry is temporarily unavailable") from error
        telemetry.transition(AgentRunStatus.QUEUED.value, "supervisor")
        try:
            snapshot = store.put_plan(run_id, plan)
            store.put_status(
                run_id,
                {
                    "run_id": run_id,
                    "status": "queued",
                    "plan_ref": snapshot.ref,
                    "plan_sha256": snapshot.sha256,
                    "submitted_at": submitted_at,
                },
            )
        except PlanStoreUnavailableError as error:
            raise HTTPException(status_code=503, detail="run storage is temporarily unavailable") from error

        carrier: dict[str, str] = {}
        telemetry.inject(carrier)
        envelope = RunEnvelope(
            run_id=run_id,
            plan_ref=snapshot.ref,
            plan_sha256=snapshot.sha256,
            spec_ref=plan.spec_set,
            target_repos=plan.target_repos,
            constraints=plan.constraints,
            priority=submission.priority,
            submitted_at=submitted_at,
            submitted_by="api",
            registry_resolutions=resolutions,
            traceparent=carrier.get("traceparent"),
            tracestate=carrier.get("tracestate"),
        )
        await starter.start_run(envelope)

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"run_id": run_id, "status": "queued", "plan_ref": snapshot.ref, "estimated_start": None},
        )

    @app.post("/api/v1/planning-runs")
    async def submit_planning_run(
        submission: PlanningRunSubmission,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        """Persist an initial work specification for a future human-gated planning workflow."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        if settings.workbench_default_project_id not in principal.projects:
            raise HTTPException(status_code=403, detail="operator is not authorized for the configured default project")
        violations = (
            validate_constraints(submission.constraints, settings)
            + validate_target_repositories(submission.target_repos, settings.allowed_git_hosts)
            + validate_spec_reference(submission.spec_set)
        )
        if violations:
            raise PlanValidationError(violations)

        run_id = str(uuid.uuid4())
        submitted_at = datetime.now(timezone.utc).isoformat()
        if submission.dry_run:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"run_id": run_id, "status": "validated", "dry_run": True},
            )

        try:
            source_artifact = store.put_source_specification(run_id, submission.initial_specification)
        except PlanStoreUnavailableError as error:
            raise HTTPException(status_code=503, detail="run storage is temporarily unavailable") from error
        await supervisor_store.create_agent_run(
            AgentRunRecord(
                run_id=run_id,
                root_run_id=run_id,
                parent_run_id=None,
                agent_name="planner",
                status=AgentRunStatus.QUEUED,
                trace_id=telemetry.trace_id() or secrets.token_hex(16),
                created_at=submitted_at,
                updated_at=submitted_at,
            )
        )
        try:
            await resolve_roles(run_id, ["planner"])
        except RegistryConflictError as error:
            raise HTTPException(status_code=503, detail="registry is temporarily unavailable") from error
        telemetry.transition(AgentRunStatus.QUEUED.value, "planner")
        record = PlanningRunRecord(
            run_id=run_id,
            status=PlanningRunStatus.PLANNING,
            source_artifact=source_artifact,
            target_repos=submission.target_repos,
            spec_set=submission.spec_set,
            constraints=submission.constraints,
            priority=submission.priority,
            submitted_at=submitted_at,
            submitted_by="api",
            project_id=settings.workbench_default_project_id,
        )
        await supervisor_store.create_planning_run(record)
        response = PlanningRunResponse(
            run_id=record.run_id,
            status=record.status,
            source_artifact=record.source_artifact,
            plan_artifact=record.plan_artifact,
            implementation_artifact=record.implementation_artifact,
            submitted_at=record.submitted_at,
        )
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.model_dump(mode="json"))

    @app.post("/api/v1/planning-runs/{run_id}/generate-plan")
    async def generate_plan(
        run_id: str,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        """Generate and persist one normalized plan for a planning run.

        This endpoint becomes worker-internal when the durable workflow gate is added.
        """

        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"planning run '{run_id}' not found")
        require_workbench_scope(record, principal)
        if record.status is PlanningRunStatus.PLANNING:
            try:
                planner_resolution = (await resolve_roles(run_id, ["planner"]))[0]
                require_tool(planner_resolution, "planning_model", "plan_generation")
            except (RegistryAuthorizationError, RegistryConflictError) as error:
                raise HTTPException(status_code=503, detail="planner registry grant is unavailable") from error
            try:
                initial_specification = store.get_source_specification(record.source_artifact.ref)
            except PlanStoreUnavailableError as error:
                raise HTTPException(status_code=503, detail="run storage is temporarily unavailable") from error
            try:
                generated_plan = await planner.generate(
                    PlanningContext(
                        initial_specification=initial_specification,
                        target_repos=record.target_repos,
                        spec_set=record.spec_set,
                        constraints=record.constraints,
                    )
                )
            except PlannerError as error:
                raise HTTPException(status_code=502, detail="planner failed to produce a valid plan") from error
            next_plan_revision = record.plan_revision + 1
            try:
                snapshot = store.put_planning_plan(run_id, next_plan_revision, generated_plan)
            except PlanStoreUnavailableError as error:
                raise HTTPException(status_code=503, detail="run storage is temporarily unavailable") from error
            workflow_id = _planning_workflow_id(run_id, next_plan_revision, snapshot.sha256)
            try:
                updated = await supervisor_store.attach_generated_plan(
                    run_id,
                    plan_artifact=ArtifactReference(ref=snapshot.ref, sha256=snapshot.sha256),
                    planner_model=settings.litellm_planner_model,
                    workflow_id=workflow_id,
                    expected_plan_revision=record.plan_revision,
                )
            except ValueError:
                # A concurrent caller may have persisted the active immutable
                # plan after this caller read the planning record. Converge on
                # that authoritative version instead of returning a 500 or
                # starting a second workflow.
                latest = await supervisor_store.get_planning_run(run_id)
                if (
                    latest is None
                    or latest.status is not PlanningRunStatus.AWAITING_PLAN_APPROVAL
                    or latest.plan_artifact is None
                    or latest.workflow_id is None
                ):
                    raise HTTPException(status_code=409, detail="planning run changed while the plan was generated")
                updated = latest
        elif record.status is PlanningRunStatus.AWAITING_PLAN_APPROVAL and record.plan_artifact is not None:
            # A start request may have timed out after plan persistence. Retry
            # the immutable artifact, never regenerate a second model plan.
            updated = record
        else:
            raise HTTPException(status_code=409, detail="planning run is not eligible for plan generation")
        assert updated.plan_artifact is not None
        try:
            carrier: dict[str, str] = {}
            telemetry.inject(carrier)
            resolutions = await resolve_roles(
                updated.run_id,
                ["planner", "developer", "reviewer", "validator", "ephemeral_environment_tester", "pull_request_publisher"],
            )
            await starter.start_run(
                RunEnvelope(
                    run_id=updated.run_id,
                    plan_ref=updated.plan_artifact.ref,
                    plan_sha256=updated.plan_artifact.sha256,
                    spec_ref=updated.spec_set,
                    target_repos=updated.target_repos,
                    constraints=updated.constraints,
                    priority=updated.priority,
                    submitted_at=updated.submitted_at,
                    submitted_by=updated.submitted_by,
                    workflow_id=updated.workflow_id,
                    requires_plan_approval=True,
                    requires_implementation_approval=True,
                    registry_resolutions=resolutions,
                    traceparent=carrier.get("traceparent"),
                    tracestate=carrier.get("tracestate"),
                )
            )
        except Exception as error:
            raise HTTPException(
                status_code=503,
                detail="plan was persisted but Temporal is unavailable; retry this request to start its workflow",
            ) from error
        response = PlanningRunResponse(
            run_id=updated.run_id,
            status=updated.status,
            source_artifact=updated.source_artifact,
            plan_artifact=updated.plan_artifact,
            implementation_artifact=updated.implementation_artifact,
            submitted_at=updated.submitted_at,
        )
        return JSONResponse(content=response.model_dump(mode="json"))

    @app.post("/api/v1/runs/{run_id}/approvals/plan")
    async def approve_plan(
        run_id: str,
        request_body: PlanApprovalRequest,
        authorization: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        """Persist and deliver one authenticated decision for the current plan artifact."""

        if not idempotency_key or len(idempotency_key) > 256:
            raise HTTPException(status_code=422, detail="Idempotency-Key header is required and must be at most 256 characters")
        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        request_sha256 = sha256(
            json.dumps(request_body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        try:
            recorded = await supervisor_store.record_plan_approval(
                run_id=run_id,
                artifact_sha256=request_body.artifact_sha256,
                decision=request_body.decision,
                actor_id=principal.subject,
                comment=request_body.comment,
                idempotency_key=idempotency_key,
                request_sha256=request_sha256,
            )
        except ApprovalConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        delivered = recorded.delivered or recorded.decision_id in await dispatcher.deliver_once(
            decision_id=recorded.decision_id,
            limit=1,
        )
        response = PlanApprovalResponse(
            decision_id=recorded.decision_id,
            run_id=recorded.run_id,
            decision=recorded.decision,
            artifact_sha256=recorded.artifact_sha256,
            actor_id=recorded.actor_id,
            delivered=delivered,
            created_at=recorded.created_at,
        )
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.model_dump(mode="json"))

    @app.post("/api/v1/runs/{run_id}/approvals/implementation")
    async def approve_implementation(
        run_id: str,
        request_body: ImplementationApprovalRequest,
        authorization: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        """Persist and deliver one authenticated decision for a frozen implementation."""

        if not idempotency_key or len(idempotency_key) > 256:
            raise HTTPException(status_code=422, detail="Idempotency-Key header is required and must be at most 256 characters")
        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        request_sha256 = sha256(
            json.dumps(request_body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        try:
            recorded = await supervisor_store.record_implementation_approval(
                run_id=run_id,
                artifact_sha256=request_body.artifact_sha256,
                decision=request_body.decision,
                actor_id=principal.subject,
                comment=request_body.comment,
                idempotency_key=idempotency_key,
                request_sha256=request_sha256,
            )
        except ApprovalConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        delivered = recorded.delivered or recorded.decision_id in await implementation_dispatcher.deliver_once(
            decision_id=recorded.decision_id, limit=1
        )
        response = ImplementationApprovalResponse(
            decision_id=recorded.decision_id,
            run_id=recorded.run_id,
            decision=recorded.decision,
            artifact_sha256=recorded.artifact_sha256,
            actor_id=recorded.actor_id,
            delivered=delivered,
            created_at=recorded.created_at,
        )
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.model_dump(mode="json"))

    @app.get("/api/v1/runs/{run_id}/status")
    async def get_run_status(run_id: str) -> dict:
        agent_run = await supervisor_store.get_agent_run(run_id)
        if agent_run is not None:
            response = AgentRunResponse(**agent_run.__dict__).model_dump(mode="json")
            # Preserve the legacy lower-case field while exposing the canonical
            # state explicitly for new clients.
            response["lifecycle_status"] = response["status"]
            response["status"] = agent_run.status.value.lower()
            # The durable SQL projection is authoritative for lifecycle state;
            # execution evidence remains in the immutable run-status object.
            # Return that evidence without letting it overwrite canonical state.
            try:
                execution = store.get_status(run_id)
            except PlanStoreUnavailableError:
                execution = None
            if execution is not None:
                response["execution_status"] = execution.get("status")
                for key, value in execution.items():
                    response.setdefault(key, value)
            return response
        try:
            record = store.get_status(run_id)
        except PlanStoreUnavailableError as error:
            raise HTTPException(status_code=503, detail="run storage is temporarily unavailable") from error
        if record is None:
            raise HTTPException(status_code=404, detail=f"run '{run_id}' not found")
        return record

    @app.get("/api/v1/planning-runs/{run_id}")
    async def get_planning_run(
        run_id: str,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        """Return the authoritative supervisor record for a planning run."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        response = PlanningRunResponse(
            run_id=record.run_id,
            status=record.status,
            source_artifact=record.source_artifact,
            plan_artifact=record.plan_artifact,
            implementation_artifact=record.implementation_artifact,
            submitted_at=record.submitted_at,
        )
        return JSONResponse(content=response.model_dump(mode="json"))

    async def coordination_response(record: PlanningRunRecord) -> CoordinationRunResponse:
        """Build a bounded authenticated projection without exposing artifact contents."""

        events = await supervisor_store.list_coordination_events(record.run_id)
        event_responses = []
        for event, delivered, attempts, last_error in events:
            artifact = (
                CoordinationArtifactReference(ref=event.artifact_ref, sha256=event.artifact_sha256)
                if event.artifact_ref and event.artifact_sha256
                else None
            )
            event_responses.append(
                CoordinationEventResponse(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    run_id=event.run_id,
                    occurred_at=event.occurred_at,
                    gate=CoordinationGate(event.gate) if event.gate else None,
                    artifact=artifact,
                    decision=PlanApprovalDecision(event.decision) if event.decision else None,
                    lifecycle_status=AgentRunStatus(event.lifecycle_status) if event.lifecycle_status else None,
                    delivery=CoordinationDeliveryResponse(
                        delivered=delivered,
                        attempt_count=attempts,
                        last_error=last_error,
                    ),
                )
            )
        active_gate = (
            CoordinationGate.PLAN
            if record.status is PlanningRunStatus.AWAITING_PLAN_APPROVAL
            else CoordinationGate.IMPLEMENTATION
            if record.status is PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL
            else None
        )
        return CoordinationRunResponse(
            run_id=record.run_id,
            status=record.status,
            submitted_at=record.submitted_at,
            plan_artifact=(
                CoordinationArtifactReference(ref=record.plan_artifact.ref, sha256=record.plan_artifact.sha256)
                if record.plan_artifact is not None
                else None
            ),
            implementation_artifact=(
                CoordinationArtifactReference(
                    ref=record.implementation_artifact.ref,
                    sha256=record.implementation_artifact.sha256,
                )
                if record.implementation_artifact is not None
                else None
            ),
            active_gate=active_gate,
            events=event_responses,
        )

    def require_workbench_scope(record: PlanningRunRecord, principal) -> None:
        """Fail closed without revealing whether a foreign run exists."""

        if record.project_id is None or record.project_id not in principal.projects:
            raise HTTPException(status_code=404, detail="planning run not found")

    def workbench_response(record: PlanningRunRecord, principal) -> WorkbenchRunResponse:
        require_workbench_scope(record, principal)
        artifacts = [WorkbenchArtifactSummary(kind=WorkbenchArtifactKind.SOURCE, sha256=record.source_artifact.sha256)]
        if record.plan_artifact is not None:
            artifacts.append(WorkbenchArtifactSummary(kind=WorkbenchArtifactKind.PLAN, sha256=record.plan_artifact.sha256))
        if record.implementation_artifact is not None:
            artifacts.append(
                WorkbenchArtifactSummary(
                    kind=WorkbenchArtifactKind.IMPLEMENTATION,
                    sha256=record.implementation_artifact.sha256,
                )
            )
        active_gate = (
            CoordinationGate.PLAN
            if record.status is PlanningRunStatus.AWAITING_PLAN_APPROVAL
            else CoordinationGate.IMPLEMENTATION
            if record.status is PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL
            else None
        )
        abilities = ["view"]
        if {
            settings.auth_oidc_approval_role,
            settings.auth_oidc_admin_role,
        } & principal.roles:
            abilities.append("approve")
        workflow = ["planning"]
        if record.plan_artifact is not None:
            workflow.append("plan")
        if record.implementation_artifact is not None:
            workflow.append("implementation")
        if active_gate is not None:
            workflow.append(f"{active_gate.value}_approval")
        stages = workbench_stages(record, active_gate)
        return WorkbenchRunResponse(
            run_id=record.run_id,
            project_id=record.project_id,
            status=record.status,
            submitted_at=record.submitted_at,
            workflow_id=record.workflow_id,
            stages=stages,
            workflow_graph=workbench_graph(stages),
            active_gate=active_gate,
            artifacts=artifacts,
            abilities=abilities,
            workflow=workflow,
            budget=WorkbenchBudgetSummary(
                max_cost_usd=record.constraints.max_cost_usd,
                max_wall_clock_minutes=record.constraints.max_wall_clock_minutes,
                max_review_rounds=record.constraints.max_review_rounds,
            ),
            approval_history_available="approve" in abilities,
            external_links=workbench_external_links(record),
        )

    def workbench_graph(stages: list[WorkbenchStageSummary]) -> WorkbenchWorkflowGraph:
        """Return the server-owned relay topology for the lifecycle stages it exposes."""

        nodes = [
            WorkbenchWorkflowNode(
                **stage.model_dump(),
                node_type=(
                    WorkbenchWorkflowNodeType.GATE
                    if stage.stage_id.endswith("_approval")
                    else WorkbenchWorkflowNodeType.QUEUE
                    if stage.stage_id == "specification"
                    else WorkbenchWorkflowNodeType.AGENT
                ),
            )
            for stage in stages
        ]
        return WorkbenchWorkflowGraph(
            nodes=nodes,
            edges=[
                WorkbenchWorkflowEdge(source_node_id=stages[index].stage_id, target_node_id=stage.stage_id)
                for index, stage in enumerate(stages[1:])
            ],
        )

    def workbench_stages(
        record: PlanningRunRecord, active_gate: CoordinationGate | None
    ) -> list[WorkbenchStageSummary]:
        """Project only per-stage facts that the supervisor record can prove."""

        status = record.status
        planning_state = (
            WorkbenchStageState.IN_PROGRESS
            if status is PlanningRunStatus.PLANNING
            else WorkbenchStageState.FAILED
            if status is PlanningRunStatus.PLANNING_FAILED
            else WorkbenchStageState.COMPLETED
            if record.plan_artifact is not None
            else WorkbenchStageState.UNAVAILABLE
        )
        planning_reason = (
            "The supervisor records planning in progress."
            if planning_state is WorkbenchStageState.IN_PROGRESS
            else "The supervisor records planning as failed."
            if planning_state is WorkbenchStageState.FAILED
            else "An immutable generated plan is recorded."
            if planning_state is WorkbenchStageState.COMPLETED
            else "No durable planning result is available."
        )
        plan_gate_state = (
            WorkbenchStageState.AWAITING_OPERATOR
            if active_gate is CoordinationGate.PLAN
            else WorkbenchStageState.NEEDS_REVISION
            if status is PlanningRunStatus.REVISION_REQUESTED
            and record.implementation_artifact is None
            else WorkbenchStageState.FAILED
            if status is PlanningRunStatus.REJECTED
            and record.implementation_artifact is None
            else WorkbenchStageState.COMPLETED
            if record.implementation_artifact is not None
            or (
                record.plan_artifact is not None
                and status
                in {
                    PlanningRunStatus.IMPLEMENTING,
                    PlanningRunStatus.AWAITING_IMPLEMENTATION_APPROVAL,
                    PlanningRunStatus.FINALIZING,
                    PlanningRunStatus.COMPLETED,
                }
            )
            else WorkbenchStageState.UNAVAILABLE
        )
        implementation_state = (
            WorkbenchStageState.IN_PROGRESS
            if status is PlanningRunStatus.IMPLEMENTING
            else WorkbenchStageState.COMPLETED
            if record.implementation_artifact is not None
            else WorkbenchStageState.UNAVAILABLE
        )
        implementation_gate_state = (
            WorkbenchStageState.AWAITING_OPERATOR
            if active_gate is CoordinationGate.IMPLEMENTATION
            else WorkbenchStageState.NEEDS_REVISION
            if status is PlanningRunStatus.REVISION_REQUESTED
            and record.implementation_artifact is not None
            else WorkbenchStageState.FAILED
            if status is PlanningRunStatus.REJECTED
            and record.implementation_artifact is not None
            else WorkbenchStageState.COMPLETED
            if record.implementation_artifact is not None
            and status in {PlanningRunStatus.FINALIZING, PlanningRunStatus.COMPLETED}
            else WorkbenchStageState.UNAVAILABLE
        )
        return [
            WorkbenchStageSummary(
                stage_id="specification",
                label="Specification",
                state=WorkbenchStageState.COMPLETED,
                availability=WorkbenchStageAvailability.AUTHORITATIVE,
                reason="An immutable submitted specification is recorded.",
                artifact_kind=WorkbenchArtifactKind.SOURCE,
            ),
            WorkbenchStageSummary(
                stage_id="planning",
                label="Planning",
                state=planning_state,
                availability=(WorkbenchStageAvailability.UNAVAILABLE if planning_state is WorkbenchStageState.UNAVAILABLE else WorkbenchStageAvailability.AUTHORITATIVE),
                reason=planning_reason,
                artifact_kind=WorkbenchArtifactKind.PLAN if record.plan_artifact is not None else None,
            ),
            WorkbenchStageSummary(
                stage_id="plan_approval",
                label="Plan approval",
                state=plan_gate_state,
                availability=(WorkbenchStageAvailability.UNAVAILABLE if plan_gate_state is WorkbenchStageState.UNAVAILABLE else WorkbenchStageAvailability.AUTHORITATIVE),
                reason=(
                    "An operator decision on the immutable plan is required."
                    if plan_gate_state is WorkbenchStageState.AWAITING_OPERATOR
                    else "An operator rejected the immutable plan."
                    if plan_gate_state is WorkbenchStageState.FAILED
                    else "An operator requested revision of the immutable plan."
                    if plan_gate_state is WorkbenchStageState.NEEDS_REVISION
                    else "The supervisor advanced beyond the plan approval gate."
                    if plan_gate_state is WorkbenchStageState.COMPLETED
                    else "No durable plan-gate resolution is available."
                ),
                artifact_kind=WorkbenchArtifactKind.PLAN if record.plan_artifact is not None else None,
            ),
            WorkbenchStageSummary(
                stage_id="implementation",
                label="Implementation",
                state=implementation_state,
                availability=(WorkbenchStageAvailability.UNAVAILABLE if implementation_state is WorkbenchStageState.UNAVAILABLE else WorkbenchStageAvailability.AUTHORITATIVE),
                reason=(
                    "The supervisor records implementation in progress."
                    if implementation_state is WorkbenchStageState.IN_PROGRESS
                    else "An immutable implementation result is recorded."
                    if implementation_state is WorkbenchStageState.COMPLETED
                    else "No durable implementation result is available."
                ),
                artifact_kind=WorkbenchArtifactKind.IMPLEMENTATION if record.implementation_artifact is not None else None,
            ),
            WorkbenchStageSummary(
                stage_id="implementation_approval",
                label="Implementation approval",
                state=implementation_gate_state,
                availability=(WorkbenchStageAvailability.UNAVAILABLE if implementation_gate_state is WorkbenchStageState.UNAVAILABLE else WorkbenchStageAvailability.AUTHORITATIVE),
                reason=(
                    "An operator decision on the immutable implementation result is required."
                    if implementation_gate_state is WorkbenchStageState.AWAITING_OPERATOR
                    else "An operator rejected the immutable implementation result."
                    if implementation_gate_state is WorkbenchStageState.FAILED
                    else "An operator requested revision of the immutable implementation result."
                    if implementation_gate_state is WorkbenchStageState.NEEDS_REVISION
                    else "The supervisor advanced beyond the implementation approval gate."
                    if implementation_gate_state is WorkbenchStageState.COMPLETED
                    else "No durable implementation-gate resolution is available."
                ),
                artifact_kind=WorkbenchArtifactKind.IMPLEMENTATION if record.implementation_artifact is not None else None,
            ),
        ]

    def workbench_external_links(record: PlanningRunRecord) -> list[WorkbenchExternalLink]:
        """Expose only repository destinations derived from a validated run target."""

        links: list[WorkbenchExternalLink] = []
        for target in record.target_repos:
            parsed = urlparse(target)
            try:
                port = parsed.port
            except ValueError:
                continue
            if (
                parsed.scheme != "https"
                or parsed.hostname not in settings.allowed_git_hosts
                or port not in {None, 443}
                or parsed.username
                or parsed.password
                or parsed.query
                or not parsed.path
            ):
                continue
            url = f"https://{parsed.netloc}{parsed.path.removesuffix('.git')}"
            links.append(WorkbenchExternalLink(kind="repository", label="Repository", url=url))
        return links[:10]

    def workbench_execution(record: PlanningRunRecord) -> tuple[WorkbenchExecutionSummary | None, float | None, int | None]:
        """Read only bounded fields from verified immutable implementation evidence.

        A storage or parse problem intentionally leaves the summary unavailable;
        an operational inventory request must not fail because a detail artifact
        cannot currently be read.
        """

        artifact = record.implementation_artifact
        if artifact is None:
            return None, None, None
        try:
            document = json.loads(store.get_verified_artifact(artifact, max_bytes=100_000).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, PlanStoreUnavailableError, ValueError):
            return None, None, None
        if not isinstance(document, dict) or document.get("run_id") != record.run_id:
            return None, None, None
        phases = document.get("phase_results")
        if not isinstance(phases, list):
            return None, None, None
        safe_phases = [item for item in phases if isinstance(item, dict) and isinstance(item.get("phase_id"), str)]
        succeeded = sum(item.get("succeeded") is True for item in safe_phases)
        failed = sum(item.get("succeeded") is False for item in safe_phases)
        verification: list[dict[str, bool]] = []
        for item in safe_phases:
            checks = item.get("verification")
            if not isinstance(checks, list):
                continue
            verification.extend(
                check for check in checks if isinstance(check, dict) and isinstance(check.get("passed"), bool)
            )
        review = document.get("review")
        validation = document.get("validation")
        cost = document.get("cost_usd")
        turns = document.get("turns_used")
        return (
            WorkbenchExecutionSummary(
                phase_count=len(safe_phases),
                succeeded_phase_count=succeeded,
                failed_phase_count=failed,
                verification_passed=sum(item["passed"] is True for item in verification),
                verification_failed=sum(item["passed"] is False for item in verification),
                review_status=review.get("status") if isinstance(review, dict) and isinstance(review.get("status"), str) else None,
                validation_status=(
                    validation.get("status") if isinstance(validation, dict) and isinstance(validation.get("status"), str) else None
                ),
            ),
            (
                float(cost)
                if isinstance(cost, (int, float)) and not isinstance(cost, bool) and isfinite(cost) and cost >= 0
                else None
            ),
            turns if isinstance(turns, int) and not isinstance(turns, bool) and turns >= 0 else None,
        )

    async def workbench_detail_response(record: PlanningRunRecord, principal) -> WorkbenchRunResponse:
        """Enrich one already-authorized run with durable audit and evidence facts."""

        base = workbench_response(record, principal)
        approvals = await supervisor_store.list_workbench_approvals(record.run_id) if base.approval_history_available else []
        execution, actual_cost_usd, turns_used = workbench_execution(record)
        return base.model_copy(
            update={
                "approval_history": [
                    WorkbenchApprovalSummary(
                        decision_id=item.decision_id,
                        gate=CoordinationGate(item.gate),
                        decision=item.decision,
                        artifact_sha256=item.artifact_sha256,
                        actor_id=item.actor_id,
                        created_at=item.created_at,
                        delivered=item.delivered,
                    )
                    for item in approvals
                ],
                "execution": execution,
                "budget": base.budget.model_copy(update={"actual_cost_usd": actual_cost_usd, "turns_used": turns_used}),
            }
        )

    def workbench_revision(response: object) -> str:
        """Derive a stable ETag from a fully scoped Workbench representation."""

        if not hasattr(response, "model_dump"):
            raise TypeError("Workbench revisions require a Pydantic response model")
        return sha256(json.dumps(response.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def workbench_etag_matches(if_none_match: str | None, revision: str) -> bool:
        """Use weak ETag comparison for a GET representation without trusting header syntax."""

        if not if_none_match:
            return False
        for validator in if_none_match.split(","):
            candidate = validator.strip()
            if candidate == "*":
                return True
            if candidate.lower().startswith("w/"):
                candidate = candidate[2:].strip()
            if len(candidate) >= 2 and candidate.startswith('"') and candidate.endswith('"'):
                candidate = candidate[1:-1]
            if candidate == revision:
                return True
        return False

    def workbench_gate(value: str | None) -> CoordinationGate | None:
        try:
            return CoordinationGate(value) if value else None
        except ValueError:
            return None

    def workbench_plan_decision(value: str | None) -> PlanApprovalDecision | None:
        try:
            return PlanApprovalDecision(value) if value else None
        except ValueError:
            return None

    def workbench_agent_status(value: str | None) -> AgentRunStatus | None:
        try:
            return AgentRunStatus(value) if value else None
        except ValueError:
            return None

    def workbench_stage_ids(event_type: str, payload: dict[str, object], decision: str | None) -> list[str]:
        """Project all materially affected, canonical stages into the Workbench timeline contract."""

        explicit_stage = payload.get("stage_id")
        if explicit_stage in {
            "specification",
            "planning",
            "plan_approval",
            "implementation",
            "implementation_approval",
        }:
            return [explicit_stage]
        if event_type == "specification_recorded":
            return ["specification"]
        if event_type == "planning_started":
            return ["planning"]
        if event_type == "plan_approval_requested":
            return ["planning", "plan_approval"]
        if event_type == "plan_approval_recorded":
            return {
                "approve": ["plan_approval", "implementation"],
                "request_revision": ["plan_approval", "planning"],
                "reject": ["plan_approval"],
            }.get(decision, ["plan_approval"])
        if event_type == "implementation_approval_requested":
            return ["implementation", "implementation_approval"]
        if event_type == "implementation_approval_recorded":
            return ["implementation_approval"]
        return []

    async def workbench_timeline_response(record: PlanningRunRecord) -> WorkbenchTimelineResponse:
        """Build a bounded timeline without exposing storage references or sink errors."""

        events = await supervisor_store.list_coordination_events(record.run_id, limit=100)
        items = []
        for event, delivered, attempts, _last_error in events:
            stage_ids = workbench_stage_ids(event.event_type, event.payload, event.decision)
            items.append(
                WorkbenchTimelineEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    occurred_at=event.occurred_at,
                    stage_id=stage_ids[-1] if stage_ids else None,
                    stage_ids=stage_ids,
                    gate=workbench_gate(event.gate),
                    artifact_sha256=event.artifact_sha256,
                    decision=workbench_plan_decision(event.decision),
                    lifecycle_status=workbench_agent_status(event.lifecycle_status),
                    delivered=delivered,
                    delivery_attempt_count=attempts,
                )
            )
        draft = WorkbenchTimelineResponse(items=items, revision="")
        return draft.model_copy(update={"revision": workbench_revision(draft)})

    @app.get("/api/v1/workbench/runs")
    async def list_workbench_runs(
        authorization: str | None = Header(default=None),
        if_none_match: str | None = Header(default=None, alias="If-None-Match"),
        limit: int = 50,
        project_id: str | None = None,
    ) -> Response:
        """List only server-authorized run summaries for the Operator Workbench."""

        if limit < 1 or limit > 100:
            raise HTTPException(status_code=422, detail="limit must be between 1 and 100")
        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        if project_id is not None and project_id not in principal.projects:
            raise HTTPException(status_code=404, detail="Workbench project not found")
        project_ids = frozenset((project_id,)) if project_id is not None else principal.projects
        records = await supervisor_store.list_workbench_runs(project_ids=project_ids, limit=limit)
        items = [workbench_response(record, principal) for record in records]
        draft = WorkbenchRunListResponse(items=items, revision="")
        revision = workbench_revision(draft)
        if workbench_etag_matches(if_none_match, revision):
            return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers={"ETag": revision})
        response = WorkbenchRunListResponse(items=items, revision=revision)
        return JSONResponse(content=response.model_dump(mode="json"), headers={"ETag": revision})

    @app.get("/api/v1/workbench/projects")
    async def list_workbench_projects(authorization: str | None = Header(default=None)) -> JSONResponse:
        """List only projects that the authenticated Workbench principal may select."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        response = WorkbenchProjectListResponse(
            items=[WorkbenchProjectResponse(project_id=project_id) for project_id in sorted(principal.projects)]
        )
        return JSONResponse(content=response.model_dump(mode="json"))

    @app.get("/api/v1/workbench/runs/{run_id}")
    async def get_workbench_run(
        run_id: str,
        authorization: str | None = Header(default=None),
        if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    ) -> Response:
        """Return one scope-filtered Workbench detail projection."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        response = await workbench_detail_response(record, principal)
        revision = workbench_revision(response)
        if workbench_etag_matches(if_none_match, revision):
            return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers={"ETag": revision})
        return JSONResponse(content=response.model_dump(mode="json"), headers={"ETag": revision})

    @app.get("/api/v1/workbench/runs/{run_id}/evidence/{kind}")
    async def get_workbench_evidence(
        run_id: str,
        kind: WorkbenchArtifactKind,
        artifact_sha256: str,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        """Read bounded verified evidence selected only by a server-owned run artifact."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        artifact = {
            WorkbenchArtifactKind.SOURCE: record.source_artifact,
            WorkbenchArtifactKind.PLAN: record.plan_artifact,
            WorkbenchArtifactKind.IMPLEMENTATION: record.implementation_artifact,
        }[kind]
        if artifact is None or artifact.sha256 != artifact_sha256:
            raise HTTPException(status_code=404, detail="evidence not found")
        try:
            content = store.get_verified_artifact(artifact, max_bytes=100_000).decode("utf-8")
        except UnicodeDecodeError as error:
            raise HTTPException(status_code=422, detail="evidence is not renderable text") from error
        except PlanStoreUnavailableError as error:
            raise HTTPException(status_code=503, detail="evidence storage is temporarily unavailable") from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return JSONResponse(
            content=WorkbenchEvidenceResponse(
                kind=kind,
                sha256=artifact.sha256,
                content_type="application/json",
                content=content,
            ).model_dump(mode="json")
        )

    @app.get("/api/v1/workbench/runs/{run_id}/timeline")
    async def get_workbench_timeline(
        run_id: str,
        authorization: str | None = Header(default=None),
        if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    ) -> Response:
        """Return one bounded, scoped lifecycle timeline for an authorized Workbench run."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        response = await workbench_timeline_response(record)
        if workbench_etag_matches(if_none_match, response.revision):
            return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers={"ETag": response.revision})
        return JSONResponse(content=response.model_dump(mode="json"), headers={"ETag": response.revision})

    @app.post("/api/v1/workbench/runs/{run_id}/feedback", status_code=status.HTTP_202_ACCEPTED)
    async def record_workbench_feedback(
        run_id: str,
        request_body: WorkbenchFeedbackRequest,
        authorization: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        """Record scope-authorized, immutable review context that cannot alter workflow state."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        if not idempotency_key or len(idempotency_key) > 256:
            raise HTTPException(status_code=422, detail="Idempotency-Key header is required and must be at most 256 characters")
        comment = request_body.comment.strip()
        request_sha256 = sha256(
            json.dumps(
                request_body.model_dump(mode="json") | {"comment": comment}, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        try:
            feedback = await supervisor_store.record_workbench_feedback(
                run_id=run_id,
                intent=request_body.intent,
                artifact_sha256=request_body.artifact_sha256,
                stage_id=request_body.stage_id,
                actor_id=principal.subject,
                comment=comment,
                idempotency_key=idempotency_key,
                request_sha256=request_sha256,
            )
        except ApprovalConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=WorkbenchFeedbackResponse(
                feedback_id=feedback.feedback_id,
                run_id=feedback.run_id,
                intent=feedback.intent,
                artifact_sha256=feedback.artifact_sha256,
                stage_id=feedback.stage_id,
                actor_id=feedback.actor_id,
                comment=feedback.comment,
                created_at=feedback.created_at,
            ).model_dump(mode="json"),
        )

    @app.get("/api/v1/workbench/runs/{run_id}/feedback")
    async def list_workbench_feedback(
        run_id: str,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        """Return bounded immutable review context without granting execution authority."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_viewer(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        records = await supervisor_store.list_workbench_feedback(run_id)
        return JSONResponse(
            content=WorkbenchFeedbackListResponse(
                items=[
                    WorkbenchFeedbackResponse(
                        feedback_id=item.feedback_id,
                        run_id=item.run_id,
                        intent=item.intent,
                        artifact_sha256=item.artifact_sha256,
                        stage_id=item.stage_id,
                        actor_id=item.actor_id,
                        comment=item.comment,
                        created_at=item.created_at,
                    )
                    for item in records
                ]
            ).model_dump(mode="json")
        )

    @app.get("/api/v1/planning-runs/{run_id}/coordination")
    async def get_coordination_run(
        run_id: str,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        """Return one authenticated Supervisor-owned coordination projection."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        return JSONResponse(content=(await coordination_response(record)).model_dump(mode="json"))

    @app.get("/api/v1/coordination/runs")
    async def list_coordination_runs(
        authorization: str | None = Header(default=None),
        limit: int = 50,
    ) -> JSONResponse:
        """Return a bounded authenticated coordination queue for a future Workbench."""

        if limit < 1 or limit > 100:
            raise HTTPException(status_code=422, detail="limit must be between 1 and 100")
        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        records = await supervisor_store.list_workbench_runs(project_ids=principal.projects, limit=limit)
        response = CoordinationRunListResponse(items=[await coordination_response(record) for record in records])
        return JSONResponse(content=response.model_dump(mode="json"))

    @app.post("/api/v1/coordination/runs/{run_id}/actions/{gate}")
    async def submit_coordination_action(
        run_id: str,
        gate: CoordinationGate,
        request_body: CoordinationApprovalActionRequest,
        authorization: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        """Delegate an authenticated normalized action to the existing approval authority."""

        principal = await authenticator.authenticate(authorization)
        authenticator.require_approver(principal)
        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="planning run not found")
        require_workbench_scope(record, principal)
        if not idempotency_key or len(idempotency_key) > 256:
            raise HTTPException(status_code=422, detail="Idempotency-Key header is required and must be at most 256 characters")
        request_sha256 = sha256(
            json.dumps(request_body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        try:
            if gate is CoordinationGate.PLAN:
                recorded = await supervisor_store.record_plan_approval(
                    run_id=run_id,
                    artifact_sha256=request_body.artifact_sha256,
                    decision=request_body.decision,
                    actor_id=principal.subject,
                    comment=request_body.comment,
                    idempotency_key=idempotency_key,
                    request_sha256=request_sha256,
                )
                delivered = recorded.delivered or recorded.decision_id in await dispatcher.deliver_once(
                    decision_id=recorded.decision_id, limit=1
                )
            else:
                recorded = await supervisor_store.record_implementation_approval(
                    run_id=run_id,
                    artifact_sha256=request_body.artifact_sha256,
                    decision=ImplementationApprovalDecision(request_body.decision.value),
                    actor_id=principal.subject,
                    comment=request_body.comment,
                    idempotency_key=idempotency_key,
                    request_sha256=request_sha256,
                )
                delivered = recorded.delivered or recorded.decision_id in await implementation_dispatcher.deliver_once(
                    decision_id=recorded.decision_id, limit=1
                )
        except ApprovalConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "decision_id": recorded.decision_id,
                "run_id": recorded.run_id,
                "gate": gate.value,
                "decision": recorded.decision.value,
                "artifact_sha256": recorded.artifact_sha256,
                "actor_id": recorded.actor_id,
                "delivered": delivered,
                "created_at": recorded.created_at,
            },
        )

    return app


def _planning_workflow_id(run_id: str, plan_revision: int, plan_sha256: str) -> str:
    """Bind every plan version to a distinct Temporal workflow execution."""

    return f"{run_id}:plan:{plan_revision}:{plan_sha256[:16]}"


app = create_app()
