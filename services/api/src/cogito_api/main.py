from __future__ import annotations

import json
import uuid
import asyncio
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from hashlib import sha256

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from minio import Minio
from opentelemetry.context import attach, detach

from .auth import ApprovalAuthenticator
from .config import Settings, load_settings
from .dag import validate_constraints, validate_phase_dag, validate_spec_reference, validate_target_repositories
from .models import (
    AgentRunResponse,
    AgentRunStatus,
    ArtifactReference,
    PlanApprovalRequest,
    PlanApprovalResponse,
    PlanningRunResponse,
    PlanningRunStatus,
    PlanningRunSubmission,
    RunEnvelope,
    RunSubmission,
    Violation,
)
from .outbox import PlanApprovalOutboxDispatcher, stop_dispatcher
from .observability import Telemetry, TelemetrySettings
from .planner import LiteLLMPlanner, Planner, PlannerError, PlanningContext
from .storage import MinioPlanStore, PlanStore, PlanStoreUnavailableError
from .supervisor import AgentRunRecord, ApprovalConflictError, PlanningRunRecord, PostgresSupervisorStore, SupervisorStore
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
    telemetry = Telemetry(TelemetrySettings.from_environment())
    authenticator = ApprovalAuthenticator(settings)

    dispatcher = PlanApprovalOutboxDispatcher(supervisor_store, starter)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        delivery_task = asyncio.create_task(dispatcher.run())
        try:
            yield
        finally:
            await stop_dispatcher(delivery_task)
            telemetry.shutdown()
            close = getattr(supervisor_store, "close", None)
            if close is not None:
                await close()

    app = FastAPI(title="Cogito API", lifespan=lifespan)

    @app.middleware("http")
    async def trace_request(request: Request, call_next):
        parent = telemetry.extract(dict(request.headers))
        token = attach(parent)
        try:
            with telemetry.span("cogito.api.request", {"http.request.method": request.method}):
                response = await call_next(request)
                telemetry.request(request.method, response.status_code)
                telemetry.event("cogito.api.response", {"http.response.status_code": str(response.status_code)})
                return response
        finally:
            detach(token)

    @app.exception_handler(RequestValidationError)
    async def handle_schema_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _violation_response(_schema_violations(exc))

    @app.exception_handler(PlanValidationError)
    async def handle_plan_error(request: Request, exc: PlanValidationError) -> JSONResponse:
        return _violation_response(exc.violations)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

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
            traceparent=carrier.get("traceparent"),
            tracestate=carrier.get("tracestate"),
        )
        await starter.start_run(envelope)

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"run_id": run_id, "status": "queued", "plan_ref": snapshot.ref, "estimated_start": None},
        )

    @app.post("/api/v1/planning-runs")
    async def submit_planning_run(submission: PlanningRunSubmission) -> JSONResponse:
        """Persist an initial work specification for a future human-gated planning workflow."""

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
        )
        await supervisor_store.create_planning_run(record)
        response = PlanningRunResponse(
            run_id=record.run_id,
            status=record.status,
            source_artifact=record.source_artifact,
            plan_artifact=record.plan_artifact,
            submitted_at=record.submitted_at,
        )
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.model_dump(mode="json"))

    @app.post("/api/v1/planning-runs/{run_id}/generate-plan")
    async def generate_plan(run_id: str) -> JSONResponse:
        """Generate and persist one normalized plan for a planning run.

        This endpoint becomes worker-internal when the durable workflow gate is added.
        """

        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"planning run '{run_id}' not found")
        if record.status is PlanningRunStatus.PLANNING:
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

    @app.get("/api/v1/runs/{run_id}/status")
    async def get_run_status(run_id: str) -> dict:
        agent_run = await supervisor_store.get_agent_run(run_id)
        if agent_run is not None:
            response = AgentRunResponse(**agent_run.__dict__).model_dump(mode="json")
            # Preserve the legacy lower-case field while exposing the canonical
            # state explicitly for new clients.
            response["lifecycle_status"] = response["status"]
            response["status"] = agent_run.status.value.lower()
            return response
        try:
            record = store.get_status(run_id)
        except PlanStoreUnavailableError as error:
            raise HTTPException(status_code=503, detail="run storage is temporarily unavailable") from error
        if record is None:
            raise HTTPException(status_code=404, detail=f"run '{run_id}' not found")
        return record

    @app.get("/api/v1/planning-runs/{run_id}")
    async def get_planning_run(run_id: str) -> JSONResponse:
        """Return the authoritative supervisor record for a planning run."""

        record = await supervisor_store.get_planning_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"planning run '{run_id}' not found")
        response = PlanningRunResponse(
            run_id=record.run_id,
            status=record.status,
            source_artifact=record.source_artifact,
            plan_artifact=record.plan_artifact,
            submitted_at=record.submitted_at,
        )
        return JSONResponse(content=response.model_dump(mode="json"))

    return app


def _planning_workflow_id(run_id: str, plan_revision: int, plan_sha256: str) -> str:
    """Bind every plan version to a distinct Temporal workflow execution."""

    return f"{run_id}:plan:{plan_revision}:{plan_sha256[:16]}"


app = create_app()
