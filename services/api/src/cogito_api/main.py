from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from minio import Minio

from .config import Settings, load_settings
from .dag import validate_constraints, validate_phase_dag, validate_spec_reference, validate_target_repositories
from .models import (
    PlanningRunResponse,
    PlanningRunStatus,
    PlanningRunSubmission,
    ArtifactReference,
    RunEnvelope,
    RunSubmission,
    Violation,
)
from .planner import LiteLLMPlanner, Planner, PlanningContext
from .storage import MinioPlanStore, PlanStore
from .supervisor import PlanningRunRecord, PostgresSupervisorStore, SupervisorStore
from .temporal import RunStarter, TemporalRunStarter


class PlanValidationError(Exception):
    def __init__(self, violations: list[Violation]):
        self.violations = violations


def _violation_response(violations: list[Violation]) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
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

    app = FastAPI(title="Cogito API")

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

        source_artifact = store.put_source_specification(run_id, submission.initial_specification)
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
        if record.status is not PlanningRunStatus.PLANNING:
            raise HTTPException(status_code=409, detail="planning run is not eligible for plan generation")
        initial_specification = store.get_source_specification(record.source_artifact.ref)
        generated_plan = await planner.generate(
            PlanningContext(
                initial_specification=initial_specification,
                target_repos=record.target_repos,
                spec_set=record.spec_set,
                constraints=record.constraints,
            )
        )
        snapshot = store.put_plan(run_id, generated_plan)
        updated = await supervisor_store.attach_generated_plan(
            run_id,
            plan_artifact=ArtifactReference(ref=snapshot.ref, sha256=snapshot.sha256),
            planner_model=settings.litellm_planner_model,
        )
        response = PlanningRunResponse(
            run_id=updated.run_id,
            status=updated.status,
            source_artifact=updated.source_artifact,
            plan_artifact=updated.plan_artifact,
            submitted_at=updated.submitted_at,
        )
        return JSONResponse(content=response.model_dump(mode="json"))

    @app.get("/api/v1/runs/{run_id}/status")
    async def get_run_status(run_id: str) -> dict:
        record = store.get_status(run_id)
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


app = create_app()
