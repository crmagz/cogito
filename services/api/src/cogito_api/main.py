from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from minio import Minio

from .config import Settings, load_settings
from .dag import validate_constraints, validate_phase_dag
from .models import RunSubmission, Violation
from .storage import MinioPlanStore, PlanStore


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


def create_app(store: PlanStore | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    store = store or MinioPlanStore(
        Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        ),
        settings.plans_bucket,
    )

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
        violations = validate_phase_dag(plan.phases) + validate_constraints(plan.constraints, settings)
        if violations:
            raise PlanValidationError(violations)

        run_id = str(uuid.uuid4())

        if submission.dry_run:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"run_id": run_id, "status": "validated", "dry_run": True},
            )

        submitted_at = datetime.now(timezone.utc).isoformat()
        plan_ref = store.put_plan(run_id, plan)
        store.put_status(
            run_id,
            {"run_id": run_id, "status": "queued", "plan_ref": plan_ref, "submitted_at": submitted_at},
        )

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"run_id": run_id, "status": "queued", "plan_ref": plan_ref, "estimated_start": None},
        )

    @app.get("/api/v1/runs/{run_id}/status")
    async def get_run_status(run_id: str) -> dict:
        record = store.get_status(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"run '{run_id}' not found")
        return record

    return app


app = create_app()
