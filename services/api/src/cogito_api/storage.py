from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
from typing import Protocol
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error
from minio.retention import COMPLIANCE, Retention

from .models import AiPlan, ArtifactReference


class PlanStore(Protocol):
    def put_plan(self, run_id: str, plan: AiPlan) -> "PlanSnapshot": ...

    def put_status(self, run_id: str, status: dict) -> None: ...

    def get_status(self, run_id: str) -> dict | None: ...

    def put_source_specification(self, run_id: str, initial_specification: str) -> ArtifactReference: ...

    def get_source_specification(self, source_artifact_ref: str) -> str: ...


@dataclass(frozen=True)
class PlanSnapshot:
    """Immutable identity returned after persisting a plan document."""

    ref: str
    sha256: str


def plan_snapshot_bytes(plan: AiPlan) -> bytes:
    """Serialize a plan deterministically so API and worker can verify its identity."""

    return json.dumps(
        plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def source_specification_bytes(initial_specification: str) -> bytes:
    """Serialize untrusted source text canonically before compliance-retained storage."""

    return json.dumps(
        {"initial_specification": initial_specification},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()


class MinioPlanStore:
    def __init__(
        self,
        client: Minio,
        status_bucket: str,
        plan_snapshots_bucket: str,
        plan_snapshot_retention_days: int,
    ):
        if plan_snapshot_retention_days < 1:
            raise ValueError("plan snapshot retention must be at least one day")
        self._client = client
        self._status_bucket = status_bucket
        self._plan_snapshots_bucket = plan_snapshots_bucket
        self._plan_snapshot_retention_days = plan_snapshot_retention_days

    def put_plan(self, run_id: str, plan: AiPlan) -> PlanSnapshot:
        data = plan_snapshot_bytes(plan)
        self._put_snapshot(f"plans/{run_id}/plan.json", data)
        return PlanSnapshot(
            ref=f"s3://{self._plan_snapshots_bucket}/plans/{run_id}/plan.json",
            sha256=sha256(data).hexdigest(),
        )

    def put_source_specification(self, run_id: str, initial_specification: str) -> ArtifactReference:
        data = source_specification_bytes(initial_specification)
        self._put_snapshot(f"runs/{run_id}/source-spec.json", data)
        return ArtifactReference(
            ref=f"s3://{self._plan_snapshots_bucket}/runs/{run_id}/source-spec.json",
            sha256=sha256(data).hexdigest(),
        )

    def get_source_specification(self, source_artifact_ref: str) -> str:
        """Load and validate a source artifact from the configured immutable bucket."""

        parsed = urlparse(source_artifact_ref)
        if parsed.scheme != "s3" or parsed.netloc != self._plan_snapshots_bucket:
            raise ValueError("source artifact does not target the configured immutable snapshot bucket")
        response = self._client.get_object(self._plan_snapshots_bucket, parsed.path.lstrip("/"))
        try:
            body = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        initial_specification = body.get("initial_specification")
        if not isinstance(initial_specification, str):
            raise ValueError("source artifact is not a valid initial specification")
        return initial_specification

    def put_status(self, run_id: str, status: dict) -> None:
        data = json.dumps(status).encode()
        self._put_object(self._status_bucket, f"plans/{run_id}/status.json", data)

    def get_status(self, run_id: str) -> dict | None:
        try:
            response = self._client.get_object(self._status_bucket, f"plans/{run_id}/status.json")
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

    def _put_snapshot(self, object_name: str, data: bytes) -> None:
        """Persist a compliance-retained, content-addressed plan snapshot."""

        retention = Retention(
            COMPLIANCE,
            datetime.now(timezone.utc) + timedelta(days=self._plan_snapshot_retention_days),
        )
        self._client.put_object(
            self._plan_snapshots_bucket,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type="application/json",
            retention=retention,
        )

    def _put_object(self, bucket: str, object_name: str, data: bytes) -> None:
        self._client.put_object(
            bucket,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
