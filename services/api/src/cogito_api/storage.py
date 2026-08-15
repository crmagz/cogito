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

from .models import AiPlan, ArtifactReference, ProductSpecification, SpecificationEvaluation


class PlanStoreUnavailableError(RuntimeError):
    """The object store could not safely complete an API storage operation."""


class PlanStore(Protocol):
    def put_plan(self, run_id: str, plan: AiPlan) -> "PlanSnapshot": ...

    def put_planning_plan(self, run_id: str, revision: int, plan: AiPlan) -> "PlanSnapshot": ...

    def put_status(self, run_id: str, status: dict) -> None: ...

    def get_status(self, run_id: str) -> dict | None: ...

    def put_source_specification(self, run_id: str, initial_specification: str) -> ArtifactReference: ...

    def put_product_specification(
        self, run_id: str, revision: int, specification: ProductSpecification
    ) -> ArtifactReference: ...

    def put_specification_evaluation(
        self, run_id: str, specification_revision: int, evaluation: SpecificationEvaluation
    ) -> ArtifactReference: ...

    def get_source_specification(self, source_artifact_ref: str) -> str: ...

    def get_verified_artifact(self, artifact: ArtifactReference, *, max_bytes: int) -> bytes: ...


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


def product_specification_bytes(specification: ProductSpecification) -> bytes:
    """Serialize a structured product specification deterministically for immutable storage."""

    return json.dumps(
        specification.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def specification_evaluation_bytes(evaluation: SpecificationEvaluation) -> bytes:
    """Serialize immutable evaluation evidence deterministically."""

    return json.dumps(
        evaluation.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
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

    def put_planning_plan(self, run_id: str, revision: int, plan: AiPlan) -> PlanSnapshot:
        """Store a generated plan under its content digest so revisions never overwrite it."""

        if revision < 1:
            raise ValueError("planning artifact revision must be positive")
        data = plan_snapshot_bytes(plan)
        digest = sha256(data).hexdigest()
        object_name = f"plans/{run_id}/revisions/{revision}/{digest}/plan.json"
        self._put_snapshot(object_name, data)
        return PlanSnapshot(ref=f"s3://{self._plan_snapshots_bucket}/{object_name}", sha256=digest)

    def put_source_specification(self, run_id: str, initial_specification: str) -> ArtifactReference:
        data = source_specification_bytes(initial_specification)
        self._put_snapshot(f"runs/{run_id}/source-spec.json", data)
        return ArtifactReference(
            ref=f"s3://{self._plan_snapshots_bucket}/runs/{run_id}/source-spec.json",
            sha256=sha256(data).hexdigest(),
        )

    def put_product_specification(
        self, run_id: str, revision: int, specification: ProductSpecification
    ) -> ArtifactReference:
        """Store one content-addressed product specification draft without overwriting another revision."""

        if revision < 1:
            raise ValueError("product specification revision must be positive")
        data = product_specification_bytes(specification)
        digest = sha256(data).hexdigest()
        object_name = f"runs/{run_id}/product-specifications/{revision}/{digest}/specification.json"
        self._put_snapshot(object_name, data)
        return ArtifactReference(ref=f"s3://{self._plan_snapshots_bucket}/{object_name}", sha256=digest)

    def put_specification_evaluation(
        self, run_id: str, specification_revision: int, evaluation: SpecificationEvaluation
    ) -> ArtifactReference:
        if specification_revision < 1:
            raise ValueError("product specification revision must be positive")
        data = specification_evaluation_bytes(evaluation)
        digest = sha256(data).hexdigest()
        object_name = f"runs/{run_id}/specification-evaluations/{specification_revision}/{digest}/evaluation.json"
        self._put_snapshot(object_name, data)
        return ArtifactReference(ref=f"s3://{self._plan_snapshots_bucket}/{object_name}", sha256=digest)

    def get_source_specification(self, source_artifact_ref: str) -> str:
        """Load and validate a source artifact from the configured immutable bucket."""

        parsed = urlparse(source_artifact_ref)
        if parsed.scheme != "s3" or parsed.netloc != self._plan_snapshots_bucket:
            raise ValueError("source artifact does not target the configured immutable snapshot bucket")
        try:
            response = self._client.get_object(self._plan_snapshots_bucket, parsed.path.lstrip("/"))
        except S3Error as error:
            raise PlanStoreUnavailableError("source specification storage is unavailable") from error
        try:
            body = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        initial_specification = body.get("initial_specification")
        if not isinstance(initial_specification, str):
            raise ValueError("source artifact is not a valid initial specification")
        return initial_specification

    def get_verified_artifact(self, artifact: ArtifactReference, *, max_bytes: int) -> bytes:
        """Load one already-authorized immutable artifact with a strict byte limit."""

        parsed = urlparse(artifact.ref)
        if (
            max_bytes < 1
            or parsed.scheme != "s3"
            or parsed.netloc not in {self._status_bucket, self._plan_snapshots_bucket}
            or not parsed.path.lstrip("/")
        ):
            raise ValueError("artifact does not target a configured immutable bucket")
        try:
            response = self._client.get_object(parsed.netloc, parsed.path.lstrip("/"))
        except S3Error as error:
            raise PlanStoreUnavailableError("artifact storage is unavailable") from error
        try:
            body = response.read(max_bytes + 1)
        finally:
            response.close()
            response.release_conn()
        if len(body) > max_bytes:
            raise ValueError("artifact exceeds the Workbench evidence limit")
        if sha256(body).hexdigest() != artifact.sha256:
            raise ValueError("artifact digest does not match its immutable reference")
        return body

    def put_status(self, run_id: str, status: dict) -> None:
        data = json.dumps(status).encode()
        self._put_object(self._status_bucket, f"plans/{run_id}/status.json", data)

    def get_status(self, run_id: str) -> dict | None:
        try:
            response = self._client.get_object(self._status_bucket, f"plans/{run_id}/status.json")
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise PlanStoreUnavailableError("run status storage is unavailable") from exc
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

    def _put_snapshot(self, object_name: str, data: bytes) -> None:
        """Persist a compliance-retained, content-addressed plan snapshot."""

        try:
            self._client.stat_object(self._plan_snapshots_bucket, object_name)
            # A content-addressed object already exists only for the same
            # canonical artifact and must never be overwritten under retention.
            return
        except S3Error as error:
            if error.code not in {"NoSuchKey", "NoSuchObject"}:
                raise PlanStoreUnavailableError("plan snapshot storage is unavailable") from error

        retention = Retention(
            COMPLIANCE,
            datetime.now(timezone.utc) + timedelta(days=self._plan_snapshot_retention_days),
        )
        try:
            self._client.put_object(
                self._plan_snapshots_bucket,
                object_name,
                BytesIO(data),
                length=len(data),
                content_type="application/json",
                retention=retention,
            )
        except S3Error as error:
            raise PlanStoreUnavailableError("plan snapshot storage is unavailable") from error

    def _put_object(self, bucket: str, object_name: str, data: bytes) -> None:
        try:
            self._client.put_object(
                bucket,
                object_name,
                BytesIO(data),
                length=len(data),
                content_type="application/json",
            )
        except S3Error as error:
            raise PlanStoreUnavailableError("run status storage is unavailable") from error
