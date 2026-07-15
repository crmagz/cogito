from __future__ import annotations

import json
from io import BytesIO
from typing import Protocol

from minio import Minio
from minio.error import S3Error

from .models import AiPlan


class PlanStore(Protocol):
    def put_plan(self, run_id: str, plan: AiPlan) -> str: ...

    def put_status(self, run_id: str, status: dict) -> None: ...

    def get_status(self, run_id: str) -> dict | None: ...


class MinioPlanStore:
    def __init__(self, client: Minio, bucket: str):
        self._client = client
        self._bucket = bucket

    def put_plan(self, run_id: str, plan: AiPlan) -> str:
        data = plan.model_dump_json(indent=2).encode()
        self._put_object(f"plans/{run_id}/plan.json", data)
        return f"s3://{self._bucket}/plans/{run_id}/plan.json"

    def put_status(self, run_id: str, status: dict) -> None:
        data = json.dumps(status).encode()
        self._put_object(f"plans/{run_id}/status.json", data)

    def get_status(self, run_id: str) -> dict | None:
        try:
            response = self._client.get_object(self._bucket, f"plans/{run_id}/status.json")
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

    def _put_object(self, object_name: str, data: bytes) -> None:
        self._client.put_object(
            self._bucket,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
