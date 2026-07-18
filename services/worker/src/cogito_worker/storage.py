from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from typing import Protocol
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error


class RunStore(Protocol):
    def get_plan(self, plan_ref: str) -> dict: ...

    def get_status(self, run_id: str) -> dict | None: ...

    def put_status(self, run_id: str, status: dict) -> None: ...


class MinioRunStore:
    def __init__(self, client: Minio, bucket: str):
        self._client = client
        self._bucket = bucket

    def get_plan(self, plan_ref: str) -> dict:
        parsed = urlparse(plan_ref)
        object_name = parsed.path.lstrip("/")
        return self._get_object(object_name)

    def get_status(self, run_id: str) -> dict | None:
        try:
            return self._get_object(f"plans/{run_id}/status.json")
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise

    def put_status(self, run_id: str, status: dict) -> None:
        data = json.dumps(status).encode()
        self._client.put_object(
            self._bucket,
            f"plans/{run_id}/status.json",
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

    def _get_object(self, object_name: str) -> dict:
        response = self._client.get_object(self._bucket, object_name)
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
