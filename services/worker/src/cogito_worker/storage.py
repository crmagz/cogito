from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from typing import Protocol
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error


class RunStore(Protocol):
    def get_plan(self, plan_ref: str) -> dict: ...

    def get_status(self, run_id: str) -> dict | None: ...

    def put_status(self, run_id: str, status: dict) -> None: ...


class SpecStore(Protocol):
    """Retrieves immutable spec-set archives from object storage."""

    def get_archive(self, ref: str) -> bytes:
        """Return the archive for an exact spec-set reference."""


_SPEC_REF_PATTERN = re.compile(
    r"(?P<name_version>[A-Za-z0-9][A-Za-z0-9._-]*@[A-Za-z0-9][A-Za-z0-9._-]*)#sha256=(?P<digest>[0-9a-fA-F]{64})"
)


@dataclass(frozen=True)
class SpecReference:
    """An immutable spec archive reference and its expected content digest."""

    value: str
    name_version: str
    digest: str


def validate_spec_ref(ref: str) -> SpecReference:
    """Validate and parse an exact name@version reference with an archive digest."""

    match = _SPEC_REF_PATTERN.fullmatch(ref)
    if match is None:
        raise ValueError("spec references must use name@version#sha256=<64 hex characters>")
    return SpecReference(
        value=ref,
        name_version=match.group("name_version"),
        digest=match.group("digest").lower(),
    )


class MinioRunStore:
    def __init__(self, client: Minio, status_bucket: str, plan_snapshots_bucket: str):
        self._client = client
        self._status_bucket = status_bucket
        self._plan_snapshots_bucket = plan_snapshots_bucket

    def get_plan(self, plan_ref: str) -> dict:
        parsed = urlparse(plan_ref)
        if parsed.scheme != "s3" or parsed.netloc != self._plan_snapshots_bucket:
            raise ValueError("plan reference does not target the configured immutable plan snapshot bucket")
        object_name = parsed.path.lstrip("/")
        return self._get_object(self._plan_snapshots_bucket, object_name)

    def get_status(self, run_id: str) -> dict | None:
        try:
            return self._get_object(self._status_bucket, f"plans/{run_id}/status.json")
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise

    def put_status(self, run_id: str, status: dict) -> None:
        data = json.dumps(status).encode()
        self._client.put_object(
            self._status_bucket,
            f"plans/{run_id}/status.json",
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

    def _get_object(self, bucket: str, object_name: str) -> dict:
        response = self._client.get_object(bucket, object_name)
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()


class MinioSpecStore:
    """MinIO-backed immutable spec archive store with bounded reads."""

    def __init__(self, client: Minio, bucket: str, prefix: str, max_archive_bytes: int):
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._max_archive_bytes = max_archive_bytes

    def get_archive(self, ref: str) -> bytes:
        """Load an exact immutable archive without allowing key traversal."""

        spec_ref = validate_spec_ref(ref)

        object_name = "/".join(
            part for part in (self._prefix, spec_ref.name_version, "spec-set.tar.gz") if part
        )
        response = self._client.get_object(self._bucket, object_name)
        try:
            content_length = response.headers.get("content-length")
            if content_length is not None and int(content_length) > self._max_archive_bytes:
                raise ValueError("spec archive exceeds the configured size limit")
            data = response.read(self._max_archive_bytes + 1)
            if len(data) > self._max_archive_bytes:
                raise ValueError("spec archive exceeds the configured size limit")
            if sha256(data).hexdigest() != spec_ref.digest:
                raise ValueError("spec archive digest does not match the requested spec reference")
            return data
        finally:
            response.close()
            response.release_conn()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
