from __future__ import annotations

import json
from io import BytesIO

from minio.error import S3Error
from minio.retention import COMPLIANCE

from cogito_api.models import AiPlan
from cogito_api.storage import MinioPlanStore


class FakeMinioClient:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, object]] = []
        self.object_names: set[tuple[str, str]] = set()

    def stat_object(self, bucket_name: str, object_name: str) -> None:
        if (bucket_name, object_name) not in self.object_names:
            raise S3Error(None, "NoSuchKey", "missing", None, None, None)

    def put_object(self, bucket_name: str, object_name: str, data: BytesIO, **kwargs: object) -> None:
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "data": data.read(),
                **kwargs,
            }
        )
        self.object_names.add((bucket_name, object_name))


def test_plan_store_writes_compliance_retained_snapshots_to_a_separate_bucket(valid_plan: dict) -> None:
    client = FakeMinioClient()
    store = MinioPlanStore(client, "plans", "plan-snapshots", plan_snapshot_retention_days=30)

    snapshot = store.put_plan("run-1", AiPlan.model_validate(valid_plan))
    store.put_status("run-1", {"run_id": "run-1", "status": "queued"})

    snapshot_call, status_call = client.put_calls
    assert snapshot.ref == "s3://plan-snapshots/plans/run-1/plan.json"
    assert snapshot_call["bucket_name"] == "plan-snapshots"
    assert snapshot_call["object_name"] == "plans/run-1/plan.json"
    assert snapshot_call["retention"].mode == COMPLIANCE
    assert json.loads(snapshot_call["data"])["title"] == valid_plan["title"]
    assert status_call["bucket_name"] == "plans"
    assert "retention" not in status_call


def test_planning_plan_uses_a_content_addressed_immutable_revision_key(valid_plan: dict) -> None:
    client = FakeMinioClient()
    store = MinioPlanStore(client, "plans", "plan-snapshots", plan_snapshot_retention_days=30)

    first = store.put_planning_plan("run-1", AiPlan.model_validate(valid_plan))
    second = store.put_planning_plan("run-1", AiPlan.model_validate(valid_plan))

    assert first == second
    assert "/revisions/" in first.ref
    assert first.sha256 in first.ref
    assert len(client.put_calls) == 1
