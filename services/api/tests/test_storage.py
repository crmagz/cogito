from __future__ import annotations

import json
from io import BytesIO

from minio.retention import COMPLIANCE

from cogito_api.models import AiPlan
from cogito_api.storage import MinioPlanStore


class FakeMinioClient:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, object]] = []

    def put_object(self, bucket_name: str, object_name: str, data: BytesIO, **kwargs: object) -> None:
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "data": data.read(),
                **kwargs,
            }
        )


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
