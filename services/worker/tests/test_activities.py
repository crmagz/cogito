from __future__ import annotations

import pytest
from temporalio.testing import ActivityEnvironment

from cogito_worker.activities import WorkerActivities

from .fakes import InMemoryRunStore


@pytest.fixture
def store() -> InMemoryRunStore:
    return InMemoryRunStore()


@pytest.fixture
def activities(store: InMemoryRunStore) -> WorkerActivities:
    return WorkerActivities(store)


@pytest.fixture
def env() -> ActivityEnvironment:
    return ActivityEnvironment()


async def test_load_plan_returns_plan_from_store(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    store.plans["s3://plans/plans/run-1/plan.json"] = {"title": "Test plan"}

    result = await env.run(activities.load_plan, "s3://plans/plans/run-1/plan.json")

    assert result == {"title": "Test plan"}


async def test_report_status_creates_status_when_none_exists(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    await env.run(activities.report_status, "run-1", "claimed")

    assert store.statuses["run-1"]["status"] == "claimed"
    assert store.statuses["run-1"]["run_id"] == "run-1"
    assert "updated_at" in store.statuses["run-1"]


async def test_report_status_preserves_existing_fields(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    store.statuses["run-1"] = {"run_id": "run-1", "status": "queued", "plan_ref": "s3://plans/plans/run-1/plan.json"}

    await env.run(activities.report_status, "run-1", "completed")

    assert store.statuses["run-1"]["status"] == "completed"
    assert store.statuses["run-1"]["plan_ref"] == "s3://plans/plans/run-1/plan.json"
