from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cogito_api.config import Settings
from cogito_api.main import create_app

from .fakes import FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore


def make_settings(**overrides) -> Settings:
    defaults = dict(
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_secure=False,
        plans_bucket="plans",
        plan_snapshots_bucket="plan-snapshots",
        plan_snapshot_retention_days=30,
        max_wall_clock_minutes=240,
        max_cost_usd=50.0,
        max_review_rounds=10,
        max_turns_per_phase=500,
        temporal_host="localhost:7233",
        temporal_namespace="default",
        temporal_task_queue="developer-tasks",
        allowed_git_hosts=("github.com",),
        supervisor_database_host="localhost",
        supervisor_database_port=5432,
        supervisor_database_name="cogito",
        supervisor_database_user="cogito",
        supervisor_database_password="cogito",
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
def store() -> InMemoryPlanStore:
    return InMemoryPlanStore()


@pytest.fixture
def starter() -> FakeRunStarter:
    return FakeRunStarter()


@pytest.fixture
def supervisor_store() -> InMemorySupervisorStore:
    return InMemorySupervisorStore()


@pytest.fixture
def client(
    store: InMemoryPlanStore,
    starter: FakeRunStarter,
    supervisor_store: InMemorySupervisorStore,
) -> TestClient:
    app = create_app(
        store=store,
        settings=make_settings(),
        starter=starter,
        supervisor_store=supervisor_store,
    )
    return TestClient(app)


@pytest.fixture
def valid_plan() -> dict:
    return {
        "title": "Add rate limiting to API gateway",
        "summary": "Implement token-bucket rate limiting on /api/v2 endpoints to prevent abuse.",
        "target_repos": ["https://github.com/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"],
        "spec_set": "typescript-backend@v2.1#sha256=" + "a" * 64,
        "phases": [
            {
                "id": "phase-1",
                "name": "Rate limiter module",
                "description": "Implement token-bucket algorithm as a standalone module",
                "tasks": ["Create src/middleware/rate-limiter.ts"],
                "acceptance_criteria": ["Module exports middleware function"],
                "verification": ["npm run typecheck"],
                "depends_on": [],
            },
            {
                "id": "phase-2",
                "name": "Integration",
                "description": "Wire rate limiter into gateway",
                "tasks": ["Register middleware in request pipeline"],
                "acceptance_criteria": ["Rate limiting active on all routes"],
                "verification": ["npm run test"],
                "depends_on": ["phase-1"],
            },
        ],
        "constraints": {
            "max_wall_clock_minutes": 45,
            "max_cost_usd": 3.0,
            "max_review_rounds": 2,
            "max_turns_per_phase": 150,
            "backup_reserve_turns": 25,
        },
    }
