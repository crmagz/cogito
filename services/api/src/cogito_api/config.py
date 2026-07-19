from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    plans_bucket: str
    plan_snapshots_bucket: str
    plan_snapshot_retention_days: int
    max_wall_clock_minutes: int
    max_cost_usd: float
    max_review_rounds: int
    max_turns_per_phase: int
    temporal_host: str
    temporal_namespace: str
    temporal_task_queue: str
    allowed_git_hosts: tuple[str, ...]
    supervisor_database_host: str
    supervisor_database_port: int
    supervisor_database_name: str
    supervisor_database_user: str
    supervisor_database_password: str
    litellm_endpoint: str
    litellm_planner_model: str
    litellm_planner_api_key: str
    litellm_planner_timeout_seconds: float

    @property
    def supervisor_database_url(self) -> str:
        """Return a SQLAlchemy async URL without exposing password composition to callers."""

        return (
            "postgresql+psycopg://"
            f"{quote(self.supervisor_database_user, safe='')}:{quote(self.supervisor_database_password, safe='')}"
            f"@{self.supervisor_database_host}:{self.supervisor_database_port}/{self.supervisor_database_name}"
        )

    @property
    def supervisor_database_sync_url(self) -> str:
        """Return a psycopg connection URL for migration/bootstrap commands."""

        return (
            "postgresql://"
            f"{quote(self.supervisor_database_user, safe='')}:{quote(self.supervisor_database_password, safe='')}"
            f"@{self.supervisor_database_host}:{self.supervisor_database_port}/{self.supervisor_database_name}"
        )


def load_settings() -> Settings:
    allowed_hosts = json.loads(os.environ.get("COGITO_ALLOWED_GIT_HOSTS", '["github.com"]'))
    if (
        not isinstance(allowed_hosts, list)
        or not allowed_hosts
        or not all(isinstance(host, str) and host.strip() for host in allowed_hosts)
    ):
        raise ValueError("COGITO_ALLOWED_GIT_HOSTS must be a non-empty JSON string array")
    return Settings(
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        minio_secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        plans_bucket=os.environ.get("MINIO_PLANS_BUCKET", "plans"),
        plan_snapshots_bucket=os.environ.get("MINIO_PLAN_SNAPSHOTS_BUCKET", "plan-snapshots"),
        plan_snapshot_retention_days=int(os.environ.get("MINIO_PLAN_SNAPSHOT_RETENTION_DAYS", "30")),
        max_wall_clock_minutes=int(os.environ.get("COGITO_MAX_WALL_CLOCK_MINUTES", "240")),
        max_cost_usd=float(os.environ.get("COGITO_MAX_COST_USD", "50")),
        max_review_rounds=int(os.environ.get("COGITO_MAX_REVIEW_ROUNDS", "10")),
        max_turns_per_phase=int(os.environ.get("COGITO_MAX_TURNS_PER_PHASE", "500")),
        temporal_host=os.environ.get("COGITO_TEMPORAL_HOST", "cogito-temporal-frontend:7233"),
        temporal_namespace=os.environ.get("COGITO_TEMPORAL_NAMESPACE", "default"),
        temporal_task_queue=os.environ.get("COGITO_TEMPORAL_TASK_QUEUE", "developer-tasks"),
        allowed_git_hosts=tuple(allowed_hosts),
        supervisor_database_host=os.environ.get("COGITO_SUPERVISOR_DATABASE_HOST", "cogito-postgresql"),
        supervisor_database_port=int(os.environ.get("COGITO_SUPERVISOR_DATABASE_PORT", "5432")),
        supervisor_database_name=os.environ.get("COGITO_SUPERVISOR_DATABASE_NAME", "cogito"),
        supervisor_database_user=os.environ.get("COGITO_SUPERVISOR_DATABASE_USER", "postgres"),
        supervisor_database_password=os.environ.get("COGITO_SUPERVISOR_DATABASE_PASSWORD", "cogito"),
        litellm_endpoint=os.environ.get("COGITO_LITELLM_ENDPOINT", "http://cogito-litellm:4000"),
        litellm_planner_model=os.environ.get("COGITO_LITELLM_PLANNER_MODEL", "balanced"),
        litellm_planner_api_key=os.environ.get("COGITO_LITELLM_PLANNER_API_KEY", ""),
        litellm_planner_timeout_seconds=float(
            os.environ.get("COGITO_LITELLM_PLANNER_TIMEOUT_SECONDS", "60")
        ),
    )
