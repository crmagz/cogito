from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse


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
    deployment_mode: str
    auth_mode: str
    auth_static_token: str
    auth_static_subject: str
    auth_static_projects: tuple[str, ...]
    auth_static_roles: tuple[str, ...]
    auth_oidc_issuer: str
    auth_oidc_audience: str
    auth_oidc_jwks_url: str
    auth_oidc_role_claim: str
    auth_oidc_approval_role: str
    auth_oidc_project_claim: str
    auth_oidc_viewer_role: str
    auth_oidc_admin_role: str
    workbench_default_project_id: str
    registry_catalog_path: str
    notification_enabled: bool
    notification_provider: str
    notification_webhook_url: str
    notification_webhook_hmac_secret: str
    notification_slack_channel_id: str
    notification_slack_bot_token: str
    notification_slack_workbench_url: str
    notification_timeout_seconds: float
    reconciliation_enabled: bool
    reconciliation_poll_seconds: int
    reconciliation_batch_size: int
    reconciliation_stall_seconds: int

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
    static_projects = _json_string_array("COGITO_AUTH_STATIC_PROJECTS", '["default"]')
    static_roles = _json_string_array("COGITO_AUTH_STATIC_ROLES", '["cogito-viewer", "cogito-approver"]')
    settings = Settings(
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
        deployment_mode=os.environ.get("COGITO_DEPLOYMENT_MODE", "development"),
        auth_mode=os.environ.get("COGITO_AUTH_MODE", "static"),
        auth_static_token=os.environ.get("COGITO_AUTH_STATIC_TOKEN", ""),
        auth_static_subject=os.environ.get("COGITO_AUTH_STATIC_SUBJECT", "local-operator"),
        auth_static_projects=static_projects,
        auth_static_roles=static_roles,
        auth_oidc_issuer=os.environ.get("COGITO_AUTH_OIDC_ISSUER", ""),
        auth_oidc_audience=os.environ.get("COGITO_AUTH_OIDC_AUDIENCE", ""),
        auth_oidc_jwks_url=os.environ.get("COGITO_AUTH_OIDC_JWKS_URL", ""),
        auth_oidc_role_claim=os.environ.get("COGITO_AUTH_OIDC_ROLE_CLAIM", "roles"),
        auth_oidc_approval_role=os.environ.get("COGITO_AUTH_OIDC_APPROVAL_ROLE", "cogito-approver"),
        auth_oidc_project_claim=os.environ.get("COGITO_AUTH_OIDC_PROJECT_CLAIM", "cogito_projects"),
        auth_oidc_viewer_role=os.environ.get("COGITO_AUTH_OIDC_VIEWER_ROLE", "cogito-viewer"),
        auth_oidc_admin_role=os.environ.get("COGITO_AUTH_OIDC_ADMIN_ROLE", "cogito-admin"),
        workbench_default_project_id=os.environ.get("COGITO_WORKBENCH_DEFAULT_PROJECT_ID", "default"),
        registry_catalog_path=os.environ.get(
            "COGITO_REGISTRY_CATALOG_PATH",
            str(_default_registry_catalog_path()),
        ),
        notification_enabled=os.environ.get("COGITO_NOTIFICATION_ENABLED", "false").lower() == "true",
        notification_provider=os.environ.get("COGITO_NOTIFICATION_PROVIDER", "webhook").lower(),
        notification_webhook_url=os.environ.get("COGITO_NOTIFICATION_WEBHOOK_URL", ""),
        notification_webhook_hmac_secret=os.environ.get("COGITO_NOTIFICATION_WEBHOOK_HMAC_SECRET", ""),
        notification_slack_channel_id=os.environ.get("COGITO_NOTIFICATION_SLACK_CHANNEL_ID", ""),
        notification_slack_bot_token=os.environ.get("COGITO_NOTIFICATION_SLACK_BOT_TOKEN", ""),
        notification_slack_workbench_url=os.environ.get("COGITO_NOTIFICATION_SLACK_WORKBENCH_URL", ""),
        notification_timeout_seconds=float(os.environ.get("COGITO_NOTIFICATION_TIMEOUT_SECONDS", "10")),
        reconciliation_enabled=os.environ.get("COGITO_RECONCILIATION_ENABLED", "true").lower() == "true",
        reconciliation_poll_seconds=int(os.environ.get("COGITO_RECONCILIATION_POLL_SECONDS", "5")),
        reconciliation_batch_size=int(os.environ.get("COGITO_RECONCILIATION_BATCH_SIZE", "100")),
        reconciliation_stall_seconds=int(os.environ.get("COGITO_RECONCILIATION_STALL_SECONDS", "30")),
    )
    _validate_auth_configuration(settings)
    return settings


def _default_registry_catalog_path() -> Path:
    """Use the checked-in catalog locally and the image-mounted catalog in production."""

    local_catalog = Path(__file__).parents[4] / "components"
    return local_catalog if local_catalog.is_dir() else Path("/app/components")


def _json_string_array(name: str, default: str) -> tuple[str, ...]:
    """Read a bounded JSON string-array configuration value without coercion."""

    try:
        value = json.loads(os.environ.get(name, default))
    except json.JSONDecodeError as error:
        raise ValueError(f"{name} must be a JSON string array") from error
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError(f"{name} must be a non-empty JSON string array")
    return tuple(value)


def _validate_auth_configuration(settings: Settings) -> None:
    """Fail closed for invalid operator-approval authentication configuration."""

    if settings.deployment_mode not in {"development", "production"}:
        raise ValueError("COGITO_DEPLOYMENT_MODE must be development or production")
    if settings.auth_mode not in {"static", "oidc"}:
        raise ValueError("COGITO_AUTH_MODE must be static or oidc")
    if settings.deployment_mode == "production" and settings.auth_mode != "oidc":
        raise ValueError("production deployments require COGITO_AUTH_MODE=oidc")
    if settings.auth_mode == "oidc" and not all(
        (settings.auth_oidc_issuer, settings.auth_oidc_audience, settings.auth_oidc_jwks_url)
    ):
        raise ValueError("OIDC approval authentication requires issuer, audience, and JWKS URL")
    if not 1 <= len(settings.workbench_default_project_id) <= 128 or not settings.workbench_default_project_id.strip():
        raise ValueError("COGITO_WORKBENCH_DEFAULT_PROJECT_ID must not be empty")
    if settings.auth_mode == "static" and settings.workbench_default_project_id not in settings.auth_static_projects:
        raise ValueError(
            "COGITO_WORKBENCH_DEFAULT_PROJECT_ID must be included in COGITO_AUTH_STATIC_PROJECTS for static auth"
        )
    if settings.notification_timeout_seconds <= 0 or settings.notification_timeout_seconds > 60:
        raise ValueError("COGITO_NOTIFICATION_TIMEOUT_SECONDS must be greater than zero and at most 60")
    if not 1 <= settings.reconciliation_poll_seconds <= 3600:
        raise ValueError("COGITO_RECONCILIATION_POLL_SECONDS must be between 1 and 3600")
    if not 1 <= settings.reconciliation_batch_size <= 1000:
        raise ValueError("COGITO_RECONCILIATION_BATCH_SIZE must be between 1 and 1000")
    if settings.reconciliation_stall_seconds < settings.reconciliation_poll_seconds * 2:
        raise ValueError("COGITO_RECONCILIATION_STALL_SECONDS must be at least twice the poll interval")
    if not settings.notification_enabled:
        return
    if settings.notification_provider == "webhook":
        if not settings.notification_webhook_hmac_secret:
            raise ValueError("COGITO_NOTIFICATION_WEBHOOK_HMAC_SECRET is required when notifications are enabled")
        parsed = urlparse(settings.notification_webhook_url)
        if parsed.username or parsed.password or not parsed.hostname or parsed.fragment:
            raise ValueError("COGITO_NOTIFICATION_WEBHOOK_URL must be an absolute URL without credentials or a fragment")
        if settings.deployment_mode == "production" and parsed.scheme != "https":
            raise ValueError("production notification webhook URL must use HTTPS")
        if settings.deployment_mode == "development" and parsed.scheme not in {"http", "https"}:
            raise ValueError("development notification webhook URL must use HTTP or HTTPS")
        return
    if settings.notification_provider != "slack":
        raise ValueError("COGITO_NOTIFICATION_PROVIDER must be webhook or slack")
    if not settings.notification_slack_bot_token:
        raise ValueError("COGITO_NOTIFICATION_SLACK_BOT_TOKEN is required when Slack notifications are enabled")
    if not 1 <= len(settings.notification_slack_channel_id) <= 64 or not settings.notification_slack_channel_id.strip():
        raise ValueError("COGITO_NOTIFICATION_SLACK_CHANNEL_ID must be a non-empty channel identifier")
    parsed = urlparse(settings.notification_slack_workbench_url)
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("COGITO_NOTIFICATION_SLACK_WORKBENCH_URL must be an absolute HTTPS URL without credentials, query, or fragment")
