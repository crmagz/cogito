from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from urllib.parse import quote

_MAX_GITHUB_APP_INSTALLATION_TOKEN_JOB_SECONDS = 3300


@dataclass(frozen=True)
class McpGatewayServer:
    """Trusted gateway configuration for one immutable MCP server release."""

    gateway_server_id: str
    route: str
    server_manifest_sha256: str
    tool_names: tuple[str, ...]
    repository_scope: str | None = None


@dataclass(frozen=True)
class Settings:
    temporal_host: str
    temporal_namespace: str
    task_queue: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    plans_bucket: str
    plan_snapshots_bucket: str
    specs_bucket: str
    specs_prefix: str
    specs_max_archive_bytes: int
    specs_max_extracted_bytes: int
    execution_namespace: str
    allowed_git_hosts: tuple[str, ...]
    execution_image: str
    execution_image_pull_policy: str
    execution_workspace_root: str
    execution_idle_seconds: int
    execution_startup_timeout_seconds: int
    execution_cleanup_timeout_seconds: int
    execution_active_deadline_seconds: int
    execution_ttl_seconds_after_finished: int
    execution_termination_grace_period_seconds: int
    execution_workspace_size_limit: str
    execution_resources: dict[str, object]
    execution_minio_endpoint: str
    execution_minio_secure: bool
    execution_object_store_secret: str
    execution_object_store_access_key_secret_key: str
    execution_object_store_secret_key_secret_key: str
    execution_litellm_endpoint: str
    execution_litellm_model: str
    execution_litellm_key_secret: str
    execution_litellm_key_secret_key: str
    execution_litellm_management_key: str
    execution_github_app_id: str
    execution_github_app_installation_id: str
    execution_github_app_private_key: str
    execution_github_app_api_url: str
    execution_github_app_api_version: str
    execution_github_app_git_host: str
    execution_git_author_name: str
    execution_git_author_email: str
    execution_command_output_limit_bytes: int
    reviewer_litellm_endpoint: str
    reviewer_primary_model: str
    reviewer_secondary_model: str
    reviewer_primary_api_key: str
    reviewer_secondary_api_key: str
    reviewer_timeout_seconds: float
    github_api_url: str
    github_pull_request_token: str
    github_base_branch: str
    supervisor_database_host: str
    supervisor_database_port: int
    supervisor_database_name: str
    supervisor_database_user: str
    supervisor_database_password: str
    execution_mcp_gateway_servers: dict[str, McpGatewayServer] = field(default_factory=dict)

    @property
    def supervisor_database_url(self) -> str:
        return (
            "postgresql+psycopg://"
            f"{quote(self.supervisor_database_user, safe='')}:{quote(self.supervisor_database_password, safe='')}"
            f"@{self.supervisor_database_host}:{self.supervisor_database_port}/{self.supervisor_database_name}"
        )


def load_settings() -> Settings:
    allowed_git_hosts = json.loads(os.environ.get("COGITO_ALLOWED_GIT_HOSTS", '["github.com"]'))
    execution_resources = json.loads(
        os.environ.get(
            "COGITO_EXECUTION_RESOURCES",
            '{"requests":{"cpu":"100m","memory":"256Mi","ephemeral-storage":"1Gi"},"limits":{"cpu":"1","memory":"1Gi","ephemeral-storage":"2Gi"}}',
        )
    )
    if (
        not isinstance(allowed_git_hosts, list)
        or not allowed_git_hosts
        or not all(isinstance(host, str) and host.strip() for host in allowed_git_hosts)
    ):
        raise ValueError("COGITO_ALLOWED_GIT_HOSTS must be a non-empty JSON string array")
    if not isinstance(execution_resources, dict):
        raise ValueError("COGITO_EXECUTION_RESOURCES must be a JSON object")
    execution_mcp_gateway_servers = _mcp_gateway_servers("COGITO_EXECUTION_MCP_GATEWAY_SERVERS", "{}")
    execution_active_deadline_seconds = int(os.environ.get("COGITO_EXECUTION_ACTIVE_DEADLINE_SECONDS", "3300"))
    if execution_active_deadline_seconds > _MAX_GITHUB_APP_INSTALLATION_TOKEN_JOB_SECONDS:
        raise ValueError("COGITO_EXECUTION_ACTIVE_DEADLINE_SECONDS must leave a GitHub App token refresh margin")
    return Settings(
        temporal_host=os.environ.get("COGITO_TEMPORAL_HOST", "localhost:7233"),
        temporal_namespace=os.environ.get("COGITO_TEMPORAL_NAMESPACE", "default"),
        task_queue=os.environ.get("COGITO_TEMPORAL_TASK_QUEUE", "developer-tasks"),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        minio_secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        plans_bucket=os.environ.get("MINIO_PLANS_BUCKET", "plans"),
        plan_snapshots_bucket=os.environ.get("MINIO_PLAN_SNAPSHOTS_BUCKET", "plan-snapshots"),
        specs_bucket=os.environ.get("MINIO_SPECS_BUCKET", "specs"),
        specs_prefix=os.environ.get("MINIO_SPECS_PREFIX", "specs"),
        specs_max_archive_bytes=int(os.environ.get("MINIO_SPECS_MAX_ARCHIVE_BYTES", str(10 * 1024 * 1024))),
        specs_max_extracted_bytes=int(
            os.environ.get("MINIO_SPECS_MAX_EXTRACTED_BYTES", str(25 * 1024 * 1024))
        ),
        execution_namespace=os.environ.get("COGITO_EXECUTION_NAMESPACE", "cogito-executions"),
        allowed_git_hosts=tuple(allowed_git_hosts),
        execution_image=os.environ.get("COGITO_EXECUTION_IMAGE", "cogito-worker:local"),
        execution_image_pull_policy=os.environ.get("COGITO_EXECUTION_IMAGE_PULL_POLICY", "IfNotPresent"),
        execution_workspace_root=os.environ.get("COGITO_EXECUTION_WORKSPACE_ROOT", "/workspace"),
        execution_idle_seconds=int(os.environ.get("COGITO_EXECUTION_IDLE_SECONDS", "3300")),
        execution_startup_timeout_seconds=int(os.environ.get("COGITO_EXECUTION_STARTUP_TIMEOUT_SECONDS", "30")),
        execution_cleanup_timeout_seconds=int(os.environ.get("COGITO_EXECUTION_CLEANUP_TIMEOUT_SECONDS", "90")),
        execution_active_deadline_seconds=execution_active_deadline_seconds,
        execution_ttl_seconds_after_finished=int(
            os.environ.get("COGITO_EXECUTION_TTL_SECONDS_AFTER_FINISHED", "300")
        ),
        execution_termination_grace_period_seconds=int(
            os.environ.get("COGITO_EXECUTION_TERMINATION_GRACE_PERIOD_SECONDS", "10")
        ),
        execution_workspace_size_limit=os.environ.get("COGITO_EXECUTION_WORKSPACE_SIZE_LIMIT", "2Gi"),
        execution_resources=execution_resources,
        execution_minio_endpoint=os.environ.get(
            "COGITO_EXECUTION_MINIO_ENDPOINT", os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        ),
        execution_minio_secure=os.environ.get(
            "COGITO_EXECUTION_MINIO_SECURE", os.environ.get("MINIO_SECURE", "false")
        ).lower()
        == "true",
        execution_object_store_secret=os.environ.get("COGITO_EXECUTION_OBJECT_STORE_SECRET", "cogito-minio"),
        execution_object_store_access_key_secret_key=os.environ.get(
            "COGITO_EXECUTION_OBJECT_STORE_ACCESS_KEY_SECRET_KEY", "rootUser"
        ),
        execution_object_store_secret_key_secret_key=os.environ.get(
            "COGITO_EXECUTION_OBJECT_STORE_SECRET_KEY_SECRET_KEY", "rootPassword"
        ),
        execution_litellm_endpoint=os.environ.get("COGITO_EXECUTION_LITELLM_ENDPOINT", "http://cogito-litellm:4000"),
        execution_litellm_model=os.environ.get("COGITO_EXECUTION_LITELLM_MODEL", "complex"),
        execution_litellm_key_secret=os.environ.get("COGITO_EXECUTION_LITELLM_KEY_SECRET", "cogito-developer-key"),
        execution_litellm_key_secret_key=os.environ.get(
            "COGITO_EXECUTION_LITELLM_KEY_SECRET_KEY", "api-key"
        ),
        execution_litellm_management_key=os.environ.get("COGITO_EXECUTION_LITELLM_MANAGEMENT_KEY", ""),
        execution_github_app_id=os.environ.get("COGITO_EXECUTION_GITHUB_APP_ID", ""),
        execution_github_app_installation_id=os.environ.get("COGITO_EXECUTION_GITHUB_APP_INSTALLATION_ID", ""),
        execution_github_app_private_key=os.environ.get("COGITO_EXECUTION_GITHUB_APP_PRIVATE_KEY", ""),
        execution_github_app_api_url=os.environ.get("COGITO_EXECUTION_GITHUB_APP_API_URL", "https://api.github.com"),
        execution_github_app_api_version=os.environ.get("COGITO_EXECUTION_GITHUB_APP_API_VERSION", "2022-11-28"),
        execution_github_app_git_host=os.environ.get("COGITO_EXECUTION_GITHUB_APP_GIT_HOST", "github.com"),
        execution_git_author_name=os.environ.get("COGITO_EXECUTION_GIT_AUTHOR_NAME", "Cogito Agent"),
        execution_git_author_email=os.environ.get("COGITO_EXECUTION_GIT_AUTHOR_EMAIL", "cogito@local.invalid"),
        execution_command_output_limit_bytes=int(
            os.environ.get("COGITO_EXECUTION_COMMAND_OUTPUT_LIMIT_BYTES", str(256 * 1024))
        ),
        reviewer_litellm_endpoint=os.environ.get(
            "COGITO_REVIEWER_LITELLM_ENDPOINT",
            os.environ.get("COGITO_EXECUTION_LITELLM_ENDPOINT", "http://cogito-litellm:4000"),
        ),
        reviewer_primary_model=os.environ.get("COGITO_REVIEWER_PRIMARY_MODEL", "balanced"),
        reviewer_secondary_model=os.environ.get("COGITO_REVIEWER_SECONDARY_MODEL", "complex"),
        reviewer_primary_api_key=os.environ.get("COGITO_REVIEWER_PRIMARY_LITELLM_API_KEY", ""),
        reviewer_secondary_api_key=os.environ.get("COGITO_REVIEWER_SECONDARY_LITELLM_API_KEY", ""),
        reviewer_timeout_seconds=float(os.environ.get("COGITO_REVIEWER_TIMEOUT_SECONDS", "60")),
        github_api_url=os.environ.get("COGITO_GITHUB_API_URL", "https://api.github.com"),
        github_pull_request_token=os.environ.get("COGITO_GITHUB_PULL_REQUEST_TOKEN", ""),
        github_base_branch=os.environ.get("COGITO_GITHUB_BASE_BRANCH", "main"),
        supervisor_database_host=os.environ.get("COGITO_SUPERVISOR_DATABASE_HOST", "cogito-postgresql"),
        supervisor_database_port=int(os.environ.get("COGITO_SUPERVISOR_DATABASE_PORT", "5432")),
        supervisor_database_name=os.environ.get("COGITO_SUPERVISOR_DATABASE_NAME", "cogito"),
        supervisor_database_user=os.environ.get("COGITO_SUPERVISOR_DATABASE_USER", "postgres"),
        supervisor_database_password=os.environ.get("COGITO_SUPERVISOR_DATABASE_PASSWORD", "cogito"),
        execution_mcp_gateway_servers=execution_mcp_gateway_servers,
    )


def _mcp_gateway_servers(name: str, default: str) -> dict[str, McpGatewayServer]:
    """Load exact gateway mappings keyed by immutable ``registration_id@version``."""

    try:
        value = json.loads(os.environ.get(name, default))
    except json.JSONDecodeError as error:
        raise ValueError(f"{name} must be a JSON object") from error
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    servers: dict[str, McpGatewayServer] = {}
    for release, server in value.items():
        if not isinstance(release, str) or not re.fullmatch(
            r"[a-z][a-z0-9_-]{0,127}@[0-9]+(?:\.[0-9]+){0,2}", release
        ):
            raise ValueError(f"{name} keys must be registration_id@version values")
        if not isinstance(server, dict) or set(server) not in (
            {"gateway_server_id", "route", "server_manifest_sha256", "tool_names"},
            {"gateway_server_id", "route", "server_manifest_sha256", "tool_names", "repository_scope"},
        ):
            raise ValueError(f"{name}.{release} must define the complete immutable gateway mapping")
        gateway_server_id = server["gateway_server_id"]
        route = server["route"]
        manifest_sha256 = server["server_manifest_sha256"]
        tool_names = server["tool_names"]
        repository_scope = server.get("repository_scope")
        if not isinstance(gateway_server_id, str) or not re.fullmatch(r"[a-f0-9]{32}", gateway_server_id):
            raise ValueError(f"{name}.{release}.gateway_server_id must be a 32-character lowercase digest")
        if not isinstance(route, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", route):
            raise ValueError(f"{name}.{release}.route is invalid")
        if not isinstance(manifest_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", manifest_sha256):
            raise ValueError(f"{name}.{release}.server_manifest_sha256 must be a SHA-256 digest")
        if (
            not isinstance(tool_names, list)
            or not tool_names
            or not all(isinstance(tool, str) and re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", tool) for tool in tool_names)
            or len(set(tool_names)) != len(tool_names)
        ):
            raise ValueError(f"{name}.{release}.tool_names must be unique explicit tool names")
        if repository_scope is not None and (
            not isinstance(repository_scope, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository_scope)
        ):
            raise ValueError(f"{name}.{release}.repository_scope must be an owner/repository value")
        servers[release] = McpGatewayServer(
            gateway_server_id=gateway_server_id,
            route=route,
            server_manifest_sha256=manifest_sha256,
            tool_names=tuple(tool_names),
            repository_scope=repository_scope.casefold() if repository_scope is not None else None,
        )
    return servers
