"""Run-scoped LiteLLM key and Kubernetes Secret lifecycle management."""

from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone
import hashlib
import json
from math import isfinite
import secrets
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .models import McpToolGrant

_MCP_INVOCATION_EVIDENCE_POLL_ATTEMPTS = 12
_GITHUB_INSTALLATION_TOKEN_REFRESH_MARGIN_SECONDS = 300
_MCP_INVOCATION_EVIDENCE_REQUEST_TIMEOUT_SECONDS = 1
_MCP_INVOCATION_EVIDENCE_MAX_GRANTS = 64
_MCP_INVOCATION_EVIDENCE_MAX_RESPONSE_BYTES = 1_000_000


@dataclass(frozen=True)
class RunBudget:
    """Immutable limits used to mint one execution-only gateway key."""

    run_id: str
    max_cost_usd: float
    model: str
    expires_in_seconds: int
    mcp_tool_permissions: dict[str, tuple[str, ...]] = field(default_factory=dict)
    agent_registration_id: str = ""
    agent_manifest_sha256: str = ""


class RunKeyManager(Protocol):
    """Creates and removes the opaque Secret mounted by one execution Job."""

    async def provision(self, budget: RunBudget) -> str: ...

    async def cleanup(self, run_id: str, secret_name: str) -> None: ...


class RunGitCredentialManager(Protocol):
    """Creates and removes the repository credential Secret for one execution."""

    async def provision(
        self, run_id: str, target_repositories: Sequence[str], minimum_validity_seconds: int
    ) -> "RunGitCredential": ...

    async def cleanup(self, run_id: str, secret_name: str) -> None: ...


@dataclass(frozen=True)
class RunGitCredential:
    """Reference to one run credential and whether it replaced a stale prior Secret."""

    secret_name: str
    replaced_existing: bool = False


class KubernetesLiteLLMRunKeyManager:
    """Provision one model-limited, budget-limited key and its private Secret."""

    def __init__(self, namespace: str, endpoint: str, management_key: str) -> None:
        if not management_key:
            raise ValueError("LiteLLM run-key management credential is not configured")
        try:
            from kubernetes import client, config
            from kubernetes.client.exceptions import ApiException
        except ImportError as error:
            raise RuntimeError("run-key provisioning requires the kubernetes dependency") from error
        config.load_incluster_config()
        self._namespace = namespace
        self._endpoint = endpoint.rstrip("/")
        self._management_key = management_key
        self._core_api = client.CoreV1Api()
        self._client = client
        self._api_exception: type[Exception] = ApiException

    async def provision(self, budget: RunBudget) -> str:
        """Create or reuse the deterministic run Secret without returning its token."""

        _validate_budget(budget)
        secret_name = run_key_secret_name(budget.run_id)
        existing = await self._read_secret(secret_name)
        if existing is not None:
            if _secret_token(existing):
                return secret_name
            await self._delete_secret(secret_name)

        token = f"sk-cogito-{secrets.token_urlsafe(32)}"
        try:
            await asyncio.to_thread(
                self._post_json,
                "/key/generate",
                _run_key_payload(token, budget),
            )
            body = self._client.V1Secret(
                metadata=self._client.V1ObjectMeta(
                    name=secret_name,
                    labels={"cogito.dev/run-hash": _run_hash(budget.run_id)},
                ),
                type="Opaque",
                data={"api-key": base64.b64encode(token.encode()).decode()},
            )
            await asyncio.to_thread(self._core_api.create_namespaced_secret, self._namespace, body)
        except Exception:
            # A token never enters logs, workflow inputs, or status metadata.
            await self._delete_gateway_key(token)
            raise
        return secret_name

    async def cleanup(self, run_id: str, secret_name: str) -> None:
        """Revoke the gateway key before deleting the labelled run Secret."""

        if secret_name != run_key_secret_name(run_id):
            raise ValueError("run key Secret does not match the execution run")
        secret = await self._read_secret(secret_name)
        if secret is None:
            return
        token = _secret_token(secret)
        if token:
            await self._delete_gateway_key(token)
        await self._delete_secret(secret_name)

    async def collect_mcp_invocations(
        self,
        run_id: str,
        secret_name: str,
        grants: Sequence[McpToolGrant],
        server_routes: Mapping[tuple[str, str], str],
    ) -> dict[str, object]:
        """Return a bounded gateway observation for the current run key only."""

        if secret_name != run_key_secret_name(run_id):
            raise ValueError("run key Secret does not match the execution run")
        expected = _expected_mcp_tools(grants, server_routes)
        if not expected:
            return {"version": 1, "status": "not_applicable", "events": []}
        try:
            secret = await self._read_secret(secret_name)
            token = _secret_token(secret) if secret is not None else None
            if not token:
                return _unavailable_invocation_evidence("run_key_unavailable")
            # Never filter audit records with the run key: LiteLLM access logs
            # retain request URLs. The user ID is a derived, non-secret value.
            path = f"/spend/logs?{urlencode({'user_id': run_audit_user_id(run_id)})}"
            latest: dict[str, object] | None = None
            for _ in range(_MCP_INVOCATION_EVIDENCE_POLL_ATTEMPTS):
                records = await asyncio.to_thread(self._get_json, path)
                latest = _mcp_invocation_evidence(records, expected)
                await asyncio.sleep(0.75)
        except Exception:  # Gateway response details can contain protected request data.
            return _unavailable_invocation_evidence("gateway_audit_unavailable")
        if latest is not None:
            return latest
        return _unavailable_invocation_evidence("gateway_audit_not_visible")

    async def _read_secret(self, name: str):
        try:
            return await asyncio.to_thread(self._core_api.read_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status == 404:
                return None
            raise

    async def _delete_secret(self, name: str) -> None:
        try:
            await asyncio.to_thread(self._core_api.delete_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status != 404:
                raise

    async def _delete_gateway_key(self, token: str) -> None:
        await asyncio.to_thread(self._post_json, "/key/delete", {"keys": [token]})

    def _post_json(self, path: str, payload: dict[str, object]) -> None:
        request = Request(
            f"{self._endpoint}{path}",
            data=json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {self._management_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=30) as response:  # nosec B310: endpoint is operator controlled
            if response.status < 200 or response.status >= 300:
                raise RuntimeError("LiteLLM run-key management request was rejected")

    def _get_json(self, path: str) -> object:
        request = Request(
            f"{self._endpoint}{path}",
            headers={"Authorization": f"Bearer {self._management_key}"},
            method="GET",
        )
        with urlopen(request, timeout=_MCP_INVOCATION_EVIDENCE_REQUEST_TIMEOUT_SECONDS) as response:  # nosec B310: endpoint is operator controlled
            if response.status < 200 or response.status >= 300:
                raise RuntimeError("LiteLLM invocation evidence request was rejected")
            content_length = response.headers.get("Content-Length")
            if content_length is not None and int(content_length) > _MCP_INVOCATION_EVIDENCE_MAX_RESPONSE_BYTES:
                raise ValueError("LiteLLM invocation evidence response exceeds the maximum size")
            payload = response.read(_MCP_INVOCATION_EVIDENCE_MAX_RESPONSE_BYTES + 1)
            if len(payload) > _MCP_INVOCATION_EVIDENCE_MAX_RESPONSE_BYTES:
                raise ValueError("LiteLLM invocation evidence response exceeds the maximum size")
            return json.loads(payload)


class KubernetesGitHubAppRunCredentialManager:
    """Mints one GitHub App installation token per run and stores it only in that run's Secret."""

    def __init__(self, namespace: str, app_id: str, installation_id: str, private_key: str, api_url: str, api_version: str, git_host: str) -> None:
        if not app_id.isdecimal() or not installation_id.isdecimal() or not private_key.strip():
            raise ValueError("GitHub App credentials are required for execution repository access")
        if urlsplit(api_url).scheme != "https" or not urlsplit(api_url).netloc:
            raise ValueError("GitHub App API URL must be an absolute HTTPS URL")
        if not git_host or "/" in git_host or ":" in git_host:
            raise ValueError("GitHub App Git host must be a hostname")
        try:
            from kubernetes import client, config
            from kubernetes.client.exceptions import ApiException
        except ImportError as error:
            raise RuntimeError("run Git credential provisioning requires the kubernetes dependency") from error
        config.load_incluster_config()
        self._namespace = namespace
        self._app_id = app_id
        self._installation_id = installation_id
        self._private_key = private_key
        self._api_url = api_url.rstrip("/")
        self._api_version = api_version
        self._git_host = git_host.lower().rstrip(".")
        self._core_api = client.CoreV1Api()
        self._client = client
        self._api_exception: type[Exception] = ApiException

    async def provision(
        self, run_id: str, target_repositories: Sequence[str], minimum_validity_seconds: int
    ) -> RunGitCredential:
        """Mint a repository-narrowed installation token for the exact execution targets."""

        if minimum_validity_seconds < 1:
            raise ValueError("GitHub App token minimum validity must be positive")
        secret_name = run_git_secret_name(run_id)
        legacy_secret_name = _legacy_run_git_secret_name(run_id)
        legacy_secret = await self._read_secret(legacy_secret_name)
        existing = await self._read_secret(secret_name)
        if existing is not None:
            if _github_installation_token_is_fresh(existing, minimum_validity_seconds):
                if legacy_secret is None:
                    return RunGitCredential(secret_name)
                await self._delete_secret(legacy_secret_name)
                return RunGitCredential(secret_name, replaced_existing=True)
            await self._delete_secret(secret_name)
            replaced_existing = True
        else:
            replaced_existing = legacy_secret is not None
        if legacy_secret is not None:
            await self._delete_secret(legacy_secret_name)
        token, expires_at = await asyncio.to_thread(
            self._mint_installation_token, _github_repository_names(target_repositories, self._git_host)
        )
        body = self._client.V1Secret(
            metadata=self._client.V1ObjectMeta(
                name=secret_name,
                labels={"cogito.dev/run-hash": _run_hash(run_id)},
                annotations={"cogito.dev/github-app-token-expires-at": str(expires_at)},
            ),
            type="Opaque",
            data={"token": base64.b64encode(token.encode()).decode()},
        )
        await asyncio.to_thread(self._core_api.create_namespaced_secret, self._namespace, body)
        return RunGitCredential(secret_name, replaced_existing=replaced_existing)

    async def cleanup(self, run_id: str, secret_name: str) -> None:
        if secret_name not in {run_git_secret_name(run_id), _legacy_run_git_secret_name(run_id)}:
            raise ValueError("run Git Secret does not match the execution run")
        await self._delete_secret(secret_name)

    async def _read_secret(self, name: str):
        try:
            return await asyncio.to_thread(self._core_api.read_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status == 404:
                return None
            raise

    async def _delete_secret(self, name: str) -> None:
        try:
            await asyncio.to_thread(self._core_api.delete_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status != 404:
                raise

    def _mint_installation_token(self, repositories: list[str]) -> tuple[str, int]:
        """Request a write-capable token scoped to this installation's exact repository names."""

        response = httpx.post(
            f"{self._api_url}/app/installations/{self._installation_id}/access_tokens",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._github_app_jwt()}",
                "X-GitHub-Api-Version": self._api_version,
            },
            json={"repositories": repositories, "permissions": {"contents": "write"}},
            timeout=30,
        )
        if response.status_code != 201:
            raise RuntimeError("GitHub App installation token request was rejected")
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("GitHub App installation token response was not an object")
        token = payload.get("token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("GitHub App installation token response did not contain a token")
        permissions = payload.get("permissions")
        if not isinstance(permissions, dict) or permissions.get("contents") != "write":
            raise RuntimeError("GitHub App installation token lacks Contents write permission")
        repositories_response = payload.get("repositories")
        if not isinstance(repositories_response, list):
            raise RuntimeError("GitHub App installation token response did not confirm repository scope")
        granted_repositories = {
            repository.get("name").casefold()
            for repository in repositories_response
            if isinstance(repository, dict) and isinstance(repository.get("name"), str)
        }
        if granted_repositories != {repository.casefold() for repository in repositories} or len(repositories_response) != len(repositories):
            raise RuntimeError("GitHub App installation token scope does not match execution repositories")
        expires_at = _github_installation_token_expiry(payload.get("expires_at"))
        return token, expires_at

    def _github_app_jwt(self) -> str:
        now = int(time.time())
        header = _base64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
        claims = _base64url(json.dumps({"iat": now - 60, "exp": now + 540, "iss": self._app_id}, separators=(",", ":")).encode())
        signing_input = f"{header}.{claims}".encode()
        private_key = serialization.load_pem_private_key(self._private_key.encode(), password=None)
        signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        return f"{header}.{claims}.{_base64url(signature)}"


def run_key_secret_name(run_id: str) -> str:
    """Return a deterministic name that reveals no raw run identifier."""

    return f"cogito-run-key-{_run_hash(run_id)}"


def run_git_secret_name(run_id: str) -> str:
    """Return a versioned deterministic name for the run-private GitHub App credential Secret."""

    return f"cogito-run-git-v2-{_run_hash(run_id)}"


def _legacy_run_git_secret_name(run_id: str) -> str:
    """Return the v1 name accepted only to clean up credentials from a rolling upgrade."""

    return f"cogito-run-git-{_run_hash(run_id)}"


def _run_hash(run_id: str) -> str:
    return hashlib.sha256(run_id.encode()).hexdigest()[:20]


def _github_repository_names(target_repositories: Sequence[str], git_host: str = "github.com") -> list[str]:
    """Return repository names scoped to exactly one GitHub App installation account."""

    owner: str | None = None
    names: list[str] = []
    for target in target_repositories:
        parsed = urlsplit(target)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.hostname.lower().rstrip(".") != git_host.lower().rstrip(".")
            or parsed.username
            or parsed.password
        ):
            raise ValueError(f"execution repositories must be unauthenticated https://{git_host}/owner/repository URLs")
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) != 2 or not parts[0] or not parts[1].endswith(".git"):
            raise ValueError(f"execution repositories must be https://{git_host}/owner/repository.git URLs")
        repository_owner, repository = parts[0], parts[1][:-4]
        if owner is None:
            owner = repository_owner
        elif owner.lower() != repository_owner.lower():
            raise ValueError("execution repositories must belong to the same GitHub App installation account")
        normalized_repository = repository.casefold()
        if not repository or normalized_repository in names:
            raise ValueError("execution repositories must be unique")
        names.append(normalized_repository)
    if not names or len(names) > 500:
        raise ValueError("GitHub App tokens must be scoped to between one and 500 repositories")
    return names


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def run_audit_user_id(run_id: str) -> str:
    """Return the non-secret gateway audit correlator for one run."""

    return f"cogito-{_run_hash(run_id)}"


def _secret_token(secret: object, key: str = "api-key") -> str | None:
    data = getattr(secret, "data", None) or {}
    encoded = data.get(key)
    if not isinstance(encoded, str):
        return None
    try:
        return base64.b64decode(encoded, validate=True).decode()
    except (ValueError, UnicodeDecodeError):
        return None


def _github_installation_token_is_fresh(secret: object, minimum_validity_seconds: int) -> bool:
    """Return whether a run Secret contains an unexpired GitHub App token with required metadata."""

    if not _secret_token(secret, key="token"):
        return False
    metadata = getattr(secret, "metadata", None)
    annotations = getattr(metadata, "annotations", None) or {}
    expires_at = annotations.get("cogito.dev/github-app-token-expires-at")
    if not isinstance(expires_at, str):
        return False
    try:
        return int(expires_at) > (
            int(time.time()) + minimum_validity_seconds + _GITHUB_INSTALLATION_TOKEN_REFRESH_MARGIN_SECONDS
        )
    except ValueError:
        return False


def _github_installation_token_expiry(value: object) -> int:
    if not isinstance(value, str):
        raise RuntimeError("GitHub App installation token response did not contain an expiry")
    try:
        expires_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RuntimeError("GitHub App installation token expiry was invalid") from error
    if expires_at.tzinfo is None:
        raise RuntimeError("GitHub App installation token expiry must include a timezone")
    expiry_seconds = int(expires_at.astimezone(timezone.utc).timestamp())
    if expiry_seconds <= int(time.time()) + _GITHUB_INSTALLATION_TOKEN_REFRESH_MARGIN_SECONDS:
        raise RuntimeError("GitHub App installation token expires too soon")
    return expiry_seconds


def _validate_budget(budget: RunBudget) -> None:
    if not isfinite(budget.max_cost_usd) or budget.max_cost_usd <= 0 or budget.expires_in_seconds < 1 or not budget.model:
        raise ValueError("run budget must have a positive cost, expiry, and model")
    if not isinstance(budget.mcp_tool_permissions, Mapping):
        raise ValueError("MCP tool permissions must be a mapping")
    if bool(budget.agent_registration_id) != bool(budget.agent_manifest_sha256):
        raise ValueError("run budget must identify both the agent registration and manifest or neither")
    if budget.agent_manifest_sha256 and (
        len(budget.agent_manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in budget.agent_manifest_sha256)
    ):
        raise ValueError("run budget agent manifest must be a lowercase SHA-256 digest")
    for server_id, tool_names in budget.mcp_tool_permissions.items():
        if (
            not isinstance(server_id, str)
            or len(server_id) != 32
            or any(character not in "0123456789abcdef" for character in server_id)
        ):
            raise ValueError("MCP gateway server IDs must be 32-character lowercase digests")
        if (
            not isinstance(tool_names, Sequence)
            or isinstance(tool_names, str)
            or not tool_names
            or len(set(tool_names)) != len(tool_names)
            or any(not isinstance(tool_name, str) or not tool_name for tool_name in tool_names)
        ):
            raise ValueError("MCP tool permissions must be explicit unique tool names")


def _expected_mcp_tools(
    grants: Sequence[McpToolGrant], server_routes: Mapping[tuple[str, str], str]
) -> dict[str, McpToolGrant]:
    """Map one pinned grant to its only permitted gateway tool identity."""

    expected: dict[str, McpToolGrant] = {}
    for grant in grants:
        route = server_routes.get((grant.server_id, grant.server_version))
        if not isinstance(route, str) or not route:
            raise ValueError("MCP invocation evidence is missing a trusted gateway route")
        tool = f"{route}/{grant.tool_name}"
        if tool in expected:
            raise ValueError("MCP invocation evidence contains duplicate gateway tool identities")
        expected[tool] = grant
    if len(expected) > _MCP_INVOCATION_EVIDENCE_MAX_GRANTS:
        raise ValueError("MCP invocation evidence exceeds the maximum pinned tool grants")
    return expected


def _mcp_invocation_evidence(records: object, expected: Mapping[str, McpToolGrant]) -> dict[str, object]:
    """Reduce untrusted gateway records to immutable grant-bound invocation counts."""

    if not isinstance(records, list):
        raise ValueError("LiteLLM invocation evidence response must be a list")
    observed: Counter[tuple[str, str]] = Counter()
    for record in records:
        if not isinstance(record, Mapping):
            continue
        tool = record.get("mcp_namespaced_tool_name")
        status = record.get("status")
        if not isinstance(tool, str) or tool not in expected or not isinstance(status, str) or not status:
            continue
        outcome = "success" if status == "success" else "failure"
        observed[(tool, outcome)] += 1
    events: list[dict[str, object]] = []
    for (tool, outcome), count in sorted(observed.items()):
        grant = expected[tool]
        events.append(
            {
                "server_id": grant.server_id,
                "server_version": grant.server_version,
                "server_manifest_sha256": grant.server_manifest_sha256,
                "tool_name": grant.tool_name,
                "input_schema_sha256": grant.input_schema_sha256,
                "outcome": outcome,
                "invocation_count": count,
            }
        )
    return {"version": 1, "status": "observed", "events": events}


def _unavailable_invocation_evidence(reason: str) -> dict[str, object]:
    """Return a versioned, non-assertive evidence state for audit failures."""

    return {"version": 1, "status": "unavailable", "reason": reason, "events": []}


def _run_key_payload(token: str, budget: RunBudget) -> dict[str, object]:
    """Build the non-persisted LiteLLM virtual-key request for one run."""

    metadata: dict[str, object] = {"cogito_run_hash": _run_hash(budget.run_id)}
    if budget.agent_registration_id:
        metadata.update(
            {
                "cogito_agent_registration": budget.agent_registration_id,
                "cogito_agent_manifest_sha256": budget.agent_manifest_sha256,
            }
        )
    payload: dict[str, object] = {
        "key": token,
        "key_alias": run_audit_user_id(budget.run_id),
        "user_id": run_audit_user_id(budget.run_id),
        "models": [budget.model],
        "max_budget": budget.max_cost_usd,
        "budget_duration": f"{budget.expires_in_seconds}s",
        "key_type": "llm_api",
        "metadata": metadata,
    }
    if budget.mcp_tool_permissions:
        payload["object_permission"] = {
            "mcp_servers": sorted(budget.mcp_tool_permissions),
            "mcp_tool_permissions": {
                server_id: list(tool_names)
                for server_id, tool_names in sorted(budget.mcp_tool_permissions.items())
            },
        }
    return payload
